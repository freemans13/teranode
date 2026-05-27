package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// sweepDAHRange evaluates every parent with spend activity (spent_at_height) or
// mine activity (mined_at_height) in (fromH, toH] and bidirectionally sets
// delete_at_height: stamp completion_height+1+retention when (mined AND fully
// spent AND not preserved/unmined); clear to NULL otherwise. completion_height =
// GREATEST(max spent_at_height of its spends, mined_at_height). BRIN indexes make
// candidate enumeration touch only the recent heap ranges. limit bounds candidates.
func (s *Store) sweepDAHRange(ctx context.Context, fromH, toH int64, limit int) (int, error) {
	retention := int64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, nil
	}

	tag, err := s.pool.Exec(ctx, `
WITH candidates AS (
    SELECT hash FROM (
        SELECT DISTINCT prev_tx_hash AS hash FROM spends
        WHERE spent_at_height > $1 AND spent_at_height <= $2
        UNION
        SELECT hash FROM txs
        WHERE mined_at_height > $1 AND mined_at_height <= $2
        UNION
        -- Already-stamped txs must stay enumerable so a reorg unspend can clear
        -- their DAH bidirectionally even after their spend rows are deleted
        -- (bounded by the px_delete_at_height partial index).
        SELECT hash FROM txs
        WHERE delete_at_height IS NOT NULL
    ) c
    LIMIT $3
),
state AS (
    SELECT t.hash, t.preserve_until, t.unmined_since, t.block_ids,
           (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash) AS out_count,
           (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash) AS spent_count,
           GREATEST(COALESCE((SELECT max(s.spent_at_height) FROM spends s WHERE s.prev_tx_hash = t.hash), 0),
                    COALESCE(t.mined_at_height, 0)) AS completion_height
    FROM txs t JOIN candidates c ON c.hash = t.hash
)
UPDATE txs t SET delete_at_height = CASE
    WHEN st.preserve_until IS NOT NULL THEN t.delete_at_height
    WHEN st.unmined_since IS NOT NULL THEN t.delete_at_height
    WHEN st.block_ids IS NULL OR array_length(st.block_ids, 1) IS NULL THEN t.delete_at_height
    WHEN st.spent_count = st.out_count AND st.out_count > 0 THEN st.completion_height + 1 + $4
    ELSE NULL
    END
FROM state st
WHERE t.hash = st.hash`, fromH, toH, limit, retention)
	if err != nil {
		return 0, errors.NewStorageError("[dahSweep] range (%d,%d]: %v", fromH, toH, err)
	}

	return int(tag.RowsAffected()), nil
}

// sweepDAHUpTo sweeps from the persisted watermark up to toH and advances the
// watermark to toH on success.
func (s *Store) sweepDAHUpTo(ctx context.Context, toH int64, limit int) (int, error) {
	var from int64
	if err := s.pool.QueryRow(ctx, `SELECT last_swept_height FROM dah_watermark WHERE id = 1`).Scan(&from); err != nil {
		return 0, errors.NewStorageError("[dahSweep] read watermark: %v", err)
	}

	if toH <= from {
		return 0, nil
	}

	n, err := s.sweepDAHRange(ctx, from, toH, limit)
	if err != nil {
		return 0, err
	}

	if _, err := s.pool.Exec(ctx, `UPDATE dah_watermark SET last_swept_height = $1 WHERE id = 1`, toH); err != nil {
		return n, errors.NewStorageError("[dahSweep] advance watermark: %v", err)
	}

	return n, nil
}

// dahSafeTip returns the highest height Worker 2 may scan: the persisted block
// height minus a safety lag, so no spend tagged in range can still be mid-commit.
func (s *Store) dahSafeTip(lag int64) int64 {
	h := int64(s.blockHeight.Load())
	if h <= lag {
		return 0
	}

	return h - lag
}
