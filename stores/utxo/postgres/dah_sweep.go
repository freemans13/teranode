package postgres

import (
	"bytes"
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// sweepDAHRange evaluates every parent with spend activity (spent_at_height) or
// mine activity (mined_at_height) in (fromH, toH] and bidirectionally sets
// delete_at_height: stamp completion_height+1+retention when (mined AND fully
// spent AND not preserved/unmined); clear to NULL otherwise. completion_height =
// GREATEST(max spent_at_height of its spends, mined_at_height). BRIN indexes make
// candidate enumeration touch only the recent heap ranges (both arms are bounded
// by (fromH, toH]). limit bounds candidates. The target DAH is computed once as
// new_dah in the state CTE and the UPDATE only writes when it actually changes
// (IS DISTINCT FROM), avoiding re-stamping the same value every sweep (MVCC churn).
// Reorg clears are handled directly by Unspend, so no unbounded enumeration of
// already-stamped txs is needed here.
//
// The query is pinned to index access via SET LOCAL enable_seqscan = off, scoped
// to a dedicated transaction: the BRIN selectivity estimate is unreliable (after
// ANALYZE the planner can value the height-range bitmap scan at full-table rows
// and flip the mine-activity arm — and the candidates join — to a Seq Scan of
// txs). That is catastrophic when the range sits above the highest mined height
// (matches zero rows yet scans the whole table). candidates is MATERIALIZED so
// the bounded (<= limit) set drives a PK nested loop over txs rather than a
// hash join that could re-introduce a txs seq scan.
func (s *Store) sweepDAHRange(ctx context.Context, fromH, toH int64, limit int) (int, error) {
	retention := int64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, nil
	}

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.NewStorageError("[dahSweep] begin: %v", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	if _, err := pgxTx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return 0, errors.NewStorageError("[dahSweep] set local enable_seqscan: %v", err)
	}

	tag, err := pgxTx.Exec(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT hash FROM (
        SELECT DISTINCT prev_tx_hash AS hash FROM spends
        WHERE spent_at_height > $1 AND spent_at_height <= $2
        UNION
        SELECT hash FROM txs
        WHERE mined_at_height > $1 AND mined_at_height <= $2
    ) c
    LIMIT $3
),
state AS (
    SELECT t.hash,
           CASE
               -- "keep current" cases: new_dah == current so the IS DISTINCT FROM
               -- guard below makes them no-ops (no MVCC churn).
               WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
               WHEN t.unmined_since IS NOT NULL THEN t.delete_at_height
               WHEN t.block_ids IS NULL OR array_length(t.block_ids, 1) IS NULL THEN t.delete_at_height
               -- allSpent AND fully within the safe range: the completion height
               -- (max spent_at_height, mined) must be <= toH ($2). Without this
               -- bound the mine-arm (or a multi-spend tx the spends-arm enumerates
               -- via an in-range spend) could stamp DAH off a spend still inside
               -- the safe-tip lag window — a spend that may still be mid-commit.
               WHEN (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash)
                    = (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash)
                    AND (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash) > 0
                    AND GREATEST(COALESCE((SELECT max(s.spent_at_height) FROM spends s WHERE s.prev_tx_hash = t.hash), 0),
                                 COALESCE(t.mined_at_height, 0)) <= $2
                   THEN GREATEST(COALESCE((SELECT max(s.spent_at_height) FROM spends s WHERE s.prev_tx_hash = t.hash), 0),
                                 COALESCE(t.mined_at_height, 0)) + 1 + $4
               ELSE NULL
           END AS new_dah
    FROM txs t JOIN candidates c ON c.hash = t.hash
)
UPDATE txs t SET delete_at_height = st.new_dah
FROM state st
WHERE t.hash = st.hash
  AND t.delete_at_height IS DISTINCT FROM st.new_dah`, fromH, toH, limit, retention)
	if err != nil {
		return 0, errors.NewStorageError("[dahSweep] range (%d,%d]: %v", fromH, toH, err)
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return 0, errors.NewStorageError("[dahSweep] commit range (%d,%d]: %v", fromH, toH, err)
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

// runDAHCursor is the Worker 2 background loop: it wakes on a ticker, sweeps
// sweepDAHUpTo(dahSafeTip(lag)) in a tight inner loop until fewer than batch
// rows are touched, then sleeps until the next tick. The loop exits cleanly
// when ctx is cancelled.
func (s *postgresPrunerService) runDAHCursor(ctx context.Context) {
	cfg := s.store.settings.UtxoStore
	batch := cfg.PostgresDAHSweepBatchSize
	if batch <= 0 {
		batch = 50000
	}
	interval := time.Duration(cfg.PostgresDAHSweepIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	backstopTicker := time.NewTicker(backstopInterval)
	defer backstopTicker.Stop()
	var backstopByte int // rotates 0x00..0xff

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				n, err := s.store.sweepDAHUpTo(ctx, s.store.dahSafeTip(lag), batch)
				if err != nil {
					s.logger.Infof("[dahCursor] sweep error (retry next tick): %v", err)
					break
				}
				if n < batch {
					break
				}
				if ctx.Err() != nil {
					return
				}
			}
		case <-backstopTicker.C:
			b := backstopByte
			backstopByte = (backstopByte + 1) & 0xff
			n, err := s.store.backstopReconcile(ctx, b, b, batch)
			if err != nil {
				s.logger.Infof("[dahBackstop] slice 0x%02x error (best-effort, continuing): %v", b, err)
				continue
			}
			if n > 0 {
				s.logger.Infof("[dahBackstop] slice 0x%02x recovered %d missed tx(s)", b, n)
			}
		}
	}
}

// backstopInterval is the period between keyspace-slice ticks in the backstop
// reconciliation loop embedded in runDAHCursor (G5 guarantee-of-last-resort).
// One byte-slice is processed per tick; the full 256-byte keyspace is covered
// every 256 ticks (≈4.3 hours at the default 60s/tick).
const backstopInterval = 60 * time.Second

// backstopReconcile stamps DAH for mined, fully-spent, un-stamped txs whose hash
// leading byte is in [loByte, hiByte]. Keyspace-sliced so each run is bounded;
// the cursor rotates slices over time. Guarantee-of-last-resort (G5) against any
// enumeration gap in the height-range sweep; normally finds nothing. Stamp-only.
//
// The slice is expressed as a bytea range on the hash primary key
// (hash >= lo AND hash < hi) so the candidate scan rides the hash btree and
// touches only ~1/256 of the table per single-byte slice, rather than a full
// partition scan. txids are always exactly 32 bytes and uniformly distributed,
// so [b,0..0] <= hash < [b+1,0..0] selects exactly first-byte == b. For the
// top slice (hiByte == 0xff) the upper bound is a 33-byte sentinel
// [0xff×32, 0x00] which is lexicographically greater than any 32-byte txid,
// so the all-0xff hash is included.
func (s *Store) backstopReconcile(ctx context.Context, loByte, hiByte int, limit int) (int, error) {
	retention := int64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, nil
	}

	lo := make([]byte, 32)
	lo[0] = byte(loByte)

	var hi []byte
	if hiByte < 0xff {
		hi = make([]byte, 32)
		hi[0] = byte(hiByte + 1)
	} else {
		hi = append(bytes.Repeat([]byte{0xff}, 32), 0x00)
	}

	tag, err := s.pool.Exec(ctx, `
		WITH cand AS (
			SELECT t.hash,
			       GREATEST(COALESCE((SELECT max(sp.spent_at_height) FROM spends sp WHERE sp.prev_tx_hash = t.hash), 0),
			                COALESCE(t.mined_at_height, 0)) AS completion_height
			FROM txs t
			WHERE t.hash >= $1 AND t.hash < $2
			  AND t.delete_at_height IS NULL
			  AND t.preserve_until IS NULL
			  AND t.unmined_since IS NULL
			  AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) IS NOT NULL
			  AND (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash) > 0
			  AND (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash)
			      = (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash)
			LIMIT $3
		)
		UPDATE txs t SET delete_at_height = c.completion_height + 1 + $4
		FROM cand c WHERE t.hash = c.hash`,
		lo, hi, limit, retention)
	if err != nil {
		return 0, errors.NewStorageError("[dahBackstop] [%d,%d]: %v", loByte, hiByte, err)
	}
	return int(tag.RowsAffected()), nil
}

// RewindDAHWatermark moves the sweep watermark BACK to forkHeight so that a reorg
// causes (forkHeight, tip] to be re-swept: the new chain's spends are tagged at
// heights the watermark may already have passed, and must be re-evaluated. The
// guard last_swept_height > forkHeight ensures the watermark is never advanced
// forward by this call (it only ever rewinds).
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE dah_watermark SET last_swept_height = $1 WHERE id = 1 AND last_swept_height > $1`,
		forkHeight,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark to %d: %v", forkHeight, err)
	}

	return nil
}
