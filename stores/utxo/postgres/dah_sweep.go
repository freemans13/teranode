package postgres

import (
	"bytes"
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// dahSafeTip returns the highest height Worker 2 may scan: the persisted block
// height minus a safety lag, so no spend tagged in range can still be mid-commit.
func (s *Store) dahSafeTip(lag int64) int64 {
	h := int64(s.blockHeight.Load())
	if h <= lag {
		return 0
	}

	return h - lag
}

// runDAHCursor is the Worker 2 background entry point: it drives the server-side
// dah_sweep_batch() procedure (runDAHCursorProc) and owns the cursorWg lifetime.
// The procedure is bootstrapped during schema creation (store.New fails if it
// cannot be installed), so the cursor can always assume it is present.
func (s *postgresPrunerService) runDAHCursor(ctx context.Context) {
	defer s.cursorWg.Done() // let stop() wait for this goroutine to fully exit
	s.runDAHCursorProc(ctx)
}

// backstopInterval is the IDLE period between keyspace-slice ticks in the
// backstop reconciliation loop embedded in runDAHCursor (G5
// guarantee-of-last-resort). When a slice finds orphans the loop SELF-ACCELERATES
// (see runDAHCursorProc): it bursts through the remaining keyspace back-to-back so
// a burst of missed txs is cleared in seconds rather than the ≈4.3 hours a pure
// one-slice-per-60s crawl would take. It only relaxes back to this idle cadence
// once a full pass finds nothing. In steady state (no orphans) it stays here,
// touching ~1/256 of the table per tick — near-zero cost.
const backstopInterval = 60 * time.Second

// backstopFastInterval is the cadence the backstop drops to while it is actively
// recovering orphans (a slice just stamped > 0). Short, so a persistent orphan
// source (e.g. mining lagging spends under heavy load) is drained continuously
// instead of waiting a full idle interval between passes.
const backstopFastInterval = 1 * time.Second

// backstopInitialDelay is when the FIRST backstop pass fires after startup. Short
// so a store that starts with a pre-existing orphan backlog (or a synthetic load
// that produces orphans immediately) begins recovering right away instead of
// idling a full backstopInterval first.
const backstopInitialDelay = 2 * time.Second

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

	// Same set-based aggregation as sweepDAHRange: take a bounded hash-range
	// slice of stamp-candidates (cheap row predicates + LIMIT, riding the hash
	// btree), aggregate spends over just that slice using the stored
	// out_count/spendable_count scalars + out_spendables bitmap, then stamp the
	// ones that are fully spent. Runs on the maintenance pool so its recovery
	// scans never steal connections from the validator hot path.
	//
	// When the pending_deletes flag is ON the UPDATE is wrapped in a CTE so that
	// any newly-stamped row is also upserted into pending_deletes in the same
	// statement. When the flag is OFF the plain UPDATE is used (no side-table).
	var sql string
	if s.settings.UtxoStore.PostgresUsePendingDeletesTable {
		sql = `
		WITH slice AS (
			SELECT t.hash, t.mined_at_height, t.out_spendables,
			       t.out_count AS total_out,
			       t.spendable_count AS spendable_out
			FROM txs t
			WHERE t.hash >= $1 AND t.hash < $2
			  AND t.delete_at_height IS NULL
			  AND t.preserve_until IS NULL
			  AND t.unmined_since IS NULL
			  AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) IS NOT NULL
			LIMIT $3
		),
		spend_agg AS (
			SELECT s.prev_tx_hash AS hash,
			       count(*) FILTER (WHERE CASE WHEN s.prev_output_idx < sl.total_out THEN get_bit(sl.out_spendables, s.prev_output_idx) = 1 ELSE false END) AS spent_count,
			       max(s.spent_at_height) AS max_spent
			FROM spends s JOIN slice sl ON sl.hash = s.prev_tx_hash
			GROUP BY s.prev_tx_hash
		),
		eligible AS (
			SELECT sl.hash,
			       GREATEST(COALESCE(sa.max_spent, 0), COALESCE(sl.mined_at_height, 0)) AS completion_height
			FROM slice sl
			LEFT JOIN spend_agg sa ON sa.hash = sl.hash
			WHERE sl.total_out > 0
			  AND sl.spendable_out = COALESCE(sa.spent_count, 0)
		),
		upd AS (
			UPDATE txs t SET delete_at_height = (e.completion_height + 1 + $4)::int
			FROM eligible e WHERE t.hash = e.hash
			RETURNING t.hash, t.delete_at_height
		)
		INSERT INTO pending_deletes (hash, delete_at_height)
		SELECT hash, delete_at_height FROM upd WHERE delete_at_height IS NOT NULL
		ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`
	} else {
		sql = `
		WITH slice AS (
			SELECT t.hash, t.mined_at_height, t.out_spendables,
			       t.out_count AS total_out,
			       t.spendable_count AS spendable_out
			FROM txs t
			WHERE t.hash >= $1 AND t.hash < $2
			  AND t.delete_at_height IS NULL
			  AND t.preserve_until IS NULL
			  AND t.unmined_since IS NULL
			  AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) IS NOT NULL
			LIMIT $3
		),
		spend_agg AS (
			SELECT s.prev_tx_hash AS hash,
			       count(*) FILTER (WHERE CASE WHEN s.prev_output_idx < sl.total_out THEN get_bit(sl.out_spendables, s.prev_output_idx) = 1 ELSE false END) AS spent_count,
			       max(s.spent_at_height) AS max_spent
			FROM spends s JOIN slice sl ON sl.hash = s.prev_tx_hash
			GROUP BY s.prev_tx_hash
		),
		eligible AS (
			SELECT sl.hash,
			       GREATEST(COALESCE(sa.max_spent, 0), COALESCE(sl.mined_at_height, 0)) AS completion_height
			FROM slice sl
			LEFT JOIN spend_agg sa ON sa.hash = sl.hash
			WHERE sl.total_out > 0
			  AND sl.spendable_out = COALESCE(sa.spent_count, 0)
		)
		UPDATE txs t SET delete_at_height = (e.completion_height + 1 + $4)::int
		FROM eligible e WHERE t.hash = e.hash`
	}

	tag, err := s.maint().Exec(ctx, sql, lo, hi, limit, int32(retention))
	if err != nil {
		return 0, errors.NewStorageError("[dahBackstop] [%d,%d]", loByte, hiByte, err)
	}
	return int(tag.RowsAffected()), nil
}

// RewindDAHWatermark moves the sweep watermark BACK to forkHeight so that a reorg
// causes (forkHeight, tip] to be re-swept: the new chain's spends are tagged at
// heights the watermark may already have passed, and must be re-evaluated. The
// guard last_swept_height > forkHeight ensures the watermark is never advanced
// forward by this call (it only ever rewinds).
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	// Rewind every partition's watermark (forward-only guard per partition) so the
	// re-swept range (forkHeight, tip] is re-evaluated on all 8 parallel sweepers.
	if _, err := s.pool.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = $1 WHERE last_swept_height > $1`,
		forkHeight,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark to %d", forkHeight, err)
	}

	return nil
}
