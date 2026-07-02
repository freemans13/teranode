package postgres

import (
	"context"
	"fmt"

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

// RewindDAHWatermark moves the sweep watermark BACK to forkHeight so that a reorg
// causes (forkHeight, tip] to be re-swept: the new chain's spends are tagged at
// heights the watermark may already have passed, and must be re-evaluated.
//
// The forward-only fold (dah_sweep_batch) maintains spent_progress by ADDING each
// band's spendable spends (spent_progress += n) with no reset. Re-sweeping a range
// whose spends are STILL PRESENT (survived the reorg on the new chain) would count
// them a SECOND time, over-inflating the counter. For a partially-spent mined tx
// that over-count can land exactly on spendable_count, stamping delete_at_height and
// pruning a tx whose remaining output is still an unspent UTXO — the later spend then
// fails TX_NOT_FOUND (the IBD data-loss wedge, mainnet h63266 / testnet …5e5ea,
// 2026-07-02). So before rewinding, this RESETS every affected tx's counter to its
// baseline over spends AT OR BELOW forkHeight and clears any stamp that depended on
// the re-swept range; the subsequent fold then re-derives (forkHeight, tip] correctly.
// Only txs with a spend above forkHeight are touched, so the cost stays O(spends in
// the rewound range), matching the O(new-spends) design goal.
//
// Per partition it takes the SAME advisory lock the fold uses (20240684 + partition)
// so the reset+rewind cannot interleave with an in-flight fold band on that partition.
// The watermark rewind is guarded (last_swept_height > forkHeight) so it only ever
// moves backward.
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	for p := 0; p < numPartitions; p++ {
		if err := s.rewindDAHWatermarkPartition(ctx, p, forkHeight); err != nil {
			return err
		}
	}

	return nil
}

// rewindDAHWatermarkPartition resets the Setter-C counters for one hash partition's
// affected txs to their <= forkHeight baseline and rewinds that partition's watermark,
// all in one transaction under the partition's fold advisory lock.
func (s *Store) rewindDAHWatermarkPartition(ctx context.Context, partition int, forkHeight int64) error {
	suffix := fmt.Sprintf("%02d", partition)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[dahSweep] rewind begin (partition %d)", partition, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize with the fold on this partition (blocking, xact-scoped lock).
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(20240684+partition)); err != nil {
		return errors.NewStorageError("[dahSweep] rewind lock (partition %d)", partition, err)
	}

	// Reset affected txs (those with any spend above forkHeight) to the counter state
	// implied by ONLY their spends at or below forkHeight, and clear any stamp — the
	// completion those stamps recorded necessarily involved a re-swept (>forkHeight)
	// spend, so it must be re-derived by the fold. Non-affected txs are untouched.
	resetSQL := fmt.Sprintf(`
		WITH affected AS (
			SELECT DISTINCT prev_tx_hash AS hash
			  FROM spends_p%[1]s
			 WHERE spent_at_height > $1
		),
		base AS (
			SELECT t.hash,
			       COALESCE(count(s.*) FILTER (
			           WHERE s.prev_output_idx < t.out_count
			             AND get_bit(t.out_spendables, s.prev_output_idx) = 1
			             AND s.spent_at_height <= $1
			       ), 0) AS base_progress,
			       max(s.spent_at_height) FILTER (
			           WHERE s.prev_output_idx < t.out_count
			             AND get_bit(t.out_spendables, s.prev_output_idx) = 1
			             AND s.spent_at_height <= $1
			       ) AS base_lsh
			  FROM affected a
			  JOIN txs_p%[1]s t ON t.hash = a.hash
			  LEFT JOIN spends_p%[1]s s ON s.prev_tx_hash = t.hash
			 GROUP BY t.hash
		),
		cleared AS (
			UPDATE txs_p%[1]s t
			   SET spent_progress    = b.base_progress,
			       last_spend_height = b.base_lsh,
			       delete_at_height  = NULL
			  FROM base b
			 WHERE t.hash = b.hash
			RETURNING t.hash
		)
		DELETE FROM pending_deletes_p%[1]s pd
		 USING cleared c
		 WHERE pd.hash = c.hash
	`, suffix)

	if _, err := tx.Exec(ctx, resetSQL, forkHeight); err != nil {
		return errors.NewStorageError("[dahSweep] rewind counter reset (partition %d) to %d", partition, forkHeight, err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = $1 WHERE partition = $2 AND last_swept_height > $1`,
		forkHeight, partition,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark (partition %d) to %d", partition, forkHeight, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.NewStorageError("[dahSweep] rewind commit (partition %d)", partition, err)
	}

	return nil
}
