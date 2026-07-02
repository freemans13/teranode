package postgres

import (
	"context"

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
// heights the watermark may already have passed, and must be re-evaluated. The
// guard last_swept_height > forkHeight ensures the watermark is never advanced
// forward by this call (it only ever rewinds).
//
// The forward-only fold maintains spent_progress by ADDING each band's spends, so a
// re-sweep after a rewind can double-count spends that survived the reorg, inflating
// the counter. That over-count is NO LONGER able to cause data loss: the DAH stamp
// (both the sweep fold and the mine path) is authorised by a GROUND-TRUTH recount of
// the tx's spends, not by the counter, so a drifted counter can never stamp a
// not-fully-spent tx for deletion. The reconcile backstop corrects the counter itself.
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = $1 WHERE last_swept_height > $1`,
		forkHeight,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark to %d", forkHeight, err)
	}

	return nil
}
