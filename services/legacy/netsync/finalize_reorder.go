package netsync

// finalizeReorderBuffer re-orders block-finalization jobs that complete their
// (concurrent) PhaseA tx-work out of height order back into strict ascending,
// contiguous height order for PhaseB.
//
// Finalization (ProcessBlock → AddBlock → mined_set) MUST run in height order so
// that when a block is added its parent block is already on chain. PhaseA
// (output creation + trusted spend) is order-independent below the checkpoint
// and runs concurrently, so jobs can finish in any order. This buffer releases a
// job only once every lower height since the start has been released, guaranteeing
// the in-order contract.
//
// The start height is taken from the first job added. In headers-first catch-up
// the pipeline resumes from the committed chain tip and requests bodies in
// ascending order, so the first job seen is tip+1 — contiguous with the chain.
// A job whose height is below the next-to-finalize height is stale (its block is
// already finalized / on chain) and is dropped rather than finalized again.
type finalizeReorderBuffer struct {
	next    uint32 // next height to finalize
	started bool   // whether next has been initialised from the first job
	pending map[uint32]*finalizeJob
}

func newFinalizeReorderBuffer() *finalizeReorderBuffer {
	return &finalizeReorderBuffer{
		pending: make(map[uint32]*finalizeJob),
	}
}

// setStart establishes the first height to finalize. It is authoritative and
// must be set (once, at first dispatch on the in-order consumer) before any
// out-of-order PhaseA completion can call add — otherwise add's fallback would
// mistake the first completion for the start. Idempotent: only the first call
// takes effect, so a repeat or a later out-of-order dispatch cannot move the
// cursor.
func (b *finalizeReorderBuffer) setStart(h uint32) {
	if !b.started {
		b.next = h
		b.started = true
	}
}

// add records a completed PhaseA job and returns the jobs that are now ready to
// finalize, in ascending contiguous height order (possibly empty). Stale jobs
// (height below the next-to-finalize) are dropped and yield nothing.
func (b *finalizeReorderBuffer) add(job *finalizeJob) []*finalizeJob {
	if !b.started {
		b.next = job.blockHeight
		b.started = true
	}

	if job.blockHeight < b.next {
		// Already finalized — stale duplicate, drop it.
		return nil
	}

	b.pending[job.blockHeight] = job

	var ready []*finalizeJob
	for {
		j, ok := b.pending[b.next]
		if !ok {
			break
		}

		ready = append(ready, j)
		delete(b.pending, b.next)
		b.next++
	}

	return ready
}

// len reports how many jobs are buffered waiting for a lower height to arrive.
func (b *finalizeReorderBuffer) len() int {
	return len(b.pending)
}

// waitingFor reports the next height the buffer needs in order to make progress,
// and whether a start height has been established yet. A watchdog uses this to
// detect a stalled gap (this height missing while jobs pile up behind it).
func (b *finalizeReorderBuffer) waitingFor() (uint32, bool) {
	return b.next, b.started
}
