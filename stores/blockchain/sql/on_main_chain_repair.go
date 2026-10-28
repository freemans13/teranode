package sql

import (
	"sync"
	"time"
)

const (
	// onMainChainRepairInitialBackoff is the wait before the second repair attempt. The
	// first attempt runs as soon as the failure is recorded.
	onMainChainRepairInitialBackoff = time.Second
	// onMainChainRepairMaxBackoff caps the doubling wait between attempts.
	onMainChainRepairMaxBackoff = 30 * time.Second
	// noRepairFloor marks a record with no known lowest stale height.
	noRepairFloor int64 = -1
)

// onMainChainRepairState records a failed on_main_chain write that the running process
// still owes a repair for.
//
// Four paths write on_main_chain and then swallow a failure of the write that was meant
// to finish the job: InvalidateBlock and RevalidateBlock commit their UPDATE and then log
// a failed full rebuild, the startup goroutine logs a failed rebuild, and StoreBlock's
// slow path logs a failed sibling-fork clear or cannot classify the block it just wrote.
// Before this record existed, the guard dropped with the call, every flag reader went
// back to trusting the column, and the flags stayed wrong until a restart or the next
// operator action.
//
// While a repair is owed, mainChainRebuilding carries one extra count on this record's
// behalf, so every flag reader keeps taking its flag-free walk, and the repair loop
// retries rebuildOnMainChainFlag with backoff until an attempt lands. The count is
// released by whichever repair lands first: the loop's own attempt, or a full rebuild a
// later operator action runs successfully.
type onMainChainRepairState struct {
	mu sync.Mutex
	// pending is true while a repair is owed.
	pending bool
	// full is true when the owed repair must walk to genesis: the failed write was one
	// whose fork point can lie arbitrarily deep, so the bounded window is not enough.
	full bool
	// floor is the lowest height a bounded repair must reach, recorded by the marks and
	// kept across retries. A bounded walk's own window is measured from the tip at the
	// time of the attempt, and blocks keep arriving while the repair keeps failing, so
	// without the floor the tip could carry the window past the rows that need the
	// repair. noRepairFloor when no bounded mark is owed.
	floor int64
	// generation counts marks. The loop compares it before and after an attempt, so a
	// failure recorded while the attempt ran is not cleared by that attempt's success.
	generation uint64
	// attempts counts failed loop attempts since the repair became owed.
	attempts int
	// backoff is the wait before the second attempt; it doubles from there. Tests
	// shorten it.
	backoff time.Duration
	// wake carries one signal per mark to the repair loop. It is buffered, so a mark
	// recorded before the loop is listening is not lost.
	wake chan struct{}
	// stopped is closed when the repair loop has exited.
	stopped chan struct{}
}

// markOnMainChainDirty records that a write to on_main_chain failed after an earlier
// write in the same path committed, so the column may no longer describe the best chain.
//
// full says whether the owed repair must walk to genesis. floor is the lowest height
// whose flag the failed write may have left wrong; a bounded mark that cannot name one
// (noRepairFloor) is owed a full walk, because no bounded window can be shown to cover
// it. path labels the failure counter. The guard is raised on the transition from clean
// to owed and held until the repair lands, so the caller's own bracketing of
// mainChainRebuilding, if any, can be released as before.
//
// A failure while the store is shutting down is not recorded: it is almost always the
// shutdown cancelling the write, and the record would die with the process anyway.
func (s *SQL) markOnMainChainDirty(full bool, floor int64, path string, cause error) {
	if s.closing() {
		s.logger.Infof("on_main_chain: %s did not finish because the store is shutting down: %v", path, cause)

		return
	}

	initPrometheusMetrics()

	r := &s.onMainChainRepair

	r.mu.Lock()

	if !r.pending {
		r.pending = true
		r.attempts = 0
		r.floor = noRepairFloor

		s.mainChainRebuilding.Add(1)
		prometheusOnMainChainRepairPending.Inc()
	}

	if !full && floor == noRepairFloor {
		full = true
	}

	r.full = r.full || full

	if !full && (r.floor == noRepairFloor || floor < r.floor) {
		r.floor = floor
	}

	r.generation++

	r.mu.Unlock()

	// GetBlockByHeight and others answer from the response cache before they consult the
	// guard, so an answer cached from the stale column between the failed write and now
	// would otherwise outlive the mark.
	s.ResetResponseCache()

	prometheusOnMainChainWriteFailures.WithLabelValues(path).Inc()
	s.logger.Errorf("on_main_chain: %s left the column inconsistent, flag readers take the flag-free walk until the background repair lands: %v", path, cause)

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// onMainChainRepaired tells the record that a rebuild has just landed outside the loop.
// A full walk satisfies any owed repair, because it leaves every flag right. A bounded
// walk satisfies only an owed bounded repair.
func (s *SQL) onMainChainRepaired(full bool) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.pending || (r.full && !full) {
		return
	}

	s.clearOnMainChainRepairLocked("a later rebuild")
}

// clearOnMainChainRepairLocked releases the record. The caller holds r.mu.
func (s *SQL) clearOnMainChainRepairLocked(how string) {
	initPrometheusMetrics()

	r := &s.onMainChainRepair

	attempts := r.attempts
	r.pending = false
	r.full = false
	r.floor = noRepairFloor
	r.attempts = 0

	s.mainChainRebuilding.Add(-1)
	prometheusOnMainChainRepairPending.Dec()

	s.logger.Infof("on_main_chain repair landed by %s after %d failed background attempt(s), flag readers are back on the column", how, attempts)
}

// onMainChainRepairLoop waits for a mark and then retries the owed repair until it lands
// or the store shuts down. Started once by New.
func (s *SQL) onMainChainRepairLoop() {
	r := &s.onMainChainRepair

	defer close(r.stopped)
	defer s.releaseOnMainChainRepairGauge()

	for {
		select {
		case <-s.backgroundDone:
			return
		case <-r.wake:
		}

		if !s.repairOnMainChainUntilClean() {
			return
		}
	}
}

// repairOnMainChainUntilClean runs repair attempts with a doubling backoff until the
// record is clear. It returns false when the store shut down first.
func (s *SQL) repairOnMainChainUntilClean() bool {
	r := &s.onMainChainRepair

	r.mu.Lock()
	wait := r.backoff
	r.mu.Unlock()

	for {
		r.mu.Lock()

		if !r.pending {
			r.mu.Unlock()

			return true
		}

		generation := r.generation
		full := r.full
		floor := r.floor
		attempt := r.attempts + 1

		r.mu.Unlock()

		err := s.runOnMainChainRepair(full, floor)

		r.mu.Lock()

		if err == nil {
			if !r.pending {
				// A rebuild outside the loop landed between this attempt's walk and
				// now, and onMainChainRepaired already released the count. Releasing
				// it again would drive mainChainRebuilding below zero. A mark recorded
				// after that release sets pending again with a newer generation, so it
				// takes the retry path below instead of this one.
				r.mu.Unlock()

				return true
			}

			if r.generation == generation {
				s.clearOnMainChainRepairLocked("the background repair")
				r.mu.Unlock()

				return true
			}

			// A new failure was recorded while this attempt ran, so run again against it.
			r.mu.Unlock()

			continue
		}

		r.attempts++

		r.mu.Unlock()

		s.logger.Warnf("on_main_chain repair attempt %d failed (full walk %v, floor %d), next attempt in %s: %v", attempt, full, floor, wait, err)

		select {
		case <-s.backgroundDone:
			return false
		case <-time.After(wait):
		}

		wait *= 2
		if wait > onMainChainRepairMaxBackoff {
			wait = onMainChainRepairMaxBackoff
		}
	}
}

// runOnMainChainRepair runs one attempt. rebuildOnMainChainFlag raises the guard itself
// for its duration, on top of the count this record holds.
//
// A bounded attempt runs under slowPathMu, the lock the StoreBlock paths that owed it
// held, so no slow-path writer interleaves with a walk over the rows at the tip. It is
// short: the window is the tip's plus the recorded floor, under a 30 s budget.
//
// A full attempt does not take slowPathMu. It can run for up to 30 min, and while the
// database stays slow it is retried every 30 s, so holding the lock would stop block
// storage for as long as the fault lasts. It runs the way the startup rebuild always has:
// readers are on their flag-free walk because of the guard, rows a concurrent StoreBlock
// inserts are flagged by that StoreBlock, and on Postgres a write-write conflict with a
// concurrent slow path aborts the REPEATABLE READ walk, which rebuildOnMainChainFlag
// retries.
func (s *SQL) runOnMainChainRepair(full bool, floor int64) error {
	timeout := rebuildOffChainSetTimeout
	if full {
		timeout = migrationFullRebuildTimeout
	}

	ctx, cancel := s.shutdownAwareContext(timeout)
	defer cancel()

	if !full {
		s.slowPathMu.Lock()
		defer s.slowPathMu.Unlock()
	}

	if err := s.rebuildOnMainChainFlagFrom(ctx, full, floor); err != nil {
		return err
	}

	// Drop any answer a reader cached from the column between the failed write and the
	// mark. Everything cached since came from a flag-free walk and is still right.
	s.ResetResponseCache()

	return nil
}

// releaseOnMainChainRepairGauge drops this store's contribution to the pending gauge when
// its repair loop exits with a repair still owed, so a closed store does not hold the
// process-wide gauge up for the stores that outlive it.
func (s *SQL) releaseOnMainChainRepairGauge() {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending {
		// Nothing can land the repair now, so stop a later acknowledgment from releasing
		// the gauge a second time. The guard stays up: the store is closed.
		r.pending = false
		prometheusOnMainChainRepairPending.Dec()
	}
}

// closing reports whether Close has signalled the store's background work to stop.
func (s *SQL) closing() bool {
	select {
	case <-s.backgroundDone:
		return true
	default:
		return false
	}
}
