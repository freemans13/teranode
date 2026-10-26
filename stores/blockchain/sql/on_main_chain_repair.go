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
	// onMainChainRepairEscalateAfter is the number of failed bounded attempts after which
	// the next attempt walks to genesis. A bounded walk covers a window measured from the
	// current tip, and blocks keep arriving while the repair keeps failing, so a long run
	// of failures can carry the tip past the rows that need the repair. The full walk
	// reads every row and cannot miss them.
	onMainChainRepairEscalateAfter = 3
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
// full says whether the owed repair must walk to genesis. path labels the failure
// counter. The guard is raised on the transition from clean to owed and held until the
// repair lands, so the caller's own bracketing of mainChainRebuilding, if any, can be
// released as before.
func (s *SQL) markOnMainChainDirty(full bool, path string, cause error) {
	initPrometheusMetrics()

	r := &s.onMainChainRepair

	r.mu.Lock()

	if !r.pending {
		r.pending = true
		r.attempts = 0

		s.mainChainRebuilding.Add(1)
		prometheusOnMainChainRepairPending.Set(1)
	}

	r.full = r.full || full
	r.generation++

	r.mu.Unlock()

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
	r.attempts = 0

	s.mainChainRebuilding.Add(-1)
	prometheusOnMainChainRepairPending.Set(0)

	s.logger.Infof("on_main_chain repair landed by %s after %d failed background attempt(s), flag readers are back on the column", how, attempts)
}

// onMainChainRepairLoop waits for a mark and then retries the owed repair until it lands
// or the store shuts down. Started once by New.
func (s *SQL) onMainChainRepairLoop() {
	r := &s.onMainChainRepair

	defer close(r.stopped)

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
		full := r.full || r.attempts >= onMainChainRepairEscalateAfter
		attempt := r.attempts + 1

		r.mu.Unlock()

		err := s.runOnMainChainRepair(full)

		r.mu.Lock()

		if err == nil {
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

		s.logger.Warnf("on_main_chain repair attempt %d failed (full walk %v), next attempt in %s: %v", attempt, full, wait, err)

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

// runOnMainChainRepair runs one attempt under the same lock the paths that owed the
// repair held, so no slow-path writer interleaves with the walk. rebuildOnMainChainFlag
// raises the guard itself for its duration, on top of the count this record holds.
func (s *SQL) runOnMainChainRepair(full bool) error {
	timeout := rebuildOffChainSetTimeout
	if full {
		timeout = migrationFullRebuildTimeout
	}

	ctx, cancel := s.shutdownAwareContext(timeout)
	defer cancel()

	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	if err := s.rebuildOnMainChainFlag(ctx, full); err != nil {
		return err
	}

	// Drop any answer a reader cached from the column between the failed write and the
	// mark. Everything cached since came from a flag-free walk and is still right.
	s.ResetResponseCache()

	return nil
}
