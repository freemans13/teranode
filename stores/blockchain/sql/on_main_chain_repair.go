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
// later operator action runs successfully while no loop attempt is in flight.
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
	// chainWrites counts the starts and ends of slow-path writes that move chain
	// membership: a fork-path StoreBlock, the sibling-fork clear, InvalidateBlock and
	// RevalidateBlock. writing counts those still in flight. An attempt that does not
	// hold slowPathMu reads its rows from a snapshot taken when it began, so a membership
	// write that commits during the attempt can be overwritten by the attempt's older
	// view without any row conflict to abort either. The loop compares both before and
	// after an attempt and runs again when either moved.
	chainWrites uint64
	writing     int
	// running is true while the loop has an attempt in flight. A rebuild outside the loop
	// does not release the record then, because the attempt can commit after it from an
	// older snapshot; the loop decides once the attempt has finished.
	running bool
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
	initPrometheusMetrics()

	r := &s.onMainChainRepair

	r.mu.Lock()

	// Checked under r.mu: the loop releases its gauge under r.mu, and only after Close has
	// signalled, so a mark either sees the store closing here or is seen by that release.
	if s.closing() {
		r.mu.Unlock()

		s.logger.Infof("on_main_chain: %s did not finish because the store is shutting down: %v", path, cause)

		return
	}

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
//
// While a loop attempt is running the record is left alone. That attempt may commit after
// this rebuild from a snapshot older than it, so only the loop can tell whether the flags
// are right once it has finished. The callers run inside beginOnMainChainWrite, so the
// loop sees the overlap and runs again.
func (s *SQL) onMainChainRepaired(full bool) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.pending || r.running || (r.full && !full) {
		return
	}

	s.clearOnMainChainRepairLocked("a later rebuild")
}

// beginOnMainChainWrite records the start of a slow-path write that moves chain
// membership and returns the function that records its end. The caller holds slowPathMu
// and calls the returned function after its last commit, so a repair attempt whose
// snapshot predates any of those commits sees the counters move and runs again.
func (s *SQL) beginOnMainChainWrite() func() {
	r := &s.onMainChainRepair

	r.mu.Lock()
	r.writing++
	r.chainWrites++
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		r.writing--
		r.chainWrites++
		r.mu.Unlock()
	}
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
		chainWrites := r.chainWrites
		full := r.full
		floor := r.floor
		attempt := r.attempts + 1
		r.running = true

		r.mu.Unlock()

		err := s.runOnMainChainRepair(full, floor)

		r.mu.Lock()

		r.running = false

		if err == nil {
			// pending is still true here: nothing else clears the record while an attempt
			// runs, because onMainChainRepaired leaves it alone until running drops.
			raced := r.chainWrites != chainWrites || r.writing > 0
			if !raced && r.generation == generation {
				s.clearOnMainChainRepairLocked("the background repair")
				r.mu.Unlock()

				return true
			}

			// A new failure was recorded while this attempt ran, or a membership write
			// overlapped it and the attempt may have committed an older view over it.
			// Either way the flags are not known to be right, so run again at once.
			r.mu.Unlock()

			if raced {
				s.logger.Infof("on_main_chain repair attempt %d overlapped a chain write, running it again", attempt)
			}

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
// On PostgreSQL a full attempt does not take slowPathMu. It can run for up to 30 min, and
// while the database stays slow it is retried every 30 s, so holding the lock would stop
// block storage for as long as the fault lasts. Readers are on their flag-free walk
// because of the guard, and rows a concurrent StoreBlock inserts are flagged by that
// StoreBlock. A membership write that commits during the attempt can touch only rows the
// attempt does not, so REPEATABLE READ aborts neither, and the attempt can then commit
// its older view over the write. The loop catches that through beginOnMainChainWrite and
// runs the attempt again before it releases the guard. A fork-path StoreBlock whose
// reconcile updates a row the walk has already updated still waits for the walk to
// commit, with slowPathMu held.
//
// On SQLite a full attempt does take slowPathMu. SQLite lets one writer in at a time, so a
// long walk would make each StoreBlock that needs to write fail with SQLITE_BUSY after the
// 5 s busy timeout; waiting on the lock delays block storage without failing it.
func (s *SQL) runOnMainChainRepair(full bool, floor int64) error {
	timeout := rebuildOffChainSetTimeout
	if full {
		timeout = migrationFullRebuildTimeout
	}

	ctx, cancel := s.shutdownAwareContext(timeout)
	defer cancel()

	if !full || !s.unlockedFullRepair {
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
