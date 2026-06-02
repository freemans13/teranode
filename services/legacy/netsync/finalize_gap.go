package netsync

import "time"

const (
	// finalizeStallCheckInterval is how often the finalizer checks for a stalled
	// gap (a missing block height holding up otherwise-completed higher blocks).
	finalizeStallCheckInterval = 1 * time.Second

	// finalizeStallTimeout is how long the finalizer must be stuck waiting for the
	// same height — with higher blocks already buffered behind it — before it
	// re-requests the gap. Comfortably above normal transient out-of-order arrival
	// (which resolves in well under a second) so it never fires on healthy reorder.
	finalizeStallTimeout = 5 * time.Second
)

// gapStallDetector recognises when block finalization is wedged behind a missing
// height. The reorder buffer finalizes strictly in height order, so if the block
// at the awaited height is never delivered (a lost body), every higher block that
// completes its PhaseA piles up in the buffer and finalization halts. The
// pipeline path returns nil per block and so never trips the stage-1
// missing-parent self-heal; this detector is the pipeline's equivalent trigger.
//
// It is owned and called by the single finalizer goroutine, so it needs no
// locking. observe is pure (time is passed in) for testability.
type gapStallDetector struct {
	timeout time.Duration

	height uint32    // the height currently being waited on
	since  time.Time // when waiting on that height began (zero = not waiting)
}

// observe records the finalizer's current state and reports whether a stalled gap
// should be re-requested. It fires when the buffer has waited on the same height,
// with at least one higher block buffered behind it, for at least timeout — then
// re-arms for another full timeout so it nudges periodically rather than every
// tick. Forward progress (the awaited height advancing) or an empty buffer resets
// it, so healthy operation never fires.
func (d *gapStallDetector) observe(waitingHeight uint32, started bool, pending int, now time.Time) bool {
	if !started || pending == 0 {
		d.height = 0
		d.since = time.Time{}

		return false
	}

	if waitingHeight != d.height || d.since.IsZero() {
		d.height = waitingHeight
		d.since = now

		return false
	}

	if now.Sub(d.since) >= d.timeout {
		d.since = now // re-arm for another timeout window

		return true
	}

	return false
}

// markDispatched records that a block height's PhaseA has been dispatched (its
// tx-work has begun), so the gap watchdog knows it is in-flight rather than
// missing. Called by the blockQueue consumer for every pipelined block.
func (sm *SyncManager) markDispatched(height uint32) {
	sm.dispatchedMu.Lock()
	defer sm.dispatchedMu.Unlock()

	if sm.dispatched == nil {
		sm.dispatched = make(map[uint32]struct{})
	}

	sm.dispatched[height] = struct{}{}
}

// markFinalized clears a height once it has been finalized, so it no longer
// counts as in-flight. Called by the finalizer after each block.
func (sm *SyncManager) markFinalized(height uint32) {
	sm.dispatchedMu.Lock()
	defer sm.dispatchedMu.Unlock()

	delete(sm.dispatched, height)
}

// isDispatched reports whether a height's PhaseA has been dispatched and not yet
// finalized (i.e. it is in-flight, not missing).
func (sm *SyncManager) isDispatched(height uint32) bool {
	sm.dispatchedMu.Lock()
	defer sm.dispatchedMu.Unlock()

	_, ok := sm.dispatched[height]

	return ok
}

// pruneDispatchedBelow drops finalized/stale heights below the finalize floor so
// the in-flight set stays bounded even if a re-requested block is dispatched
// after the finalizer already passed its height.
func (sm *SyncManager) pruneDispatchedBelow(floor uint32) {
	sm.dispatchedMu.Lock()
	defer sm.dispatchedMu.Unlock()

	for h := range sm.dispatched {
		if h < floor {
			delete(sm.dispatched, h)
		}
	}
}

// gapShouldResync decides whether a stalled finalizer should re-request a gap.
// It re-requests only when the stall detector has fired AND the awaited height
// was never dispatched (a genuinely missing/lost body). A height that is
// dispatched-but-not-yet-finalized is in-flight — the finalizer is simply behind,
// not blocked on a missing block — so it must NOT be re-requested (that was the
// false-positive that thrashed the fetcher on betfair).
func (sm *SyncManager) gapShouldResync(detectorFired bool, waiting uint32) bool {
	return detectorFired && !sm.isDispatched(waiting)
}

// maybeResyncFinalizeGap re-requests the gap from the tip via the sync peer when
// the finalizer is stalled behind a missing block height. Rate-limited through
// the same slot as the consumer-side missing-parent self-heal so the two triggers
// issue at most one getblocks per interval. No-op (without consuming the rate
// slot) when there is no sync peer to ask.
func (sm *SyncManager) maybeResyncFinalizeGap(height uint32) {
	peer := sm.loadSyncPeer()
	if peer == nil {
		sm.logger.Warnf("[finalizeLoop] stalled waiting for block height %d with blocks buffered ahead, but no sync peer to re-request from", height)
		return
	}

	now := time.Now().UnixNano()

	last := sm.lastMissingParentResyncNano.Load()
	if now-last < int64(missingParentResyncInterval) {
		return // a recent gap re-request is already in flight
	}

	if !sm.lastMissingParentResyncNano.CompareAndSwap(last, now) {
		return
	}

	sm.logger.Infof("[finalizeLoop] stalled waiting for block height %d with blocks buffered ahead; re-requesting gap from tip (self-heal)", height)

	if err := sm.requestBlocksFromTip(peer); err != nil {
		sm.logger.Errorf("[finalizeLoop] gap re-request failed: %v", err)
	}
}
