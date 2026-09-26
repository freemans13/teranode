package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// newPrefetchManager builds a minimal SyncManager wired only for the block
// prefetch gate. A budget of 0 leaves prefetch disabled.
func newPrefetchManager(budget int64) *SyncManager {
	sm := &SyncManager{logger: ulogger.TestLogger{}}
	if budget > 0 {
		sm.blockPrefetchBudgetBytes = budget
		sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
		sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
	}

	return sm
}

// TestPeerStateResolvingPrimary covers the stream→primary resolution walk shared
// by handleBlockMsg/handleHeadersMsg/handleInvMsg/BlockRequested: a registered
// peer resolves to itself, an unregistered stream sub-peer resolves to its
// association's registered primary, and an unknown peer resolves to nothing.
func TestPeerStateResolvingPrimary(t *testing.T) {
	newSM := func() *SyncManager {
		return &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}
	}

	t.Run("registered primary resolves to itself", func(t *testing.T) {
		sm := newSM()
		primary := &peerpkg.Peer{}
		want := &peerSyncState{}
		sm.peerStates.Set(primary, want)

		state, resolved, exists := sm.peerStateResolvingPrimary(primary)
		require.True(t, exists)
		require.Same(t, primary, resolved)
		require.Same(t, want, state)
	})

	t.Run("stream sub-peer resolves to its registered primary", func(t *testing.T) {
		sm := newSM()
		primary := &peerpkg.Peer{}
		want := &peerSyncState{}
		sm.peerStates.Set(primary, want)

		stream := &peerpkg.Peer{}
		stream.SetAssociation(peerpkg.NewAssociation([]byte{0x01}, primary))

		state, resolved, exists := sm.peerStateResolvingPrimary(stream)
		require.True(t, exists)
		require.Same(t, primary, resolved)
		require.Same(t, want, state)
	})

	t.Run("unknown peer resolves to nothing", func(t *testing.T) {
		sm := newSM()
		peer := &peerpkg.Peer{}

		state, resolved, exists := sm.peerStateResolvingPrimary(peer)
		require.False(t, exists)
		require.Same(t, peer, resolved)
		require.Nil(t, state)
	})
}

func TestAcquireBlockPrefetch_Disabled(t *testing.T) {
	sm := newPrefetchManager(0)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 999)
	require.NoError(t, err)
	require.Equal(t, int64(0), w)

	// Release of a zero reservation must be a no-op, not a panic.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w) })
}

// TestAcquireBlockPrefetch_FloorsTinyBlocks proves a block smaller than the
// per-in-flight floor is charged the floor, not its serialized size, so a flood
// of minimal blocks cannot admit an unbounded number of goroutines within the
// byte budget.
func TestAcquireBlockPrefetch_FloorsTinyBlocks(t *testing.T) {
	sm := newPrefetchManager(4 * minInFlightBlockWeight)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 81) // minimal zero-tx block
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w, "a tiny block must be charged the floor weight")
	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w)
}

// TestAcquireBlockPrefetch_OversizedAdmittedAlone proves a block larger than the
// whole budget is admitted (weight clamped to the budget) rather than
// deadlocking, and that it then consumes the entire budget until released —
// i.e. huge blocks process one at a time, preserving the original backpressure.
func TestAcquireBlockPrefetch_OversizedAdmittedAlone(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget*100)
	require.NoError(t, err)
	require.Equal(t, int64(budget), w, "oversized weight must clamp to the budget")

	// Budget is now fully consumed: nothing else can be admitted until release.
	require.False(t, sm.blockPrefetchBudget.TryAcquire(1))

	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w)
	require.True(t, sm.blockPrefetchBudget.TryAcquire(1))
}

// TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter proves the gate
// backpressures the read-loop when the budget is full, registers a waiter while
// blocked (so the stall detector can tell self-backpressure from a slow peer),
// and unblocks on release.
func TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	first, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	acquired := make(chan int64, 1)

	go func() {
		w, e := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x02}, minInFlightBlockWeight)
		if e == nil {
			acquired <- w
		}
	}()

	// The second acquire must block on the full budget and register as a waiter.
	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)
	require.True(t, sm.localReadBackpressured())

	select {
	case <-acquired:
		t.Fatal("second acquire returned before the budget was released")
	case <-time.After(50 * time.Millisecond):
	}

	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, first)

	select {
	case w := <-acquired:
		require.Equal(t, int64(minInFlightBlockWeight), w)
	case <-time.After(time.Second):
		t.Fatal("second acquire did not unblock after release")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)
	require.False(t, sm.localReadBackpressured())
}

// TestAcquireBlockPrefetch_CtxCancel proves a read-loop blocked on the budget is
// released (with nothing reserved) when its context is cancelled on shutdown.
func TestAcquireBlockPrefetch_CtxCancel(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, e := sm.AcquireBlockPrefetch(ctx, nil, chainhash.Hash{0x02}, minInFlightBlockWeight)
		done <- e
	}()

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)

	cancel()

	select {
	case e := <-done:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after context cancellation")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)

	// A cancelled acquire reserved nothing, so it must not leak its hash in the
	// dedup set: the two halves of the gate stay paired even on the failure path.
	sm.inFlightBlocksMu.Lock()
	_, leaked := sm.inFlightBlocks[chainhash.Hash{0x02}]
	sm.inFlightBlocksMu.Unlock()
	require.False(t, leaked, "a cancelled acquire must remove the hash it inserted before parking")
}

// TestAcquireBlockPrefetch_QuitAbort proves a budget-parked read-loop unblocks on
// peer teardown (its quit channel closing), not only on ctx cancellation —
// mirroring awaitBlockResult, since sp.ctx is the long-lived Init context that
// Stop() does not cancel.
func TestAcquireBlockPrefetch_QuitAbort(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	quit := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		// Non-cancellable ctx: only quit can unblock this, proving quit is honored.
		_, e := sm.AcquireBlockPrefetch(context.Background(), quit, chainhash.Hash{0x02}, minInFlightBlockWeight)
		done <- e
	}()

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)

	close(quit) // peer torn down

	select {
	case e := <-done:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after the peer quit channel closed")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)
}

// TestAcquireBlockPrefetch_DeduplicatesInFlight proves the in-flight-by-hash set
// is the dedup half of the admission gate: a second acquire of a hash already in
// flight is dropped with the benign ErrDuplicateBlockInFlight sentinel WITHOUT
// reserving budget, the set keeps exactly one copy, and the hash becomes
// acquirable again only after release (paired 1:1 with the budget weight).
func TestAcquireBlockPrefetch_DeduplicatesInFlight(t *testing.T) {
	const budget = 4 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)
	h := chainhash.Hash{0x01}

	// First copy of H is admitted and reserves the floor weight.
	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w)

	// Second copy of H is a duplicate: benign sentinel, nothing reserved.
	dupW, dupErr := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)
	require.Equal(t, int64(0), dupW)

	// The duplicate reserved no budget: everything but the single admitted copy is
	// still free (probe with TryAcquire, then hand it straight back).
	require.True(t, sm.blockPrefetchBudget.TryAcquire(budget-minInFlightBlockWeight))
	sm.blockPrefetchBudget.Release(budget - minInFlightBlockWeight)

	// The set still holds exactly one copy of H.
	sm.inFlightBlocksMu.Lock()
	_, present := sm.inFlightBlocks[h]
	require.True(t, present)
	require.Len(t, sm.inFlightBlocks, 1)
	sm.inFlightBlocksMu.Unlock()

	// Release H: the hash leaves the set alongside the budget, so a fresh copy of
	// H is admissible again.
	sm.ReleaseBlockPrefetch(h, w)

	sm.inFlightBlocksMu.Lock()
	require.Empty(t, sm.inFlightBlocks)
	sm.inFlightBlocksMu.Unlock()

	w2, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w2)
	sm.ReleaseBlockPrefetch(h, w2)
}

// TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget proves a duplicate does
// not eat budget: a DIFFERENT hash G can still be admitted for the budget the
// duplicate of H would have (wrongly) consumed. This is the regression the dedup
// set exists to prevent — N copies of one requested, near-budget-sized block
// filling the whole budget and parking every peer's read-loop.
func TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)
	h := chainhash.Hash{0x0a}
	g := chainhash.Hash{0x0b}

	// H takes half the budget.
	wH, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)

	// A duplicate of H must not reserve the remaining half.
	_, dupErr := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)

	// So a different hash G can still be admitted against the remaining budget.
	wG, err := sm.AcquireBlockPrefetch(context.Background(), nil, g, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), wG)

	sm.ReleaseBlockPrefetch(h, wH)
	sm.ReleaseBlockPrefetch(g, wG)
}

// TestAcquireBlockPrefetch_DisabledSkipsDedup proves the synchronous/kill-switch
// path (nil budget) neither dedups nor touches the (nil) in-flight set: repeated
// acquires of the same hash all return (0, nil), matching the one-block-per-peer
// backpressure the synchronous path already provides.
func TestAcquireBlockPrefetch_DisabledSkipsDedup(t *testing.T) {
	sm := newPrefetchManager(0)
	require.Nil(t, sm.inFlightBlocks)
	h := chainhash.Hash{0x01}

	for i := 0; i < 3; i++ {
		w, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, 999)
		require.NoError(t, err)
		require.Equal(t, int64(0), w)
	}

	require.Nil(t, sm.inFlightBlocks)
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(h, 0) })
}

// TestLocalReadBackpressured now covers only the waiter half of the old test.
// The backlog half (a queued / mid-validation blockQueueMsg backlog also
// counting as self-backpressure, gated on a lastChainProgress staleness check)
// is gone along with blockBacklog and lastChainProgress themselves: there is no
// decoded block queue left to accumulate a backlog on. localReadBackpressured
// (manager.go) is now exactly "a budget is configured and something is parked
// on it".
func TestLocalReadBackpressured(t *testing.T) {
	t.Run("kill switch (budget nil): never backpressured", func(t *testing.T) {
		sm := newPrefetchManager(0)
		require.False(t, sm.localReadBackpressured())

		sm.blockPrefetchWaiters.Add(1)
		require.False(t, sm.localReadBackpressured(), "a nil budget has no waiter semaphore of its own to report on")
	})

	t.Run("enabled: suppresses exactly while a read-loop is parked on the budget", func(t *testing.T) {
		sm := newPrefetchManager(100)
		require.False(t, sm.localReadBackpressured())

		sm.blockPrefetchWaiters.Add(1)
		require.True(t, sm.localReadBackpressured())

		sm.blockPrefetchWaiters.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})
}

// TestHandleCheckSyncPeer_PrefetchBackpressure proves the stall detector
// suppresses rotation while the node is backpressured by its own block
// processing — a read-loop parked on the prefetch budget — so a healthy peer is
// not rotated merely because a block is slow to validate. It still rotates a
// genuinely idle stalled peer once that self-backpressure clears.
//
// This used to also cover a queued / mid-validation backlog suppressing
// rotation on its own, gated on a lastChainProgress staleness check; both are
// gone with the decoded block queue (blockBacklog, lastChainProgress), so the
// only self-backpressure signal left is the prefetch waiter count.
func TestHandleCheckSyncPeer_PrefetchBackpressure(t *testing.T) {
	newStalledState := func() *syncPeerState {
		return &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute),
			ticks:         1,
			violations:    maxNetworkViolations - 1,
		}
	}

	newSyncManager := func(sp *peerpkg.Peer, sps *syncPeerState) *SyncManager {
		sm := &SyncManager{
			logger:                   ulogger.TestLogger{},
			peerStates:               txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed:  51200,
			blockPrefetchBudgetBytes: 100,
			blockPrefetchBudget:      semaphore.NewWeighted(100),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false)
		sm.peerStates.Set(sp, &peerSyncState{})

		return sm
	}

	t.Run("keeps sync peer while a read-loop is blocked on prefetch budget", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		sm.blockPrefetchWaiters.Add(1) // read-loop parked in AcquireBlockPrefetch

		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		require.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates a genuinely idle stalled peer (no waiters)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// Nothing queued and no read-loop parked: the stale last-block-time is the
		// peer's fault, so rotation runs (and panics in this minimal SyncManager,
		// which proves it ran rather than being suppressed).
		require.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}
