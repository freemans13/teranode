// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for F1 (race, don't kill): a head-of-line stall past BlockStallTimeout
// re-assigns the stalled head block to ANOTHER peer without disconnecting the peer
// that held it, and only disconnects that peer if the SAME head block stays stuck
// past HeadStallDisconnectTimeout. This stops the 2s head-of-line churn that, when
// the head peer is the sync peer, resets the headers-first download every 2s and
// freezes the single-sourced header frontier for minutes. Reuses the
// buildHeadStallManager harness (frontier head so the backpressure suppression is
// carved out and the stall logic is reached).

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCheckHeadStall_RacesHeadBlockWithoutDisconnect: within the race window a
// stalled frontier head block is re-enqueued for re-fetch (handed to another peer)
// and the peer holding it is KEPT connected — no header-reset churn.
func TestCheckHeadStall_RacesHeadBlockWithoutDisconnect(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, head := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second

	// Frontier: head == cached+1, so the backpressure suppression is carved out and
	// the stall logic is reached.
	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	// First checkHeadStall stamps headStallSince=now (fresh) -> within the 30s race
	// window -> race, not disconnect.
	sm.checkHeadStall(time.Now())

	require.Never(t, func() bool { return !peerA.Connected() }, 300*time.Millisecond, 20*time.Millisecond,
		"within the race window the head peer must be KEPT connected (raced, not disconnected)")
	require.True(t, peerB.Connected(), "peer B must not be touched")

	sm.assignedMu.Lock()
	_, stillAssigned := sm.assignedTo[head]
	_, queuedForRefetch := sm.refetchBlocks[head]
	sm.assignedMu.Unlock()
	require.False(t, stillAssigned, "the raced head block must be removed from assignedTo")
	require.True(t, queuedForRefetch, "the raced head block must be re-enqueued for re-fetch (handed to another peer)")
}

// TestCheckHeadStall_DisconnectsAfterRaceWindow: once the SAME head block has been
// stuck past HeadStallDisconnectTimeout (the race genuinely failed), the peer
// holding it is disconnected — the backstop is preserved, not disabled.
func TestCheckHeadStall_DisconnectsAfterRaceWindow(t *testing.T) {
	const headHeight = 100
	sm, peerA, _, head := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	// The head block has already been the stalled head for longer than the disconnect
	// window (races have failed): pre-set the cross-reassignment tracker.
	sm.headStallHash = head
	sm.headStallSince = time.Now().Add(-31 * time.Second)

	sm.checkHeadStall(time.Now())

	require.Eventually(t, func() bool { return !peerA.Connected() }, 2*time.Second, 5*time.Millisecond,
		"a head block stuck past the disconnect window must disconnect the peer holding it")
}

// TestCheckHeadStall_RollbackDisconnectsImmediately: HeadStallDisconnectTimeout=0
// is the byte-identical rollback — disconnect at BlockStallTimeout with no race.
func TestCheckHeadStall_RollbackDisconnectsImmediately(t *testing.T) {
	const headHeight = 100
	sm, peerA, _, _ := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 0 // rollback

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	sm.checkHeadStall(time.Now())

	require.Eventually(t, func() bool { return !peerA.Connected() }, 2*time.Second, 5*time.Millisecond,
		"rollback (0) must disconnect the head peer immediately at BlockStallTimeout")
}

// TestCheckHeadStall_RacePrimesThroughputBaseline connects F1 to F2.
//
// The F2 gate refuses to veto a disconnect it did not measure, and it needs a
// byte sample at least headStallRateWindow old before it can compute a rate. The
// race window is the only place there is time to grow one, so each race pass
// seeds the baseline if the peer has none. Without this the gate would arrive at
// every real disconnect decision unmeasured, and F2 would protect nobody.
func TestCheckHeadStall_RacePrimesThroughputBaseline(t *testing.T) {
	const headHeight = 100

	sm, peerA, _, _ := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.BlockStallMinRate = 102400

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	peerState, ok := sm.peerStates.Get(peerA)
	require.True(t, ok, "precondition: the head peer must be in the peer-state map")
	require.Zero(t, peerState.lastAssocSampleAt.Load(),
		"precondition: the peer must start with no byte sample")

	now := time.Now()
	sm.checkHeadStall(now)

	require.Equal(t, now.UnixNano(), peerState.lastAssocSampleAt.Load(),
		"a race pass must seed the F2 baseline so the gate can measure by the time the window expires")
	require.Equal(t, peerA.AssociationReadBytes(), peerState.lastAssocReadBytes.Load(),
		"the seeded baseline must be the peer's live association byte total")
}

// TestCheckHeadStall_RaceDoesNotResampleAMaturingBaseline: the race pass seeds a
// baseline, it never refreshes one. Refreshing on every ~20ms refill tick would
// restart the measurement window continuously and it would never mature, so the
// gate could never produce a rate — the same failure as having no baseline, but
// silent.
func TestCheckHeadStall_RaceDoesNotResampleAMaturingBaseline(t *testing.T) {
	const headHeight = 100

	sm, peerA, _, _ := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.BlockStallMinRate = 102400

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	peerState, ok := sm.peerStates.Get(peerA)
	require.True(t, ok, "precondition: the head peer must be in the peer-state map")

	now := time.Now()
	seededAt := now.Add(-500 * time.Millisecond).UnixNano()
	peerState.lastAssocReadBytes.Store(1)
	peerState.lastAssocSampleAt.Store(seededAt)

	sm.checkHeadStall(now)

	require.Equal(t, seededAt, peerState.lastAssocSampleAt.Load(),
		"a usable baseline must be left alone so its measurement window can mature")
	require.EqualValues(t, 1, peerState.lastAssocReadBytes.Load(),
		"a usable baseline must be left alone so its measurement window can mature")
}

// TestCheckHeadStall_RacedBlockToleratesLateDelivery reproduces the cascade
// measured live on mainnet 2026-07-20. F1 races a stalled head block to another
// peer and keeps the original connected — but freeing the block deletes it from
// that peer's requestedBlocks. If the raced-TO peer commits the block first, its
// hash leaves headerHeightIndex, so the original peer's slightly-late copy matches
// neither the per-peer ledger nor the live index and the delivered-block handler
// disconnects it for "Got unrequested block". That drop lands on the (usually sync)
// peer and re-anchors the frontier — the exact churn F1 exists to remove.
//
// The fix registers the raced hash in recentlyNeededUntil so blockStillNeeded
// tolerates the late duplicate. RED before the fix: blockStillNeeded returns false
// once the hash leaves the index.
func TestCheckHeadStall_RacedBlockToleratesLateDelivery(t *testing.T) {
	const headHeight = 100
	sm, peerA, _, head := buildHeadStallManager(t, headHeight)
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	// Race the head block away (F1) — peer A is kept connected.
	sm.checkHeadStall(time.Now())
	require.True(t, peerA.Connected(), "precondition: the raced peer is kept connected")

	// The raced-TO peer delivers and commits the block first, so its hash leaves the
	// live header index (this is what handleBlockPreamble does on commit).
	sm.headerMu.Lock()
	delete(sm.headerHeightIndex, head)
	sm.headerMu.Unlock()

	// The original peer's late duplicate of that exact block must be TOLERATED, not
	// punished as unrequested spam — otherwise the sync peer is disconnected here.
	require.True(t, sm.blockStillNeeded(head),
		"a late delivery of a raced-away block must be tolerated (recentlyNeededUntil grace), not treated as unrequested")
}
