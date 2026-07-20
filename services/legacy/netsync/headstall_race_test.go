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
