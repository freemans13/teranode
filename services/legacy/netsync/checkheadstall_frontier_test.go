// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the frontier carve-out in checkHeadStall. Under park-ahead,
// localReadBackpressured() is almost always true during IBD (the parked backlog
// keeps re-stamping progress), which used to suppress the 2s head-of-line peer
// swap unconditionally — so a silent peer holding the exact block the committer
// needs next (cachedBlockAssemblyHeight+1) aged 53-170s to the coarse
// TTL/rotation recovery while the whole commit pipeline sat idle. The carve-out
// skips the backpressure suppression ONLY when the outstanding head IS that
// frontier block, restoring the fast swap for the genuine-starvation case while
// leaving the suppression intact for ordinary self-backpressure (the 9163bc08f
// disconnect-cascade guard).

import (
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// buildHeadStallManager wires the minimal SyncManager checkHeadStall needs, with
// two fetch peers and self-backpressure ARMED (blockBacklog>0, prefetch budget
// nil => localReadBackpressured() is unconditionally true). The head block is
// assigned to peerA in the distant past so it is well beyond BlockStallTimeout.
// cachedBlockAssemblyHeight and baHeightPolled are set by the caller to place the
// head either AT the commit frontier (cached+1) or above it.
func buildHeadStallManager(t *testing.T, headHeight int32) (*SyncManager, *peer.Peer, *peer.Peer, chainhash.Hash) {
	t.Helper()

	chainParams := chaincfg.MainNetParams

	var (
		muA, muB sync.Mutex
		gotA     []*wire.MsgGetData
		gotB     []*wire.MsgGetData
	)
	peerA := captureGetDataPeer(t, &chainParams, 60, &muA, &gotA)
	peerB := captureGetDataPeer(t, &chainParams, 61, &muB, &gotB)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second
	// These carve-out tests exercise the IMMEDIATE-disconnect path (the pre-F1
	// behaviour): 0 disables the F1 re-fetch-race window so a stalled head peer is
	// disconnected at BlockStallTimeout, which is what they assert. The F1 race path
	// (default 30s) is covered by headstall_race_test.go.
	tSettings.Legacy.HeadStallDisconnectTimeout = 0

	stateA := newEligiblePeerState()
	stateB := newEligiblePeerState()
	t.Cleanup(func() {
		stateA.requestedBlocks.Stop()
		stateA.requestedTxns.Stop()
		stateB.requestedBlocks.Stop()
		stateB.requestedTxns.Stop()
	})

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(peerA, stateA)
	sm.peerStates.Set(peerB, stateB)
	sm.headersFirstMode.Store(true)

	// Arm self-backpressure: a non-empty backlog with a nil prefetch budget makes
	// localReadBackpressured() unconditionally true — the exact condition that used
	// to suppress the head-stall swap during IBD.
	sm.blockBacklog.Store(1)
	require.True(t, sm.localReadBackpressured(),
		"precondition: self-backpressure must be armed for this test")

	now := time.Now()
	head := chainhash.Hash{0x11}
	bHigh := chainhash.Hash{0x12}

	// Head assigned to A long ago (stalled well past the 2s timeout).
	assignForStall(sm, head, headHeight, peerA, stateA, now.Add(-10*time.Second))
	// A higher block on B, also old, so B is not the head.
	assignForStall(sm, bHigh, headHeight+1, peerB, stateB, now.Add(-10*time.Second))

	require.True(t, peerA.Connected(), "precondition: peer A connected")
	require.True(t, peerB.Connected(), "precondition: peer B connected")

	return sm, peerA, peerB, head
}

// TestCheckHeadStall_FrontierCarveOutFiresUnderBackpressure: when the stalled
// head IS the commit frontier (cachedBlockAssemblyHeight+1), the head peer must
// be disconnected even though self-backpressure is armed — otherwise the whole
// commit pipeline stays idle waiting for a block a silent peer will never send.
func TestCheckHeadStall_FrontierCarveOutFiresUnderBackpressure(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, _ := buildHeadStallManager(t, headHeight)

	// Commit frontier is exactly one below the head: head == cached+1.
	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	sm.checkHeadStall(time.Now())

	require.Eventually(t, func() bool { return !peerA.Connected() }, 2*time.Second, 5*time.Millisecond,
		"frontier head peer must be disconnected despite backpressure (carve-out)")
	require.True(t, peerB.Connected(), "non-head peer B must NOT be disconnected")
}

// TestCheckHeadStall_NonFrontierStillSuppressed: when the stalled head is ABOVE
// the commit frontier, the ordinary self-backpressure suppression must still
// hold — this is not head-of-line starvation, and disconnecting here would
// reintroduce the 9163bc08f rotation cascade.
func TestCheckHeadStall_NonFrontierStillSuppressed(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, _ := buildHeadStallManager(t, headHeight)

	// Commit frontier is well below the head: head == cached+5, not cached+1.
	sm.cachedBlockAssemblyHeight.Store(headHeight - 5)
	sm.baHeightPolled.Store(true)

	sm.checkHeadStall(time.Now())

	// Give any (erroneous) async disconnect a chance to land before asserting.
	require.Never(t, func() bool { return !peerA.Connected() }, 300*time.Millisecond, 20*time.Millisecond,
		"non-frontier head under backpressure must stay connected (suppression preserved)")
	require.True(t, peerB.Connected(), "peer B must NOT be disconnected")
}

// TestCheckHeadStall_NotPolledUsesSuppression: before the block-assembly height
// poller has succeeded (baHeightPolled false), frontierStalled cannot be
// asserted, so the ordinary suppression must apply — no disconnect under
// backpressure even if the head happens to sit at height cached+1 by coincidence.
func TestCheckHeadStall_NotPolledUsesSuppression(t *testing.T) {
	const headHeight = 100
	sm, peerA, _, _ := buildHeadStallManager(t, headHeight)

	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(false) // poller has not run yet

	sm.checkHeadStall(time.Now())

	require.Never(t, func() bool { return !peerA.Connected() }, 300*time.Millisecond, 20*time.Millisecond,
		"without a polled BA height the carve-out must not fire; suppression holds")
}
