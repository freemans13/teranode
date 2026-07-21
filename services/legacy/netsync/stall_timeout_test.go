// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for Task 2.4: the head-of-line stalling-timeout (Bitcoin Core
// BLOCK_STALLING_TIMEOUT model). Under disjoint multi-peer download blocks
// commit in ascending height order, so if the peer assigned the NEXT-NEEDED
// (lowest-height outstanding) block stalls, the ordered commit stalls even
// though higher blocks have arrived. checkHeadStall detects the head block
// stalling past legacy_blockStallTimeout, disconnects ONLY that head peer, and
// frees its assignments (assignedTo/assignedAt/requestedBlocks) so the next
// scheduler tick reassigns them to other peers.
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

// assignForStall records a block as assigned to a peer exactly as
// assignBlocksAcrossPeers does after a successful getdata send: into the peer's
// requestedBlocks, the global ledger, and the two drain-owned tracking maps.
func assignForStall(sm *SyncManager, hash chainhash.Hash, height int32, p *peer.Peer, st *peerSyncState, at time.Time) {
	st.requestedBlocks.Set(hash, struct{}{})
	sm.requestedBlocks.Set(hash, struct{}{})
	sm.addAssignment(hash, p, at)
	sm.headerHeightIndex[hash] = height
}

// TestCheckHeadStall_DisconnectsStalledHeadPeerAndFreesAssignments is the core
// RED/GREEN lock-in. Peer A holds the HEAD (lowest-height) block plus another;
// peer B holds two higher blocks. The head was assigned in the distant past
// (well beyond BlockStallTimeout). One stall-check tick must:
//   - disconnect ONLY peer A (the head peer),
//   - free ALL of A's assignments (removed from assignedTo/assignedAt AND
//     requestedBlocks, both per-peer and global) so the next assign re-requests
//     them,
//   - leave peer B completely untouched (still connected, still holding its
//     blocks in every map).
func TestCheckHeadStall_DisconnectsStalledHeadPeerAndFreesAssignments(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	var (
		muA, muB sync.Mutex
		gotA     []*wire.MsgGetData
		gotB     []*wire.MsgGetData
	)
	peerA := captureGetDataPeer(t, &chainParams, 50, &muA, &gotA)
	peerB := captureGetDataPeer(t, &chainParams, 51, &muB, &gotB)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second
	// This test pins the IMMEDIATE-disconnect path (pre-F1 behaviour): 0 disables
	// the F1 re-fetch-race window so a stalled head peer is dropped at
	// BlockStallTimeout on the first tick, which is what it asserts below. The
	// race path (default 30s) is covered by headstall_race_test.go.
	tSettings.Legacy.HeadStallDisconnectTimeout = 0

	stateA := newEligiblePeerState()
	stateB := newEligiblePeerState()
	defer stateA.requestedBlocks.Stop()
	defer stateA.requestedTxns.Stop()
	defer stateB.requestedBlocks.Stop()
	defer stateB.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(peerA, stateA)
	sm.peerStates.Set(peerB, stateB)
	sm.headersFirstMode.Store(true)

	now := time.Now()
	// HEAD = lowest height (100), assigned to A long ago (stalled).
	head := chainhash.Hash{0x01}
	aOther := chainhash.Hash{0x02}
	bLow := chainhash.Hash{0x03}
	bHigh := chainhash.Hash{0x04}

	assignForStall(sm, head, 100, peerA, stateA, now.Add(-10*time.Second))
	assignForStall(sm, aOther, 103, peerA, stateA, now.Add(-10*time.Second))
	assignForStall(sm, bLow, 101, peerB, stateB, now.Add(-10*time.Second))
	assignForStall(sm, bHigh, 102, peerB, stateB, now.Add(-10*time.Second))

	require.True(t, peerA.Connected(), "precondition: peer A connected")
	require.True(t, peerB.Connected(), "precondition: peer B connected")

	// --- run the stall check ---
	sm.checkHeadStall(now)

	// Peer A (head peer) disconnected.
	require.Eventually(t, func() bool { return !peerA.Connected() }, 2*time.Second, 5*time.Millisecond,
		"head peer A past the timeout must be disconnected")

	// Peer B untouched.
	require.True(t, peerB.Connected(), "non-head peer B must NOT be disconnected")

	// ALL of A's assignments freed from every map.
	for _, h := range []chainhash.Hash{head, aOther} {
		_, inTo := sm.assignedTo[h]
		require.False(t, inTo, "A's block %v must be removed from assignedTo", h)
		_, inPeer := stateA.requestedBlocks.Get(h)
		require.False(t, inPeer, "A's block %v must be removed from A's requestedBlocks", h)
		_, inGlobal := sm.requestedBlocks.Get(h)
		require.False(t, inGlobal, "A's block %v must be removed from the global requestedBlocks", h)
	}

	// B's assignments untouched in every map.
	for _, h := range []chainhash.Hash{bLow, bHigh} {
		inner, inTo := sm.assignedTo[h]
		require.True(t, inTo, "B's block %v must remain in assignedTo", h)
		_, mapsToB := inner[peerB]
		require.True(t, mapsToB, "B's block %v must still map to peer B", h)
		_, inPeer := stateB.requestedBlocks.Get(h)
		require.True(t, inPeer, "B's block %v must remain in B's requestedBlocks", h)
		_, inGlobal := sm.requestedBlocks.Get(h)
		require.True(t, inGlobal, "B's block %v must remain in the global requestedBlocks", h)
	}
}

// TestCheckHeadStall_WithinTimeoutNoDisconnect asserts the honest case: the head
// block was assigned recently (within BlockStallTimeout), so NO peer is
// disconnected and NO assignment is freed.
func TestCheckHeadStall_WithinTimeoutNoDisconnect(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	var (
		muA, muB sync.Mutex
		gotA     []*wire.MsgGetData
		gotB     []*wire.MsgGetData
	)
	peerA := captureGetDataPeer(t, &chainParams, 52, &muA, &gotA)
	peerB := captureGetDataPeer(t, &chainParams, 53, &muB, &gotB)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second

	stateA := newEligiblePeerState()
	stateB := newEligiblePeerState()
	defer stateA.requestedBlocks.Stop()
	defer stateA.requestedTxns.Stop()
	defer stateB.requestedBlocks.Stop()
	defer stateB.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(peerA, stateA)
	sm.peerStates.Set(peerB, stateB)
	sm.headersFirstMode.Store(true)

	now := time.Now()
	head := chainhash.Hash{0x01}
	bHigh := chainhash.Hash{0x02}

	// Head assigned only 500ms ago — WITHIN the 2s timeout.
	assignForStall(sm, head, 100, peerA, stateA, now.Add(-500*time.Millisecond))
	assignForStall(sm, bHigh, 101, peerB, stateB, now.Add(-500*time.Millisecond))

	sm.checkHeadStall(now)

	// Give any (erroneous) disconnect time to propagate before asserting.
	time.Sleep(100 * time.Millisecond)

	require.True(t, peerA.Connected(), "head within timeout must NOT be disconnected")
	require.True(t, peerB.Connected(), "peer B must NOT be disconnected")

	_, inTo := sm.assignedTo[head]
	require.True(t, inTo, "head within timeout must remain assigned")
	_, inGlobal := sm.requestedBlocks.Get(head)
	require.True(t, inGlobal, "head within timeout must remain in global requestedBlocks")
}

// TestCheckHeadStall_FlagOffNoop asserts the single-peer path is unaffected:
// with ParallelFetchPeers <= 1 the stall check is a no-op even when the head is
// long past the timeout.
func TestCheckHeadStall_FlagOffNoop(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	var (
		muA  sync.Mutex
		gotA []*wire.MsgGetData
	)
	peerA := captureGetDataPeer(t, &chainParams, 54, &muA, &gotA)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1 // flag OFF
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second

	stateA := newEligiblePeerState()
	defer stateA.requestedBlocks.Stop()
	defer stateA.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(peerA, stateA)
	sm.headersFirstMode.Store(true)

	now := time.Now()
	head := chainhash.Hash{0x01}
	assignForStall(sm, head, 100, peerA, stateA, now.Add(-10*time.Second))

	sm.checkHeadStall(now)
	time.Sleep(100 * time.Millisecond)

	require.True(t, peerA.Connected(), "flag-off: stall check must be a no-op, peer stays connected")
	_, inTo := sm.assignedTo[head]
	require.True(t, inTo, "flag-off: assignments must be untouched")
}

// TestBlockStallTimeoutDefault verifies the setting default.
func TestBlockStallTimeoutDefault(t *testing.T) {
	s := test.CreateBaseTestSettings(t)
	require.Equal(t, 2*time.Second, s.Legacy.BlockStallTimeout)
}
