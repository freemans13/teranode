// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the svnode-aligned fetch scheduler (legacy_svnodeAlignedFetch).
//
// They reproduce the LIVE mainnet big-block IBD stall (height ~701475): with big
// (~100MB) blocks the memory-scaled in-flight budget (calculateMaxInFlightBlocks)
// lets ONE sync peer hold dozens-to-~100 blocks in flight, far ahead of the
// committed frontier, so the next-needed block (committed+1) gets ~1/100 of the
// pipe and the tip freezes-then-bursts. The aligned gate replaces that with
// svnode's fixed MAX_BLOCKS_IN_TRANSIT_PER_PEER = 16 count, size-blind, filled
// lowest-height-first from the download frontier — so committed+1 is always
// 1-of-16 and never starved.
//
// The tests drive the real fetch entry points (maintainInFlightWindow for the
// single-peer live path, assignBlocksAcrossPeers for the multi-peer path) against
// the in-memory peer/blockchain-mock infrastructure shared with assign_blocks_test.go.

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
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// bigBlockTracker returns a blockSizeTracker whose rolling average is one ~100MB
// block, so calculateMaxInFlightBlocks (the gate-OFF governor) computes a LARGE
// memory budget: byteBound = 30×100MB / 100MB = 30 in flight. This is the exact
// shape of the live stall — a memory-scaled ceiling far above svnode's fixed 16.
func bigBlockTracker() *blockSizeTracker {
	const oneHundredMB = int64(100 * 1024 * 1024)
	bst := newBlockSizeTrackerWithBudgets(10, 50000, 30*oneHundredMB)
	bst.addBlockStats(oneHundredMB, 1) // avgSize=100MB, avgTxCount=1 => byteBound=30
	return bst
}

// seedHeaderList pushes a leading seed node (base, never fetched) followed by
// `hashes` as fetchable header nodes, and returns the fetchable elements. It sets
// headerListSeed to the leading node so the download-frontier anchor skips it.
func seedHeaderList(sm *SyncManager, base chainhash.Hash, hashes []chainhash.Hash) []*list.Element {
	seed := sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	sm.headerListSeed = seed

	elems := make([]*list.Element, len(hashes))
	for i := range hashes {
		elems[i] = sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]}) //nolint:gosec
	}

	return elems
}

// requestedHashes returns the set of hashes currently recorded in a peer's
// in-flight ledger.
func requestedHashes(state *peerSyncState, all []chainhash.Hash) map[chainhash.Hash]bool {
	out := make(map[chainhash.Hash]bool)
	for _, h := range all {
		if _, ok := state.requestedBlocks.Get(h); ok {
			out[h] = true
		}
	}

	return out
}

// TestSvnodeAligned_SinglePeer_GateOff_OverfetchesBigBlocks is the
// characterisation of the live stall: with big blocks and the gate OFF, the
// single sync peer holds the memory-scaled 30 blocks in flight — far above 16.
func TestSvnodeAligned_SinglePeer_GateOff_OverfetchesBigBlocks(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd0}
	const runway = 40
	_, hashes := buildHeaderChain(base, runway, 20000)

	var mu sync.Mutex
	var got []*wire.MsgGetData
	syncPeer := captureGetDataPeer(t, &chainParams, 50, &mu, &got)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1
	tSettings.Legacy.SvnodeAlignedFetch = false // gate OFF: today's behaviour
	tSettings.Legacy.FrontierLogInterval = 0

	syncState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: bigBlockTracker(),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	seedHeaderList(sm, base, hashes)
	sm.startHeader = sm.headerList.Front().Next() // gate-off uses the monotonic cursor

	sm.maintainInFlightWindow()

	require.Equal(t, 30, syncState.requestedBlocks.Len(),
		"gate OFF: the memory-scaled budget lets the single peer hold 30 (>16) big blocks in flight — the far-ahead over-fetch")
	require.Greater(t, syncState.requestedBlocks.Len(), tSettings.Legacy.MaxBlocksInTransitPerPeer,
		"gate OFF characterisation: in-flight must climb far above the svnode cap of 16")
}

// TestSvnodeAligned_SinglePeer_CapsAt16_LowestFirst asserts the primary fix: with
// the gate ON and the SAME big blocks, the single peer holds exactly 16 blocks in
// flight (size-blind), and they are the LOWEST 16 starting at the frontier — so
// committed+1 is always in the request set and block 16 (far ahead) is not.
func TestSvnodeAligned_SinglePeer_CapsAt16_LowestFirst(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd1}
	const runway = 40
	_, hashes := buildHeaderChain(base, runway, 21000)

	var mu sync.Mutex
	var got []*wire.MsgGetData
	syncPeer := captureGetDataPeer(t, &chainParams, 51, &mu, &got)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1
	tSettings.Legacy.SvnodeAlignedFetch = true // gate ON
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.FrontierLogInterval = 0

	syncState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: bigBlockTracker(), // big blocks: proves the cap is size-blind
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	seedHeaderList(sm, base, hashes)

	sm.maintainInFlightWindow()

	require.Equal(t, 16, syncState.requestedBlocks.Len(),
		"gate ON: fixed per-peer cap of 16 regardless of the 100MB block size")

	req := requestedHashes(syncState, hashes)
	for i := 0; i < 16; i++ {
		require.True(t, req[hashes[i]], "the lowest 16 blocks must be requested; block %d (committed+%d) missing", i, i+1)
	}
	require.True(t, req[hashes[0]], "committed+1 (the frontier block) must ALWAYS be in the request set")
	require.False(t, req[hashes[16]], "block 16 (17th, far ahead) must NOT be requested — the far-ahead pile never forms")
}

// TestSvnodeAligned_SinglePeer_FrontierAnchorSlides_NoClimb proves the frontier
// anchor (root cause #2/#3): the walk cursor never climbs above ~16 over the
// frontier, and as the frontier block commits the window slides forward by
// exactly the contiguous run — committed+1 stays in the set every tick, with no
// refetch churn.
func TestSvnodeAligned_SinglePeer_FrontierAnchorSlides_NoClimb(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd2}
	const runway = 40
	_, hashes := buildHeaderChain(base, runway, 22000)

	var mu sync.Mutex
	var got []*wire.MsgGetData
	syncPeer := captureGetDataPeer(t, &chainParams, 52, &mu, &got)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1
	tSettings.Legacy.SvnodeAlignedFetch = true
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.FrontierLogInterval = 0

	syncState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: bigBlockTracker(),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	elems := seedHeaderList(sm, base, hashes)

	// Tick 1: fill the window with the lowest 16.
	sm.maintainInFlightWindow()
	require.Equal(t, 16, syncState.requestedBlocks.Len())
	require.True(t, requestedHashes(syncState, hashes)[hashes[0]], "tick 1: committed+1 must be requested")

	// Re-tick WITHOUT delivering anything: the cap holds, the cursor does not
	// climb, no far-ahead block is pulled, and nothing is queued for re-fetch.
	sm.maintainInFlightWindow()
	require.Equal(t, 16, syncState.requestedBlocks.Len(), "no delivery: window stays at 16, cursor does not climb")
	require.False(t, requestedHashes(syncState, hashes)[hashes[16]], "no delivery: no far-ahead block is pulled")
	require.Empty(t, sm.refetchBlocks, "no delivery: nothing is churned into re-fetch")

	// Simulate committed+1 (hashes[0]) committing: clear its in-flight entry and
	// trim its header node (the real arrive-at-front trim). The download frontier
	// advances to hashes[1].
	syncState.requestedBlocks.Delete(hashes[0])
	sm.requestedBlocks.Delete(hashes[0])
	sm.headerList.Remove(elems[0])

	// Tick 3: the freed slot is refilled from the NEW frontier — exactly one new
	// block (hashes[16]) enters, the new committed+1 (hashes[1]) is still in flight,
	// and the window has slid forward by exactly the committed run with no churn.
	sm.maintainInFlightWindow()
	require.Equal(t, 16, syncState.requestedBlocks.Len(), "window refills to exactly 16 after the frontier advances")

	req := requestedHashes(syncState, hashes)
	require.True(t, req[hashes[1]], "the new committed+1 (hashes[1]) must still be in the request set")
	require.True(t, req[hashes[16]], "the freed slot is refilled from the new frontier (hashes[16])")
	require.False(t, req[hashes[17]], "the cursor slides by exactly one — hashes[17] must NOT be pulled")
	require.Empty(t, sm.refetchBlocks, "a clean frontier advance produces no re-fetch churn")
}

// TestSvnodeAligned_MultiPeer_SizeBlind16xPeers asserts the multi-peer variant:
// with ParallelFetchPeers=3 and big blocks, total in-flight = 16 × downloading
// peers = 48, size-blind (BlockDownloadWindow stays a loose ceiling, the byte cap
// is dropped), and each peer holds no more than 16 — the lowest heights first.
func TestSvnodeAligned_MultiPeer_SizeBlind16xPeers(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd3}
	const runway = 80
	_, hashes := buildHeaderChain(base, runway, 23000)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 53, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 54, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 55, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.SvnodeAlignedFetch = true
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.BlockDownloadWindow = 1024 // loose ceiling: must NOT bind
	tSettings.Legacy.FrontierLogInterval = 0

	syncState := newEligiblePeerState()
	stateB := newEligiblePeerState()
	stateC := newEligiblePeerState()
	for _, st := range []*peerSyncState{syncState, stateB, stateC} {
		defer st.requestedBlocks.Stop()
		defer st.requestedTxns.Stop()
	}

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:       make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:    make(map[chainhash.Hash]struct{}),
		headerList:       list.New(),
		blockSizeTracker: bigBlockTracker(), // big blocks: proves 48 total is size-blind
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	seedHeaderList(sm, base, hashes)

	sm.assignBlocksAcrossPeers()

	total := syncState.requestedBlocks.Len() + stateB.requestedBlocks.Len() + stateC.requestedBlocks.Len()
	require.Equal(t, 48, total,
		"total in-flight = 16 × 3 downloading peers = 48, size-blind (the byte cap is dropped, BlockDownloadWindow=1024 does not bind)")

	require.LessOrEqual(t, syncState.requestedBlocks.Len(), 16, "per-peer cap of 16 must hold")
	require.LessOrEqual(t, stateB.requestedBlocks.Len(), 16, "per-peer cap of 16 must hold")
	require.LessOrEqual(t, stateC.requestedBlocks.Len(), 16, "per-peer cap of 16 must hold")

	// The 48 assigned blocks must be the LOWEST 48 (frontier-first, disjoint).
	assigned := make(map[chainhash.Hash]int)
	for _, st := range []*peerSyncState{syncState, stateB, stateC} {
		for _, h := range hashes {
			if _, ok := st.requestedBlocks.Get(h); ok {
				assigned[h]++
			}
		}
	}
	for i := 0; i < 48; i++ {
		require.Equal(t, 1, assigned[hashes[i]], "the lowest 48 blocks must each be assigned exactly once (frontier-first, disjoint)")
	}
	require.False(t, assigned[hashes[48]] > 0, "no block beyond the lowest 48 may be assigned")
}
