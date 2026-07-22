// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for topUpBlockFetch — the per-block top-up helper that routes through
// assignBlocksAcrossPeers when ParallelFetchPeers > 1 and falls back to
// fetchHeaderBlocks when the flag is off (byte-identical to today).
//
// RED test: before topUpBlockFetch is added, the three call sites still call
// fetchHeaderBlocks directly, so only the sync peer receives blocks regardless
// of ParallelFetchPeers — the test fails. GREEN after the helper replaces them.
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

// TestTopUpBlockFetch_MultiPeerDistributes is the RED/GREEN lock-in for the
// topUpBlockFetch helper. With ParallelFetchPeers=3 and 3 eligible peers,
// a single topUpBlockFetch() call MUST distribute blocks disjointly across all
// three peers — NOT exclusively to the sync peer as fetchHeaderBlocks would.
//
// Before the fix (RED): the three per-block top-up sites call fetchHeaderBlocks
// directly, so only the sync peer is fed even when the flag is on.
// After the fix (GREEN): topUpBlockFetch routes to assignBlocksAcrossPeers
// when ParallelFetchPeers > 1 and all three peers receive disjoint blocks.
func TestTopUpBlockFetch_MultiPeerDistributes(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	// Runway of 15 fetchable blocks; budget=9 (< 3*K=15), budget binds.
	base := chainhash.Hash{0xd0}
	const runway = 15
	_, hashes := buildHeaderChain(base, runway, 8000)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 50, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 51, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 52, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 5 // K
	tSettings.Legacy.BlockDownloadWindow = 9       // Budget (< 3*K=15, so budget binds)

	syncState := newEligiblePeerState()
	stateB := newEligiblePeerState()
	stateC := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()
	defer stateB.requestedBlocks.Stop()
	defer stateB.requestedTxns.Stop()
	defer stateC.requestedBlocks.Stop()
	defer stateC.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Seed the header list.
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next()

	// Confirm the budget constraint holds: budget = BlockDownloadWindow = 9 (the
	// memory-scaled byte cap is retired; K=5 across 3 peers gives 15, so
	// BlockDownloadWindow binds first).
	budget := tSettings.Legacy.BlockDownloadWindow
	require.Equal(t, 9, budget, "precondition: budget must bind at 9")

	// --- call topUpBlockFetch (the helper this test exercises) ---
	sm.topUpBlockFetch()

	// Wait for all three peers' getdata messages to arrive.
	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()
		total := len(collectInvHashes(got1)) + len(collectInvHashes(got2)) + len(collectInvHashes(got3))
		return total >= budget
	}, 2*time.Second, 10*time.Millisecond, "topUpBlockFetch with flag>1: all three peers must share budget blocks")

	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	inv1 := collectInvHashes(got1)
	inv2 := collectInvHashes(got2)
	inv3 := collectInvHashes(got3)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	total := len(inv1) + len(inv2) + len(inv3)
	require.Equal(t, budget, total, "total assigned must equal budget")

	// Disjoint: no hash assigned to more than one peer.
	seen := make(map[chainhash.Hash]int)
	for _, h := range inv1 {
		seen[h]++
	}
	for _, h := range inv2 {
		seen[h]++
	}
	for _, h := range inv3 {
		seen[h]++
	}
	for h, n := range seen {
		require.Equal(t, 1, n, "hash %v assigned to %d peers, must be exactly 1 (disjoint)", h, n)
	}
	require.Equal(t, budget, len(seen), "union of assigned hashes must equal budget with no overlap")

	// At least TWO distinct peers must have received blocks: single-peer
	// fetchHeaderBlocks would give everything to the sync peer alone, so if
	// only one peer has blocks the old (unfixed) path is still active.
	nonEmpty := 0
	if len(inv1) > 0 {
		nonEmpty++
	}
	if len(inv2) > 0 {
		nonEmpty++
	}
	if len(inv3) > 0 {
		nonEmpty++
	}
	require.GreaterOrEqual(t, nonEmpty, 2,
		"topUpBlockFetch with flag>1: blocks must be distributed to at least 2 peers (got only 1 — single-peer path still active)")
}

// TestTopUpBlockFetch_FlagOffSinglePeer asserts that with ParallelFetchPeers=1
// topUpBlockFetch is byte-identical to fetchHeaderBlocks: only the sync peer
// receives a getdata, no other peer is touched.
func TestTopUpBlockFetch_FlagOffSinglePeer(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xe0}
	const runway = 5
	_, hashes := buildHeaderChain(base, runway, 7000)

	var (
		muS, muO   sync.Mutex
		gotS, gotO []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 53, &muS, &gotS)
	otherPeer := captureGetDataPeer(t, &chainParams, 54, &muO, &gotO)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1 // flag OFF

	syncState := newEligiblePeerState()
	otherState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()
	defer otherState.requestedBlocks.Stop()
	defer otherState.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(otherPeer, otherState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// The frontier walk skips headerListSeed, so mark the dummy as the seed.
	sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next()

	sm.topUpBlockFetch()

	// Sync peer must get exactly the runway blocks.
	require.Eventually(t, func() bool {
		muS.Lock()
		defer muS.Unlock()
		return len(collectInvHashes(gotS)) == runway
	}, 2*time.Second, 10*time.Millisecond, "flag-off: sync peer must receive the runway getdata")

	// Other peer must receive nothing.
	time.Sleep(200 * time.Millisecond)
	muO.Lock()
	require.Zero(t, len(collectInvHashes(gotO)), "flag-off: no non-sync peer may receive a getdata")
	muO.Unlock()

	require.Zero(t, otherState.requestedBlocks.Len(), "flag-off: other peer requestedBlocks must stay empty")
	require.Equal(t, runway, syncState.requestedBlocks.Len(), "flag-off: sync peer must hold the whole runway")
}
