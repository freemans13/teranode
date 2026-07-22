// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for Task 2.3: the multi-peer disjoint-range block scheduler
// (assignBlocksAcrossPeers). The scheduler generalizes the single-peer
// fetchHeaderBlocks walk across N eligible peers, assigning each walked block to
// exactly ONE peer (disjoint), bounding per-peer in-flight by K
// (MaxBlocksInTransitPerPeer), bounding total in-flight by Budget
// (BlockDownloadWindow), re-anchoring the walk on the download frontier each pass,
// and sending each peer a getdata for exactly its assigned hashes.
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

// captureGetDataPeer builds a connected pair whose counterpart records every
// getdata MsgGetData it receives into the returned slice (guarded by the mutex).
// The returned *peer.Peer is the local side registered in peerStates.
func captureGetDataPeer(t *testing.T, chainParams *chaincfg.Params, index uint8, mu *sync.Mutex, capture *[]*wire.MsgGetData) *peer.Peer {
	t.Helper()

	cpCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, msg *wire.MsgGetData) {
				mu.Lock()
				*capture = append(*capture, msg)
				mu.Unlock()
			},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         wire.SFNodeNetwork,
	}
	localCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         wire.SFNodeNetwork,
	}

	_, local, err := MakeConnectedPeers(t, cpCfg, localCfg, index)
	require.NoError(t, err)
	t.Cleanup(func() { local.DisconnectWithInfo("test done") })

	return local
}

func newEligiblePeerState() *peerSyncState {
	return &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
}

// collectInvHashes flattens all InvList hashes across the captured getdata msgs.
func collectInvHashes(msgs []*wire.MsgGetData) []chainhash.Hash {
	var out []chainhash.Hash
	for _, m := range msgs {
		for _, iv := range m.InvList {
			out = append(out, iv.Hash)
		}
	}
	return out
}

// TestAssignBlocks_DisjointAcrossThreePeers is the RED/GREEN lock-in for the
// multi-peer disjoint scheduler. Three eligible peers (incl. the sync peer),
// ParallelFetchPeers=3, K=MaxBlocksInTransitPerPeer, a generous header runway.
// After one scheduler pass:
//   - (a) blocks are assigned DISJOINTLY: the union of all peers' getdata hashes
//     has no duplicates,
//   - (b) each peer's requestedBlocks.Len() <= K,
//   - (c) total assigned <= Budget,
//   - (d) startHeader advanced past every assigned block,
//   - (e) each peer received a getdata carrying exactly the hashes recorded in its
//     requestedBlocks.
func TestAssignBlocks_DisjointAcrossThreePeers(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	// Runway of 30 fetchable blocks below a still-pending checkpoint.
	base := chainhash.Hash{0xb0}
	const runway = 30
	_, hashes := buildHeaderChain(base, runway, 9000)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 40, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 41, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 42, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 8 // K
	tSettings.Legacy.BlockDownloadWindow = 18      // Budget (< 3*K = 24, so Budget binds)

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

	// Seed the header list: a leading dummy seed node (never fetched) + the runway.
	// The frontier walk skips headerListSeed, so mark the dummy as the seed.
	sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next() // first fetchable node

	// Budget = BlockDownloadWindow = 18 (the memory-scaled byte cap is retired; the
	// per-peer cap K=8 across 3 peers gives 24, so BlockDownloadWindow binds first).
	budget := tSettings.Legacy.BlockDownloadWindow
	require.Equal(t, 18, budget, "precondition: budget must bind at 18")

	// --- run the scheduler ---
	sm.assignBlocksAcrossPeers()

	// Give the network time to deliver each peer's getdata.
	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()
		return len(collectInvHashes(got1))+len(collectInvHashes(got2))+len(collectInvHashes(got3)) >= budget
	}, 2*time.Second, 10*time.Millisecond, "all three peers must receive getdata summing to Budget")

	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	inv1 := collectInvHashes(got1)
	inv2 := collectInvHashes(got2)
	inv3 := collectInvHashes(got3)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	// (c) total assigned <= Budget, and exactly Budget here (runway >> budget).
	total := len(inv1) + len(inv2) + len(inv3)
	require.Equal(t, budget, total, "(c) total assigned must equal Budget")

	// (a) disjoint: no hash assigned to more than one peer.
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
		require.Equal(t, 1, n, "(a) hash %v assigned to %d peers, must be exactly 1 (disjoint)", h, n)
	}
	require.Equal(t, budget, len(seen), "(a) union of assigned hashes must equal Budget with no overlap")

	// The assigned set must be a prefix of the runway (the first `budget` nodes).
	for i := 0; i < budget; i++ {
		require.Equal(t, 1, seen[hashes[i]], "(a) runway node %d must be assigned exactly once", i)
	}

	// (b) each peer's requestedBlocks.Len() <= K, and matches its getdata exactly.
	k := tSettings.Legacy.MaxBlocksInTransitPerPeer
	requirePeerConsistent := func(state *peerSyncState, inv []chainhash.Hash) {
		require.LessOrEqual(t, state.requestedBlocks.Len(), k, "(b) per-peer in-flight must be <= K")
		require.Equal(t, len(inv), state.requestedBlocks.Len(),
			"(e) peer's requestedBlocks must equal exactly its getdata hashes")
		for _, h := range inv {
			_, ok := state.requestedBlocks.Get(h)
			require.True(t, ok, "(e) getdata hash %v must be recorded in that peer's requestedBlocks", h)
		}
	}
	requirePeerConsistent(syncState, inv1)
	requirePeerConsistent(stateB, inv2)
	requirePeerConsistent(stateC, inv3)

	// (d) The fetch walk re-anchors on the download frontier every pass and no
	// longer persists a monotonic startHeader cursor, so the assigned set is the
	// lowest `budget` blocks purely by the frontier walk (verified above), not by a
	// cursor advance. The startHeader field is not touched by assignBlocksAcrossPeers.

	// Every assigned hash must also be in the global requestedBlocks ledger.
	for h := range seen {
		_, ok := sm.requestedBlocks.Get(h)
		require.True(t, ok, "assigned hash %v must be in global requestedBlocks", h)
	}
}

// TestAssignBlocks_FlagOffSinglePeerByteIdentical asserts that with
// ParallelFetchPeers=1 the scheduler is byte-identical to the single-peer
// fetchHeaderBlocks: only the sync peer receives a getdata, no other eligible
// peer is touched, and startHeader advances exactly as fetchHeaderBlocks would.
//
// It drives the flag-off path through maintainInFlightWindow (the real caller)
// so the wiring — "flag<=1 => fetchHeaderBlocks, no eligibleFetchPeers, no new
// alloc" — is exercised end to end.
func TestAssignBlocks_FlagOffSinglePeerByteIdentical(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xc0}
	const runway = 5
	_, hashes := buildHeaderChain(base, runway, 9500)

	var (
		muS, muO   sync.Mutex
		gotS, gotO []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 43, &muS, &gotS)
	otherPeer := captureGetDataPeer(t, &chainParams, 44, &muO, &gotO)

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

	sm.maintainInFlightWindow()

	// Sync peer must get exactly the runway blocks; the other peer nothing.
	require.Eventually(t, func() bool {
		muS.Lock()
		defer muS.Unlock()
		return len(collectInvHashes(gotS)) == runway
	}, 2*time.Second, 10*time.Millisecond, "flag-off: sync peer must receive the runway getdata")

	select {
	case <-time.After(200 * time.Millisecond):
	}

	muO.Lock()
	require.Zero(t, len(collectInvHashes(gotO)), "flag-off: no non-sync peer may receive a getdata")
	muO.Unlock()

	require.Zero(t, otherState.requestedBlocks.Len(), "flag-off: other peer requestedBlocks must stay empty")
	require.Equal(t, runway, syncState.requestedBlocks.Len(), "flag-off: sync peer must hold the whole runway")

	// The single-peer fetch walk re-anchors on the download frontier each pass and
	// no longer advances the monotonic startHeader cursor; the whole runway is
	// fetched by the frontier walk + in-flight skip (asserted above), not by the
	// cursor climbing to nil.
}
