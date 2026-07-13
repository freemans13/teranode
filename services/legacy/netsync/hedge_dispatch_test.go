// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// TestHedgeDispatch_ParallelFetchPeers2_HedgesOutstandingHashesToSecondPeer is the
// RED/GREEN lock-in test for Task 1.3 (hedge dispatch). With ParallelFetchPeers=2,
// when the sync peer already has N blocks outstanding in its requestedBlocks,
// maintainInFlightWindow must ALSO send a getdata for those SAME hashes to one
// other eligible peer (syncCandidate=true, connected), and record them into that
// peer's requestedBlocks BEFORE the send. startHeader must be untouched (the hedge
// must not advance the shared header runway).
//
// With ParallelFetchPeers=1 (flag-off), the second peer receives nothing and its
// requestedBlocks stays empty — byte-identical to pre-feature behaviour.
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

// TestHedgeDispatch_ParallelFetchPeers2 drives maintainInFlightWindow with two
// peers and ParallelFetchPeers=2. The sync peer already has N blocks in its
// requestedBlocks at or above the dynamic cap, so fetchHeaderBlocks is SKIPPED
// this tick. The hedge must:
//   - snapshot the sync peer's outstanding requestedBlocks (N hashes)
//   - send a getdata for those hashes to the second eligible peer
//   - record all N hashes into the second peer's requestedBlocks
//   - leave startHeader unchanged (hedge does not advance the header runway)
func TestHedgeDispatch_ParallelFetchPeers2(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	// Build hashes to use as outstanding block hashes on the sync peer.
	base := chainhash.Hash{0xa1}
	_, hashes := buildHeaderChain(base, 3, 6001)
	outstandingHashes := hashes // all 3 are "already outstanding" on sync peer

	// --- sync peer: has a connected counterpart (not needed for the hedge path,
	// but MakeConnectedPeers requires it; OnGetData is a no-op) ---
	syncCpCfg := peer.Config{
		Listeners:        peer.MessageListeners{OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {}},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, syncPeer, err := MakeConnectedPeers(t, syncCpCfg, syncPeerCfg, 20)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	// --- second peer: capture getdata messages it receives ---
	var (
		hedgeMu      sync.Mutex
		hedgeGetData []*wire.MsgGetData
	)
	secondCpCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, msg *wire.MsgGetData) {
				hedgeMu.Lock()
				hedgeGetData = append(hedgeGetData, msg)
				hedgeMu.Unlock()
			},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	secondPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, secondPeer, err := MakeConnectedPeers(t, secondCpCfg, secondPeerCfg, 21)
	require.NoError(t, err)
	defer secondPeer.DisconnectWithInfo("test done")

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 2

	// dynamicMax with no samples = noSampleInFlightDefault = 20.
	// Seed the sync peer's requestedBlocks to exactly the cap so fetchHeaderBlocks
	// is SKIPPED this tick (peerState.requestedBlocks.Len() >= dynamicMax).
	// This isolates the hedge path cleanly.
	dynamicCap := newBlockSizeTracker(20).calculateMaxInFlightBlocks() // = 20

	syncState := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer syncState.requestedTxns.Stop()
	defer syncState.requestedBlocks.Stop()

	// Fill the sync peer's requestedBlocks to exactly the cap with our 3 real
	// outstanding hashes plus padding hashes to reach the cap.
	for _, h := range outstandingHashes {
		syncState.requestedBlocks.Set(h, struct{}{})
	}
	padBase := chainhash.Hash{0xff}
	for i := len(outstandingHashes); i < dynamicCap; i++ {
		padHash := padBase
		padHash[0] = byte(i) //nolint:gosec
		syncState.requestedBlocks.Set(padHash, struct{}{})
	}
	require.Equal(t, dynamicCap, syncState.requestedBlocks.Len(), "precondition: sync peer at cap")

	// Second-peer state: nothing in-flight.
	secondState := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer secondState.requestedTxns.Stop()
	defer secondState.requestedBlocks.Stop()

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
	sm.peerStates.Set(secondPeer, secondState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Seed the header list with a dummy + 1 runway node so hasRunway=true
	// (maintainInFlightWindow returns early if runway == nil).
	runwayHash := chainhash.Hash{0xee}
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	sm.headerList.PushBack(&headerNode{height: 1, hash: &runwayHash})
	startEl := sm.headerList.Front().Next() // points to runwayHash node
	sm.startHeader = startEl

	// Fire maintainInFlightWindow. The sync peer is at cap so fetchHeaderBlocks
	// is skipped; the hedge path must still fire.
	sm.maintainInFlightWindow()

	// startHeader must be unchanged: the hedge must not advance the runway.
	sm.headerMu.Lock()
	startAfter := sm.startHeader
	sm.headerMu.Unlock()
	require.Same(t, startEl, startAfter,
		"startHeader must be unchanged by the hedge (hedge must not advance the header runway)")

	// Give the network a moment to deliver the getdata to the second peer.
	require.Eventually(t, func() bool {
		hedgeMu.Lock()
		defer hedgeMu.Unlock()
		return len(hedgeGetData) > 0
	}, time.Second, 10*time.Millisecond,
		"second peer must receive a getdata (hedge) from maintainInFlightWindow with ParallelFetchPeers=2")

	hedgeMu.Lock()
	captured := hedgeGetData[0]
	hedgeMu.Unlock()

	// The hedge getdata must carry all hashes from the sync peer's requestedBlocks.
	require.Equal(t, dynamicCap, len(captured.InvList),
		"hedge getdata must contain all outstanding hashes from the sync peer requestedBlocks")

	for _, inv := range captured.InvList {
		require.Equal(t, wire.InvTypeBlock, inv.Type, "hedge inv must be InvTypeBlock")
	}

	// The 3 real outstanding hashes must appear in the hedge.
	hedgedHashes := make(map[chainhash.Hash]bool)
	for _, inv := range captured.InvList {
		hedgedHashes[inv.Hash] = true
	}
	for _, h := range outstandingHashes {
		require.True(t, hedgedHashes[h], "outstanding hash %v must appear in the hedge getdata", h)
	}

	// All outstanding hashes must be recorded in secondPeer's requestedBlocks.
	require.Equal(t, dynamicCap, secondState.requestedBlocks.Len(),
		"all outstanding hashes must be recorded in second peer requestedBlocks")
	for _, h := range outstandingHashes {
		_, ok := secondState.requestedBlocks.Get(h)
		require.True(t, ok, "outstanding hash %v must be in second peer requestedBlocks", h)
	}
}

// TestHedgeDispatch_FlagOff_ParallelFetchPeers1_NoHedge asserts that with
// ParallelFetchPeers=1 (flag-off) the second peer receives no getdata at all
// and its requestedBlocks stays empty — byte-identical to pre-feature behaviour.
func TestHedgeDispatch_FlagOff_ParallelFetchPeers1_NoHedge(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xa2}
	_, hashes := buildHeaderChain(base, 2, 6002)
	outstandingHashes := hashes

	syncCpCfg := peer.Config{
		Listeners:        peer.MessageListeners{OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {}},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, syncPeer, err := MakeConnectedPeers(t, syncCpCfg, syncPeerCfg, 22)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	secondGetData := make(chan struct{}, 4)
	secondCpCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {
				select {
				case secondGetData <- struct{}{}:
				default:
				}
			},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	secondPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, secondPeer, err := MakeConnectedPeers(t, secondCpCfg, secondPeerCfg, 23)
	require.NoError(t, err)
	defer secondPeer.DisconnectWithInfo("test done")

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1 // flag OFF

	dynamicCap := newBlockSizeTracker(20).calculateMaxInFlightBlocks()

	syncState := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer syncState.requestedTxns.Stop()
	defer syncState.requestedBlocks.Stop()
	for _, h := range outstandingHashes {
		syncState.requestedBlocks.Set(h, struct{}{})
	}
	padBase := chainhash.Hash{0xfe}
	for i := len(outstandingHashes); i < dynamicCap; i++ {
		padHash := padBase
		padHash[0] = byte(i) //nolint:gosec
		syncState.requestedBlocks.Set(padHash, struct{}{})
	}

	secondState := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer secondState.requestedTxns.Stop()
	defer secondState.requestedBlocks.Stop()

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
	sm.peerStates.Set(secondPeer, secondState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	runwayHash := chainhash.Hash{0xed}
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	sm.headerList.PushBack(&headerNode{height: 1, hash: &runwayHash})
	sm.startHeader = sm.headerList.Front().Next()

	sm.maintainInFlightWindow()

	// Flag off: second peer must receive NO getdata.
	select {
	case <-secondGetData:
		t.Fatal("second peer received a getdata with ParallelFetchPeers=1 (flag-off must suppress hedge)")
	case <-time.After(200 * time.Millisecond):
		// Correct: no hedge dispatched.
	}

	// Second peer's requestedBlocks must remain empty.
	require.Zero(t, secondState.requestedBlocks.Len(),
		"second peer requestedBlocks must stay empty when ParallelFetchPeers=1")
}
