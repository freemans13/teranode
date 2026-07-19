// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// PREVENTION tests for the frontier-orphan wedge (live mainnet 2026-07-19). The
// recovery scan (reconcileFrontierGap) is a safety net; these lock in the fix at
// SOURCE so the orphan never forms.
//
// The single-peer fetchHeaderBlocks walk advances the monotonic startHeader cursor
// past every walked hash BEFORE it sends the getdata. Two paths used to strand a
// hash below that cursor where the forward walk can never re-reach it:
//
//  1. SUCCESSFUL send recorded the request only in requestedBlocks (60s TTL), never
//     in assignedTo. Both timeout scans (reconcileLostAssignments, checkHeadStall)
//     iterate assignedTo only, so once the TTL lapsed the block was tracked in NO
//     ledger — a ledgerless orphan pinning the strictly-ascending committed tip.
//     FIX: record assignedTo/assignedAt too, symmetric with assignBlocksAcrossPeers.
//
//  2. DROPPED send (full output queue) returned after the cursor had advanced,
//     enqueuing nothing. FIX: re-queue the dropped hashes to refetchBlocks so the
//     next pass's re-fetch drain re-requests them, mirroring assignBlocksAcrossPeers.

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

// blockNotFoundClient returns a blockchain mock whose GetBlockHeader always reports
// "not found", so haveInventory reports false and every walked header is fetchable.
func blockNotFoundClient() *blockchain2.Mock {
	c := &blockchain2.Mock{}
	c.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()
	return c
}

// TestFetchHeaderBlocks_SuccessfulSendRecordsAssignedTo: the primary prevention.
// After a successful getdata the fetched hashes must appear in assignedTo (mapped to
// the sync peer) and assignedAt, so the timeout scans can recover a block whose
// requestedBlocks TTL lapses before it arrives. Previously they landed ONLY in
// requestedBlocks and became ledgerless orphans on TTL expiry.
func TestFetchHeaderBlocks_SuccessfulSendRecordsAssignedTo(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xc0}
	const runway = 30
	_, hashes := buildHeaderChain(base, runway, 9000)

	var (
		mu  sync.Mutex
		got []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 50, &mu, &got)

	syncState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 8

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		blockchainClient:  blockNotFoundClient(),
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		hc := hashes[i]
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hc})
	}
	sm.startHeader = sm.headerList.Front().Next()

	sm.fetchHeaderBlocks()

	sm.assignedMu.Lock()
	defer sm.assignedMu.Unlock()

	require.NotEmpty(t, sm.assignedTo,
		"a successful single-peer getdata must record assignedTo so the timeout scans can recover it")
	require.Equal(t, len(sm.assignedTo), len(sm.assignedAt),
		"assignedTo and assignedAt must stay in lock-step")
	for h, p := range sm.assignedTo {
		require.Same(t, syncPeer, p, "every assigned block must map to the sync peer that fetched it")
		_, hasTime := sm.assignedAt[h]
		require.True(t, hasTime, "every assignedTo entry must have an assignedAt timestamp")
		_, inFlight := sm.requestedBlocks.Get(h)
		require.True(t, inFlight, "a sent block must also be in the global requestedBlocks ledger")
	}
}

// TestFetchHeaderBlocks_DroppedSendReQueuesRefetch: the second prevention path. When
// the output queue is full the getdata is dropped, but the cursor has already moved
// past the hashes — they must be re-queued to refetchBlocks (not discarded), and must
// NOT pollute the in-flight ledgers (no phantom entries).
func TestFetchHeaderBlocks_DroppedSendReQueuesRefetch(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd0}
	const runway = 30
	_, hashes := buildHeaderChain(base, runway, 9000)

	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 8

	// Connected peer with a full output queue and no draining goroutine: every
	// TryQueueMessage observes a full queue and returns false.
	syncPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &syncPeerCfg, "10.7.7.7:8333")
	require.NoError(t, err)
	syncPeer.TstMarkConnected()
	syncPeer.TstFillOutputQueue()

	syncState := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		blockchainClient:  blockNotFoundClient(),
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		hc := hashes[i]
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hc})
	}
	sm.startHeader = sm.headerList.Front().Next()

	sm.fetchHeaderBlocks()

	sm.assignedMu.Lock()
	defer sm.assignedMu.Unlock()

	require.NotEmpty(t, sm.refetchBlocks,
		"a dropped single-peer getdata must re-queue its hashes to refetchBlocks, not discard them")
	require.Empty(t, sm.assignedTo, "a dropped getdata must leave no assignedTo entry")
	require.Empty(t, sm.assignedAt, "a dropped getdata must leave no assignedAt entry")
	for h := range sm.refetchBlocks {
		_, inFlight := sm.requestedBlocks.Get(h)
		require.False(t, inFlight, "a dropped getdata must leave no phantom requestedBlocks entry")
	}
}
