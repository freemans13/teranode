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

// TestFetchHeaderBlocks_SuccessfulSendStaysOutOfAssignedTo: the single-peer path
// must record the in-flight block in requestedBlocks but must NOT record it in
// assignedTo. checkHeadStall scans assignedTo and disconnects the peer holding the
// frontier block if it is not delivered within its aggressive 2s timeout; the
// single-peer path fetches the frontier during cold start when the first block
// legitimately takes longer than 2s, so recording it in assignedTo livelocked IBD
// at the first block (mainnet 2026-07-20). Recovery of a frontier lost on this path
// belongs to reconcileFrontierGap (keyed off the committed-tip frontier), not the
// assignedTo timeout scans. This test locks that separation in.
func TestFetchHeaderBlocks_SuccessfulSendStaysOutOfAssignedTo(t *testing.T) {
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
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// The frontier walk skips headerListSeed, so mark the dummy as the seed.
	sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		hc := hashes[i]
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hc})
	}
	sm.startHeader = sm.headerList.Front().Next()

	sm.fetchHeaderBlocks()

	sm.assignedMu.Lock()
	defer sm.assignedMu.Unlock()

	require.Empty(t, sm.assignedTo,
		"the single-peer path must NOT record assignedTo — that feeds checkHeadStall's 2s frontier fast-swap and livelocks cold-start IBD")

	// A successful single-peer getdata must record the sent blocks in the in-flight
	// ledger. The walk no longer advances a startHeader cursor — it re-anchors on the
	// download frontier each pass — so count the recorded in-flight blocks directly.
	require.Positive(t, sm.requestedBlocks.Len(),
		"a successful single-peer getdata must record the sent blocks in requestedBlocks (in-flight ledger)")
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
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
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
	for h := range sm.refetchBlocks {
		_, inFlight := sm.requestedBlocks.Get(h)
		require.False(t, inFlight, "a dropped getdata must leave no phantom requestedBlocks entry")
	}
}
