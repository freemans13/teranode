// Copyright (c) 2026 The Teranode developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

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

// TestAssignBlocksAcrossPeers_SingleSparePeerStillDrainsRefetch reproduces the
// exact wedge state behind the 2026-07-24 "restart -> flurry -> stall" livelock
// and locks in the svnode-aligned fix.
//
// The state: the SYNC peer is at its MaxBlocksInTransitPerPeer cap (legitimately
// mid-transfer on fat blocks), exactly ONE other peer has spare capacity, and an
// orphaned frontier block sits in refetchBlocks (freed by checkHeadStall or a
// dropped send). Before the fix, assignBlocksAcrossPeers took the len(targets)<2
// fallback into the sync-peer-only fetchHeaderBlocks(), which never touches the
// refetch queue — so the orphan had NO surviving re-request path and the commit
// pipeline stalled behind it until a process restart. svnode cannot express this
// state: its want-list is re-derived for every peer with a free slot
// (FindNextBlocksToDownload, net_processing.cpp:335). After the fix, a single
// spare target runs the full pass: orphan drain first, then the frontier walk.
func TestAssignBlocksAcrossPeers_SingleSparePeerStillDrainsRefetch(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xa1}
	const runway = 12
	_, hashes := buildHeaderChain(base, runway, 7000)

	// The orphaned frontier block: lowest height, present in refetchBlocks and the
	// header height index, absent from every in-flight ledger.
	orphan := hashes[0]

	var (
		mu  sync.Mutex
		got []*wire.MsgGetData
	)

	// Sync peer: connected but AT its per-peer cap (no spare) — must contribute no
	// target. It must NOT receive the orphan.
	syncPeer := captureGetDataPeer(t, &chainParams, 60, &mu, &got)
	syncState := newEligiblePeerState()

	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()

	// The single OTHER eligible peer with spare capacity — the one svnode would
	// hand the orphan to, and the one the old fallback ignored.
	var spareGot []*wire.MsgGetData

	sparePeer := captureGetDataPeer(t, &chainParams, 61, &mu, &spareGot)
	spareState := newEligiblePeerState()

	defer spareState.requestedBlocks.Stop()
	defer spareState.requestedTxns.Stop()

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
	sm.peerStates.Set(sparePeer, spareState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Fill the sync peer's per-peer in-flight ledger to the cap so it has zero
	// spare — filler hashes distinct from the header chain.
	k := tSettings.Legacy.MaxBlocksInTransitPerPeer
	for i := 0; i < k; i++ {
		filler := chainhash.Hash{0xee, byte(i)}
		syncState.requestedBlocks.Set(filler, struct{}{})
	}

	// Header list: seed + the chain; heights indexed so the refetch drain can
	// height-sort the orphan first.
	sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		hc := hashes[i]
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hc})
		sm.headerHeightIndex[hc] = int32(i + 1)
	}
	sm.startHeader = sm.headerList.Front().Next()

	// The orphan: queued for refetch, in no in-flight ledger.
	sm.refetchBlocks[orphan] = struct{}{}

	sm.assignBlocksAcrossPeers()

	// The orphan must have been drained: recorded in the global and the SPARE
	// peer's in-flight ledgers, gone from refetchBlocks.
	_, inFlight := sm.requestedBlocks.Get(orphan)
	require.True(t, inFlight,
		"orphan must be re-requested (global in-flight ledger) even with a single spare target — the pre-fix fallback left it stranded forever")

	_, onSpare := spareState.requestedBlocks.Get(orphan)
	require.True(t, onSpare,
		"orphan must be assigned to the one spare peer (the sync peer is at cap)")

	_, onSync := syncState.requestedBlocks.Get(orphan)
	require.False(t, onSync, "orphan must not be assigned to the capped sync peer")

	sm.assignedMu.Lock()
	_, stillQueued := sm.refetchBlocks[orphan]
	sm.assignedMu.Unlock()
	require.False(t, stillQueued, "a drained orphan must leave the refetch queue")

	// And the getdata must actually have gone to the spare peer.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		for _, msg := range spareGot {
			for _, iv := range msg.InvList {
				if iv.Hash == orphan {
					return true
				}
			}
		}

		return false
	}, 5*time.Second, 50*time.Millisecond, "spare peer must receive a getdata containing the orphan")
}

// TestAssignBlocksAcrossPeers_ZeroSpareTargetsReturns locks the degenerate case:
// with NO peer holding spare capacity the pass must return without panicking and
// without touching the refetch queue (the orphan stays queued for the next tick,
// when checkHeadStall or an arrival frees capacity).
func TestAssignBlocksAcrossPeers_ZeroSpareTargetsReturns(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xb2}
	_, hashes := buildHeaderChain(base, 4, 7100)
	orphan := hashes[0]

	var (
		mu  sync.Mutex
		got []*wire.MsgGetData
	)

	syncPeer := captureGetDataPeer(t, &chainParams, 62, &mu, &got)
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
		refetchBlocks:     map[chainhash.Hash]struct{}{orphan: {}},
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	k := tSettings.Legacy.MaxBlocksInTransitPerPeer
	for i := 0; i < k; i++ {
		filler := chainhash.Hash{0xdd, byte(i)}
		syncState.requestedBlocks.Set(filler, struct{}{})
	}

	sm.assignBlocksAcrossPeers()

	sm.assignedMu.Lock()
	_, stillQueued := sm.refetchBlocks[orphan]
	sm.assignedMu.Unlock()
	require.True(t, stillQueued, "with zero spare targets the orphan must stay queued for the next tick — never dropped")

	_, inFlight := sm.requestedBlocks.Get(orphan)
	require.False(t, inFlight, "nothing can be in flight when no peer had spare capacity")
}

// TestCheckHeadStall_SolePeerNeverFires locks the sole-peer guard (adversarial
// review of the livelock fix, finding 1): with NO alternative eligible peer, a
// stale head must never be freed or its holder disconnected — the block can only
// be re-assigned back to the same peer (duplicate getdata churn at 2s, or a hard
// 30s disconnect/reconnect loop on a genuinely slow sole link). The stall
// response resumes as soon as a second eligible peer exists (asserted by
// TestCheckHeadStall_SuppressedWhileBackpressured, which now constructs one).
func TestCheckHeadStall_SolePeerNeverFires(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 8
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second

	holder, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{ChainParams: &chainParams}, "127.0.0.9:8333")
	require.NoError(t, err)

	h := chainhash.Hash{0xcc}

	sm := &SyncManager{
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		headerList:        list.New(),
		headerHeightIndex: map[chainhash.Hash]int32{h: 700000},
		assignedTo:        map[chainhash.Hash]map[*peer.Peer]time.Time{h: {holder: time.Now().Add(-time.Minute)}}, // stale
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}
	defer sm.requestedBlocks.Stop()

	// The holder is the ONLY peer (and not even an eligible fetch peer here —
	// peerStates empty mirrors the sole-upstream shape where nobody else exists).
	sm.checkHeadStall(time.Now())

	sm.assignedMu.Lock()
	_, still := sm.assignedTo[h]
	sm.assignedMu.Unlock()
	require.True(t, still,
		"a stale head must NOT be freed when no alternative eligible peer exists — freeing the sole peer only produces duplicate getdata churn or a 30s disconnect loop")
}
