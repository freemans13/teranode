// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Regression tests for the headers-first FRONTIER-ORPHAN wedge (live mainnet
// 2026-07-19, tip frozen at 653070). The single-peer fetchHeaderBlocks path
// (fired at every checkpoint boundary even under ParallelFetchPeers>1) records a
// getdata ONLY in requestedBlocks (60s TTL), never in assignedTo, and on a
// dropped send enqueues nothing to refetchBlocks. The two recovery scans
// (reconcileLostAssignments, checkHeadStall) iterate assignedTo only — so a
// frontier block lost on that path is tracked in NO ledger, sits below the
// monotonic startHeader cursor, and is re-requestable only by the accidental
// cursor rewind a silent-peer rotation performs every ~3 min. reconcileFrontierGap
// closes the split-brain: it checks the actual frontier (headerList.Front(),
// skipping the seed) and re-enqueues it to refetchBlocks when it has stayed
// orphaned longer than BlockInFlightTimeout — DEBOUNCED so a block that is merely
// mid-flight and briefly untracked is never re-requested (the tick runs every
// ~20ms; re-requesting each tick would churn/log-storm).

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const frontierGapTestTimeout = 10 * time.Second

func newFrontierGapSM(t *testing.T, parallelPeers int) *SyncManager {
	t.Helper()
	chainParams := chaincfg.MainNetParams
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = parallelPeers
	tSettings.Legacy.BlockInFlightTimeout = frontierGapTestTimeout

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	return sm
}

// pushHeader appends a real (non-seed) header node to the list.
func pushHeader(sm *SyncManager, h chainhash.Hash, height int32) {
	hc := h
	sm.headerList.PushBack(&headerNode{height: height, hash: &hc})
}

// TestReconcileFrontierGap_LedgerlessFrontierRefetched: the exact wedge — the
// frontier is at Front() but tracked in NO ledger (dropped getdata / expired
// TTL). The first tick ARMS the debounce (no re-fetch); a tick after
// BlockInFlightTimeout re-enqueues it.
func TestReconcileFrontierGap_LedgerlessFrontierRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	base := time.Now()

	sm.reconcileFrontierGap(base)
	_, queuedEarly := sm.refetchBlocks[frontier]
	require.False(t, queuedEarly, "first tick only arms the debounce; no immediate re-fetch")

	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))
	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued,
		"a frontier orphaned longer than BlockInFlightTimeout must be re-enqueued")
}

// TestReconcileFrontierGap_DebounceHoldsForInFlight: an orphaned frontier that is
// still WITHIN BlockInFlightTimeout (merely mid-flight, briefly untracked) must
// NOT be re-requested — this is the no-storm guarantee.
func TestReconcileFrontierGap_DebounceHoldsForInFlight(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout / 2)) // still < timeout

	_, queued := sm.refetchBlocks[frontier]
	require.False(t, queued,
		"within BlockInFlightTimeout the frontier must NOT be re-requested (no per-tick storm)")
}

// TestReconcileFrontierGap_SkipsLeadingSeed: Front() is the committed-tip seed;
// the frontier is the next node. Seed skipped, real frontier recovered.
func TestReconcileFrontierGap_SkipsLeadingSeed(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	seed := chainhash.Hash{0xff}
	sc := seed
	seedEl := sm.headerList.PushBack(&headerNode{height: 653070, hash: &sc})
	sm.headerListSeed = seedEl

	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))

	_, seedQueued := sm.refetchBlocks[seed]
	require.False(t, seedQueued, "the retained leading seed must never be re-requested")
	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued, "the real frontier below the seed must be re-enqueued")
}

// TestReconcileFrontierGap_LiveFrontierNotRefetched: frontier still outstanding
// (in requestedBlocks) — never re-enqueued, even after the timeout.
func TestReconcileFrontierGap_LiveFrontierNotRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)
	sm.requestedBlocks.Set(frontier, struct{}{})

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))

	_, queued := sm.refetchBlocks[frontier]
	require.False(t, queued,
		"a frontier still outstanding (in requestedBlocks) must NOT be re-enqueued")
}

// TestReconcileFrontierGap_WindowOwnedNotRefetched: frontier already arrived /
// parked — not orphaned, never re-enqueued.
func TestReconcileFrontierGap_WindowOwnedNotRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)
	sm.windowOwnedBlocks.Set(frontier, 653071)

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))

	_, queued := sm.refetchBlocks[frontier]
	require.False(t, queued, "a window-owned (in-hand) frontier must NOT be re-enqueued")
}

// TestReconcileFrontierGap_CommittedFrontierRealigned: the mainnet-680594 wedge.
// The header-list front is latched on already-committed blocks (a commit bypassed
// the arrive-at-front trim). Re-fetching them is futile — haveInventory skips a
// committed block — so instead the stale leading nodes must be TRIMMED, realigning
// the cursor to the first genuinely-uncommitted block, WITHOUT any re-fetch enqueue.
func TestReconcileFrontierGap_CommittedFrontierRealigned(t *testing.T) {
	sm := newFrontierGapSM(t, 3)

	committedA := chainhash.Hash{0xa1}
	committedB := chainhash.Hash{0xb2}
	uncommitted := chainhash.Hash{0xc3}
	pushHeader(sm, committedA, 680571)
	pushHeader(sm, committedB, 680572)
	pushHeader(sm, uncommitted, 680573)
	sm.headerHeightIndex[committedA] = 680571
	sm.headerHeightIndex[committedB] = 680572
	sm.headerHeightIndex[uncommitted] = 680573
	sm.startHeader = sm.headerList.Front()

	// Blockchain has A and B committed (valid); the true frontier is not found.
	mockBC := &blockchain2.Mock{}
	mockBC.On("GetBlockHeader", mock.Anything, &committedA).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 680571}, nil).Maybe()
	mockBC.On("GetBlockHeader", mock.Anything, &committedB).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 680572}, nil).Maybe()
	mockBC.On("GetBlockHeader", mock.Anything, &uncommitted).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()
	sm.blockchainClient = mockBC

	// The stale frontier is pinned in refetchBlocks (as in the live wedge — a
	// committed block never drains), which the old "not orphaned" check masked.
	sm.refetchBlocks[committedA] = struct{}{}

	base := time.Now()
	sm.reconcileFrontierGap(base)                                           // arm on committedA
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second)) // fire → realign

	front := sm.headerList.Front()
	require.NotNil(t, front, "header list must not be emptied")
	require.Equal(t, uncommitted, *front.Value.(*headerNode).hash,
		"committed leading nodes must be trimmed, realigning the cursor to the first uncommitted block")

	_, aIndexed := sm.headerHeightIndex[committedA]
	require.False(t, aIndexed, "a trimmed node's height-index entry must be removed")
	require.Equal(t, sm.headerList.Front(), sm.startHeader,
		"startHeader must be advanced off the trimmed nodes")
}

// TestReconcileFrontierGap_UncommittedFrontierStillRefetched: a genuinely-missing
// (uncommitted) frontier must still take the re-fetch path, not be trimmed.
func TestReconcileFrontierGap_UncommittedFrontierStillRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)

	frontier := chainhash.Hash{0xd4}
	pushHeader(sm, frontier, 680571)
	sm.headerHeightIndex[frontier] = 680571

	mockBC := &blockchain2.Mock{}
	mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()
	sm.blockchainClient = mockBC

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))

	require.Equal(t, frontier, *sm.headerList.Front().Value.(*headerNode).hash,
		"an uncommitted frontier must NOT be trimmed")
	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued, "an uncommitted orphaned frontier must be re-enqueued for re-fetch")
}

// TestReconcileFrontierGap_SinglePeerNoop: ParallelFetchPeers<=1 is a no-op.
func TestReconcileFrontierGap_SinglePeerNoop(t *testing.T) {
	sm := newFrontierGapSM(t, 1)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	base := time.Now()
	sm.reconcileFrontierGap(base)
	sm.reconcileFrontierGap(base.Add(frontierGapTestTimeout + time.Second))

	require.Empty(t, sm.refetchBlocks,
		"single-peer mode: reconcileFrontierGap must be a no-op")
}

// TestReconcileFrontierGap_EmptyHeaderListNoop: no headers — no-op.
func TestReconcileFrontierGap_EmptyHeaderListNoop(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	sm.reconcileFrontierGap(time.Now())
	require.Empty(t, sm.refetchBlocks, "empty header list: no-op")
}
