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
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
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
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
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
