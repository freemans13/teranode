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
// skipping the seed) and re-enqueues it to refetchBlocks when it is outstanding
// to nobody, in-hand nowhere, and queued nowhere — keyed on the committed-tip
// frontier rather than on assignedTo.

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

func newFrontierGapSM(t *testing.T, parallelPeers int) *SyncManager {
	t.Helper()
	chainParams := chaincfg.MainNetParams
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = parallelPeers

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
// TTL). One reconcile must re-enqueue it.
func TestReconcileFrontierGap_LedgerlessFrontierRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	sm.reconcileFrontierGap()

	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued,
		"an orphaned frontier (in no ledger) must be re-enqueued to refetchBlocks")
}

// TestReconcileFrontierGap_SkipsLeadingSeed: Front() is the committed-tip seed
// (no block of its own); the frontier is the next node. The seed must be skipped
// and the real frontier recovered.
func TestReconcileFrontierGap_SkipsLeadingSeed(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	seed := chainhash.Hash{0xff}
	sc := seed
	seedEl := sm.headerList.PushBack(&headerNode{height: 653070, hash: &sc})
	sm.headerListSeed = seedEl

	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	sm.reconcileFrontierGap()

	_, seedQueued := sm.refetchBlocks[seed]
	require.False(t, seedQueued, "the retained leading seed must never be re-requested")
	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued, "the real frontier below the seed must be re-enqueued")
}

// TestReconcileFrontierGap_LiveFrontierNotRefetched: the frontier is still
// outstanding to a peer (in requestedBlocks) — it must NOT be re-enqueued.
func TestReconcileFrontierGap_LiveFrontierNotRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)
	sm.requestedBlocks.Set(frontier, struct{}{})

	sm.reconcileFrontierGap()

	_, queued := sm.refetchBlocks[frontier]
	require.False(t, queued,
		"a frontier still outstanding (in requestedBlocks) must NOT be re-enqueued")
}

// TestReconcileFrontierGap_WindowOwnedNotRefetched: the frontier already arrived
// and is parked/owned by the window pipeline — not orphaned, must NOT re-enqueue.
func TestReconcileFrontierGap_WindowOwnedNotRefetched(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)
	sm.windowOwnedBlocks.Set(frontier, 653071)

	sm.reconcileFrontierGap()

	_, queued := sm.refetchBlocks[frontier]
	require.False(t, queued, "a window-owned (in-hand) frontier must NOT be re-enqueued")
}

// TestReconcileFrontierGap_AlreadyQueuedIdempotent: already in refetchBlocks —
// no duplicate work, still present.
func TestReconcileFrontierGap_AlreadyQueuedIdempotent(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)
	sm.refetchBlocks[frontier] = struct{}{}

	sm.reconcileFrontierGap()

	_, queued := sm.refetchBlocks[frontier]
	require.True(t, queued, "an already-queued frontier stays queued")
	require.Len(t, sm.refetchBlocks, 1, "no duplicate/extra entries")
}

// TestReconcileFrontierGap_SinglePeerNoop: ParallelFetchPeers<=1 uses the
// original single-peer machinery; reconcileFrontierGap is a no-op there.
func TestReconcileFrontierGap_SinglePeerNoop(t *testing.T) {
	sm := newFrontierGapSM(t, 1)
	frontier := chainhash.Hash{0x01}
	pushHeader(sm, frontier, 653071)

	sm.reconcileFrontierGap()

	require.Empty(t, sm.refetchBlocks,
		"single-peer mode: reconcileFrontierGap must be a no-op")
}

// TestReconcileFrontierGap_EmptyHeaderListNoop: no headers (e.g. just after
// resetHeaderState) — nothing to reconcile.
func TestReconcileFrontierGap_EmptyHeaderListNoop(t *testing.T) {
	sm := newFrontierGapSM(t, 3)
	sm.reconcileFrontierGap()
	require.Empty(t, sm.refetchBlocks, "empty header list: no-op")
}
