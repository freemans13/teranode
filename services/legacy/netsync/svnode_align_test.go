// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Centrepiece tests for the svnode-aligned fetch scheduler
// (legacy_svnodeAlignedFetch): they REPRODUCE the live mainnet big-block IBD
// stall (height ~701475) as a divergence from bitcoin-sv/svnode, then lock in
// that flipping the gate on ALIGNS Teranode's scheduler to svnode's proven
// design.
//
// The live shape (from ground truth): blocks commit STRICTLY IN ORDER, but the
// fetcher spends the whole 73-88 MB/s pipe pulling blocks 60-100 heights AHEAD
// of the committed frontier. Those far-ahead blocks cannot commit (they wait on
// the gap), pile into the park, and the ONE next-needed block (committed+1) gets
// ~1/100 of the bandwidth — freeze-then-burst, ~90% of the pipe wasted.
//
// svnode never bounds in-flight by memory. It bounds it by a FIXED, size-blind
// MAX_BLOCKS_IN_TRANSIT_PER_PEER = 16 per peer (validation.h:111), always spent
// on the LOWEST-height missing blocks starting at the download frontier
// (pindexLastCommonBlock, recomputed EVERY pass and advanced on block ARRIVAL —
// np.cpp:379,386,449-461). So committed+1 is always 1-of-16 and gets a full
// share of the pipe, and the in-flight set is CONTIGUOUS from the frontier, not
// scattered across ~100 far-ahead heights.
//
// These tests encode two svnode-faithful invariants the hand-tuned Teranode
// knobs failed to hold, plus the byte-identical-default guarantee:
//
//	I1  DEPTH IS HEALTHY, NOT STARVED — the in-flight set is a full 16, never
//	    collapsed to 1-of-100. (guards against under-fetch)
//	I2  DEPTH IS CONTIGUOUS FROM THE FRONTIER — the in-flight heights are the
//	    lowest missing run immediately above committed, NOT spread far ahead;
//	    committed+1 is ALWAYS pursued. (guards against the live over-fetch)
//	I3  DEFAULT (gate off) IS BYTE-IDENTICAL to today's behaviour.
//
// They reuse the harness from svnode_aligned_fetch_test.go (bigBlockTracker,
// seedHeaderList, requestedHashes) and assign_blocks_test.go (captureGetDataPeer,
// newEligiblePeerState) and drive the real fetch entry points directly.

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

// newAlignTestManager builds a minimal drain-goroutine SyncManager wired for the
// single-peer headers-first fetch path, with the given gate state and a
// big-block size tracker (the exact shape of the live stall — a memory-scaled
// ceiling far above svnode's fixed 16). The returned peer's getdata sends are
// captured but the tests assert on the synchronously-recorded requestedBlocks
// ledger, which is the authoritative in-flight (bandwidth) set.
func newAlignTestManager(t *testing.T, index uint8, seed byte, runway int, nonce uint32, gateOn bool) (*SyncManager, *peer.Peer, *peerSyncState, chainhash.Hash, []chainhash.Hash) {
	t.Helper()

	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{seed}
	_, hashes := buildHeaderChain(base, runway, nonce)

	var mu sync.Mutex
	var got []*wire.MsgGetData
	syncPeer := captureGetDataPeer(t, &chainParams, index, &mu, &got)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1
	tSettings.Legacy.SvnodeAlignedFetch = gateOn
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.FrontierLogInterval = 0

	syncState := newEligiblePeerState()
	t.Cleanup(func() {
		syncState.requestedBlocks.Stop()
		syncState.requestedTxns.Stop()
	})

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
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(syncPeer, syncState)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	seedHeaderList(sm, base, hashes)
	// Off the gate the walk uses the monotonic startHeader cursor; on the gate it
	// re-anchors on the download frontier every pass and startHeader is ignored.
	sm.startHeader = sm.headerList.Front().Next()

	return sm, syncPeer, syncState, base, hashes
}

// maxRequestedHeight returns the highest 0-based index (== height-1) among
// `hashes` currently in the peer's in-flight ledger, or -1 if none. It measures
// how FAR AHEAD of the frontier the in-flight set reaches — the live-stall
// metric: contiguous-from-frontier vs scattered ~100 heights ahead.
func maxRequestedHeight(state *peerSyncState, hashes []chainhash.Hash) int {
	hi := -1
	for i, h := range hashes {
		if _, ok := state.requestedBlocks.Get(h); ok {
			hi = i
		}
	}
	return hi
}

// TestSvnodeAlign_Centrepiece_GateOff_FrontierStarvesAsCursorClimbs is the RED
// characterisation of the live stall on the single-peer path. With the gate OFF
// the walk persists a MONOTONIC startHeader cursor: once it has requested the
// window it never looks back. So when committed+1 is freed (its getdata dropped,
// or the head-stall detector released it) but the cursor has already climbed
// past it, the next fetch spends its freed slot on a block FAR AHEAD of the
// frontier and leaves committed+1 outstanding to nobody — the very
// freeze-then-burst wedge measured live (the fetcher pulls ahead while the
// frontier starves).
func TestSvnodeAlign_Centrepiece_GateOff_FrontierStarvesAsCursorClimbs(t *testing.T) {
	sm, _, syncState, _, hashes := newAlignTestManager(t, 60, 0xe0, 60, 30000, false)

	// Tick 1: the memory-scaled budget (big blocks => 30) fills the window with
	// the lowest 30, and the monotonic cursor climbs to hashes[30].
	sm.fetchHeaderBlocks()
	require.Equal(t, 30, syncState.requestedBlocks.Len(),
		"gate OFF: the memory-scaled budget lets the single peer hold 30 (>16) big blocks in flight")
	require.True(t, requestedHashes(syncState, hashes)[hashes[0]], "tick 1: committed+1 is initially requested")

	// The frontier block (committed+1) is freed WITHOUT the frontier advancing —
	// the getdata was dropped, or the head-stall detector released it. Its
	// header node stays in the list (nothing has committed), so committed+1 is
	// still the next-needed block.
	syncState.requestedBlocks.Delete(hashes[0])
	sm.requestedBlocks.Delete(hashes[0])

	// Tick 2: one slot is free. The monotonic cursor sits at hashes[30], so the
	// walk spends the slot on a FAR-AHEAD block and never re-pursues committed+1.
	sm.fetchHeaderBlocks()

	req := requestedHashes(syncState, hashes)
	require.False(t, req[hashes[0]],
		"gate OFF live stall: committed+1 is NOT re-pursued — the frontier starves outstanding to nobody")
	require.True(t, req[hashes[30]],
		"gate OFF live stall: the freed slot is spent on a block 30 heights AHEAD of the frontier (bandwidth pulled far ahead)")
}

// TestSvnodeAlign_Centrepiece_GateOn_ReanchorsFrontier_PursuesCommittedPlus1 is
// the GREEN alignment: the SAME freed-frontier scenario with the gate ON.
// svnode recomputes the download frontier every pass (pindexLastCommonBlock,
// np.cpp:379,386), so the walk re-anchors at the front, sees committed+1 is no
// longer in flight, and re-pursues it FIRST — while blocks already in flight
// just above it are skipped and no new far-ahead block is bought. Bandwidth goes
// to the next-needed block, exactly as svnode.
func TestSvnodeAlign_Centrepiece_GateOn_ReanchorsFrontier_PursuesCommittedPlus1(t *testing.T) {
	sm, _, syncState, _, hashes := newAlignTestManager(t, 61, 0xe1, 60, 31000, true)

	// Tick 1: fixed size-blind cap of 16, contiguous from the frontier.
	sm.fetchHeaderBlocks()
	require.Equal(t, 16, syncState.requestedBlocks.Len(),
		"gate ON: fixed per-peer cap of 16 regardless of the 100MB block size")
	require.True(t, requestedHashes(syncState, hashes)[hashes[0]], "tick 1: committed+1 is requested")

	// Free committed+1 without advancing the frontier (same event as the gate-off
	// case: dropped getdata / head-stall release).
	syncState.requestedBlocks.Delete(hashes[0])
	sm.requestedBlocks.Delete(hashes[0])

	// Tick 2: the walk re-anchors at the frontier. committed+1 (hashes[0]) is no
	// longer in flight, so it is re-requested; hashes[1..15] are still in flight
	// and skipped; the cap is 16 so no far-ahead block is bought.
	sm.fetchHeaderBlocks()

	req := requestedHashes(syncState, hashes)
	require.True(t, req[hashes[0]],
		"gate ON alignment: committed+1 is ALWAYS re-pursued — the frontier is refilled first")
	require.False(t, req[hashes[16]],
		"gate ON alignment: no far-ahead block is bought to fill the freed slot — bandwidth stays on the frontier")
	require.Equal(t, 16, syncState.requestedBlocks.Len(),
		"gate ON: in-flight depth returns to a healthy 16 (no starvation), not scattered ahead")
}

// TestSvnodeAlign_InFlightHealthyAndContiguousFromFrontier proves invariants I1
// (healthy depth) and I2 (contiguous from the frontier) together on a deep
// runway. The live bug let ONE peer scatter its in-flight set across ~100
// far-ahead heights; the aligned scheduler keeps the in-flight set a full 16 AND
// clustered immediately above committed — the highest in-flight height is
// exactly frontier+15, never frontier+99.
func TestSvnodeAlign_InFlightHealthyAndContiguousFromFrontier(t *testing.T) {
	// A 120-block runway is deep enough that a memory-scaled/monotonic scheduler
	// COULD scatter far ahead; the aligned one must not.
	sm, _, syncState, _, hashes := newAlignTestManager(t, 62, 0xe2, 120, 32000, true)

	sm.fetchHeaderBlocks()

	// I1: healthy depth — a full 16, not collapsed to a starved 1.
	require.Equal(t, 16, syncState.requestedBlocks.Len(),
		"I1 healthy depth: the in-flight set is a full 16, never collapsed to 1-of-100")

	// I2: contiguous from the frontier — exactly the lowest 16 heights, and NO
	// height beyond frontier+15. This is the anti-scatter assertion.
	req := requestedHashes(syncState, hashes)
	for i := 0; i < 16; i++ {
		require.True(t, req[hashes[i]],
			"I2 contiguity: block %d (committed+%d) must be in the in-flight set", i, i+1)
	}
	require.True(t, req[hashes[0]], "I2: committed+1 (the frontier block) must always be pursued")
	require.Equal(t, 15, maxRequestedHeight(syncState, hashes),
		"I2 anti-scatter: the highest in-flight height is exactly frontier+15 — the in-flight set never reaches far ahead")
}

// TestSvnodeAlign_MultiPeer_ScarceBandwidthGoesToFrontier is the multi-peer
// analog of the stall on assignBlocksAcrossPeers. Three peers are each already
// nearly full of FAR-AHEAD in-flight work (heights 50-94), leaving only 1 spare
// slot each — a scarce 3-block bandwidth budget this pass. svnode-aligned
// lowest-height-first assignment must spend that scarce budget on the FRONTIER
// (committed+1, +2, +3), never on growing the far-ahead pile even higher.
func TestSvnodeAlign_MultiPeer_ScarceBandwidthGoesToFrontier(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xe3}
	const runway = 120
	_, hashes := buildHeaderChain(base, runway, 33000)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 63, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 64, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 65, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.SvnodeAlignedFetch = true
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.BlockDownloadWindow = 1024 // loose ceiling: the per-peer spare is the real throttle
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
		blockSizeTracker: bigBlockTracker(),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	seedHeaderList(sm, base, hashes)

	// Pre-load each peer with 15 DISJOINT far-ahead in-flight blocks (the pile
	// that formed before the fix): heights 50-64, 65-79, 80-94. Each peer is now
	// at 15/16 — spare 1 — so this pass can add only 3 blocks total. Record them
	// in both the per-peer ledger (for the spare math) and the global ledger (so
	// the frontier walk skips them as already in flight).
	preload := func(st *peerSyncState, lo, hi int) {
		for i := lo; i < hi; i++ {
			st.requestedBlocks.Set(hashes[i], struct{}{})
			sm.requestedBlocks.Set(hashes[i], struct{}{})
		}
	}
	preload(syncState, 49, 64) // 15 blocks, heights 50-64
	preload(stateB, 64, 79)    // 15 blocks, heights 65-79
	preload(stateC, 79, 94)    // 15 blocks, heights 80-94

	sm.assignBlocksAcrossPeers()

	// Exactly 3 blocks were newly assignable this pass, and svnode lowest-first
	// spends them on the FRONTIER: committed+1, +2, +3 — never on heights above
	// the existing far-ahead pile.
	assignedNew := make(map[chainhash.Hash]int)
	for _, st := range []*peerSyncState{syncState, stateB, stateC} {
		for i := 0; i < 4; i++ { // frontier band
			if _, ok := st.requestedBlocks.Get(hashes[i]); ok {
				assignedNew[hashes[i]]++
			}
		}
	}
	require.Equal(t, 1, assignedNew[hashes[0]],
		"scarce multi-peer bandwidth goes to committed+1 FIRST (assigned exactly once)")
	require.Equal(t, 1, assignedNew[hashes[1]], "then committed+2")
	require.Equal(t, 1, assignedNew[hashes[2]], "then committed+3")
	require.Equal(t, 0, assignedNew[hashes[3]],
		"the scarce budget is exhausted at the frontier — committed+4 is NOT reached this pass")

	// The far-ahead pile did NOT grow: no block above the pre-existing heights
	// (>= height 96, i.e. hashes[95..]) was newly assigned.
	for i := 95; i < runway; i++ {
		for _, st := range []*peerSyncState{syncState, stateB, stateC} {
			_, ok := st.requestedBlocks.Get(hashes[i])
			require.False(t, ok,
				"the far-ahead pile must NOT grow: block at height %d was newly assigned", i+1)
		}
	}

	// Each peer respects the per-peer cap of 16.
	require.LessOrEqual(t, syncState.requestedBlocks.Len(), 16)
	require.LessOrEqual(t, stateB.requestedBlocks.Len(), 16)
	require.LessOrEqual(t, stateC.requestedBlocks.Len(), 16)
}

// TestSvnodeAlign_DefaultGateOff_ByteIdentical is invariant I3: with the gate
// off (the default), every aligned decision point reduces EXACTLY to today's
// pre-feature behaviour. The load-bearing helpers are pure pass-throughs — the
// in-flight governor is the memory-scaled cap, the walk anchor is the monotonic
// startHeader cursor, the runway clamp is the pre-gate value — and a real fetch
// pass requests exactly the lowest calculateMaxInFlightBlocks() hashes from the
// cursor, byte-for-byte what the pre-feature scheduler did.
func TestSvnodeAlign_DefaultGateOff_ByteIdentical(t *testing.T) {
	sm, _, syncState, _, hashes := newAlignTestManager(t, 66, 0xe4, 40, 34000, false)

	// Helper purity: gate off, the aligned helpers must equal the pre-feature
	// expressions they replaced.
	require.False(t, sm.svnodeAlignedFetch(), "the gate must be OFF (default)")

	require.Equal(t, sm.blockSizeTracker.calculateMaxInFlightBlocks(), sm.maxInFlightPerPeer(),
		"gate off: the in-flight governor IS the memory-scaled cap, byte-identical")

	sm.headerMu.Lock()
	anchor := sm.fetchWalkAnchorLocked()
	sm.headerMu.Unlock()
	require.Equal(t, sm.startHeader, anchor,
		"gate off: the fetch walk anchor IS the monotonic startHeader cursor, byte-identical")

	horizon, capped := sm.fetchRunwayHorizon()
	require.Equal(t, uint32(0), horizon)
	require.False(t, capped,
		"gate off with parkAhead inactive: the runway clamp is the pre-gate (0,false), unchanged")

	// Behavioural equality: a real fetch pass requests exactly the lowest
	// calculateMaxInFlightBlocks() hashes from the cursor (30 for these big
	// blocks) — the exact pre-feature getdata window.
	want := sm.blockSizeTracker.calculateMaxInFlightBlocks()
	sm.fetchHeaderBlocks()
	require.Equal(t, want, syncState.requestedBlocks.Len(),
		"gate off: in-flight is exactly the memory-scaled window (%d), today's behaviour", want)

	req := requestedHashes(syncState, hashes)
	for i := 0; i < want; i++ {
		require.True(t, req[hashes[i]], "gate off: the lowest %d hashes from the cursor are requested; block %d missing", want, i)
	}
	require.False(t, req[hashes[want]],
		"gate off: nothing beyond the memory-scaled window is requested — the cursor stopped exactly at the pre-feature bound")
}
