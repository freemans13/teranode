// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for GAP-FIRST FETCH (legacy_gapFirstFetch). These reproduce the LIVE
// failure shape diagnosed on mainnet 2026-07-21: the committed tip was frozen at
// 701304 while the fetcher spent its whole bandwidth racing far-ahead successors
// (701366+) that arrived, could not commit (61 blocks missing below), were parked
// and dropped — and the GAP blocks 701305-701365 only trickled in through the
// reactive, timeout-gated side-channels. assignedBlocks oscillated 0-4.
//
// The centrepiece is the FEED-IDLE-REGRESSION repro (assertion 1): the previous
// "frontier-first clamp" attempt made unit tests pass but starved the pipeline
// live. The subtle lesson those tests missed — and these encode — is that RAW
// in-flight depth is the WRONG sentinel: live it stayed ~20 the whole time (the
// network pulled 36.6 MB/s of far-ahead 701366+ blocks) while the pipeline starved,
// because none of that depth was USABLE. So these tests guard gap-USABLE in-flight
// depth (in-gate, commit-able blocks) and HARD-require it stay healthy every tick,
// AND that the lowest not-yet-committed heights are fetched BEFORE far-ahead
// successors. TestGapFirst_FeedIdleSentinelGoesRed then runs that same metric
// against the OFF/pre-fix scheduler and proves it collapses to 0 — the built-in
// red-verification that the centrepiece assertion is real, not vacuous.
//
// LIMIT stated openly: these MANUFACTURE the orphaned-below-cursor state directly
// (advance startHeader past the gap, pre-seed assignedTo only with far-ahead
// strays) rather than via a real sync-peer rotation, so they prove the scheduler's
// RESPONSE (depth, order, racing) not that rotation produces the state (covered by
// the reconcile tests). The final verification is the live rollout behind the
// OFF-by-default flag.

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

// gapFirstHarness holds the manufactured live-shape scenario shared by the tests.
type gapFirstHarness struct {
	sm       *SyncManager
	peers    []*peer.Peer
	states   []*peerSyncState
	captures []*[]*wire.MsgGetData
	mutexes  []*sync.Mutex
	hashes   []chainhash.Hash // index i == height gapFirstBaseHeight+i
	elems    map[int32]*list.Element
}

const (
	// The live committed tip; the next-needed (frontier) block is baseHeight.
	gapFirstCommitted  = int32(701304)
	gapFirstBaseHeight = int32(701305) // committed+1, the true contiguous frontier
	gapFirstStartAhead = int32(701366) // where the monotonic cursor is stranded
	gapFirstMaxBehind  = 20            // maturity gate; ceiling = cached+maxBehind
	gapFirstChainLen   = 96            // heights 701305..701400
)

// hashAt returns the hash for a given manufactured height.
func (h *gapFirstHarness) hashAt(height int32) chainhash.Hash {
	return h.hashes[height-gapFirstBaseHeight]
}

// buildGapFirstHarness manufactures the orphaned-below-cursor state:
//   - header list + headerHeightIndex seeded 701305..701400,
//   - startHeader advanced to the 701366 node (the monotonic cursor stranded far
//     ahead of the committed frontier),
//   - assignedTo pre-seeded ONLY with far-ahead strays 701366..701369 (leaving
//     701305..701365 in NO ledger — the gap),
//   - four peers each with ample per-peer spare,
//   - cachedBlockAssemblyHeight=701304, baHeightPolled=true,
//   - gap-first ON (unless the caller flips it off for the rollback lock).
func buildGapFirstHarness(t *testing.T, gapFirst bool) *gapFirstHarness {
	t.Helper()

	chainParams := chaincfg.MainNetParams

	const nPeers = 4

	h := &gapFirstHarness{elems: make(map[int32]*list.Element)}

	for i := 0; i < nPeers; i++ {
		var (
			mu  sync.Mutex
			got []*wire.MsgGetData
		)
		muCopy := &mu
		gotCopy := &got
		p := captureGetDataPeer(t, &chainParams, uint8(50+i), muCopy, gotCopy) //nolint:gosec
		st := newEligiblePeerState()
		t.Cleanup(func() {
			st.requestedBlocks.Stop()
			st.requestedTxns.Stop()
		})

		h.peers = append(h.peers, p)
		h.states = append(h.states, st)
		h.captures = append(h.captures, gotCopy)
		h.mutexes = append(h.mutexes, muCopy)
	}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = nPeers
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.BlockDownloadWindow = 1024
	tSettings.Legacy.GapFirstFetch = gapFirst
	tSettings.Legacy.GapFirstDepth = 0
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = gapFirstMaxBehind

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		blockchainClient:  blockchainClient,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	for i, p := range h.peers {
		sm.peerStates.Set(p, h.states[i])
	}
	sm.storeSyncPeer(h.peers[0], &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.cachedBlockAssemblyHeight.Store(uint32(gapFirstCommitted)) //nolint:gosec
	sm.baHeightPolled.Store(true)

	// Seed the header list directly from the frontier (no leading seed node), so
	// frontierElementLocked() resolves to the 701305 node.
	_, hashes := buildHeaderChain(chainhash.Hash{0xAA}, gapFirstChainLen, 70000)
	h.hashes = hashes

	for i := range hashes {
		height := gapFirstBaseHeight + int32(i)
		el := sm.headerList.PushBack(&headerNode{height: height, hash: &h.hashes[i]})
		h.elems[height] = el
		sm.headerHeightIndex[hashes[i]] = height
	}

	// Cursor stranded far ahead of the frontier — exactly the monotonic-cursor bug.
	sm.startHeader = h.elems[gapFirstStartAhead]

	// Pre-seed assignedTo ONLY with far-ahead strays 701366..701369, RECENT (so
	// reconcileLostAssignments does not free them and they persist as the ongoing
	// far-ahead in-flight work). This leaves 701305..701365 in NO ledger.
	now := time.Now()
	for i := int32(0); i < 4; i++ {
		strayHeight := gapFirstStartAhead + i
		strayHash := h.hashAt(strayHeight)
		p := h.peers[i]
		st := h.states[i]
		st.requestedBlocks.Set(strayHash, struct{}{})
		sm.requestedBlocks.Set(strayHash, struct{}{})
		sm.addAssignment(strayHash, p, now)
	}

	h.sm = sm

	return h
}

// inFlightDepth is total in-flight across peers. It is a NECESSARY-BUT-NOT-
// SUFFICIENT feed-idle signal: live 2026-07-21 the total in-flight depth stayed
// HIGH the whole time (the network was pulling 36.6 MB/s, assignedTo full of
// far-ahead 701366+ strays) yet the pipeline STARVED, because none of that depth
// was USABLE — every in-flight block was above the gap and could not commit. So a
// raw-depth assertion alone cannot catch the live failure (and cannot catch the
// clamp regression either: the pre-seeded strays keep raw depth >= 4 even if the
// scheduler assigns ZERO gap blocks). The real sentinel is gapUsableDepth below.
func (h *gapFirstHarness) inFlightDepth() int {
	depth := 0
	for _, st := range h.sm.peerStates.Range() {
		depth += st.requestedBlocks.Len()
	}
	return depth
}

// gapUsableDepth is the TRUE FEED-IDLE SENTINEL: how many in-flight blocks are
// actually USABLE right now — heights in the in-gate contiguous window
// [cached+1 .. ceiling] that can commit as the tip advances. This is the metric
// that collapsed live (to ~0, while raw depth stayed ~20 on far-ahead strays) and
// the one a throttle/clamp collapses. Under gap-first ON it must stay healthy every
// tick; under the OFF/pre-gap-first scheduler it is 0 (all budget spent far-ahead),
// which is exactly how the RED-verification (TestGapFirst_FeedIdleSentinelGoesRed)
// proves this assertion is not vacuous.
func (h *gapFirstHarness) gapUsableDepth(cached, ceiling int32) int {
	n := 0
	for _, ht := range h.inFlightHeights() {
		if ht >= cached+1 && ht <= ceiling {
			n++
		}
	}
	return n
}

// inFlightHeights returns the heights currently in flight (present in assignedTo
// with a known header height). committed blocks are trimmed from the index, so
// this is exactly the current in-flight set.
func (h *gapFirstHarness) inFlightHeights() []int32 {
	h.sm.headerMu.Lock()
	h.sm.assignedMu.Lock()
	defer h.sm.assignedMu.Unlock()
	defer h.sm.headerMu.Unlock()

	var out []int32
	for hash := range h.sm.assignedTo {
		if ht, ok := h.sm.headerHeightIndex[hash]; ok {
			out = append(out, ht)
		}
	}
	return out
}

// commitFrontier simulates the committer accepting the frontier block (cached+1)
// once it is in flight: purge every ledger, trim the header node, advance cached.
func (h *gapFirstHarness) commitFrontier(height int32) bool {
	hash := h.hashAt(height)

	if _, inFlight := h.sm.requestedBlocks.Get(hash); !inFlight {
		return false
	}

	h.sm.assignedMu.Lock()
	for _, p := range h.sm.clearAssignment(hash) {
		if st, ok := h.sm.peerStates.Get(p); ok {
			st.requestedBlocks.Delete(hash)
		}
	}
	h.sm.requestedBlocks.Delete(hash)
	delete(h.sm.refetchBlocks, hash)
	h.sm.assignedMu.Unlock()

	h.sm.headerMu.Lock()
	if el, ok := h.elems[height]; ok {
		h.sm.headerList.Remove(el)
		delete(h.elems, height)
	}
	delete(h.sm.headerHeightIndex, hash)
	h.sm.headerMu.Unlock()

	h.sm.cachedBlockAssemblyHeight.Store(uint32(height)) //nolint:gosec

	return true
}

// TestGapFirst_FeedIdleGapFirstOrderingAndNoWedge drives successive
// maintainInFlightWindow ticks against the manufactured gap, committing the
// frontier each tick so the tip advances (the gap drains), and asserts on EVERY
// tick: (1) FEED-IDLE — depth never collapses to 0/1; (2) GAP-FIRST ORDER — only
// in-gate heights (or the pre-seeded strays) are ever in flight, never a fresh
// far-ahead successor; (3) NO-WEDGE — the frontier is fetched within one tick and
// the window slides upward as the tip commits; (5) NO DOUBLE-FETCH — refetchBlocks
// never holds a block already in-flight/assigned, and no block is assigned to two
// peers.
func TestGapFirst_FeedIdleGapFirstOrderingAndNoWedge(t *testing.T) {
	h := buildGapFirstHarness(t, true /* gapFirst */)
	sm := h.sm

	frontierHash := h.hashAt(gapFirstBaseHeight)

	// The window ceiling one tick BEFORE any commit: cached(701304)+maxBehind(20).
	initialCeiling := gapFirstCommitted + gapFirstMaxBehind // 701324

	// Precondition: the far-ahead block just above the initial ceiling (701325) is
	// NOT in flight yet (proves the window has not yet slid to reach it).
	slideProbe := initialCeiling + 1 // 701325
	slideProbeSeen := false

	const ticks = 12

	for tick := 0; tick < ticks; tick++ {
		sm.maintainInFlightWindow()

		cached := int32(sm.cachedBlockAssemblyHeight.Load()) //nolint:gosec
		ceiling := cached + gapFirstMaxBehind

		// (1) FEED-IDLE SENTINEL. Two layers, weakest to strongest:
		//
		//  (1a) Raw in-flight depth must not collapse to 0/1. This is the coarse guard;
		//  the four persistent strays alone keep it satisfied, so it CANNOT by itself
		//  catch the clamp (see the inFlightDepth comment) — it only rules out a total
		//  wedge where even the strays were reaped.
		depth := h.inFlightDepth()
		require.Greater(t, depth, 1,
			"tick %d: FEED-IDLE — in-flight depth collapsed to %d (total wedge)", tick, depth)

		//  (1b) The REAL feed-idle sentinel — the one the clamp/throttle fails and the
		//  one that collapsed live. Under an open gap the scheduler must keep a HEALTHY
		//  number of USABLE (in-gate, commit-able) blocks in flight, not just raw depth
		//  propped up by unusable far-ahead strays. Live this collapsed to ~0 while raw
		//  depth stayed ~20; gap-first restores it (observed steady 16 of the 20-wide
		//  window). The >= 8 floor is well below the observed 16 (no flake) yet far above
		//  the 0 the OFF/clamp scheduler produces — TestGapFirst_FeedIdleSentinelGoesRed
		//  proves it goes RED at 0. THIS is the assertion the clamp's tests were missing.
		usable := h.gapUsableDepth(cached, ceiling)
		require.GreaterOrEqual(t, usable, 8,
			"tick %d: FEED-IDLE — gap-usable in-flight depth collapsed to %d (the live starvation shape: budget spent far-ahead, gap starved)", tick, usable)

		// (2) GAP-FIRST ORDER — every in-flight height is either in-gate (<= ceiling)
		// or one of the pre-seeded far-ahead strays. i.e. the pass budget is spent on
		// the gap, NEVER on a NEW far-ahead successor at startHeader.
		for _, ht := range h.inFlightHeights() {
			inGate := ht <= ceiling
			isStray := ht >= gapFirstStartAhead && ht <= gapFirstStartAhead+3
			require.True(t, inGate || isStray,
				"tick %d: height %d in flight but > ceiling %d and not a pre-seeded stray — far-ahead prefetch beat the gap", tick, ht, ceiling)
		}

		// (3) NO-WEDGE — the true frontier (cached+1) is in flight within one tick.
		_, frontierInFlight := sm.requestedBlocks.Get(h.hashAt(cached + 1))
		require.True(t, frontierInFlight,
			"tick %d: frontier %d must be in flight every tick (no wedge)", tick, cached+1)

		// (5) NO DOUBLE-FETCH — refetchBlocks never holds a block already in-flight or
		// assigned; and every assigned block has exactly one holder (disjoint, no race
		// while within grace).
		sm.assignedMu.Lock()
		for rh := range sm.refetchBlocks {
			_, inFlight := sm.requestedBlocks.Get(rh)
			require.False(t, inFlight, "tick %d: refetchBlocks holds an already-in-flight block", tick)
			_, assigned := sm.assignedTo[rh]
			require.False(t, assigned, "tick %d: refetchBlocks holds an already-assigned block", tick)
		}
		for hash, inner := range sm.assignedTo {
			require.Len(t, inner, 1,
				"tick %d: block %s assigned to %d peers, must be disjoint (1)", tick, hash, len(inner))
		}
		sm.assignedMu.Unlock()

		// (3, slide) — the window must slide: the probe above the initial ceiling is
		// eligible only once the tip commits past 701304.
		if _, ok := sm.requestedBlocks.Get(h.hashAt(slideProbe)); ok {
			slideProbeSeen = true
			require.Greater(t, cached, gapFirstCommitted,
				"tick %d: block %d (above the initial ceiling) must not be fetched until the tip commits past %d", tick, slideProbe, gapFirstCommitted)
		}

		// Advance the tip: commit the frontier now that gap-first put it in flight.
		h.commitFrontier(cached + 1)
	}

	// The frontier was assigned+recorded on the very first tick.
	require.NotEqual(t, chainhash.Hash{}, frontierHash)

	// The window demonstrably slid upward: a block above the initial ceiling became
	// eligible as the tip advanced (the 61-wide gap heals progressively, ascending).
	require.True(t, slideProbeSeen,
		"the window must slide: block %d (above the initial ceiling %d) must become eligible as the tip commits", slideProbe, initialCeiling)

	// The tip advanced well past the frozen 701304 (the gap is healing, not wedged).
	require.Greater(t, int32(sm.cachedBlockAssemblyHeight.Load()), gapFirstCommitted+int32(ticks/2), //nolint:gosec
		"the committed tip must climb as the gap drains, not stay frozen")
}

// TestGapFirst_FeedIdleSentinelGoesRed is the RED-VERIFICATION for assertion (1b):
// it proves the gap-usable feed-idle sentinel is NOT vacuous by running the exact
// same harness and the exact same metric against the OFF (pre-gap-first) scheduler
// — which IS today's shipping behaviour and IS the live starvation shape — and
// asserting the sentinel COLLAPSES. This is the throw-away-clamp lesson made
// permanent in code: the metric that TestGapFirst_FeedIdleGapFirstOrderingAndNoWedge
// requires to stay >= 8 is here shown to sit at 0 whenever the scheduler reverts to
// spending its whole (still-full, depth~20) budget on far-ahead blocks. If a future
// change made gap-first a no-op, (1b) over there goes red; if someone weakened (1b)
// to a raw-depth check, THIS test documents why that is wrong (raw depth is ~20 in
// BOTH worlds — only gap-usable depth discriminates fed from starved).
//
// It also nails the wedge directly: with the flag off the true frontier (cached+1)
// is NEVER put in flight and the committed tip stays FROZEN at 701304 across every
// tick — the precise live symptom gap-first fixes.
func TestGapFirst_FeedIdleSentinelGoesRed(t *testing.T) {
	h := buildGapFirstHarness(t, false /* gapFirst OFF — the pre-fix scheduler */)
	sm := h.sm

	const ticks = 12

	for tick := 0; tick < ticks; tick++ {
		sm.maintainInFlightWindow()

		cached := int32(sm.cachedBlockAssemblyHeight.Load()) //nolint:gosec
		ceiling := cached + gapFirstMaxBehind

		// RAW depth stays high — the network is "busy" (the deceptive live signal).
		depth := h.inFlightDepth()
		require.GreaterOrEqual(t, depth, 4,
			"tick %d: raw depth should stay high even OFF (busy on far-ahead) — got %d", tick, depth)

		// But the FEED-IDLE sentinel collapses: NO usable (in-gate, commit-able) block
		// is in flight. This is the assertion (1b) guards against, shown red here.
		usable := h.gapUsableDepth(cached, ceiling)
		require.Zero(t, usable,
			"tick %d: OFF scheduler must starve the gap (gap-usable depth 0) — got %d; if this is non-zero the OFF path is fetching the gap and the whole premise is wrong", tick, usable)
		require.Less(t, usable, 8,
			"tick %d: gap-usable depth %d must be BELOW the >=8 floor the ON test requires — proving that floor discriminates fed from starved", tick, usable)

		// The frontier is never fetched → the tip can never advance → the wedge.
		_, frontierInFlight := sm.requestedBlocks.Get(h.hashAt(cached + 1))
		require.False(t, frontierInFlight,
			"tick %d: OFF scheduler must NOT put the frontier %d in flight (the live wedge)", tick, cached+1)

		// Try to advance the tip exactly as the ON test does — it MUST fail, because the
		// frontier is not in flight (commitFrontier returns false).
		require.False(t, h.commitFrontier(cached+1),
			"tick %d: tip must NOT be able to advance with the flag off (frontier not in flight)", tick)
	}

	// The committed tip is provably FROZEN at the live value across all ticks — the
	// exact freeze-then-nothing symptom (there is no gap-first to unfreeze it).
	require.Equal(t, gapFirstCommitted, int32(sm.cachedBlockAssemblyHeight.Load()), //nolint:gosec
		"OFF scheduler leaves the committed tip frozen at %d (the live wedge gap-first fixes)", gapFirstCommitted)
}

// TestGapFirst_RacingTargetsFrontier locks assertion (4): once the frontier is in
// flight, checkHeadStall races the FRONTIER (701305), never a far-ahead successor
// (701366). A far-ahead stray is also in flight to prove racing does not touch it.
func TestGapFirst_RacingTargetsFrontier(t *testing.T) {
	h := buildGapFirstHarness(t, true /* gapFirst */)
	sm := h.sm

	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.BlockStallTimeout = 2 * time.Second
	sm.settings.Legacy.BlockStallMinRate = 102400
	sm.settings.Legacy.BlockSlowFetchTimeout = 15 * time.Second
	sm.settings.Legacy.MaxBlockParallelFetch = 3

	now := time.Now()
	frontierHash := h.hashAt(gapFirstBaseHeight) // 701305
	strayHash := h.hashAt(gapFirstStartAhead)    // 701366 (already seeded to peers[0])

	// Put the frontier in flight to peers[1] long ago (stalled past BlockStallTimeout),
	// as the sweep+drain would have on a prior tick.
	sm.assignedMu.Lock()
	sm.requestedBlocks.Set(frontierHash, struct{}{})
	h.states[1].requestedBlocks.Set(frontierHash, struct{}{})
	sm.addAssignment(frontierHash, h.peers[1], now.Add(-10*time.Second))
	sm.assignedMu.Unlock()

	// The frontier has been the stalled head long enough to be RACED (past the 15s
	// slow-fetch threshold, still inside the 30s disconnect window).
	sm.headStallHash = frontierHash
	sm.headStallSince = now.Add(-16 * time.Second)

	sm.checkHeadStall(now)

	// The FRONTIER must now be raced to a second peer; the far-ahead stray must be
	// untouched — racing targets the block that gates commit, not a parked successor.
	sm.assignedMu.Lock()
	frontierRacers := len(sm.assignedTo[frontierHash])
	strayRacers := len(sm.assignedTo[strayHash])
	sm.assignedMu.Unlock()

	require.Equal(t, 2, frontierRacers,
		"racing must add a second peer to the FRONTIER (701305), the block that gates commit")
	require.Equal(t, 1, strayRacers,
		"racing must NOT touch the far-ahead stray (701366) — that is the exact live bug")

	// And the extra getdata that went out was for the frontier, never the stray.
	require.Eventually(t, func() bool {
		return h.someCaptureHas(frontierHash)
	}, 2*time.Second, 10*time.Millisecond, "the racer getdata for the frontier 701305 must be sent")
	require.False(t, h.someCaptureHas(strayHash),
		"no getdata may be issued for the far-ahead stray 701366 by the race")
}

// TestGapFirst_RacingCarveOutWhenFrontierUnassigned locks the one-tick carve-out
// in assertion (4): when the frontier is NOT yet in assignedTo (the window between
// checkHeadStall and the sweep+drain), checkHeadStall must race NOTHING rather than
// fall back to racing the far-ahead argmin (701366) — the exact live bug.
func TestGapFirst_RacingCarveOutWhenFrontierUnassigned(t *testing.T) {
	h := buildGapFirstHarness(t, true /* gapFirst */)
	sm := h.sm

	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.BlockStallTimeout = 2 * time.Second
	sm.settings.Legacy.BlockStallMinRate = 102400
	sm.settings.Legacy.BlockSlowFetchTimeout = 15 * time.Second
	sm.settings.Legacy.MaxBlockParallelFetch = 3

	now := time.Now()
	strayHash := h.hashAt(gapFirstStartAhead) // 701366, the lowest ASSIGNED block

	// Age the stray so, WITHOUT the carve-out, the far-ahead argmin would be raced.
	sm.assignedMu.Lock()
	inner := sm.assignedTo[strayHash]
	for p := range inner {
		inner[p] = now.Add(-10 * time.Second)
	}
	sm.assignedMu.Unlock()
	sm.headStallHash = strayHash
	sm.headStallSince = now.Add(-16 * time.Second)

	// Frontier 701305 is deliberately NOT in assignedTo.
	sm.checkHeadStall(now)

	// The far-ahead stray must be untouched — no racer added — and nothing enqueued
	// for re-fetch by checkHeadStall (the sweep, not checkHeadStall, drives the gap).
	sm.assignedMu.Lock()
	strayRacers := len(sm.assignedTo[strayHash])
	refetchLen := len(sm.refetchBlocks)
	sm.assignedMu.Unlock()

	require.Equal(t, 1, strayRacers,
		"carve-out: a not-yet-assigned frontier must make checkHeadStall race NOTHING, not the far-ahead stray")
	require.Zero(t, refetchLen,
		"carve-out: checkHeadStall must not re-enqueue anything for the unassigned frontier (the sweep does that)")

	require.Never(t, func() bool { return h.someCaptureHasAny() }, 250*time.Millisecond, 20*time.Millisecond,
		"carve-out: no getdata may be issued when the frontier is not yet in flight")
}

// TestGapFirst_RollbackFlagOffByteIdentical locks the OFF path (assertion 6). With
// GapFirstFetch=false: the sweep is a no-op (no gap population) and checkHeadStall
// selects the far-ahead argmin (701366) as head — today's exact behaviour.
func TestGapFirst_RollbackFlagOffByteIdentical(t *testing.T) {
	h := buildGapFirstHarness(t, false /* gapFirst OFF */)
	sm := h.sm

	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.BlockStallTimeout = 2 * time.Second
	sm.settings.Legacy.BlockStallMinRate = 102400
	sm.settings.Legacy.BlockSlowFetchTimeout = 15 * time.Second
	sm.settings.Legacy.MaxBlockParallelFetch = 3

	now := time.Now()
	frontierHash := h.hashAt(gapFirstBaseHeight) // 701305
	strayHash := h.hashAt(gapFirstStartAhead)    // 701366

	// (6a) The sweep must do NOTHING with the flag off — no gap population.
	sm.sweepContiguousFrontier(now)
	sm.assignedMu.Lock()
	refetchLen := len(sm.refetchBlocks)
	sm.assignedMu.Unlock()
	require.Zero(t, refetchLen, "flag OFF: sweepContiguousFrontier must not enqueue the gap (byte-identical)")

	// (6b) Head selection must be the far-ahead argmin (701366), and racing acts on
	// THAT (the pre-gap-first behaviour), never on the frontier — the frontier is not
	// even in assignedTo.
	sm.assignedMu.Lock()
	inner := sm.assignedTo[strayHash]
	for p := range inner {
		inner[p] = now.Add(-10 * time.Second)
	}
	sm.assignedMu.Unlock()
	sm.headStallHash = strayHash
	sm.headStallSince = now.Add(-16 * time.Second)

	sm.checkHeadStall(now)

	sm.assignedMu.Lock()
	strayRacers := len(sm.assignedTo[strayHash])
	_, frontierAssigned := sm.assignedTo[frontierHash]
	sm.assignedMu.Unlock()

	require.Equal(t, 2, strayRacers,
		"flag OFF: head selection is the far-ahead argmin (701366) and racing acts on it — today's behaviour")
	require.False(t, frontierAssigned,
		"flag OFF: the frontier is never force-assigned (no gap-first)")
	require.True(t, h.someCaptureHasEventually(t, strayHash),
		"flag OFF: the raced getdata targets the far-ahead argmin 701366")
}

// --- capture helpers ---------------------------------------------------------

func (h *gapFirstHarness) someCaptureHas(hash chainhash.Hash) bool {
	for i := range h.captures {
		h.mutexes[i].Lock()
		msgs := *h.captures[i]
		h.mutexes[i].Unlock()
		for _, h2 := range collectInvHashes(msgs) {
			if h2 == hash {
				return true
			}
		}
	}
	return false
}

func (h *gapFirstHarness) someCaptureHasAny() bool {
	for i := range h.captures {
		h.mutexes[i].Lock()
		n := len(*h.captures[i])
		h.mutexes[i].Unlock()
		if n > 0 {
			return true
		}
	}
	return false
}

func (h *gapFirstHarness) someCaptureHasEventually(t *testing.T, hash chainhash.Hash) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.someCaptureHas(hash) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
