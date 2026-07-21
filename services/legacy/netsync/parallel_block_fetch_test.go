// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Behavioural tests for svnode-style additive PARALLEL BLOCK FETCH
// (block_parallel_fetch.go + the checkHeadStall race branch in manager.go).
//
// The sibling parallel_fetch_test.go pins the tracker-helper invariants and the
// basic addRacerForHead gates. THIS file drives the end-to-end svnode contract
// that matters on a live financial node:
//
//  1. a slow frontier holder UNDER the rate floor is RACED (an additional getdata
//     goes to another peer) while the ORIGINAL holder is KEPT — concurrent, not
//     free-and-reassign;
//  2. the number of peers racing one hash never exceeds legacy_maxBlockParallelFetch;
//  3. a holder ABOVE the floor is "still delivering" and is NOT raced (svnode's
//     speed gate);
//  4. FIRST-WINS: the delivering peer's block clears ALL racer tracking for that
//     hash, and a LATE copy from a losing racer is a harmless silent IBD drop —
//     never a disconnect, never a second commit (the critical correctness test);
//  5. legacy_maxBlockParallelFetch=1 is the byte-identical rollback: checkHeadStall
//     runs today's proven F1 MOVE (freeHeadBlockForRace), never an additive race;
//  6. the head-of-line disconnect backstop still fires for a genuinely DEAD frontier
//     block even with racing enabled — parallel fetch is layered ON TOP of the
//     ultimate liveness catch, it does not replace it.
//
// These reuse the package harnesses named in the task: buildRacerManager /
// primeSlowUnderFloor (parallel_fetch_test.go), buildHeadStallManager /
// assignForStall (checkheadstall_frontier_test.go / stall_timeout_test.go), the
// blockchain2.Mock FSM pattern (blocks_received_counter_test.go), and
// CreateBaseTestSettings.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// primeMatureAboveFloor seeds the peer's byte baseline so peerDownloadRate reports
// a MATURE rate at or above the floor, exactly the way headstall_rate_test.go
// manufactures a "still delivering" peer: a real handshake byte total `current`,
// a baseline of 0 taken one full maturity window ago, and the floor pinned at
// current/2 so the computed rate (current bytes over ~1s) clears it. Returns the
// live byte total so the caller can set the matching floor.
func primeMatureAboveFloor(sm *SyncManager, p *peer.Peer, now time.Time) uint64 {
	st, _ := sm.peerStates.Get(p)
	current := p.AssociationReadBytes()
	st.lastAssocReadBytes.Store(0)
	st.lastAssocSampleAt.Store(now.Add(-headStallRateWindow).UnixNano())
	return current
}

// primeMatureDead seeds the peer's byte baseline so peerDownloadRate reports a
// MATURE rate of ZERO (moved no bytes across a wide window): baseline == live byte
// total, taken well past the maturity window. This is svnode's genuinely-dead
// holder — the F2 gate must let the backstop disconnect it.
func primeMatureDead(sm *SyncManager, p *peer.Peer, now time.Time) {
	st, _ := sm.peerStates.Get(p)
	st.lastAssocReadBytes.Store(p.AssociationReadBytes())
	st.lastAssocSampleAt.Store(now.Add(-10 * time.Second).UnixNano())
}

// TestPBF_FrontierUnderFloorRacesAndKeepsHolder is scenario 1. Past
// BlockSlowFetchTimeout with the sole holder measurably slow (under the floor),
// addRacerForHead issues an additional getdata to a second peer and KEEPS the
// original holder in flight — the concurrent race, not F1's free-and-reassign.
//
// RED verification (done manually, reported): flipping the trigger in this test —
// either dropping headStallSince below BlockSlowFetchTimeout, or priming peerA
// ABOVE the floor instead of under it — makes the "two holders" assertion fail,
// confirming the test is not vacuous.
func TestPBF_FrontierUnderFloorRacesAndKeepsHolder(t *testing.T) {
	sm, peerA, _, _, head := buildRacerManager(t)
	now := time.Now()
	sm.headStallHash = head
	sm.headStallSince = now.Add(-31 * time.Second) // past the 30s slow-fetch threshold
	primeSlowUnderFloor(sm, peerA, now)            // holder is mature-and-slow (rate 0 < floor)

	raced := sm.addRacerForHead(head, 100, now)
	require.True(t, raced, "a slow frontier past the slow-fetch threshold must be raced (suppresses the F1 move)")

	holders := sm.racersOf(head)
	require.Len(t, holders, 2, "racing must ADD a second holder concurrently")

	// Identify the holders by POINTER IDENTITY only. A live *peer.Peer must never be
	// compared with require.Contains/Equal/ElementsMatch: testify falls back to a
	// reflect.DeepEqual that reads every struct field, which races the peer's own I/O
	// goroutine atomically bumping its byte counters (caught by -race here). Exactly
	// one distinct new peer must appear, and its per-peer ledger must authorise the
	// future delivery (proving the additional getdata actually went out —
	// addRacerForHead only sets requestedBlocks after TryQueueMessage succeeds).
	var (
		keptOriginal bool
		newPeer      *peer.Peer
	)
	for _, p := range holders {
		if p == peerA {
			keptOriginal = true
		} else {
			newPeer = p
		}
	}
	require.True(t, keptOriginal, "the ORIGINAL holder must be KEPT in flight (concurrent, not free-and-reassign)")
	require.NotNil(t, newPeer, "a distinct additional racer peer must have been chosen")

	newState, ok := sm.peerStates.Get(newPeer)
	require.True(t, ok)
	_, authorised := newState.requestedBlocks.Get(head)
	require.True(t, authorised, "the additional racer's per-peer ledger must authorise its delivery")

	// The original holder's authorisation is untouched: it is still racing, not freed.
	stateA, _ := sm.peerStates.Get(peerA)
	_, aStill := stateA.requestedBlocks.Get(head)
	require.True(t, aStill, "the original holder's per-peer ledger must remain set (it is kept, not freed)")

	// Loser-grace stamped so a late copy from whoever loses is a harmless no-op.
	sm.headerMu.Lock()
	_, grace := sm.recentlyNeededUntil[head]
	sm.headerMu.Unlock()
	require.True(t, grace, "the loser-grace TTL must be stamped for the raced hash")
}

// TestPBF_ParallelFetchCountNeverExceedsCap is scenario 2, at a NON-default cap so
// it pins the cap to legacy_maxBlockParallelFetch specifically (not the clamp).
// With MaxBlockParallelFetch=2 and two peers already racing, no third getdata is
// issued even though a spare eligible peer exists and the block is well past the
// slow-fetch threshold.
func TestPBF_ParallelFetchCountNeverExceedsCap(t *testing.T) {
	sm, peerA, peerB, peerC, head := buildRacerManager(t)
	sm.settings.Legacy.MaxBlockParallelFetch = 2 // cap of two racers per hash
	require.Equal(t, 2, sm.clampedMaxParallelFetch(), "precondition: effective cap is 2")

	now := time.Now()

	// Two peers already race the hash (at the cap). peerC is a spare eligible peer.
	sm.addAssignment(head, peerB, now)
	stateB, _ := sm.peerStates.Get(peerB)
	stateB.requestedBlocks.Set(head, struct{}{})
	require.Len(t, sm.racersOf(head), 2, "precondition: at the cap of 2")

	sm.headStallHash = head
	sm.headStallSince = now.Add(-31 * time.Second)
	// Leave holders unprimed: an immature measurement is NOT treated as "still
	// delivering", so the active-holder gate does not short-circuit and the CAP gate
	// is the deciding gate under test.

	raced := sm.addRacerForHead(head, 100, now)
	require.True(t, raced, "at the cap addRacerForHead must suppress the move")
	require.Len(t, sm.racersOf(head), 2, "the racer count for one hash must never exceed legacy_maxBlockParallelFetch")

	// The spare eligible peer was NOT recruited.
	stateC, _ := sm.peerStates.Get(peerC)
	_, cGot := stateC.requestedBlocks.Get(head)
	require.False(t, cGot, "no additional peer may be recruited beyond the cap")
	_ = peerA
}

// TestPBF_HolderAboveFloorNotRaced is scenario 3 — svnode's speed gate. Past the
// slow-fetch threshold, but the sole holder is provably still delivering (mature
// rate at/above the floor). addRacerForHead must break out WITHOUT racing (svnode
// waits on a live transfer rather than spending a second peer's bandwidth on a
// duplicate of a fat block that is genuinely mid-flight).
func TestPBF_HolderAboveFloorNotRaced(t *testing.T) {
	sm, peerA, _, _, head := buildRacerManager(t)
	now := time.Now()
	sm.headStallHash = head
	sm.headStallSince = now.Add(-31 * time.Second) // past the slow-fetch threshold

	current := primeMatureAboveFloor(sm, peerA, now)
	require.Greater(t, current, uint64(1), "precondition: the connected peer read handshake bytes to manufacture a rate")
	sm.settings.Legacy.BlockStallMinRate = int64(current / 2) // floor below the manufactured rate

	raced := sm.addRacerForHead(head, 100, now)
	require.True(t, raced, "a holder still delivering above the floor suppresses the move (svnode waits)")
	require.Len(t, sm.racersOf(head), 1, "a holder above the rate floor must NOT be raced")
}

// TestPBF_FirstWinsClearsTrackingAndLateLoserDropped is scenario 4 — the critical
// correctness test. One block is raced to a winner and a loser. When the winner
// delivers it (through handleBlockPreamble, the first-delivery-wins site):
//   - every racer's tracking for that hash is cleared (assignedTo entry gone, the
//     global ledger cleared, the loser's per-peer ledger swept), so a later tick
//     cannot re-race a block that has already arrived; and
//   - the loser's subsequent LATE copy hits the IBD unrequested-block drop: it is
//     dropped silently (a benign ServiceError), the peer is NOT disconnected, and
//     — because the preamble returns before any processing — it can never be
//     committed a second time.
func TestPBF_FirstWinsClearsTrackingAndLateLoserDropped(t *testing.T) {
	initPrometheusMetrics()

	chainParams := chaincfg.MainNetParams

	// A deterministic block; its own hash is the raced frontier hash.
	prevHash := chainhash.Hash{0xab}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	head := msgBlock.Header.BlockHash()

	// FSM = CATCHINGBLOCKS so the losing racer's late copy takes the IBD tolerate-
	// and-drop branch (the first-wins reconciliation the design leans on).
	catching := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catching, nil)

	// The getdata capture is unused here (this test drives the delivery side, not
	// the fetch side) but captureGetDataPeer gives us genuinely-connected peers whose
	// Connected() flag flips on a real disconnect — the exact signal scenario 4 asserts.
	var (
		muW, muL   sync.Mutex
		gotW, gotL []*wire.MsgGetData
	)
	winner := captureGetDataPeer(t, &chainParams, 80, &muW, &gotW)
	loser := captureGetDataPeer(t, &chainParams, 81, &muL, &gotL)
	winnerState := newEligiblePeerState()
	loserState := newEligiblePeerState()
	t.Cleanup(func() {
		winnerState.requestedBlocks.Stop()
		winnerState.requestedTxns.Stop()
		loserState.requestedBlocks.Stop()
		loserState.requestedTxns.Stop()
	})

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         test.CreateBaseTestSettings(t), // TolerateUnrequestedBlocksInIBD defaults true
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:       make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:    make(map[chainhash.Hash]struct{}),
		// chainParams left nil: keeps the UsePrefetchIngestion disconnected-peer
		// pre-check out of the way and the RegressionNetParams guard permissive.
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(winner, winnerState)
	sm.peerStates.Set(loser, loserState)

	// Both peers race the same head block: assignedTo has both, both per-peer ledgers
	// authorise it, and the global ledger is set (>= 1 racer holds it).
	now := time.Now()
	sm.addAssignment(head, winner, now)
	sm.addAssignment(head, loser, now)
	winnerState.requestedBlocks.Set(head, struct{}{})
	loserState.requestedBlocks.Set(head, struct{}{})
	sm.requestedBlocks.Set(head, struct{}{})
	require.Len(t, sm.racersOf(head), 2, "precondition: two peers race the frontier block")

	// --- WINNER delivers first ---
	_, _, _, _, _, err := sm.handleBlockPreamble("test-winner", &blockQueueMsg{
		block:       msgBlock,
		blockHash:   head,
		blockHeight: 2,
		peer:        winner,
	})
	require.NoError(t, err, "the winning delivery must be accepted")

	// All racer tracking for the hash is cleared — no later tick can re-race it.
	require.Empty(t, sm.racersOf(head), "first delivery must clear the whole {hash -> racers} entry")
	_, globalStill := sm.requestedBlocks.Get(head)
	require.False(t, globalStill, "the global ledger must be cleared once the block has arrived")
	_, winnerStill := winnerState.requestedBlocks.Get(head)
	require.False(t, winnerStill, "the delivering peer's per-peer ledger must be cleared")
	_, loserStill := loserState.requestedBlocks.Get(head)
	require.False(t, loserStill, "the LOSER's per-peer ledger must be swept so its late copy fails the auth gate")
	require.NotContains(t, sm.refetchBlocks, head, "an arrived block must not be queued for re-fetch")

	require.True(t, loser.Connected(), "precondition for the late-copy test: the loser is still connected")

	// --- LOSER delivers the SAME block late ---
	_, _, _, _, _, lateErr := sm.handleBlockPreamble("test-loser", &blockQueueMsg{
		block:       msgBlock,
		blockHash:   head,
		blockHeight: 2,
		peer:        loser,
	})
	require.Error(t, lateErr, "the losing racer's late copy must be rejected by the preamble (no second commit)")
	require.Contains(t, lateErr.Error(), "dropped unrequested block",
		"the late loser copy must hit the IBD silent-drop path, not the disconnect path")
	require.True(t, loser.Connected(), "the losing racer must NOT be disconnected for a late duplicate")

	blockchainClient.AssertExpectations(t)
}

// TestPBF_RollbackMaxParallelOneUsesMoveNotRace is scenario 5 — the byte-identical
// rollback lever. With legacy_maxBlockParallelFetch=1, checkHeadStall's race branch
// must reduce to the pre-parallel-fetch F1 MOVE (freeHeadBlockForRace): the single
// stalled head block is freed and re-enqueued for re-fetch, NO additive getdata is
// ever issued, and no second peer ends up holding the hash.
func TestPBF_RollbackMaxParallelOneUsesMoveNotRace(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, head := buildHeadStallManager(t, headHeight)

	// Enable the F1 race WINDOW (default) but disable additive racing (the rollback).
	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	sm.settings.Legacy.MaxBlockParallelFetch = 1
	require.Equal(t, 1, sm.clampedMaxParallelFetch(), "precondition: additive racing disabled")

	// Frontier stall so the self-backpressure suppression is carved out and the head
	// block is aged past BlockStallTimeout but fresh within the race window.
	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	stateB, _ := sm.peerStates.Get(peerB)

	sm.checkHeadStall(time.Now())

	// F1 move: the head block is freed from its single holder and re-enqueued.
	require.Empty(t, sm.racersOf(head), "the move frees the head block from its single holder (no additive second holder)")
	require.Contains(t, sm.refetchBlocks, head, "the F1 move must re-enqueue the head block for re-fetch")
	sm.headerMu.Lock()
	_, grace := sm.recentlyNeededUntil[head]
	sm.headerMu.Unlock()
	require.True(t, grace, "the F1 move must stamp the loser-grace TTL (unchanged behaviour)")

	// No additive race happened: peerB was never recruited, and neither peer was
	// disconnected (the move keeps the stalled holder connected).
	_, bGotHead := stateB.requestedBlocks.Get(head)
	require.False(t, bGotHead, "with racing disabled no additional getdata may reach another peer")
	require.True(t, peerA.Connected(), "the move must keep the stalled holder connected")
	require.True(t, peerB.Connected(), "the non-head peer must be untouched")
}

// TestPBF_HeadOfLineBackstopFiresWhenNoRacerDelivers is scenario 6 — the ultimate
// liveness catch. Racing is ENABLED (default cap 3), but the frontier block is
// genuinely DEAD on its holder (mature rate 0, under the floor) and the race window
// has already expired. checkHeadStall must still disconnect the single head peer
// and free its assignments — parallel fetch is layered on top of this backstop, it
// must not disable it.
func TestPBF_HeadOfLineBackstopFiresWhenNoRacerDelivers(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, head := buildHeadStallManager(t, headHeight)

	sm.settings.Legacy.HeadStallDisconnectTimeout = 30 * time.Second
	// Racing enabled (default MaxBlockParallelFetch=3 from CreateBaseTestSettings).
	require.Greater(t, sm.clampedMaxParallelFetch(), 1, "precondition: additive racing is enabled")

	// Frontier stall (no backpressure suppression).
	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	now := time.Now()

	// The head block has been stalled longer than the whole race window, so the tick
	// lands past the race branch on the residual F2/disconnect path.
	sm.headStallHash = head
	sm.headStallSince = now.Add(-(30*time.Second + time.Second))

	// The holder is genuinely dead: mature measurement, zero bytes moved, under the
	// default 100 KB/s floor -> the F2 throughput gate cannot veto the disconnect.
	require.Greater(t, peerA.AssociationReadBytes(), uint64(1),
		"precondition: the connected peer read handshake bytes so a zero rate is a real measurement")
	primeMatureDead(sm, peerA, now)

	sm.checkHeadStall(now)

	require.Eventually(t, func() bool { return !peerA.Connected() }, 2*time.Second, 5*time.Millisecond,
		"a dead frontier block that no racer could deliver must still disconnect the head peer (backstop intact)")
	require.True(t, peerB.Connected(), "only the head peer is disconnected; the backstop never mass-disconnects")

	_, stillAssigned := sm.assignedTo[head]
	require.False(t, stillAssigned, "the disconnected head peer's assignment must be freed")
	require.Equal(t, chainhash.Hash{}, sm.headStallHash, "the head-stall tracker must reset after the disconnect")
}

// TestPBF_CheckHeadStallRacesAtDefaultTimeouts is the regression lock-in for the
// "additive race unreachable at shipped defaults" defect: with the DEFAULT
// BlockSlowFetchTimeout and HeadStallDisconnectTimeout, checkHeadStall itself
// (not addRacerForHead called directly) must reach the race branch AND satisfy
// addRacerForHead's age gate, adding a concurrent second racer.
//
// The sibling scenarios all bypass checkHeadStall's upper race-window gate by
// calling addRacerForHead directly with headStallSince pinned to -31s, so none of
// them would catch a BlockSlowFetchTimeout defaulted equal to (or above) the
// disconnect window — the case where the race branch (now-headStallSince <=
// disconnect) and the age gate (now-headStallSince >= slow-fetch) share no usable
// interval and racing silently never happens. This drives the real entry point at
// the real defaults and asserts a racer appears.
//
// It deliberately does NOT override BlockSlowFetchTimeout: the whole point is the
// SHIPPED default. The precondition require below fails loudly if a future change
// pushes the default back to (or past) the disconnect window and re-closes the
// window. RED on the pre-fix defaults (slow-fetch 30s == disconnect 30s): age
// ~22s < 30s -> addRacerForHead falls through to the F1 move, racersOf stays at 1
// and the head is re-enqueued for re-fetch instead of raced.
func TestPBF_CheckHeadStallRacesAtDefaultTimeouts(t *testing.T) {
	const headHeight = 100
	sm, peerA, peerB, head := buildHeadStallManager(t, headHeight)

	// Restore the DEFAULT disconnect window (buildHeadStallManager zeroes it for the
	// immediate-disconnect carve-out tests). BlockSlowFetchTimeout is left at its
	// CreateBaseTestSettings/NewSettings default on purpose.
	const disconnectTimeout = 30 * time.Second
	sm.settings.Legacy.HeadStallDisconnectTimeout = disconnectTimeout

	slowFetch := sm.settings.Legacy.BlockSlowFetchTimeout
	require.Greater(t, sm.clampedMaxParallelFetch(), 1, "precondition: additive racing is enabled at defaults")
	require.Less(t, slowFetch, disconnectTimeout,
		"REGRESSION GUARD: the slow-fetch default must be strictly below the disconnect window, or the race branch and the age gate share no interval and racing is unreachable")

	// Frontier stall so the self-backpressure suppression is carved out (head is the
	// exact block the committer needs next).
	sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
	sm.baHeightPolled.Store(true)

	now := time.Now()

	// Age the head stall to the MIDDLE of the overlap window [slowFetch, disconnect]:
	// past the age gate (raceable) yet inside the race branch (not yet disconnecting).
	// Pre-arm headStallHash so checkHeadStall keeps this headStallSince rather than
	// resetting it to now on the hash-changed branch. peerA is left unprimed, so its
	// rate measurement is immature and it does NOT count as "still delivering" — it
	// stays eligible to be raced.
	overlapMid := slowFetch + (disconnectTimeout-slowFetch)/2
	sm.headStallHash = head
	sm.headStallSince = now.Add(-overlapMid)

	stateB, _ := sm.peerStates.Get(peerB)
	_, bHadHeadBefore := stateB.requestedBlocks.Get(head)
	require.False(t, bHadHeadBefore, "precondition: the spare peer is not already racing the head block")

	sm.checkHeadStall(now)

	// The additive race fired THROUGH checkHeadStall at default timeouts: a second
	// racer was added concurrently and the original holder was KEPT.
	holders := sm.racersOf(head)
	require.Len(t, holders, 2, "checkHeadStall must add a concurrent second racer at default timeouts (race, not move)")

	var keptOriginal, recruitedSpare bool
	for _, p := range holders {
		switch p {
		case peerA:
			keptOriginal = true
		case peerB:
			recruitedSpare = true
		}
	}
	require.True(t, keptOriginal, "the original head holder must be KEPT in flight (concurrent race)")
	require.True(t, recruitedSpare, "the spare eligible peer must be recruited as the additional racer")

	_, bGotHead := stateB.requestedBlocks.Get(head)
	require.True(t, bGotHead, "the additional racer's per-peer ledger must authorise its delivery")

	// The move was SUPPRESSED: the head block was not freed and re-enqueued.
	require.NotContains(t, sm.refetchBlocks, head, "additive racing must suppress the F1 move (no re-enqueue)")

	// Loser-grace stamped so a late copy from whichever racer loses is a harmless no-op.
	sm.headerMu.Lock()
	_, grace := sm.recentlyNeededUntil[head]
	sm.headerMu.Unlock()
	require.True(t, grace, "the race must stamp the loser-grace TTL for the raced hash")

	require.True(t, peerA.Connected(), "racing keeps the slow holder connected")
	require.True(t, peerB.Connected(), "the recruited racer is connected")
}
