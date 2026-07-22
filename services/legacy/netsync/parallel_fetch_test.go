// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for svnode-style additive PARALLEL BLOCK FETCH (block_parallel_fetch.go):
// the multi-in-flight tracker helpers, the two-speed head-stall trigger
// (addRacerForHead), and the multi-peer-aware orphan recovery
// (reconcileLostAssignments / freePeerAssignments). The delivery side (first-wins)
// is exercised through the existing handleBlockPreamble path elsewhere.

import (
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

// TestTracker_HelperRoundTrip locks in the load-bearing invariant "outer key present
// iff inner non-empty iff >= 1 racer" across the four helpers, at multiplicity > 1.
func TestTracker_HelperRoundTrip(t *testing.T) {
	sm := &SyncManager{assignedTo: make(map[chainhash.Hash]map[*peer.Peer]time.Time)}

	h := chainhash.Hash{0x01}
	p1 := &peer.Peer{}
	p2 := &peer.Peer{}
	p3 := &peer.Peer{}
	now := time.Now()

	// Add three racers for the same hash.
	sm.addAssignment(h, p1, now)
	sm.addAssignment(h, p2, now.Add(time.Second))
	sm.addAssignment(h, p3, now.Add(2*time.Second))
	require.Len(t, sm.racersOf(h), 3, "three distinct peers must all be recorded for the same hash")

	// Removing one leaves the hash in flight.
	still := sm.removeAssignment(h, p2)
	require.True(t, still, "removing one of three racers must report the hash still in flight")
	require.Len(t, sm.racersOf(h), 2)
	_, present := sm.assignedTo[h]
	require.True(t, present, "outer key must remain while another racer holds the hash")

	// clearAssignment returns the remaining racers and empties the entry.
	racers := sm.clearAssignment(h)
	require.ElementsMatch(t, []*peer.Peer{p1, p3}, racers, "clearAssignment must return every remaining racer")
	_, present = sm.assignedTo[h]
	require.False(t, present, "outer key must be gone once cleared")

	// Removing the last holder empties the inner map and deletes the outer key.
	sm.addAssignment(h, p1, now)
	still = sm.removeAssignment(h, p1)
	require.False(t, still, "removing the last holder must report not-in-flight")
	_, present = sm.assignedTo[h]
	require.False(t, present, "outer key must be deleted when the inner map empties")
}

// TestClampedMaxParallelFetch verifies the clamp to min(peers, per-peer budget) and
// the >=1 floor (the rollback lever: 1 disables racing).
func TestClampedMaxParallelFetch(t *testing.T) {
	mk := func(maxFetch, peers, perPeer int) int {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Legacy.MaxBlockParallelFetch = maxFetch
		tSettings.Legacy.ParallelFetchPeers = peers
		tSettings.Legacy.MaxBlocksInTransitPerPeer = perPeer
		sm := &SyncManager{settings: tSettings}
		return sm.clampedMaxParallelFetch()
	}

	require.Equal(t, 3, mk(3, 5, 16), "default 3 with ample peers/budget stays 3")
	require.Equal(t, 2, mk(3, 2, 16), "clamped down to ParallelFetchPeers when that is the limit")
	require.Equal(t, 1, mk(3, 5, 1), "clamped down to MaxBlocksInTransitPerPeer when that is the limit")
	require.Equal(t, 1, mk(1, 5, 16), "MaxBlockParallelFetch=1 is the rollback: racing disabled")
	require.Equal(t, 1, mk(0, 5, 16), "a nonsensical 0 floors to 1")
}

// buildRacerManager wires the minimal SyncManager addRacerForHead needs: three
// connected eligible fetch peers, the head block already in flight to peerA, and the
// race knobs set to their svnode-faithful defaults. headStallSince is left to the
// caller to place the head above or below BlockSlowFetchTimeout.
func buildRacerManager(t *testing.T) (*SyncManager, *peer.Peer, *peer.Peer, *peer.Peer, chainhash.Hash) {
	t.Helper()

	chainParams := chaincfg.MainNetParams

	var (
		muA, muB, muC    sync.Mutex
		gotA, gotB, gotC []*wire.MsgGetData
	)
	peerA := captureGetDataPeer(t, &chainParams, 70, &muA, &gotA)
	peerB := captureGetDataPeer(t, &chainParams, 71, &muB, &gotB)
	peerC := captureGetDataPeer(t, &chainParams, 72, &muC, &gotC)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.MaxBlockParallelFetch = 3
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 16
	tSettings.Legacy.BlockSlowFetchTimeout = 30 * time.Second
	tSettings.Legacy.BlockStallMinRate = 102400

	stateA := newEligiblePeerState()
	stateB := newEligiblePeerState()
	stateC := newEligiblePeerState()
	t.Cleanup(func() {
		for _, st := range []*peerSyncState{stateA, stateB, stateC} {
			st.requestedBlocks.Stop()
			st.requestedTxns.Stop()
		}
	})

	sm := &SyncManager{
		ctx:             context.Background(),
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		chainParams:     &chainParams,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:      make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(peerA, stateA)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)

	head := chainhash.Hash{0x11}
	sm.addAssignment(head, peerA, time.Now())
	stateA.requestedBlocks.Set(head, struct{}{})
	sm.requestedBlocks.Set(head, struct{}{})

	return sm, peerA, peerB, peerC, head
}

// primeSlowUnderFloor makes peerDownloadRate report the peer as mature-but-slow
// (rate 0 < floor): a baseline older than the 1s maturity window, with the stored
// byte total equal to the live one so the delta is zero.
func primeSlowUnderFloor(sm *SyncManager, p *peer.Peer, now time.Time) {
	st, _ := sm.peerStates.Get(p)
	st.lastAssocReadBytes.Store(p.AssociationReadBytes())
	st.lastAssocSampleAt.Store(now.Add(-2 * time.Second).UnixNano())
}

// TestAddRacerForHead_SubSlowFetchTimeoutFallsThroughToMove: below
// BlockSlowFetchTimeout the head is NOT raced; addRacerForHead returns false so the
// caller runs today's proven sequential MOVE.
func TestAddRacerForHead_SubSlowFetchTimeoutFallsThroughToMove(t *testing.T) {
	sm, _, _, _, head := buildRacerManager(t)
	now := time.Now()
	sm.headStallHash = head
	sm.headStallSince = now.Add(-5 * time.Second) // only 5s stalled, < 30s slow-fetch

	raced := sm.addRacerForHead(head, 100, now)

	require.False(t, raced, "below BlockSlowFetchTimeout addRacerForHead must fall through to the move")
	require.Len(t, sm.racersOf(head), 1, "no additional racer may be added below the slow-fetch threshold")
}

// TestAddRacerForHead_DisabledFloorFallsThroughToMove: with the rate gate disabled
// (BlockStallMinRate=0) racing cannot classify a peer, so it falls through to the
// move (svnode does no parallel fetch with the floor off).
func TestAddRacerForHead_DisabledFloorFallsThroughToMove(t *testing.T) {
	sm, _, _, _, head := buildRacerManager(t)
	sm.settings.Legacy.BlockStallMinRate = 0
	now := time.Now()
	sm.headStallHash = head
	sm.headStallSince = now.Add(-40 * time.Second) // well past slow-fetch, but floor off

	raced := sm.addRacerForHead(head, 100, now)

	require.False(t, raced, "with the rate floor disabled addRacerForHead must fall through to the move")
	require.Len(t, sm.racersOf(head), 1, "no racer may be added when the rate floor is disabled")
}

// TestAddRacerForHead_RacesUnderFloorHolder: past BlockSlowFetchTimeout with the sole
// holder mature-and-slow, one ADDITIONAL racer is added to a distinct eligible peer,
// the original holder is KEPT, the new peer's per-peer ledger authorises its future
// delivery, and the loser-grace TTL is stamped.
func TestAddRacerForHead_RacesUnderFloorHolder(t *testing.T) {
	sm, peerA, _, _, head := buildRacerManager(t)
	now := time.Now()
	sm.headStallHash = head
	sm.headStallSince = now.Add(-31 * time.Second) // past the 30s slow-fetch threshold
	primeSlowUnderFloor(sm, peerA, now)            // holder is measurably slow

	raced := sm.addRacerForHead(head, 100, now)

	require.True(t, raced, "a slow head past the slow-fetch threshold must be raced (suppresses the move)")

	holders := sm.racersOf(head)
	require.Len(t, holders, 2, "racing must ADD a holder, keeping the original")

	// The original holder is still in flight; exactly one distinct new peer was added.
	var newPeer *peer.Peer
	for _, p := range holders {
		if p != peerA {
			newPeer = p
		}
	}
	require.NotNil(t, newPeer, "a distinct new racer peer must have been chosen")
	require.Contains(t, holders, peerA, "the original holder must be kept in flight")

	newState, ok := sm.peerStates.Get(newPeer)
	require.True(t, ok)
	_, authorised := newState.requestedBlocks.Get(head)
	require.True(t, authorised, "the new racer's per-peer ledger must authorise its delivery")

	sm.headerMu.Lock()
	_, grace := sm.recentlyNeededUntil[head]
	sm.headerMu.Unlock()
	require.True(t, grace, "the loser-grace TTL must be stamped so a late loser copy is a harmless no-op")
}

// TestAddRacerForHead_CapGate: once the head is already raced across clampedMax peers,
// no further racer is added even past the slow-fetch threshold.
func TestAddRacerForHead_CapGate(t *testing.T) {
	sm, peerA, peerB, peerC, head := buildRacerManager(t)
	now := time.Now()

	// All three peers already hold it (at the cap of 3).
	sm.addAssignment(head, peerB, now)
	sm.addAssignment(head, peerC, now)
	require.Len(t, sm.racersOf(head), 3, "precondition: at the cap")

	sm.headStallHash = head
	sm.headStallSince = now.Add(-31 * time.Second)
	// Leave holders unprimed: peerDownloadRate reports no baseline (not mature), so the
	// active-holder gate does not short-circuit and the CAP gate is the deciding gate.

	raced := sm.addRacerForHead(head, 100, now)

	require.True(t, raced, "at the cap addRacerForHead must suppress the move")
	require.Len(t, sm.racersOf(head), 3, "no racer may be added beyond clampedMax")
	_ = peerA
}

// TestReconcileLostAssignments_PerPeerStale: with two holders of one hash, only the
// STALE holder is dropped; the fresh sibling keeps the hash in flight (not
// re-enqueued, global ledger intact).
func TestReconcileLostAssignments_PerPeerStale(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.BlockInFlightTimeout = 10 * time.Second
	// reconcileLostAssignments uses max(BlockInFlightTimeout, IBDBlockStallTimeout);
	// zero the IBD extension so this unit test exercises the 10s stale boundary.
	tSettings.Legacy.IBDBlockStallTimeout = 0

	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:      make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	stalePeer := &peer.Peer{}
	freshPeer := &peer.Peer{}
	staleState := newEligiblePeerState()
	freshState := newEligiblePeerState()
	t.Cleanup(func() {
		staleState.requestedBlocks.Stop()
		staleState.requestedTxns.Stop()
		freshState.requestedBlocks.Stop()
		freshState.requestedTxns.Stop()
	})
	sm.peerStates.Set(stalePeer, staleState)
	sm.peerStates.Set(freshPeer, freshState)

	h := chainhash.Hash{0x22}
	now := time.Now()
	sm.addAssignment(h, stalePeer, now.Add(-30*time.Second)) // stale (> 10s)
	sm.addAssignment(h, freshPeer, now)                      // fresh
	sm.requestedBlocks.Set(h, struct{}{})
	staleState.requestedBlocks.Set(h, struct{}{})
	freshState.requestedBlocks.Set(h, struct{}{})

	sm.reconcileLostAssignments(now)

	holders := sm.racersOf(h)
	require.Len(t, holders, 1, "only the stale holder must be dropped")
	require.Equal(t, freshPeer, holders[0], "the fresh holder must remain")
	require.NotContains(t, sm.refetchBlocks, h, "a hash still held by a live racer must NOT be re-enqueued")
	_, tracked := sm.requestedBlocks.Get(h)
	require.True(t, tracked, "the global ledger must stay set while a racer still holds the hash")
	_, staleHas := staleState.requestedBlocks.Get(h)
	require.False(t, staleHas, "the stale holder's per-peer ledger must be cleared")
	_, freshHas := freshState.requestedBlocks.Get(h)
	require.True(t, freshHas, "the fresh holder's per-peer ledger must be untouched")
}

// TestFreePeerAssignments_LastHolderRule (the subtlest edit): dropping one of two
// racers keeps the hash in flight; dropping the second finally re-enqueues it.
func TestFreePeerAssignments_LastHolderRule(t *testing.T) {
	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:      make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	peerA := &peer.Peer{}
	peerB := &peer.Peer{}
	stateA := newEligiblePeerState()
	stateB := newEligiblePeerState()
	t.Cleanup(func() {
		stateA.requestedBlocks.Stop()
		stateA.requestedTxns.Stop()
		stateB.requestedBlocks.Stop()
		stateB.requestedTxns.Stop()
	})
	sm.peerStates.Set(peerA, stateA)
	sm.peerStates.Set(peerB, stateB)

	h := chainhash.Hash{0x33}
	now := time.Now()
	sm.addAssignment(h, peerA, now)
	sm.addAssignment(h, peerB, now)
	sm.requestedBlocks.Set(h, struct{}{})
	stateA.requestedBlocks.Set(h, struct{}{})
	stateB.requestedBlocks.Set(h, struct{}{})

	// Drop peerA: peerB still holds it, so the hash stays in flight.
	sm.freePeerAssignments(peerA)

	require.Len(t, sm.racersOf(h), 1, "peerB must still hold the hash after peerA leaves")
	require.NotContains(t, sm.refetchBlocks, h, "a hash still held by peerB must NOT be re-enqueued")
	_, tracked := sm.requestedBlocks.Get(h)
	require.True(t, tracked, "the global ledger must stay set while peerB holds the hash")
	_, aHas := stateA.requestedBlocks.Get(h)
	require.False(t, aHas, "the departing peer's per-peer ledger must be cleared")

	// Drop peerB: now nobody holds it, so it must be re-enqueued and globally cleared.
	sm.freePeerAssignments(peerB)

	require.Empty(t, sm.racersOf(h), "no holder remains")
	require.Contains(t, sm.refetchBlocks, h, "the last-holder departure must re-enqueue the hash for re-fetch")
	_, tracked = sm.requestedBlocks.Get(h)
	require.False(t, tracked, "the global ledger must be cleared once nobody holds the hash")
}
