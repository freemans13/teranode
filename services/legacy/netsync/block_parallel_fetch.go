// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// This file implements svnode-style PARALLEL BLOCK FETCH: racing the same slow
// FRONTIER/head block across several peers, first-delivery-wins, so a single slow
// peer holding the next-needed (contiguous frontier) block can no longer gate the
// whole in-order commit pipeline. It is the additive (svnode) layer that sits on
// TOP of the existing F1 sequential move (freeHeadBlockForRace) and F2 throughput
// gate (headStallThroughputGate).
//
// THE BYTE-IDENTICAL ROLLBACK: everything here is inert unless BOTH
// ParallelFetchPeers > 1 (the outer master gate that already disables the entire
// multi-peer scheduler) AND legacy_maxBlockParallelFetch > 1 (the inner lever). At
// clampedMaxParallelFetch()==1 the race branch in checkHeadStall never calls
// addRacerForHead, the multi-in-flight tracker never holds more than one peer per
// hash, and every helper below reduces to the single delete/set the old
// map[hash]*peer performed. This is the fine-grained in-deployment rollback for a
// live financial node.
//
// THE MULTI-IN-FLIGHT TRACKER (sm.assignedTo, declared in manager.go) is the Go
// analog of svnode's BlockDownloadTracker keyed by {blockhash, nodeid}: the SAME
// hash can be in flight to MULTIPLE peers at once. It replaced the old single-owner
// map[hash]*peer + map[hash]time.Time pair; assignedAt was deleted outright so the
// compiler forces every reader to be revisited.
//
//	sm.assignedTo  map[chainhash.Hash]map[*peerpkg.Peer]time.Time
//	               hash -> (racer peer -> the time its getdata was sent)
//
// LOAD-BEARING INVARIANT enforced ONLY through the helpers below so no reader
// open-codes multiplicity and drifts:
//
//	outer key present  iff  inner map non-empty  iff  >= 1 racer in flight for h
//	global sm.requestedBlocks[h] set  iff  >= 1 racer holds h
//
// GUARDING MUTEX: every helper here ASSUMES THE CALLER HOLDS sm.assignedMu (the
// same leaf lock that guarded the old assignedTo/assignedAt). assignedMu is never
// held across a peer send, a gRPC, or headerMu; lock order is headerMu -> assignedMu.

// addAssignment records that block h is now in flight to peer p, sent at time t.
// It lazily initialises the outer map and the per-hash inner map (mirroring the
// nil-guard at the assignBlocksAcrossPeers record site) because a nil-map WRITE
// panics, and minimal test-constructed SyncManagers may leave the map nil. Caller
// holds assignedMu. At clampedMax==1 only ever one peer per hash, so the inner map
// holds a single entry — byte-identical to the old assignedTo[h]=p assignment.
func (sm *SyncManager) addAssignment(h chainhash.Hash, p *peerpkg.Peer, t time.Time) {
	if sm.assignedTo == nil {
		sm.assignedTo = make(map[chainhash.Hash]map[*peerpkg.Peer]time.Time)
	}

	inner := sm.assignedTo[h]
	if inner == nil {
		inner = make(map[*peerpkg.Peer]time.Time)
		sm.assignedTo[h] = inner
	}

	inner[p] = t
}

// removeAssignment removes peer p from block h's in-flight set and reports whether
// ANY racer still holds h afterwards (stillInFlight). When the inner map empties it
// deletes the outer key, preserving the load-bearing invariant "outer key present
// iff >= 1 racer". Caller holds assignedMu.
//
// The stillInFlight return is the whole point of the multi-peer orphan-recovery
// edits: a caller (freePeerAssignments, reconcileLostAssignments) must NOT
// re-enqueue h to refetchBlocks nor drop the GLOBAL requestedBlocks entry while
// another live racer still holds h — doing so would either strand the block or let
// two peers fetch it. At clampedMax==1 the inner map had one entry, so this always
// empties it and returns false — byte-identical to the old delete(assignedTo,h).
func (sm *SyncManager) removeAssignment(h chainhash.Hash, p *peerpkg.Peer) (stillInFlight bool) {
	inner := sm.assignedTo[h]
	if inner == nil {
		return false
	}

	delete(inner, p)

	if len(inner) == 0 {
		delete(sm.assignedTo, h)
		return false
	}

	return true
}

// clearAssignment removes h entirely (all racers) and returns the peers that were
// racing it, so the delivery site can sweep every OTHER racer's per-peer
// requestedBlocks ledger on first-delivery-wins. Caller holds assignedMu. At
// clampedMax==1 the returned slice has at most one element — byte-identical to the
// old single delete(assignedTo,h).
func (sm *SyncManager) clearAssignment(h chainhash.Hash) []*peerpkg.Peer {
	inner := sm.assignedTo[h]
	if len(inner) == 0 {
		delete(sm.assignedTo, h)
		return nil
	}

	racers := make([]*peerpkg.Peer, 0, len(inner))
	for p := range inner {
		racers = append(racers, p)
	}

	delete(sm.assignedTo, h)

	return racers
}

// racersOf returns a snapshot of the peers currently racing h. The snapshot is a
// fresh slice of peer pointers, so it stays valid after the caller releases
// assignedMu (addRacerForHead reads it, releases the lock, then does I/O). Caller
// holds assignedMu.
func (sm *SyncManager) racersOf(h chainhash.Hash) []*peerpkg.Peer {
	inner := sm.assignedTo[h]
	if len(inner) == 0 {
		return nil
	}

	racers := make([]*peerpkg.Peer, 0, len(inner))
	for p := range inner {
		racers = append(racers, p)
	}

	return racers
}

// clampedMaxParallelFetch is the effective cap on how many peers may concurrently
// race ONE head block, clamped so it can never exceed the peers available or the
// per-peer in-flight budget:
//
//	clamp(MaxBlockParallelFetch, 1, min(ParallelFetchPeers, MaxBlocksInTransitPerPeer))
//
// Returning 1 means additive racing is disabled: checkHeadStall's race branch is
// guarded by clampedMax > 1, so at 1 addRacerForHead is never entered and the path
// is byte-identical to the pre-parallel-fetch sequential move (F1). This is the
// inner rollback lever (legacy_maxBlockParallelFetch=1) on top of the outer
// ParallelFetchPeers<=1 master gate.
func (sm *SyncManager) clampedMaxParallelFetch() int {
	m := sm.settings.Legacy.MaxBlockParallelFetch
	if m < 1 {
		m = 1
	}

	upper := sm.settings.Legacy.ParallelFetchPeers
	if k := sm.settings.Legacy.MaxBlocksInTransitPerPeer; k < upper {
		upper = k
	}

	if upper < 1 {
		upper = 1
	}

	if m > upper {
		m = upper
	}

	return m
}

// peerDownloadRate is the per-peer throughput sampler extracted VERBATIM from
// headStallThroughputGate's math so BOTH the F2 head-of-line disconnect gate and
// the new additive-race eligibility check read one identical measurement (svnode
// uses the single GetAverageBandwidth for both decisions). It samples the peer's
// association read-byte counter — byte-granular, spanning the association's streams
// so it advances while a large block is still arriving on DATA1 — against the
// per-peer atomic baseline (lastAssocReadBytes/lastAssocSampleAt).
//
// Returns (rateBytesPerSec, mature). mature is true ONLY when a usable baseline at
// least headStallRateWindow old existed and a real rate was computed; the two
// callers then apply OPPOSITE biases to the immature cases:
//
//   - headStallThroughputGate (keep-peer bias, byte-identical to before): a maturing
//     baseline means "still measuring, keep the peer" and NO baseline at all means
//     "unmeasured, must not veto the disconnect". These are distinguished by the
//     returned rate sentinel: rate == 0 for a maturing baseline (gate returns true),
//     rate == -1 for no usable baseline (gate returns false). A real computed rate
//     is always >= 0, and only ever returned with mature==true, so the sentinels
//     never collide with a genuine measurement.
//   - addRacerForHead (race bias): a holder counts as "still delivering" (do NOT
//     race it) ONLY when mature && rate >= floor; any immature holder is treated as
//     not-proven-alive and left eligible to race (mirrors svnode, where an
//     unknown/low bandwidth peer counts toward the staller set).
//
// SIDE EFFECTS mirror the original gate exactly: it resamples (stores current bytes
// + now) on the no-baseline and mature branches, and deliberately does NOT resample
// on the maturing branch (resampling every ~20ms refill tick would restart the
// window and it would never mature). Drain-goroutine only; writes the peer's sample
// fields atomically since peerSyncState has no lock.
func (sm *SyncManager) peerDownloadRate(peer *peerpkg.Peer, now time.Time) (rateBytesPerSec int64, mature bool) {
	if peer == nil {
		return -1, false
	}

	peerState, ok := sm.peerStates.Get(peer)
	if !ok {
		// The peer is gone from the state map; there is nothing to measure. Report
		// "no usable baseline" so the gate falls through to its existing behaviour.
		return -1, false
	}

	current := peer.AssociationReadBytes()
	prevAt := peerState.lastAssocSampleAt.Load()
	prev := peerState.lastAssocReadBytes.Load()

	// No usable baseline: never sampled, or the sample predates an unrelated stall
	// episode. Record one for next time and report it could not be measured (-1).
	if prevAt == 0 || now.Sub(time.Unix(0, prevAt)) > headStallRateSampleMaxAge {
		peerState.lastAssocReadBytes.Store(current)
		peerState.lastAssocSampleAt.Store(now.UnixNano())

		return -1, false
	}

	elapsed := now.Sub(time.Unix(0, prevAt))
	if elapsed < headStallRateWindow {
		// Baseline is still maturing. Leave it in place — resampling here would
		// restart the window on every tick and it would never mature. rate 0 with
		// mature==false is the "still measuring" signal (gate keeps the peer).
		return 0, false
	}

	// AssociationReadBytes sums over the streams present at sample time, so it can
	// DECREASE if a stream was torn down between samples. That is the opposite of
	// progress: treat it as zero throughput.
	var delta uint64
	if current > prev {
		delta = current - prev
	}

	rate := int64(float64(delta) / elapsed.Seconds())

	peerState.lastAssocReadBytes.Store(current)
	peerState.lastAssocSampleAt.Store(now.UnixNano())

	return rate, true
}

// addRacerForHead is the svnode additive parallel-fetch trigger (mirrors
// net_processing.cpp FindNextBlocksToDownload lines 461-505). It is called from
// checkHeadStall's race branch ONLY when clampedMaxParallelFetch() > 1. It issues
// an ADDITIONAL getdata for an already-in-flight slow head block to ONE more
// eligible peer, KEEPING the current holder(s) in flight, capped at clampedMax
// peers total for that hash. First delivery wins; a losing racer's late copy is
// absorbed by the existing IBD unrequested-block drop (handleBlockPreamble).
//
// Returns TRUE to SUPPRESS the sequential move (we are in additive-race mode — the
// block is either being raced, maximally raced, or held by a live peer), and FALSE
// to fall through to F1's proven sub-30s move. The two-speed boundary is the AGE
// GATE below: below BlockSlowFetchTimeout every head (including the frontier) still
// uses the measured 2s MOVE; at/above it we switch to svnode additive RACING. Because
// headStallSince keeps accumulating on the same headHash, once past the threshold we
// never fall back to the move — the two mechanisms never fight.
//
// Obeys the leaf-lock discipline: assignedMu and headerMu are each taken only across
// in-memory work and NEVER held across the TryQueueMessage send.
func (sm *SyncManager) addRacerForHead(h chainhash.Hash, height int32, now time.Time) bool {
	// The additive race can only tell a slow-but-alive holder from a dead one via the
	// rate floor. With the floor disabled (BlockStallMinRate <= 0) svnode does no
	// parallel fetch at all; racing blind would spend extra peers' bandwidth on a
	// duplicate of a block whose holder might be fine. Fall through (return false) to
	// F1's proven sequential move, which needs no rate signal. This keeps the floor=0
	// config on today's move-only behaviour.
	if sm.settings.Legacy.BlockStallMinRate <= 0 {
		return false
	}

	clampedMax := sm.clampedMaxParallelFetch()

	// 1. Snapshot the current racers under assignedMu, then release: the send below
	//    must run with no lock held, and racersOf returns a fresh slice that stays
	//    valid after the unlock.
	sm.assignedMu.Lock()
	racers := sm.racersOf(h)
	sm.assignedMu.Unlock()

	// 2. AGE GATE (svnode slow-fetch eligibility, DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_
	//    TIMEOUT): below BlockSlowFetchTimeout the head block is NOT yet old enough to
	//    race — fall through to the proven 2s MOVE for ALL heads including the frontier.
	//    headStallSince (set in checkHeadStall, reset only when the head HASH changes)
	//    is the true cross-reassignment stall age, so once it crosses the threshold we
	//    never return false here again for this block and the move is retired for it.
	if now.Sub(sm.headStallSince) < sm.settings.Legacy.BlockSlowFetchTimeout {
		return false
	}

	// 3. ACTIVE-HOLDER GATE (svnode IsBlockDownloadStallingFromPeer): if ANY current
	//    holder is provably still delivering (mature measurement at or above the floor),
	//    do NOT race it and do NOT move it — svnode breaks out and waits. Racing a peer
	//    that is simply mid-transfer of a fat block would waste a second peer's bandwidth
	//    on a duplicate. An immature/unmeasured holder is deliberately NOT treated as
	//    "still delivering" (the opposite bias to the F2 gate): it stays eligible to race.
	for _, holder := range racers {
		if rate, mature := sm.peerDownloadRate(holder, now); mature && rate >= sm.settings.Legacy.BlockStallMinRate {
			return true
		}
	}

	// 4. CAP GATE: the hash is already raced across the maximum number of peers. Do not
	//    add another — the head-of-line disconnect backstop (governed by headStallSince
	//    -> headStallThroughputGate) takes over from here if the block still never lands.
	if len(racers) >= clampedMax {
		return true
	}

	// 5. PICK ONE additional eligible peer that is not already racing this hash and has
	//    spare per-peer in-flight capacity (spare = MaxBlocksInTransitPerPeer minus its
	//    outstanding requestedBlocks, the same appendTarget math the disjoint scheduler
	//    uses). We add exactly ONE racer per refill tick — svnode's incremental fan-out,
	//    naturally spaced by InFlightRefillInterval. This mirrors eligibleFetchPeers'
	//    predicate (syncCandidate, connected) but additionally excludes the racer set and
	//    requires spare, neither of which eligibleFetchPeers can express.
	racerSet := make(map[*peerpkg.Peer]struct{}, len(racers))
	for _, r := range racers {
		racerSet[r] = struct{}{}
	}

	k := sm.settings.Legacy.MaxBlocksInTransitPerPeer

	var (
		newPeer  *peerpkg.Peer
		newState *peerSyncState
	)

	for p, state := range sm.peerStates.Range() {
		if _, racing := racerSet[p]; racing {
			continue
		}

		if !state.syncCandidate || !p.Connected() {
			continue
		}

		if k-state.requestedBlocks.Len() <= 0 {
			continue
		}

		newPeer = p
		newState = state

		break
	}

	if newPeer == nil {
		// No eligible spare peer to race to this tick. Suppress the move: the block is
		// still legitimately in flight to its current holder(s) and the backstop governs.
		return true
	}

	// 6. ISSUE the additional getdata with NO lock held (TryQueueMessage is non-blocking
	//    so a write-stalled peer never wedges the drain goroutine). Reuse the
	//    assignBlocksAcrossPeers send pattern.
	getData := wire.NewMsgGetDataSizeHint(1)
	iv := wire.NewInvVect(wire.InvTypeBlock, &h)

	if err := getData.AddInvVect(iv); err != nil {
		sm.logger.Warnf("[addRacerForHead] failed to add inv to getdata for peer %s: %v", newPeer, err)
		return true
	}

	if !newPeer.TryQueueMessage(getData) {
		// Dropped send: record NOTHING (phantom-free, exactly like the disjoint
		// scheduler). The current holder(s) are untouched and the block is unchanged,
		// so simply suppress the move and try again next tick.
		sm.logger.Debugf("[addRacerForHead] outputQueue full for peer %s, deferring race of head block %s", newPeer, h)
		return true
	}

	// Record the new racer only after the getdata actually went out. Under assignedMu
	// add the {hash,peer} assignment; set THIS peer's per-peer requestedBlocks so its
	// eventual delivery authenticates at the handleBlockPreamble auth gate. LEAVE the
	// GLOBAL requestedBlocks entry as-is: it is already set (>= 1 racer holds h), and
	// the invariant is "global set iff >= 1 racer".
	sm.assignedMu.Lock()
	sm.addAssignment(h, newPeer, now)
	sm.assignedMu.Unlock()

	newState.requestedBlocks.Set(h, struct{}{})

	// Stamp the loser-grace TTL for h (same mechanism as freeHeadBlockForRace): once a
	// racer wins and commits, the block leaves headerHeightIndex, so a slightly-late
	// copy from the ORIGINAL holder (now a loser) must be tolerated as a harmless
	// no-op by blockStillNeeded rather than disconnecting the peer.
	sm.headerMu.Lock()
	if sm.recentlyNeededUntil == nil {
		sm.recentlyNeededUntil = make(map[chainhash.Hash]time.Time)
	}
	sm.recentlyNeededUntil[h] = now.Add(recentlyNeededTTL)
	sm.headerMu.Unlock()

	// Prime the F2 byte baseline for the new racer so, if it too stalls, the
	// throughput gate has a mature sample by the time the disconnect window arrives.
	sm.primeHeadStallSample(newPeer, now)

	sm.logger.Infof("[addRacerForHead] racing head block %s (height %d) to additional peer %s (now %d peer(s) racing it, cap %d)",
		h, height, newPeer, len(racers)+1, clampedMax)

	return true
}
