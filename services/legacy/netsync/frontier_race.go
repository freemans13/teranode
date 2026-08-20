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

// This file lets a second peer be asked for the one block that is holding up
// headers-first sync.
//
// Blocks are committed strictly in order, so the oldest block we have asked for
// and not yet received — the download frontier — gates everything behind it.
// Every block body is requested from the sync peer, so if that peer silently
// drops one getdata, sync simply stops. The only response available today is the
// 180-second sync-peer stall timer, which disconnects the peer and then throws
// away the entire downloaded header list (resetHeaderState), forcing a fresh
// getheaders round from someone else. One dropped request therefore costs three
// minutes of nothing plus a full header re-download.
//
// Instead, once the frontier has sat unchanged for legacy_blockSlowFetchTimeout,
// we send one additional getdata for that single hash to one more connected peer
// and leave the original request in place. Whichever copy lands first is
// processed normally and the other is discarded. Nothing is ever moved or
// cancelled, only added, so there is no way for the block to end up owned by
// nobody or handed back to the same silent peer forever.
//
// This deliberately does not touch the 180-second backstop. A racer's delivery
// does not refresh the sync peer's last-block time (HandleBlockDirect only
// refreshes it via syncPeerStateFor, which is false for any other peer), so a
// sync peer that never delivers anything is still rotated on the same schedule.
// Racing buys progress inside that window; it does not extend it.

const (
	// frontierCheckInterval is how often the block handler asks whether the
	// download frontier has been stuck long enough to be worth asking a second
	// peer for. Well under legacy_blockSlowFetchTimeout so the timeout, not the
	// timer granularity, decides when a race starts.
	frontierCheckInterval = 5 * time.Second

	// racedBlockGraceTTL is how long we remember that a particular peer was
	// asked for a particular block as part of a race, so that a late copy from
	// it is dropped quietly rather than treated as an unrequested block. It
	// matches the peer package's absolute ceiling on a single block download, so
	// the grace outlives any copy that could still legitimately be on its way.
	racedBlockGraceTTL = peerpkg.MaxBlockDownloadTime

	// racedBlockGraceMaxTracked caps the number of raced block hashes held in
	// memory at once. Only the frontier is ever raced and only one racer is
	// added every five seconds, so this is far above anything reachable in
	// practice; it exists so the map can never grow without bound.
	racedBlockGraceMaxTracked = 256
)

// publishFrontier records headerList's front node — the oldest block we have
// asked for and not yet received — as the current download frontier. It takes
// headerMu, so callers already holding it must use publishFrontierLocked
// instead; sync.Mutex is not reentrant.
//
// Publishing the frontier here, rather than having the race timer read
// headerList itself, is what keeps the timer off the header list entirely, so
// it never has to take headerMu and the headerMu -> frontierMu ordering stays
// one-directional.
func (sm *SyncManager) publishFrontier(now time.Time) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	sm.publishFrontierLocked(now)
}

// publishFrontierLocked is publishFrontier's body. The caller must hold
// headerMu.
//
// The frontier is cleared rather than published when headers-first fetching is
// not running, when the header list is empty, or when the front block has not
// actually been requested yet — fetchHeaderBlocks stops early once the in-flight
// cap is reached, leaving startHeader sitting on the front node, and a block we
// never asked for is not stuck, it is simply not wanted yet.
//
// The anchor is cleared for a different reason. While a round's headers are
// still coming in, the front of the list is the previous round's anchor and the
// cursor is on the first real header behind it, so the two checks above both
// pass — and publishing it would have raceFrontierBlock ask a second peer for a
// block that is already in this node's chain. Nobody is waiting for it, so it is
// not the frontier; and the reply would take the anchor off the front early,
// which is one of the two ways the checkpoint trim used to lose a real header
// (see removeHeaderAnchorLocked).
func (sm *SyncManager) publishFrontierLocked(now time.Time) {
	if !sm.headersFirstMode.Load() || sm.headerList == nil {
		sm.clearFrontier()
		return
	}

	front := sm.headerList.Front()
	if front == nil || front == sm.startHeader {
		sm.clearFrontier()
		return
	}

	node, ok := front.Value.(*headerNode)
	if !ok || node.hash == nil || node.isAnchor {
		sm.clearFrontier()
		return
	}

	sm.setFrontier(*node.hash, node.height, now)
}

// clearFrontier records that there is currently no block whose absence is
// holding up sync.
func (sm *SyncManager) clearFrontier() {
	sm.setFrontier(chainhash.Hash{}, 0, time.Time{})
}

// setFrontier publishes a new frontier. Setting the same hash again is a no-op,
// which is what makes "how long has this been stuck" measurable: frontierSince
// only moves when the block we are waiting for actually changes. Any peers we
// had racing the previous frontier are forgotten, since they were racing a block
// nobody is waiting for any more.
func (sm *SyncManager) setFrontier(hash chainhash.Hash, height int32, now time.Time) {
	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()

	if sm.frontierHash == hash {
		return
	}

	sm.frontierHash = hash
	sm.frontierHeight = height
	sm.frontierSince = now
	sm.frontierRacers = nil
}

// frontierRaceTarget decides whether the download frontier is stuck badly enough
// to be worth asking another peer for, and if so picks the one peer to ask. It
// only reads state and sends nothing, so the whole decision can be unit-tested.
//
// Every condition below is a reason NOT to race:
//   - racing is switched off by configuration;
//   - we are not in headers-first mode, so there is no frontier;
//   - the frontier has not been stuck for long enough yet;
//   - we already have as many peers on it as configuration allows;
//   - we are throttling our own network reads because local validation is
//     behind, in which case the silence is ours, not the peer's;
//   - the sync peer's connection is visibly pulling bytes, which means it is
//     part-way through a large block rather than ignoring us — racing a peer
//     mid-transfer just buys a duplicate of a download that is already working;
//   - there is nobody else worth asking.
func (sm *SyncManager) frontierRaceTarget(now time.Time) (chainhash.Hash, int32, *peerpkg.Peer, bool) {
	var none chainhash.Hash

	if sm.settings == nil {
		return none, 0, nil, false
	}

	maxRacing := sm.settings.Legacy.MaxBlockParallelFetch
	slowAfter := sm.settings.Legacy.BlockSlowFetchTimeout

	if maxRacing < 2 || slowAfter <= 0 {
		return none, 0, nil, false
	}

	if !sm.headersFirstMode.Load() {
		return none, 0, nil, false
	}

	sm.frontierMu.Lock()
	hash := sm.frontierHash
	height := sm.frontierHeight
	since := sm.frontierSince
	racing := make(map[*peerpkg.Peer]struct{}, len(sm.frontierRacers))

	for p := range sm.frontierRacers {
		racing[p] = struct{}{}
	}
	sm.frontierMu.Unlock()

	if hash == none {
		return none, 0, nil, false
	}

	if now.Sub(since) < slowAfter {
		return none, 0, nil, false
	}

	// The peer that already owes us the block counts towards the limit, so at
	// the default of 2 exactly one extra peer is ever added.
	if len(racing)+1 >= maxRacing {
		return none, 0, nil, false
	}

	if sm.localReadBackpressured() {
		return none, 0, nil, false
	}

	sp, sps := sm.loadSyncPeerAndState()
	if sp == nil || sps == nil {
		return none, 0, nil, false
	}

	// A brand new sync peer has no throughput sample yet and is treated as not
	// downloading. That is the safe bias here: the worst case is one duplicate
	// block from a peer that turned out to be fine, whereas the opposite bias
	// would let a genuinely silent peer hide behind a missing measurement.
	if sps.hasHealthyDownloadThroughput(sm.minSyncPeerNetworkSpeed) {
		return none, 0, nil, false
	}

	if sm.peerStates == nil {
		return none, 0, nil, false
	}

	for p, state := range sm.peerStates.Range() {
		if p == sp || state == nil || !state.syncCandidate || !p.Connected() {
			continue
		}

		if _, already := racing[p]; already {
			continue
		}

		// No point asking a peer that has not told us it has the block.
		if height > 0 && p.LastBlock() < height {
			continue
		}

		return hash, height, p, true
	}

	return none, 0, nil, false
}

// raceFrontierBlock asks one additional peer for the block that is currently
// holding up sync, if there is one and if it has been stuck long enough. It is
// driven by a five-second timer in blockHandler. The original request is left
// exactly as it was, so the worst this can cost is one duplicate block.
func (sm *SyncManager) raceFrontierBlock(now time.Time) {
	hash, height, target, ok := sm.frontierRaceTarget(now)
	if !ok {
		return
	}

	// Record the extra peer before anything goes out on the wire, and abandon
	// the race if the frontier moved in the meantime — the block handler runs on
	// its own goroutine and may have taken delivery while we were deciding. A
	// getdata sent for a block we are no longer tracking would arrive as an
	// unrequested block and cost the peer its connection.
	if !sm.registerFrontierRacer(hash, target) {
		return
	}

	// Authorise the reply. Both the pre-admission check in the peer read-loop
	// and the one in handleBlockMsg ask whether this peer owes us the block, and
	// a peer that does not gets disconnected for sending it. Recording a second
	// owner is exactly what the ledger is for.
	// The frontier is already in the ledger — this adds an owner, not a block —
	// so the size cap cannot turn the race away. The check is here for the one
	// case where it can: a frontier whose record aged out of the hour-long
	// ceiling while the ledger stayed full. Racing a block we cannot vouch for
	// would punish the peer that answered, so we let the stall stand instead.
	if !sm.blockDownloads.Add(target, hash) {
		sm.logger.Warnf("[raceFrontierBlock] block download ledger full at %d blocks, not racing %s", maxTrackedBlockDownloads, hash)
		return
	}

	getData := wire.NewMsgGetDataSizeHint(1)
	if err := getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)); err != nil {
		sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)
		return
	}

	target.QueueMessage(getData, nil)

	sm.logger.Infof("[raceFrontierBlock] block %s (height %d) outstanding for %s, requesting a second copy from %s", hash, height, now.Sub(sm.frontierStartedAt()).Round(time.Second), target)
}

// frontierStartedAt reports when the current frontier block became the block we
// are waiting for, for logging.
func (sm *SyncManager) frontierStartedAt() time.Time {
	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()

	return sm.frontierSince
}

// registerFrontierRacer adds a peer to the set racing the frontier block and
// reports whether it was added. It fails when the frontier has moved on since
// the decision was taken, or when the peer is somehow already racing this hash.
func (sm *SyncManager) registerFrontierRacer(hash chainhash.Hash, p *peerpkg.Peer) bool {
	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()

	if sm.frontierHash != hash {
		return false
	}

	if sm.frontierRacers == nil {
		sm.frontierRacers = make(map[*peerpkg.Peer]struct{}, 1)
	}

	if _, already := sm.frontierRacers[p]; already {
		return false
	}

	sm.frontierRacers[p] = struct{}{}

	return true
}

// noteRaceWinner is called when a block we had raced is delivered. It cancels
// the request with everybody else we asked, and remembers that we did so.
//
// Cancelling matters because a request that will never be answered would
// otherwise sit in that peer's outstanding list for a full hour, counting
// against the in-flight limit fetchHeaderBlocks uses to decide how much more to
// ask for — a limit that drops to a single block once blocks get large enough,
// at which point one stale entry stops us fetching anything at all. The download
// ledger ages assignments out on its own, but that is only the backstop for
// requests nobody ever cancels; this is the fast path, and an hour of a stalled
// in-flight budget is far too long to wait for the slow one.
//
// Remembering matters because the peers whose request we just cancelled may
// still be part-way through sending their copy. When it lands it would look
// unrequested, and an unrequested block costs a peer its connection. Punishing a
// peer for answering a question we asked would make the recovery worse than the
// stall it fixes, so those specific peers get a pass on that specific hash.
func (sm *SyncManager) noteRaceWinner(hash chainhash.Hash) {
	sm.frontierMu.Lock()

	if sm.frontierHash != hash || len(sm.frontierRacers) == 0 {
		sm.frontierMu.Unlock()
		return
	}

	racers := sm.frontierRacers
	sm.frontierRacers = nil
	sm.frontierMu.Unlock()

	// The peer that already owed us the block is the sync peer, which is never
	// in the racer set, so add it here.
	asked := make(map[*peerpkg.Peer]struct{}, len(racers)+1)
	for p := range racers {
		asked[p] = struct{}{}
	}

	if sp := sm.loadSyncPeer(); sp != nil {
		asked[sp] = struct{}{}
	}

	for p := range asked {
		sm.blockDownloads.RemoveOwner(p, hash)
	}

	if sm.racedBlocks != nil {
		sm.racedBlocks.Set(hash, asked)
	}

	sm.logger.Debugf("[noteRaceWinner] block %s delivered, cancelling the same request with %d other peer(s)", hash, len(asked)-1)
}

// BlockRacedTo reports whether the given block arriving from the given peer is a
// late copy of a block we deliberately asked several peers for and have since
// received. Such a block is dropped quietly. Any peer we did not ask is still
// disconnected for sending an unrequested block, so the flood defence is
// unchanged for everyone else.
func (sm *SyncManager) BlockRacedTo(peer *peerpkg.Peer, blockHash *chainhash.Hash) bool {
	if sm.racedBlocks == nil || peer == nil || blockHash == nil {
		return false
	}

	asked, ok := sm.racedBlocks.Get(*blockHash)
	if !ok || len(asked) == 0 {
		return false
	}

	if _, raced := asked[peer]; raced {
		return true
	}

	// Blocks arrive on a separate stream under the BlockPriority policy, which
	// is a different Peer from the one we sent the request to, so resolve it to
	// the association's primary peer exactly as BlockRequested does.
	if _, primary, exists := sm.peerStateResolvingPrimary(peer); exists && primary != peer {
		_, raced := asked[primary]
		return raced
	}

	return false
}
