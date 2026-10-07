package netsync

import (
	"slices"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE QUEUED RE-ASK. The race judges a block whose bytes arrive under 100 KB/s; this judges the
// block the chain needs next when its owners will deliver it too late, whether its bytes have not
// started because it sits behind other blocks at its owner, or arrive at 100 KB/s or more but
// too slowly. A peer sends its queue in order, so a block asked of a slow peer early can become
// the block the chain needs while that peer is still busy with the ones ahead of it. The peer is
// neither struggling on the block, which has not started, nor quiet, because it is sending, so
// neither the race nor the quiet-owner re-ask moves it. On 2026-10-07 block 705,725 waited 18 minutes 40 seconds for its
// first byte at a peer delivering 3.5 MB/s, with the chain stopped behind it; asked of another
// peer, it arrived in 8 seconds.
//
// It judges one block, as SV Node judges one: the lowest block owed and not yet received,
// FindNextBlocksToDownload's first in-flight block (net_processing.cpp:458-499). Its timer runs
// from the request, as SV Node's does, for DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT (30 s, the
// race's raceSlowFetchAfter). Where it parts from SV Node is the test after that: SV Node asks
// another peer only when the owner's whole block bandwidth is under 100 KB/s, which a busy peer
// at 3.5 MB/s never is. Here the test is whether the block will be late, judged one of two ways
// for each owner (ownerArrival):
//   - Queued, its bytes not started: the bytes still to come on every copy the owner is sending,
//     plus one typical block for each block queued ahead of it, plus its own typical size, at the
//     owner's measured rate. A fresh copy elsewhere is costed at the typical size too, so the
//     block's size counts the same against both peers, and only a long queue ahead of it at a
//     slow owner makes another peer look faster.
//   - Arriving at raceStallRate or more for raceSlowFetchAfter: its remaining bytes at its own
//     rate, against a fresh copy elsewhere that must fetch its whole declared size from the
//     first byte. That is what keeps a large block at a healthy rate from being doubled: a plain
//     timer on an arriving block fired for every large block, and a 4 GB block at 40 MB/s,
//     100 seconds, was downloaded twice each time. Under raceStallRate the block is the race's.
// If the soonest owner lands it after the chain will need it, the soonest other peer is asked
// too, but only if the same estimate for that peer, with the block at the back of its queue, is
// at most half the owner's.
//
// The owner keeps its request and its connection: it is working, and dropping it would lose
// every block it is sending. Whichever copy lands first converts; the other is drained at the
// sink. The re-ask shares the race's mark. A block with more than one owner is judged on every
// owner's copy, and lands at the soonest of them. Another copy is asked for only when the newest
// extra copy is at least raceSlowFetchAfter old, every copy is judged, and the block has fewer than
// maxBlockCopies live copies (liveCopies), whichever rule asked. Headers-first mode only: the heights
// come from the header cache, which is empty above the last checkpoint, where the quiet-owner
// re-ask covers the ledger's blocks.

// queuedReaskFasterBy is how many times sooner another peer must deliver the block, by the
// estimate, before it is asked too. The estimate counts queued blocks at a typical size, so a
// small margin would ask on noise.
const queuedReaskFasterBy = 2

// maybeReaskQueuedBlock runs on the race's ticker: while the chain waits nothing else triggers a
// download pass. The committed tip is read here, outside any lock.
func (sm *SyncManager) maybeReaskQueuedBlock(now time.Time) {
	if sm.streams == nil || sm.blockDownloads == nil || sm.headerCache == nil || sm.blockSizeTracker == nil || !sm.headersFirstMode.Load() {
		return
	}

	typical := sm.blockSizeTracker.getAverageSize()
	if typical <= 0 {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	queues := sm.blockDownloads.Queues()

	block, _, height, found := sm.lowestOwedBlock(queues, tip)
	if !found || sm.streams.wasRaced(block.hash, now) {
		return
	}

	// Forgiven owners sending nothing are owners still, and are judged below, but are not live
	// copies (liveCopies), so they may be asked again.
	owners := sm.blockDownloads.OwnersOf(block.hash)
	live := sm.liveCopies(block.hash, owners)

	if len(owners) == 0 || len(live) >= maxBlockCopies {
		return
	}

	// Every owner's copy is judged, and the block lands at the soonest of them. One owner whose
	// copy has not had its time yet keeps the block from another peer, as SV Node gives each
	// peer a block is in flight from its slow-fetch time.
	var (
		ownerETA time.Duration
		soonest  *peerpkg.Peer
		ownSize  = sm.queuedBlockSize(typical)
		state    = "waits behind"
	)

	for i, o := range owners {
		eta, size, how, judged := sm.ownerArrival(o, block.hash, queues[o], typical, now)
		if !judged {
			return
		}

		if i == 0 || eta < ownerETA {
			ownerETA, soonest, state = eta, o, how
		}

		// A copy arriving declares the block's size; a fresh copy must fetch all of it.
		if size > 0 {
			ownSize = size
		}
	}

	var need time.Duration
	if rate := sm.commitRate.rate(); rate > 0 {
		need = time.Duration(float64(height-tip-1) / rate * float64(time.Second))
	}

	if ownerETA <= need {
		return
	}

	racer, racerETA := sm.soonestOtherPeer(queues, owners, live, height, typical, ownSize, now)
	if racer == nil || racerETA*queuedReaskFasterBy > ownerETA {
		return
	}

	if !sm.askRacer(racer, block.hash, now) {
		return
	}

	sm.waste.reAskedQueued.Add(1)

	sm.logger.Infof("[queuedReask][%s] block %d, asked %s ago, %s %s, the soonest of %d owners: estimated %s there against %s at %s; the chain needs it in %s, so %s was asked too and the owners keep their requests",
		block.hash, height, now.Sub(block.at).Round(time.Second), state, soonest, len(owners), ownerETA.Round(time.Second), racerETA.Round(time.Second), racer,
		need.Round(time.Second), racer)
}

// farOff is an arrival estimate for an owner that will not deliver: a forgiven owner sending no
// copy, or a copy that has stopped.
const farOff = time.Duration(1<<63 - 1)

// ownerArrival estimates when block h lands at owner o, and the block's declared size when its
// bytes are arriving from o, zero otherwise. judged is false when the copy must be given more time or is the race's: its request or its
// bytes are younger than raceSlowFetchAfter, or its bytes arrive under raceStallRate.
//
// A copy arriving from o is judged on its own rate: its remaining bytes at that rate, and a fresh
// copy must fetch the whole block. A copy not yet started is judged by o's queue: the bytes o is
// still sending, a typical block for each block ahead of it and its own typical size, at o's rate.
// An owner let off the block (forgiven, so not in its queue) and sending no copy will not deliver.
func (sm *SyncManager) ownerArrival(o *peerpkg.Peer, h chainhash.Hash, queue []queuedBlock, typical int64, now time.Time) (eta time.Duration, ownSize int64, state string, judged bool) {
	// A copy waiting for an admission slot is this node's delay, not the owner's.
	if sm.streams.awaitingAdmission(h, o) {
		return 0, 0, "", false
	}

	if read, total, start, arriving := sm.streams.arrivingFrom(h, o); arriving {
		elapsed := now.Sub(start)
		if elapsed < raceSlowFetchAfter || read <= 0 {
			return 0, 0, "", false
		}

		rate := float64(read) / elapsed.Seconds()
		if rate < raceStallRate {
			return 0, 0, "", false
		}

		return time.Duration(float64(total-read) / rate * float64(time.Second)), total, "arrives slowly from", true
	}

	var (
		rec    queuedBlock
		queued bool
	)

	for _, b := range queue {
		if b.hash == h {
			rec, queued = b, true

			break
		}
	}

	if !queued {
		return farOff, 0, "was let off by", true
	}

	if now.Sub(rec.at) < raceSlowFetchAfter {
		return 0, 0, "", false
	}

	rate := sm.streams.peerRate(o)
	if rate <= 0 {
		rate = sm.streams.medianRate()
	}

	if rate <= 0 {
		return 0, 0, "", false
	}

	// The queued block's own size is not known. It is counted at the largest recent block, as the
	// queue depth is: with the average, an idle slower peer looked sooner than a faster peer with
	// a queue, and on 2026-10-07 a 2,131 MB block went to a 5.1 MB/s peer.
	return sm.queuedArrival(o, queue, rec.seq, typical, sm.queuedBlockSize(typical), rate), 0, "waits behind", true
}

// lowestOwedBlock is the lowest block above tip that some peer owes, not let off, and that this
// node does not already hold: SV Node's first in-flight block. It reports the block's record at
// the owner holding it earliest in its queue.
func (sm *SyncManager) lowestOwedBlock(queues map[*peerpkg.Peer][]queuedBlock, tip int32) (queuedBlock, *peerpkg.Peer, int32, bool) {
	var (
		best       queuedBlock
		bestOwner  *peerpkg.Peer
		bestHeight int32
		found      bool
	)

	for p, queue := range queues {
		for _, b := range queue {
			height, ok := sm.headerCache.HeightOf(b.hash)
			if !ok || height <= tip {
				continue
			}

			if found && height > bestHeight {
				continue
			}

			// Held: parked, committing, or converting a complete copy. A block converting as it
			// arrives is not held yet; it is the one to judge.
			if sm.blockPark.Has(b.hash) || sm.blockCommitting(b.hash) || (sm.conversionInFlight(b.hash) && !sm.streams.arriving(b.hash)) {
				continue
			}

			if !found || height < bestHeight || b.seq < best.seq && height == bestHeight {
				best, bestOwner, bestHeight, found = b, p, height, true
			}
		}
	}

	return best, bestOwner, bestHeight, found
}

// queuedArrival estimates when a block lands at p: the bytes still to come on every copy p is
// sending now (pending), one typical block for each block queued ahead of it (before seq) and not
// arriving from p, and the block's own typical size, at rate. A queued block arriving from another
// peer still counts: p sends its own copy all the same. A seq of zero puts the block at the back of the
// queue, as a new request is.
func (sm *SyncManager) queuedArrival(p *peerpkg.Peer, queue []queuedBlock, seq uint64, typical, ownSize int64, rate float64) time.Duration {
	sending, _ := sm.streams.pending(p)

	var queued int64

	for _, b := range queue {
		if seq != 0 && b.seq >= seq {
			continue
		}

		if _, _, _, from := sm.streams.arrivingFrom(b.hash, p); from {
			continue
		}

		queued++
	}

	bytes := float64(sending + queued*typical + ownSize)

	return time.Duration(bytes / rate * float64(time.Second))
}

// soonestOtherPeer is the eligible peer that would deliver a block at height soonest with it added
// at the back of its queue. A peer that does not owe the block is chosen first. Only when there is
// none is a forgiven owner sending nothing chosen: it is not a live copy (liveCopies). An owner in
// live is never chosen. A peer with no measured rate is not chosen: its estimate would be a guess.
func (sm *SyncManager) soonestOtherPeer(queues map[*peerpkg.Peer][]queuedBlock, owners, live []*peerpkg.Peer, height int32, typical, ownSize int64, now time.Time) (*peerpkg.Peer, time.Duration) {
	eligible := sm.eligibleBlockPeers()

	// Only an active peer is given the second copy (standby_peers.go). A standby peer's queue
	// is empty, so it can look soonest, but it is on standby because it is slow.
	rates := make([]float64, 0, len(eligible))
	for _, bp := range eligible {
		if r := sm.streams.peerRate(bp.peer); r > 0 {
			rates = append(rates, r)
		}
	}

	floor := activeFloorOf(rates)
	if sm.downloadWarming(now) {
		floor = 0
	}

	pick := func(skip []*peerpkg.Peer) (*peerpkg.Peer, time.Duration) {
		var (
			best    *peerpkg.Peer
			bestETA time.Duration
		)

		for _, bp := range eligible {
			if slices.Contains(skip, bp.peer) {
				continue
			}

			if r := sm.streams.peerRate(bp.peer); r > 0 && r < floor {
				continue
			}

			if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < height {
				continue
			}

			rate := sm.streams.peerRate(bp.peer)
			if rate <= 0 {
				continue
			}

			eta := sm.queuedArrival(bp.peer, queues[bp.peer], 0, typical, ownSize, rate)
			if best == nil || eta < bestETA {
				best, bestETA = bp.peer, eta
			}
		}

		return best, bestETA
	}

	if best, eta := pick(owners); best != nil {
		return best, eta
	}

	return pick(live)
}

// queuedBlockSize is the size a block not yet arriving is counted at: the largest recent block,
// or typical when no size has been recorded.
func (sm *SyncManager) queuedBlockSize(typical int64) int64 {
	if largest := sm.blockSizeTracker.largestRecentSize(); largest > 0 {
		return largest
	}

	return typical
}
