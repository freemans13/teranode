package netsync

import (
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE QUEUED RE-ASK. The race judges a block whose bytes are arriving; this judges the one whose
// bytes have not started because it sits behind other blocks at its owner. A peer sends its
// queue in order, so a block asked of a slow peer early can become the block the chain needs
// while that peer is still busy with the ones ahead of it. The peer is neither struggling on the
// block, which has not started, nor quiet, because it is sending, so neither the race nor the
// quiet-owner re-ask moves it. On 2026-10-07 block 705,725 waited 18 minutes 40 seconds for its
// first byte at a peer delivering 3.5 MB/s, with the chain stopped behind it; asked of another
// peer, it arrived in 8 seconds.
//
// It judges one block, as SV Node judges one: the lowest block owed and not yet received,
// FindNextBlocksToDownload's first in-flight block (net_processing.cpp:458-499). Its timer runs
// from the request, as SV Node's does, for DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT (30 s, the
// race's raceSlowFetchAfter). Where it parts from SV Node is the test after that: SV Node asks
// another peer only when the owner's whole block bandwidth is under 100 KB/s, which a busy peer
// at 3.5 MB/s never is. Here the test is whether the block will be late. Its arrival at the
// owner is estimated as the bytes still to come on what the owner is sending now, plus one
// typical block for each block queued ahead of it, plus its own typical size, at the owner's
// measured rate. If that is after the chain will need it, the fastest other peer is asked too,
// but only if the same estimate for that peer, with the block at the back of its queue, is at
// most half the owner's.
//
// It never looks at a block whose bytes are arriving. That is the race's, which judges the
// block's own rate. A plain timer on an arriving block fired for every large block: a 4 GB
// block at 40 MB/s takes 100 seconds, and each one was downloaded twice. The block's own size
// counts the same against both peers here, so a large block alone never makes a second peer look
// faster; only a long queue ahead of it at a slow peer does.
//
// The owner keeps its request and its connection: it is working, and dropping it would lose
// every block it is sending. Whichever copy lands first converts; the other is drained at the
// sink. The re-ask shares the race's mark, so a block gets one extra request in raceExpiry
// whichever rule asked. Headers-first mode only: the heights come from the header cache, which
// is empty above the last checkpoint, where the quiet-owner re-ask covers the ledger's blocks.

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

	block, owner, height, found := sm.lowestOwedBlock(queues, tip)
	if !found || sm.streams.wasRaced(block.hash, now) {
		return
	}

	if owners := sm.blockDownloads.OwnersOf(block.hash); len(owners) != 1 {
		return
	}

	// When it lands at its owner, and the size a fresh copy elsewhere would have to fetch.
	var (
		ownerETA time.Duration
		ownSize  = typical
		state    = "waits behind"
	)

	if read, total, start, arriving := sm.streams.arrivingStream(block.hash); arriving {
		// Arriving: judged on its own rate. Under the race's floor it is the race's, which
		// drops the owner. A fresh copy must fetch the whole block, which is what keeps a
		// large block at a healthy rate from ever being doubled.
		elapsed := now.Sub(start)
		if elapsed < raceSlowFetchAfter || read <= 0 {
			return
		}

		rate := float64(read) / elapsed.Seconds()
		if rate < raceStallRate {
			return
		}

		ownerETA = time.Duration(float64(total-read) / rate * float64(time.Second))
		ownSize = total
		state = "arrives slowly from"
	} else {
		if now.Sub(block.at) < raceSlowFetchAfter {
			return
		}

		ownerRate := sm.streams.peerRate(owner)
		if ownerRate <= 0 {
			ownerRate = sm.streams.medianRate()
		}

		if ownerRate <= 0 {
			return
		}

		ownerETA = sm.queuedArrival(owner, queues[owner], block.seq, typical, typical, ownerRate)
	}

	var need time.Duration
	if rate := sm.commitRate.rate(); rate > 0 {
		need = time.Duration(float64(height-tip-1) / rate * float64(time.Second))
	}

	if ownerETA <= need {
		return
	}

	racer, racerETA := sm.soonestOtherPeer(queues, owner, height, typical, ownSize)
	if racer == nil || racerETA*queuedReaskFasterBy > ownerETA {
		return
	}

	if !sm.askRacer(racer, block.hash, now) {
		return
	}

	sm.waste.reAskedQueued.Add(1)

	sm.logger.Infof("[queuedReask][%s] block %d, asked %s ago, %s %s: estimated %s there against %s at %s; the chain needs it in %s, so %s was asked too and %s keeps its request",
		block.hash, height, now.Sub(block.at).Round(time.Second), state, owner, ownerETA.Round(time.Second), racerETA.Round(time.Second), racer,
		need.Round(time.Second), racer, owner)
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

// queuedArrival estimates when a block lands at p: the bytes still to come on what p is sending
// now, one typical block for each block queued ahead of it (before seq) and not yet arriving,
// and the block's own typical size, at rate. A seq of zero puts the block at the back of the
// queue, as a new request is.
func (sm *SyncManager) queuedArrival(p *peerpkg.Peer, queue []queuedBlock, seq uint64, typical, ownSize int64, rate float64) time.Duration {
	sending, _ := sm.streams.pending(p)

	var queued int64

	for _, b := range queue {
		if seq != 0 && b.seq >= seq {
			continue
		}

		if sm.streams.arriving(b.hash) {
			continue
		}

		queued++
	}

	bytes := float64(sending + queued*typical + ownSize)

	return time.Duration(bytes / rate * float64(time.Second))
}

// soonestOtherPeer is the eligible peer, other than owner, that would deliver a block at height
// soonest with it added at the back of its queue. A peer with no measured rate is not chosen:
// its estimate would be a guess.
func (sm *SyncManager) soonestOtherPeer(queues map[*peerpkg.Peer][]queuedBlock, owner *peerpkg.Peer, height int32, typical, ownSize int64) (*peerpkg.Peer, time.Duration) {
	var (
		best    *peerpkg.Peer
		bestETA time.Duration
	)

	for _, bp := range sm.eligibleBlockPeers() {
		if bp.peer == owner {
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
