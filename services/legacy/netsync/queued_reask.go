package netsync

import (
	"slices"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
// sink. The re-ask shares the race's mark. A block with more than one owner is judged on every
// owner's copy, and lands at the soonest of them. Another copy is asked for only when the newest
// extra copy is at least raceSlowFetchAfter old, every copy is judged, and fewer than
// maxBlockCopies peers owe the block, whichever rule asked. Headers-first mode only: the heights
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

	owners := sm.blockDownloads.OwnersOf(block.hash)
	if len(owners) == 0 || len(owners) >= maxBlockCopies {
		return
	}

	// Every owner's copy is judged, and the block lands at the soonest of them. One owner whose
	// copy has not had its time yet keeps the block from another peer, as SV Node gives each
	// peer a block is in flight from its slow-fetch time.
	var (
		ownerETA time.Duration
		soonest  *peerpkg.Peer
		ownSize  = typical
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

	racer, racerETA := sm.soonestOtherPeer(queues, owners, height, typical, ownSize)
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

	return sm.queuedArrival(o, queue, rec.seq, typical, typical, rate), 0, "waits behind", true
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

// soonestOtherPeer is the eligible peer, other than the owners, that would deliver a block at
// height soonest with it added at the back of its queue. A peer with no measured rate is not
// chosen: its estimate would be a guess.
func (sm *SyncManager) soonestOtherPeer(queues map[*peerpkg.Peer][]queuedBlock, owners []*peerpkg.Peer, height int32, typical, ownSize int64) (*peerpkg.Peer, time.Duration) {
	var (
		best    *peerpkg.Peer
		bestETA time.Duration
	)

	for _, bp := range sm.eligibleBlockPeers() {
		if slices.Contains(owners, bp.peer) {
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
