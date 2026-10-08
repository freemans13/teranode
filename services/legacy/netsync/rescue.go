package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE RESCUE RULE (docs/superpowers/specs/2026-10-08-legacy-deadline-scheduler-design.md 3.5).
// The deadline rule gives each block to a peer that can land it in time, but a peer can slow down
// after it took a block, and a rate is an estimate. This rule corrects such a block. A peer sends
// its queue in order, so the late block can be queued behind other blocks at a busy peer that is
// neither struggling on the block nor quiet; on 2026-10-07 block 705,725 waited 18 minutes 40
// seconds for its first byte at a peer at 3.5 MB/s, and another peer sent it in 8 seconds.
//
// It examines one block, as SV Node examines one: the lowest block owed and not yet held,
// FindNextBlocksToDownload's first in-flight block (net_processing.cpp:458-499). Its timer starts at
// the getdata and runs for DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT (30 s, raceSlowFetchAfter).
// Then it estimates when the block lands at its owner (ownerArrival):
//   - Queued, its bytes not started: the bytes the owner is sending, a mean recent block for each
//     block queued ahead of it, and the largest recent block for itself, at the owner's rate. An
//     owner with no rate after 30 s lands it very late.
//   - Arriving at raceStallRate or more for raceSlowFetchAfter: its remaining bytes at its own rate,
//     against a fresh copy that must fetch its full declared size. Thus a 4 GB block at 40 MB/s is
//     not downloaded twice. Under raceStallRate the block is the race's.
// If the owner lands it after the chain needs it, at the chain's pace (commitRateTracker.pace),
// and a different measured peer can land a full copy in half that time or less, that peer is asked
// too. The owner keeps its request and its connection; the first complete copy converts and the
// other drains at the sink. A block with more than one owner has had its one more getdata, from
// this rule or the race, and is not examined. Headers-first mode only: the heights come from the
// header cache, which is empty above the last checkpoint.

// rescueFasterBy is how many times sooner another peer must land the block, by the estimate,
// before it is asked too. The estimate counts queued blocks at a mean size, so a small margin
// would ask on noise.
const rescueFasterBy = 2

// maybeRescueLowestBlock runs on the race's ticker: while the chain waits nothing else starts a
// download pass. The committed tip is read here, outside each lock.
func (sm *SyncManager) maybeRescueLowestBlock(now time.Time) {
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
	if len(owners) != 1 {
		return
	}

	owner := owners[0]

	ownerETA, size, state, judged := sm.ownerArrival(owner, block.hash, queues[owner], typical, now)
	if !judged {
		return
	}

	if size <= 0 {
		size = sm.queuedBlockSize(typical)
	}

	var need time.Duration
	if pace := sm.commitRate.pace(); pace > 0 {
		need = time.Duration(float64(height-tip-1) / pace * float64(time.Second))
	}

	if ownerETA <= need {
		return
	}

	racer, racerETA := sm.soonestOtherPeer(queues, owner, height, typical, size)
	if racer == nil || racerETA*rescueFasterBy > ownerETA {
		return
	}

	if !sm.askRacer(racer, block.hash, now) {
		return
	}

	sm.waste.rescued.Add(1)

	sm.logger.Infof("[rescue][%s] block %d, asked %s ago, %s %s: estimated %s there against %s at %s; the chain needs it in %s, so %s was asked too and the owner keeps its request",
		block.hash, height, now.Sub(block.at).Round(time.Second), state, owner, fmtETA(ownerETA), racerETA.Round(time.Second), racer, need.Round(time.Second), racer)
}

// fmtETA prints an arrival estimate, farOff as "never".
func fmtETA(d time.Duration) string {
	if d == farOff {
		return "never"
	}

	return d.Round(time.Second).String()
}

// farOff is an arrival estimate for an owner that will not deliver: a forgiven owner sending no
// copy, or a copy that has stopped.
const farOff = time.Duration(1<<63 - 1)

// ownerArrival estimates when block h lands at owner o, and the block's declared size when its
// bytes are arriving from o, zero otherwise. judged is false when the copy must be given more
// time or is the race's: its request or its bytes are younger than raceSlowFetchAfter, or its
// bytes arrive under raceStallRate.
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

	// An owner with no rate has sent no block bytes: it lands the block very late. The median rate
	// made it look as fast as the fastest peer.
	rate := sm.streams.peerRate(o)
	if rate <= 0 {
		return farOff, 0, "has no rate at", true
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

// soonestOtherPeer is the measured peer, other than owner, that would land a fresh copy of ownSize
// soonest, at the back of its queue. A peer with no rate is not chosen: its estimate is a guess.
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

// queuedBlockSize is the size a block not yet arriving is counted at: the largest recent block,
// or typical when no size has been recorded.
func (sm *SyncManager) queuedBlockSize(typical int64) int64 {
	if largest := sm.blockSizeTracker.largestRecentSize(); largest > 0 {
		return largest
	}

	return typical
}
