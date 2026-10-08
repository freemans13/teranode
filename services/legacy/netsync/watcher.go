package netsync

import (
	"sort"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE WATCHER (PR 1699, 2026-10-08). The
// schedule gives blocks by rate and queue with no block size, because a size is not known before
// the download. The watcher corrects the blocks that turn out late. Each tick it examines up to
// watchBlocks owed blocks from the tip, in height sequence, and estimates when each lands
// (ownerArrival):
//   - Arriving: the declared size is known from the first bytes, so the time is its remaining
//     bytes at the copy's rate. Under raceStallRate the block is the race's.
//   - Queued: the bytes its owner sends first and the block, a recent mean block for each block of
//     unknown size, at the owner's rate.
// The chain applies a block only after each block below it, so a block that lands after each
// block below it adds to the chain's wait. When it adds watchMinETA or more, its request is
// watchMinAge old, and it has one owner, one more peer is asked:
//   - Arriving, with half its bytes or fewer: the peer that lands a full copy soonest, if in half
//     the owner's time or less. A copy with more than half its bytes gets no helper.
//   - Queued: the fastest peer that starts before the owner and has rescueFasterBy times its rate.
//     It lands the block sooner whatever its size.
// The owner keeps its request and its connection; the first complete copy converts and the other
// drains at the sink. One block gets one more getdata at the maximum. Headers-first mode only: the
// heights come from the header cache, which is empty above the last checkpoint.
//
// It replaces the rescue rule, which examined only the lowest owed block, 30 s after its getdata,
// against a deadline from the chain's pace.

const (
	// rescueFasterBy is how many times sooner, or faster, a helper must be.
	rescueFasterBy = 2
	// watchBlocks is how many owed blocks from the tip the watcher examines.
	watchBlocks = 64
	// watchMinAge is how long a request runs before the watcher judges it.
	watchMinAge = 10 * time.Second
	// watchMinETA is the least time a block must add to the chain's wait, landing after each block
	// below it, to get a helper: a copy for less costs more than the wait. On 2026-10-08 a helper
	// for each block that landed after the blocks below it, by any margin, discarded 8% of the
	// bytes received.
	watchMinETA = 30 * time.Second
)

// watchedBlock is an owed block the watcher examines.
type watchedBlock struct {
	rec    queuedBlock
	height int32
}

// watchOwedBlocks runs on the race's ticker: while the chain waits nothing else starts a download
// pass. The committed tip is read here, outside each lock.
func (sm *SyncManager) watchOwedBlocks(now time.Time) {
	if sm.streams == nil || sm.blockDownloads == nil || sm.headerCache == nil || sm.blockSizeTracker == nil || !sm.headersFirstMode.Load() {
		return
	}

	typical := sm.blockSizeTracker.meanRecentSize()
	if typical <= 0 {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	queues := sm.blockDownloads.Queues()
	used := make(map[*peerpkg.Peer]bool)

	var latest time.Duration

	for _, b := range sm.owedBlocksFromTip(queues, tip, watchBlocks) {
		owners := sm.blockDownloads.OwnersOf(b.rec.hash)
		if len(owners) == 0 {
			continue
		}

		eta, size, state, judged := farOff, int64(0), "", true

		for _, o := range owners {
			e, sz, st, ok := sm.ownerArrival(o, b.rec.hash, queues[o], typical, now)
			if !ok {
				judged = false

				break
			}

			if e < eta {
				eta, size, state = e, sz, st
			}
		}

		if !judged {
			continue
		}

		// The time this block adds to the chain's wait: how much later it lands than each block
		// below it. A few seconds is noise between peers, not a block that stops the chain.
		adds := eta - latest
		latest = max(latest, eta)

		if adds < watchMinETA || len(owners) != 1 || sm.streams.wasRaced(b.rec.hash, now) {
			continue
		}

		owner := owners[0]

		// A copy with more than half its bytes gets no helper: the helper must send the full
		// block, and the owner's nearly complete copy is discarded if the helper wins. On
		// 2026-10-08 from 14:31 to 14:49, 8% of the bytes received were discarded.
		if read, total, _, arriving := sm.streams.arrivingFrom(b.rec.hash, owner); arriving && total > 0 && read*2 > total {
			continue
		}

		helper, helperETA := sm.watchHelper(queues, owner, b, size, typical, eta, used)
		if helper == nil || !sm.askRacer(helper, b.rec.hash, now) {
			continue
		}

		used[helper] = true

		sm.waste.rescued.Add(1)
		sm.logger.Infof("[watch][%s] block %d, asked %s ago, %s %s: estimated %s there against %s at %s, so %s was asked too and the owner keeps its request",
			b.rec.hash, b.height, now.Sub(b.rec.at).Round(time.Second), state, owner, fmtETA(eta), helperETA.Round(time.Second), helper, helper)
	}
}

// owedBlocksFromTip is up to limit blocks above tip that a peer owes, not let off, and that this
// node does not hold, in height sequence, each with its record at the owner holding it earliest.
func (sm *SyncManager) owedBlocksFromTip(queues map[*peerpkg.Peer][]queuedBlock, tip int32, limit int) []watchedBlock {
	byHash := make(map[chainhash.Hash]watchedBlock)

	for _, queue := range queues {
		for _, b := range queue {
			height, ok := sm.headerCache.HeightOf(b.hash)
			if !ok || height <= tip {
				continue
			}

			// Held: parked, committing, or converting a complete copy. A block converting as it
			// arrives is not held yet; it is one to judge.
			if sm.blockPark.Has(b.hash) || sm.blockCommitting(b.hash) || (sm.conversionInFlight(b.hash) && !sm.streams.arriving(b.hash)) {
				continue
			}

			if cur, seen := byHash[b.hash]; !seen || b.seq < cur.rec.seq {
				byHash[b.hash] = watchedBlock{rec: b, height: height}
			}
		}
	}

	out := make([]watchedBlock, 0, len(byHash))
	for _, b := range byHash {
		out = append(out, b)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].height < out[j].height })

	if len(out) > limit {
		out = out[:limit]
	}

	return out
}

// watchHelper is the peer to ask for a late block, and when it would land the block; nil when no
// peer qualifies. size is the block's declared size when its bytes are arriving, zero when it is
// queued and its size is not known. A peer with no rate, a peer used this tick, and a peer whose
// advertised height is below the block are not helpers.
func (sm *SyncManager) watchHelper(queues map[*peerpkg.Peer][]queuedBlock, owner *peerpkg.Peer, b watchedBlock, size, typical int64, ownerETA time.Duration, used map[*peerpkg.Peer]bool) (*peerpkg.Peer, time.Duration) {
	ownerRate := sm.streams.peerRate(owner)
	ownerStart := time.Duration(0)

	if size <= 0 && ownerRate > 0 {
		ownerStart = sm.queuedArrival(owner, queues[owner], b.rec.seq, typical, 0, ownerRate)
	}

	var (
		best      *peerpkg.Peer
		bestRate  float64
		bestStart time.Duration
		bestETA   time.Duration
	)

	for _, bp := range sm.eligibleBlockPeers() {
		if bp.peer == owner || used[bp.peer] {
			continue
		}

		if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < b.height {
			continue
		}

		rate := sm.streams.peerRate(bp.peer)
		if rate <= 0 {
			continue
		}

		start := sm.queuedArrival(bp.peer, queues[bp.peer], 0, typical, 0, rate)

		if size > 0 {
			// The size is known: the peer that lands a full copy soonest, in half the owner's time.
			eta := start + time.Duration(float64(size)/rate*float64(time.Second))
			if eta*rescueFasterBy <= ownerETA && (best == nil || eta < bestETA) {
				best, bestETA = bp.peer, eta
			}

			continue
		}

		// The size is not known: the fastest peer that starts before the owner and has
		// rescueFasterBy times its rate, which lands the block sooner whatever its size.
		if ownerRate <= 0 || (start < ownerStart && rate >= rescueFasterBy*ownerRate) {
			if best == nil || rate > bestRate || (rate == bestRate && start < bestStart) {
				best, bestRate, bestStart = bp.peer, rate, start
				bestETA = start + time.Duration(float64(typical)/rate*float64(time.Second))
			}
		}
	}

	return best, bestETA
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
// time or is the race's: its request or its bytes are younger than watchMinAge, or its bytes
// arrive under raceStallRate.
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
		if elapsed < watchMinAge || read <= 0 {
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

	if now.Sub(rec.at) < watchMinAge {
		return 0, 0, "", false
	}

	// An owner with no rate has sent no block bytes: it lands the block very late. The median rate
	// made it look as fast as the fastest peer.
	rate := sm.streams.peerRate(o)
	if rate <= 0 {
		return farOff, 0, "has no rate at", true
	}

	// The queued block's own size is not known: it counts at the recent mean, as each block ahead
	// of it does.
	return sm.queuedArrival(o, queue, rec.seq, typical, typical, rate), 0, "waits behind", true
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
