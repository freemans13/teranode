package netsync

import (
	"math"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE DEADLINE RULE (docs/superpowers/specs/2026-10-08-legacy-deadline-scheduler-design.md). A
// peer sends its queue in order, and a getdata cannot be withdrawn, so the peer a block goes to
// decides when it lands. Each block has a deadline: when the chain will need it, from its distance
// to the tip and the chain's pace when it does not wait. A block size is not known before the
// download, so the new block counts at the largest recent size, and each block a peer already owes
// at the recent mean size. A block goes to the slowest measured peer that can send it before its
// deadline, which keeps the fast peers free for near blocks. When no peer can, it goes to the
// earliest arrival, and waits if that peer is full. A peer with no rate takes only a far block
// (placeUnmeasured), unless no peer has a rate, when each takes one block in height sequence.
//
// It replaces a queue sized by speed and by time, a warm-up, and an 80% standby rule. On 2026-10-08
// those gave block 755,254 (4 GB) to a 7.3 MB/s peer behind a different 4 GB block, and the chain
// stopped for 9 minutes.

// deadline is how long until the chain needs the block at height, at the pace. Zero when the pace
// is unknown: then no peer is in time and each block goes to the earliest arrival.
func (a *downloadAssigner) deadline(height int32) time.Duration {
	if a.pace <= 0 {
		return 0
	}

	return time.Duration(float64(max(0, height-a.tip-1)) / a.pace * float64(time.Second))
}

// arrival is when a block of size given to p now would land: its backlog and the block.
func (p *assignerPeer) arrival(size int64) time.Duration {
	if p.rate <= 0 {
		return time.Duration(math.MaxInt64)
	}

	return p.backlog + time.Duration(float64(size)/p.rate*float64(time.Second))
}

// queue adds one block of size to p's backlog, for a block this pass gave it.
func (p *assignerPeer) queue(size int64) {
	if p.rate > 0 && size > 0 {
		p.backlog += time.Duration(float64(size) / p.rate * float64(time.Second))
	}
}

// anyMeasured reports whether one or more peers of the pass have a rate of their own.
func (a *downloadAssigner) anyMeasured() bool {
	for _, p := range a.peers {
		if p.measured {
			return true
		}
	}

	for _, p := range a.full {
		if p.measured {
			return true
		}
	}

	return false
}

// deadlinePick is the peer for the block at height; see THE DEADLINE RULE. Between two peers with
// the same rate in time, or the same arrival, the peer with more room wins, so equal peers share
// the blocks in turn. wait reports that the
// block is left for the earliest arrival, which is full now. A peer avoid marks (it owes the block
// already) is not picked by the rule; when only such peers or only unmeasured peers can serve the
// block, takeAvoiding's tiers decide.
func (a *downloadAssigner) deadlinePick(height int32, avoid func(*peerpkg.Peer) bool) (target *assignerPeer, ok, wait bool) {
	if a == nil || a.remaining <= 0 {
		return nil, false, false
	}

	// With no rates, or no block size yet (no block has completed since the start), no arrival
	// can be estimated: each peer takes blocks in height sequence, the fastest first. With a size
	// of zero each remembered peer looked in time after a restart, and the slowest took the
	// nearest blocks.
	if !a.anyMeasured() || a.size <= 0 {
		p, ok := a.takeAvoiding(height, avoid)

		return p, ok, false
	}

	usable := func(p *assignerPeer) bool {
		return p.measured && p.canServe(height) && (avoid == nil || !avoid(p.peer))
	}

	due := a.deadline(height)

	var slowestInTime, earliest *assignerPeer

	for _, p := range a.peers {
		if p.budget <= 0 || !usable(p) || p.arrival(a.size) > due {
			continue
		}

		if slowestInTime == nil || p.rate < slowestInTime.rate || (p.rate == slowestInTime.rate && p.budget > slowestInTime.budget) {
			slowestInTime = p
		}
	}

	if slowestInTime != nil {
		return slowestInTime, true, false
	}

	for _, set := range [][]*assignerPeer{a.peers, a.full} {
		for _, p := range set {
			if usable(p) && (earliest == nil || p.arrival(a.size) < earliest.arrival(a.size) ||
				(p.arrival(a.size) == earliest.arrival(a.size) && p.budget > earliest.budget)) {
				earliest = p
			}
		}
	}

	if earliest == nil {
		p, ok := a.takeAvoiding(height, avoid)

		return p, ok, false
	}

	if earliest.budget <= 0 || earliest.owed >= lateQueueLimit {
		return nil, false, true
	}

	return earliest, true, false
}

// lateQueueLimit is the most blocks a peer may owe and still be given a block it cannot land in
// time. A peer sends its queue in order, so each late block queued at it is later again. On
// 2026-10-08 after a restart the only measured peer ran at 3.9 MB/s and, as the earliest arrival
// for each near block, took 16 blocks of up to 4 GB. Two keeps the peer busy while SV Node reads
// the next block from disk before its first byte (PopulateBlockIndexBlockDiskMetaDataNL).
const lateQueueLimit = 2

// unmeasuredMinLead is the least time before the chain needs a block for it to go to a peer with
// no rate: a copy gets a rate after liveRateWindow of transfer, and the rescue rule examines a block
// raceSlowFetchAfter after its getdata.
const unmeasuredMinLead = liveRateWindow + raceSlowFetchAfter

// placeUnmeasured gives each peer with no rate and room one far block, before the deadline rule
// places the others, so it is measured on a block the chain will not need soon. The block is the
// highest unowned block of the full window (a.far) or else of the pass's candidates, and only one
// the chain needs unmeasuredMinLead or more from now; with no pace yet, any. It gives nothing when no
// peer has a rate: then each peer takes one block in height sequence. It returns the candidates
// left. On 2026-10-08 after a restart, blocks near the tip given to unmeasured peers at 0.0 MB/s
// let the chain apply 4 blocks in 10 minutes.
func (sm *SyncManager) placeUnmeasured(a *downloadAssigner, candidates []wantedBlock, highestHeld int32) []wantedBlock {
	if a == nil || !a.anyMeasured() {
		return candidates
	}

	far := a.far
	if len(far) == 0 {
		far = []wantedBlock{}
		for i := len(candidates) - 1; i >= 0; i-- {
			far = append(far, candidates[i])
		}
	}

	placedHashes := make(map[chainhash.Hash]struct{})

	for _, p := range a.peers {
		if len(far) == 0 || a.remaining <= 0 {
			break
		}

		if p.measured || p.budget <= 0 {
			continue
		}

		block := far[0]
		if a.pace > 0 && a.deadline(block.height) < unmeasuredMinLead {
			break
		}

		if a.overBackstop && block.height > highestHeld {
			break
		}

		if !p.canServe(block.height) || sm.blockDownloads.HasOwner(p.peer, block.hash) || !sm.blockDownloads.Add(p.peer, block.hash) {
			continue
		}

		hash := block.hash
		if err := a.recordRequest(p, &hash, false); err != nil {
			sm.blockDownloads.RemoveOwner(p.peer, block.hash)

			continue
		}

		far = far[1:]
		placedHashes[block.hash] = struct{}{}

		sm.logger.Infof("[deadline] %s has no rate yet: asked it for block %d, %s before the chain needs it", p.peer, block.height, a.deadline(block.height).Round(time.Second))
	}

	if len(placedHashes) == 0 {
		return candidates
	}

	left := candidates[:0:0]
	for _, c := range candidates {
		if _, ok := placedHashes[c.hash]; !ok {
			left = append(left, c)
		}
	}

	return left
}

// unmeasuredWithRoom is how many peers of the pass have no rate and room, when one or more peers
// have a rate: the number of far blocks placeUnmeasured can give.
func (a *downloadAssigner) unmeasuredWithRoom() int {
	if a == nil || !a.anyMeasured() {
		return 0
	}

	n := 0

	for _, p := range a.peers {
		if !p.measured && p.budget > 0 {
			n++
		}
	}

	return n
}
