package netsync

import (
	"math"
	"time"

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

	if !a.anyMeasured() {
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

	if earliest.budget <= 0 {
		return nil, false, true
	}

	return earliest, true, false
}

// placeUnmeasured gives each peer with no rate and room the highest candidate of the pass, before
// the deadline rule places the others, so it is measured on a block the chain will not need soon.
// It gives nothing when no peer has a rate: then each peer takes one block in height sequence.
// It returns the candidates left. On 2026-10-08 after a restart, blocks near the tip given to
// unmeasured peers at 0.0 MB/s let the chain apply 4 blocks in 10 minutes.
func (sm *SyncManager) placeUnmeasured(a *downloadAssigner, candidates []wantedBlock, highestHeld int32) []wantedBlock {
	if a == nil || !a.anyMeasured() {
		return candidates
	}

	for _, p := range a.peers {
		if len(candidates) == 0 || a.remaining <= 0 {
			break
		}

		if p.measured || p.budget <= 0 {
			continue
		}

		block := candidates[len(candidates)-1]
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

		candidates = candidates[:len(candidates)-1]

		sm.logger.Infof("[deadline] %s has no rate yet: asked it for block %d, %s before the chain needs it", p.peer, block.height, a.deadline(block.height).Round(time.Second))
	}

	return candidates
}
