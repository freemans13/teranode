package netsync

import (
	"sort"
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// ACTIVE AND STANDBY PEERS. A peer sends its queue in order, at its own rate, and a block given to
// a slow peer near the front of a short lead holds the chain up for as long as that peer takes. On
// 2026-10-07 at heights near 709,000 the node held a 30 to 50 block lead; peers ran at 38, 27.8,
// 26, 8.2, 2.5, 1.9 and 1.5 MB/s, and the chain stopped every few blocks on a block one of the
// slow ones was sending, while the top three carried most of the 1 Gbit/s link.
//
// So the peers are ranked by measured rate, fastest first, and the fewest of them that together
// give activeShareOfBandwidth of the bandwidth all measured peers offer are active: they take
// blocks as before, the fastest with room first. The rest are standby: connected, given no
// blocks, so the chain never waits on them. When every active peer is full a block waits for one
// of them. With the peers above, 80% of 105.9 MB/s is 84.7, and the top three give 91.8, so those
// three are active. The set moves with the peers: if the active ones go, the next fastest make up
// the share. A peer with no measured rate yet is active, so it gets measured, and so is every
// peer before any is measured.
//
// A standby peer is never re-measured by blocks it is not given, so at most once per
// benchProbeInterval it is given the highest block of a pass that would otherwise wait, and only
// if it would land in time were the peer benchProbeSpeedup times faster than last measured: a
// peer that is still slow is then late only on a block the lead can absorb. A probe needs the
// chain's measured pace and a typical block size, and is skipped without them.

const (
	// activeShareOfBandwidth is the share of the measured peers' total rate the active peers must
	// make up between them. An 80/20 rule: the slowest peers that add the last fifth are where the
	// chain waits.
	activeShareOfBandwidth = 0.8
	// benchProbeInterval is the least time between two probe blocks to one standby peer.
	benchProbeInterval = 10 * time.Minute
	// benchProbeSpeedup is how much faster than last measured a probe assumes the peer may now be.
	benchProbeSpeedup = 4
)

// activeFloor is the slowest rate an active peer of this pass may have; see activeFloorOf.
func (a *downloadAssigner) activeFloor() float64 {
	if a.warming {
		return 0
	}

	var rates []float64

	for _, p := range append(append([]*assignerPeer(nil), a.peers...), a.full...) {
		if p.measured && p.rate > 0 {
			rates = append(rates, p.rate)
		}
	}

	return activeFloorOf(rates)
}

// activeFloorOf is the slowest rate an active peer may have: walking the measured rates fastest
// first, the rate that brings their running total to activeShareOfBandwidth of all of them. Zero,
// so every peer is active, when none is measured.
func activeFloorOf(rates []float64) float64 {
	rates = append([]float64(nil), rates...)

	var total float64
	for _, r := range rates {
		total += r
	}

	sort.Sort(sort.Reverse(sort.Float64Slice(rates)))

	var running float64

	for _, r := range rates {
		running += r
		if running >= total*activeShareOfBandwidth {
			return r
		}
	}

	return 0
}

// active reports whether p takes blocks: unmeasured, or at or above floor.
func (p *assignerPeer) active(floor float64) bool {
	return !p.measured || p.rate >= floor
}

// judging reports whether the pass knows enough to time a probe block.
func (a *downloadAssigner) judging() bool {
	return a != nil && a.tip > 0 && a.commitRate > 0 && a.typical > 0
}

// need is how long until the chain reaches the block at height, at the measured pace.
func (a *downloadAssigner) need(height int32) time.Duration {
	blocks := max(0, height-a.tip-1)

	return time.Duration(float64(blocks) / a.commitRate * float64(time.Second))
}

// queue adds one typical block to p's backlog, for a block this pass gave it.
func (p *assignerPeer) queue(typical int64) {
	if p.rate > 0 && typical > 0 {
		p.backlog += time.Duration(float64(typical) / p.rate * float64(time.Second))
	}
}

// inTimeAvoiding is takeAvoiding over the active peers; see ACTIVE AND STANDBY PEERS. wait
// reports that the block is left for an active peer that is full now.
func (a *downloadAssigner) inTimeAvoiding(height int32, avoid func(*peerpkg.Peer) bool) (*assignerPeer, bool, bool) {
	floor := a.activeFloor()

	if p, ok := a.takeWhere(height, avoid, func(p *assignerPeer) bool { return p.active(floor) }); ok {
		return p, true, false
	}

	if a == nil || a.remaining <= 0 {
		return nil, false, false
	}

	// Every active peer is full, whether it began the pass full or filled during it.
	for _, p := range append(append([]*assignerPeer(nil), a.peers...), a.full...) {
		if p.active(floor) && p.canServe(height) {
			return nil, false, true
		}
	}

	// No active peer can take it now or later: anyone may.
	p, ok := a.takeAvoiding(height, avoid)

	return p, ok, false
}

// probeBenchedPeers gives one standby peer that has not been probed within
// benchProbeInterval the highest block this pass left waiting, if the peer would deliver it in
// time at benchProbeSpeedup times its last rate. One probe a pass.
func (sm *SyncManager) probeBenchedPeers(a *downloadAssigner, waited []wantedBlock, highestHeld int32) {
	if !a.judging() || len(waited) == 0 {
		return
	}

	block := waited[len(waited)-1]
	if a.overBackstop && block.height > highestHeld {
		return
	}

	need := a.need(block.height)
	floor := a.activeFloor()

	for _, p := range a.peers {
		if p.budget <= 0 || p.active(floor) || p.getData != nil || p.rate <= 0 {
			continue
		}

		if last, ok := sm.benchProbes[p.peer]; ok && a.now.Sub(last) < benchProbeInterval {
			continue
		}

		faster := *p
		faster.rate *= benchProbeSpeedup
		faster.backlog /= benchProbeSpeedup

		if faster.eta(a.typical) > need || !p.canServe(block.height) || sm.blockDownloads.HasOwner(p.peer, block.hash) {
			continue
		}

		if !sm.blockDownloads.Add(p.peer, block.hash) {
			return
		}

		hash := block.hash
		if err := a.recordRequest(p, &hash, false); err != nil {
			sm.blockDownloads.RemoveOwner(p.peer, block.hash)

			return
		}

		p.queue(a.typical)

		if sm.benchProbes == nil {
			sm.benchProbes = make(map[*peerpkg.Peer]time.Time)
		}

		for q, at := range sm.benchProbes {
			if a.now.Sub(at) > time.Hour {
				delete(sm.benchProbes, q)
			}
		}

		sm.benchProbes[p.peer] = a.now

		sm.logger.Infof("[standbyPeer] probing %s, on standby at %.1f MB/s, with block %d, %s ahead of the chain", p.peer, p.rate/1e6, block.height, need.Round(time.Second))

		return
	}
}
