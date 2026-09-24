package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE QUEUED RACE. A peer sends blocks in the order it was asked, so a block the chain is about to
// need can sit behind other blocks that peer is still sending. Its peer is neither slow, which the
// slow-peer race catches, nor quiet, which the download pass's quiet rule catches, so without this
// nobody else is asked and the chain waits for the whole queue ahead of it. At height 705,000 that
// was one and a half to six minutes for a single block.
//
// It asks a second peer when all of these hold:
//   - no bytes of the block have arrived, and it was asked for queuedRaceAfter ago or more, which
//     is SV Node's DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT;
//   - exactly one peer owes it, and that peer is still sending block bytes;
//   - the chain reaches it within queuedRaceLead at the measured commit rate;
//   - another peer that claims the height owes fewer blocks than the owner, so its copy can be
//     expected sooner.
//
// Only the lowest such block is raced per check, and the race marks and the cap on races at once
// are shared with the slow-peer race.

const (
	// queuedRaceAfter is how long a block must have waited in its peer's queue before it is raced.
	queuedRaceAfter = 30 * time.Second
	// queuedRaceLead is how soon the chain must reach a block for it to be worth racing.
	queuedRaceLead = time.Minute
	// queuedRaceHorizon is how many heights above the committed tip are considered.
	queuedRaceHorizon = 8
)

// maybeRaceQueuedBlock asks a second peer for the lowest block the chain is about to need that is
// still queued behind others at a busy peer.
func (sm *SyncManager) maybeRaceQueuedBlock(now time.Time) {
	if sm.streams == nil || sm.headerCache == nil {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	h, height, owner, ok := sm.pickQueuedRace(now, tip)
	if !ok {
		return
	}

	eligible := sm.eligibleBlockPeers()

	var (
		racer       *peerpkg.Peer
		racerQueued int
	)

	for _, bp := range eligible {
		if bp.peer == owner {
			continue
		}

		if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < height {
			continue
		}

		queued := sm.blockDownloads.CountForPeer(bp.peer)
		if racer == nil || queued < racerQueued {
			racer, racerQueued = bp.peer, queued
		}
	}

	ownerQueued := sm.blockDownloads.CountForPeer(owner)

	if racer == nil || racerQueued >= ownerQueued {
		return
	}

	if !sm.askRacer(racer, h, now) {
		return
	}

	sm.logger.Infof("[queuedRace][%s] asked %s for block %d as well: %s is still sending other blocks and owes %d, %s owes %d",
		h, racer, height, owner, ownerQueued, racer, racerQueued)
}

// pickQueuedRace finds the lowest block above tip, within the lead the chain will reach in
// queuedRaceLead, that is waiting in a busy peer's queue.
func (sm *SyncManager) pickQueuedRace(now time.Time, tip int32) (chainhash.Hash, int32, *peerpkg.Peer, bool) {
	commitRate := sm.commitRate.rate()

	for height := tip + 1; height <= tip+queuedRaceHorizon; height++ {
		// With no measured rate the chain may be waiting now, so only the next block qualifies.
		if commitRate <= 0 && height > tip+1 {
			break
		}

		if commitRate > 0 && time.Duration(float64(height-tip)/commitRate*float64(time.Second)) > queuedRaceLead {
			break
		}

		h, ok := sm.headerCache.At(height)
		if !ok {
			break
		}

		if sm.blockPark.Has(h) || sm.dispatcher.inFlight(h) {
			continue
		}

		owners, asked := sm.blockDownloads.ActiveOwners(h)
		if len(owners) != 1 || now.Sub(asked) < queuedRaceAfter {
			continue
		}

		if now.Sub(sm.streams.lastBlockBytes(owners[0])) >= blockRequestRetryInterval {
			continue
		}

		if !sm.streams.queuedRaceAllowed(h, now) {
			continue
		}

		return h, height, owners[0], true
	}

	return chainhash.Hash{}, 0, nil, false
}
