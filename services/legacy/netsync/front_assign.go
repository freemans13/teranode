package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE FRONT. The next frontBlockCount blocks the chain needs are each put at the top of a
// different peer's queue. A peer sends the blocks it is asked for in the order asked, and no
// message moves one up, so a block's place in a queue is decided when it is handed out. The
// assigner used to fill one peer at a time, so the next few blocks usually shared a peer and
// each waited behind the ones before it: at height 705,000, with blocks of 175 MB to 2 GB, the
// next block's bytes began minutes after it was requested and the chain spent most of every
// five minutes waiting.
//
// Each pass, before anything else:
//   - the front is the next frontBlockCount blocks, or as many as there are eligible peers if
//     that is fewer, so a node with three peers is not held to one block at a time;
//   - a front block that is its owner's oldest outstanding request is at the top already;
//   - any other front block is asked of a peer with nothing outstanding, one front block per
//     peer; far blocks may then queue behind it at that peer, since it is still sent first;
//   - a front block that was handed out while far ahead and now sits behind others at its peer
//     is asked of an empty peer as well; whichever copy lands first is used, and the ledger
//     records both peers as owners so neither is disconnected for the other copy;
//   - blocks beyond the front are handed out only once every front block is at the top of some
//     peer, so peers drain and one comes free for the front.
//
// Only with the park on. Without it every peer's read loop holds a whole decoded block, and the
// block-size ladder's cap on peers and blocks in flight is what bounds that memory; spreading the
// front over more peers would breach it. With the park, blocks convert as they stream and are
// never held whole, so the cap has nothing to protect there.

// frontBlockCount is how many of the next blocks each get a peer of their own.
const frontBlockCount = 8

// frontBlocks is the first frontBlockCount blocks of wanted the node does not already hold and
// has not written off: the ones the chain needs next.
func (sm *SyncManager) frontBlocks(wanted []wantedBlock, peers int) []wantedBlock {
	size := min(frontBlockCount, peers)
	front := make([]wantedBlock, 0, max(size, 0))

	for _, block := range wanted {
		if len(front) >= size {
			break
		}

		if sm.recentlyFailedBlocks != nil && !sm.dispatcher.inFlight(block.hash) {
			if _, failed := sm.recentlyFailedBlocks.Get(block.hash); failed {
				continue
			}
		}

		if sm.blockPark.Has(block.hash) || sm.dispatcher.inFlight(block.hash) || sm.blockGivenUpOn(block.hash) {
			continue
		}

		if sm.holdsBlock(sm.ctx, block.hash) {
			continue
		}

		front = append(front, block)
	}

	return front
}

// coverFront puts every front block at the top of some peer's queue that it can, sending the
// requests itself, and returns how many front blocks it could not place.
func (sm *SyncManager) coverFront(front []wantedBlock, eligible []blockPeer) int {
	holding := make(map[*peerpkg.Peer]bool, len(front))
	placed := make(map[chainhash.Hash]bool, len(front))

	for _, block := range front {
		if owner := sm.blockDownloads.TopOwner(block.hash); owner != nil && !holding[owner] {
			holding[owner] = true
			placed[block.hash] = true
		}
	}

	uncovered := 0

	for i, block := range front {
		if placed[block.hash] {
			continue
		}

		var target *peerpkg.Peer

		for _, candidate := range eligible {
			p := candidate.peer

			if holding[p] || sm.blockDownloads.CountForPeer(p) > 0 || sm.blockDownloads.HasOwner(p, block.hash) {
				continue
			}

			if !(&assignerPeer{state: candidate.state}).canServe(block.height) {
				continue
			}

			target = p

			break
		}

		// No empty peer. A block nobody has been asked for is still asked of the least-loaded
		// peer, behind its queue, rather than not at all: it is then raced to the first peer
		// that empties. It still counts as uncovered, so the read-ahead holds back and peers
		// drain. A block somebody already owes is left for that race.
		topOfQueue := target != nil
		if !topOfQueue {
			uncovered++

			if len(sm.blockDownloads.OwnersOf(block.hash)) > 0 {
				continue
			}

			target = sm.leastLoadedPeer(eligible, holding, block.height)
		}

		if target == nil || !sm.blockDownloads.Add(target, block.hash) {
			if topOfQueue {
				uncovered++
			}

			continue
		}

		getData := wire.NewMsgGetDataSizeHint(1)
		hash := block.hash

		if err := getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)); err != nil {
			sm.blockDownloads.RemoveOwner(target, block.hash)
			uncovered++

			continue
		}

		holding[target] = true
		target.QueueMessage(getData, nil)

		sm.logger.Debugf("[coverFront][%s] block %d, front position %d, asked of %s (top of its queue: %t)", block.hash, block.height, i+1, target, topOfQueue)
	}

	return uncovered
}

// leastLoadedPeer is the eligible peer owing the fewest blocks that holds no other front block and
// can serve height, or nil.
func (sm *SyncManager) leastLoadedPeer(eligible []blockPeer, holding map[*peerpkg.Peer]bool, height int32) *peerpkg.Peer {
	var (
		best     *peerpkg.Peer
		bestLoad int
	)

	for _, candidate := range eligible {
		p := candidate.peer
		if holding[p] || !(&assignerPeer{state: candidate.state}).canServe(height) {
			continue
		}

		if load := sm.blockDownloads.CountForPeer(p); best == nil || load < bestLoad {
			best, bestLoad = p, load
		}
	}

	return best
}
