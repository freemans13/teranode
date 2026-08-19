package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// parkSweepInterval is how often the block-queue consumer looks over the park
// for blocks that have been waiting too long, or whose parent turned up without
// a commit event this node saw. It runs on that goroutine and not on the outer
// message handler because a commit is minutes of work, and the outer handler
// dispatches disconnects, invs and headers for every peer.
//
// A var rather than a const so the two tests that prove this ticker is really
// wired up can run in milliseconds instead of half a minute. Nothing in
// production writes it, and the only reader takes its value once, when the block
// handler starts.
var parkSweepInterval = 30 * time.Second

// drainParkedDescendants commits everything parked behind a block that has just
// been committed, and then everything parked behind those, and so on.
//
// It walks an explicit stack rather than recursing: a chain of parked blocks can
// be maxParkedEntries long, and recursion would nest that many frames, each one
// holding a decoded block. Exactly one block is decoded at a time and it is
// released before the next is read.
func (sm *SyncManager) drainParkedDescendants(committed chainhash.Hash) {
	if !sm.blockPark.Enabled() {
		return
	}

	stack := []chainhash.Hash{committed}

	for len(stack) > 0 {
		parent := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, entry := range sm.blockPark.TakeChildren(parent) {
			if sm.commitParkedBlock(entry) {
				stack = append(stack, entry.hash)
			}
		}
	}
}

// commitParkedBlock reads one parked block back off disk and commits it,
// reporting whether it went in — which is what tells the drain whether to look
// for blocks parked behind it in turn.
//
// It deliberately does NOT go back through handleBlockMsg. That function's front
// half is wrong for a block committed from disk and would break it twice over:
// the peer lookup reports "unknown peer" for a peer that has since been evicted,
// which fails the whole block, and the ownership check sees an obligation that
// was released when the block first arrived, which disconnects a peer for
// delivering a block we asked for.
//
// The entry has already been taken out of the park index by the caller. Its blob
// is still on disk and still charged against the budget, so every path out of
// here goes through applyParkDisposition, which is what settles that.
func (sm *SyncManager) commitParkedBlock(entry parkedBlock) bool {
	msgBlock, err := sm.blockPark.Read(sm.ctx, entry.hash)
	if err != nil {
		// A read can fail because the blob is bad, but it can equally fail
		// because the store had no permit free inside the park's deadline or
		// because the node is shutting down — and neither of those says anything
		// about the block. parkReadFailure tells them apart; treating them alike
		// destroys fully downloaded blocks under ordinary load.
		d := parkReadFailure(err)

		sm.logger.Warnf("[commitParkedBlock][%s] parked block could not be read back (%s): %v", entry.hash, d.reason, err)
		sm.applyParkDisposition(entry, d)

		return false
	}

	if err = sm.HandleBlockDirect(sm.ctx, entry.peer, entry.hash, msgBlock); err != nil {
		return sm.parkedBlockFailed(entry, err)
	}

	// The header list is only ever advanced by an arriving block that matches
	// its front. A block committed from disk never passes that code, so without
	// this the front sticks on a block that is already in the chain, the next
	// block never matches it, the frontier is never republished and the
	// checkpoint transition never fires — headers-first sync would wedge one
	// block after the first successful drain.
	isCheckpointBlock, _ := sm.advanceHeaderListFor(entry.hash)

	sm.applyParkDisposition(entry, parkDispositionCommitted)

	if sm.blockFailureBackoff != nil {
		sm.blockFailureBackoff.Delete(entry.hash)
	}

	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Delete(entry.hash)
	}

	sm.noteCommittedParkedBlock(entry)

	if isCheckpointBlock {
		// A parked block CAN be the checkpoint block, and if the next round of
		// headers is never asked for, headers-first sync stops here for good. So
		// this one falls back to the current sync peer when the peer that
		// delivered the block has gone.
		if err = sm.checkpointBlockCommitted(sm.livePeer(entry.peer), entry.hash); err != nil {
			sm.logger.Errorf("[commitParkedBlock][%s] failed to move past the checkpoint: %v", entry.hash, err)
		}

		return true
	}

	sm.fetchMoreHeaderBlocks(sm.livePeer(entry.peer))

	return true
}

// parkedBlockFailed decides what to do with a parked block that would not
// commit, and reports false so the drain stops walking that branch. The decision
// itself is parkCommitFailure's; all this does is log it and carry it out.
func (sm *SyncManager) parkedBlockFailed(entry parkedBlock, err error) bool {
	d := parkCommitFailure(err)

	if d.blob == parkBlobKeep {
		sm.logger.Infof("[commitParkedBlock][%s] leaving the block parked (%s), parent %s: %v", entry.hash, d.reason, entry.prevBlock, err)
	} else {
		sm.logger.Errorf("[commitParkedBlock][%s] giving the block up (%s): %v", entry.hash, d.reason, err)
	}

	sm.applyParkDisposition(entry, d)

	return false
}

// noteCommittedParkedBlock does the peer bookkeeping for a block committed from
// disk. It is applied only when the peer that delivered the block is still
// registered, because a departed peer's height is not news and a nil peer
// (every block recovered from disk after a restart) has none.
func (sm *SyncManager) noteCommittedParkedBlock(entry parkedBlock) {
	height := entry.height

	if height <= 0 {
		_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &entry.hash)
		if err != nil {
			sm.logger.Warnf("[commitParkedBlock][%s] could not read back the committed height: %v", entry.hash, err)
		} else if h, convErr := safeconversion.Uint32ToInt32(meta.Height); convErr != nil {
			sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, convErr)
		} else {
			height = h
		}
	}

	sm.logger.Infof("accepted block %v at height %d from the park", entry.hash, height)

	sm.rejectedTxns.Clear()

	if entry.peer == nil || height <= 0 {
		return
	}

	state, ok := sm.peerStates.Get(entry.peer)
	if !ok {
		return
	}

	if sps, ok := sm.syncPeerStateFor(entry.peer); ok {
		sps.updateLastBlockTime()
	}

	entry.peer.UpdateLastBlockHeight(height)
	state.noteBestKnownHeight(height)
}

// livePeer returns the peer a post-commit action should be aimed at: the one
// that delivered the block while it is still connected, and otherwise the
// current sync peer, which may be nil. Used for the actions that keep sync
// moving — never for misbehaviour signals, which must not be redirected.
func (sm *SyncManager) livePeer(recorded *peerpkg.Peer) *peerpkg.Peer {
	if recorded != nil && recorded.Connected() {
		return recorded
	}

	return sm.loadSyncPeer()
}

// resumeHeaderWalk sends the download walk out again from wherever the cursor
// now is.
//
// Every rewind moves the cursor back and sends nothing. What actually issues a
// getdata is fetchHeaderBlocks, and its only callers are a block arriving, a
// headers message arriving, and the pipeline top-up after a block is committed —
// all of which are things that happen because sync is moving. In the regime the
// rewinds exist for, sync is not moving: the block that was given up on was the
// one everything else was queued behind, so no later block is coming to carry
// the rewound cursor out with it, and a node would sit on a perfectly good
// cursor until the stall detector rotated the peer and threw the cursor away.
//
// It sends the walk out again ONLY when the cursor is sitting on the front of
// the header list, and that condition is the whole of its safety.
//
// A rewound cursor is on the front: a block that was the front when it arrived
// goes back on the front, because headers only ever leave the list from there.
// So the case this exists for is covered. What the condition excludes is the
// window between the header batches of a checkpoint round, where the front is
// still the PREVIOUS round's anchor — a block already in the chain, which no
// peer will ever deliver again — and the cursor is on the first real header
// behind it. Fetching there wedges the round: the blocks arrive, none of them
// matches the front, so none comes off the list; the checkpoint batch then
// trims the anchor and the front becomes a block that has already been
// delivered, which nothing after it will ever match either. The checkpoint
// block is never recognised as the checkpoint, the next round of headers is
// never asked for, and sync sits until the stall detector rotates the peer.
//
// The other thing it excludes is an ordinary forward walk, where the cursor has
// been advanced past the front by the requests already in flight. Nothing is
// owed there: arriving blocks top the pipeline up by themselves.
//
// Called from the park sweep's ticker, on the block-queue consumer, so it costs
// one comparison per tick when there is nothing to do.
func (sm *SyncManager) resumeHeaderWalk() {
	sm.headerMu.Lock()
	cursorOnTheFront := sm.headerList != nil && sm.startHeader != nil && sm.startHeader == sm.headerList.Front()
	sm.headerMu.Unlock()

	if !cursorOnTheFront {
		return
	}

	sm.fetchMoreHeaderBlocks(sm.loadSyncPeer())
}

// sweepParkedBlocks is the safety net for blocks whose parent never arrives
// through a commit this node saw.
//
// Two things need it. A block can be parked for a reason other than a genuinely
// absent parent, because a missing parent is not the only thing that surfaces as
// ErrBlockNotFound. And a block recovered from disk after a restart never sees a
// commit event for a parent that was already in the chain when the node started,
// so nothing would ever drain it.
//
// It runs on the block-queue consumer, the same goroutine that commits blocks in
// order, and its chain lookups are capped per tick so it can never turn into a
// scan of the whole park.
func (sm *SyncManager) sweepParkedBlocks(now time.Time) {
	if !sm.blockPark.Enabled() {
		return
	}

	for _, entry := range sm.blockPark.Expire(now) {
		sm.logger.Warnf("[sweepParkedBlocks][%s] %s, giving the block up after %s: parent %s", entry.hash, parkDispositionExpired.reason, parkEntryTTL, entry.prevBlock)
		sm.applyParkDisposition(entry, parkDispositionExpired)
	}

	for _, candidate := range sm.blockPark.StuckCandidates(now, parkSweepRPCBudget) {
		exists, err := sm.blockchainClient.GetBlockExists(sm.ctx, &candidate.prevBlock)
		if err != nil {
			sm.logger.Warnf("[sweepParkedBlocks][%s] could not check whether parent %s is stored: %v", candidate.hash, candidate.prevBlock, err)
			continue
		}

		if !exists {
			continue
		}

		entry, ok := sm.blockPark.Take(candidate.hash)
		if !ok {
			continue
		}

		sm.logger.Infof("[sweepParkedBlocks][%s] parent %s is in the chain after all, committing the parked block", entry.hash, entry.prevBlock)

		if sm.commitParkedBlock(entry) {
			sm.drainParkedDescendants(entry.hash)
		}
	}
}
