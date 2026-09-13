package netsync

import peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"

// assignWantedBlocks asks peers for the blocks this node wants next.
//
// There is no loop and no cursor. The pass computes the wanted range from the
// best block processed, drops what is already owed, hands the rest out, and
// returns. Everything it needs is recomputed each time, so there is no position
// to strand, nothing to rewind, and no state that can disagree with the chain.
//
// It replaces a walk that kept a cursor into the header list. That cursor
// answered three different questions at once — where to resume, whether a round
// was still valid, and whether the front had been asked for — and each fix for
// one broke another. In one day on mainnet that walk produced a read-ahead limit
// that raised itself as downloads arrived, a park filling to its 4,096-entry cap
// and refusing the one block that would extend the tip, a node moving 1.8 blocks
// a minute against a 23ms commit path, and a cursor stranded above the limit
// with 955,208 headers queued and nothing requested.
//
// Nothing calls this yet. The wiring is a later task.
func (sm *SyncManager) assignWantedBlocks() {
	wanted := sm.wantedBlocks()

	candidates := sm.unownedBlocks(wanted)
	if len(candidates) == 0 {
		return
	}

	assigner := sm.newDownloadAssigner()
	if assigner == nil {
		return
	}

	sm.requestBlocks(assigner, candidates)

	// One send per peer that got work, with headerMu released. This is the only
	// place in the pass that talks to a peer at all.
	assigner.send(sm)
}

// wantedBlocks is the locked half of the pass: the range the node wants next,
// bounded by the same scaled read-ahead depth that governs the header walk.
//
// Split out so the lock has a body with no way to reach a peer or another
// service from inside it. headerMu is held across two pure in-memory reads and
// released before anything else happens.
func (sm *SyncManager) wantedBlocks() []wantedBlock {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	best := sm.committedHeight()

	// The read-ahead ceiling is an absolute height and is already scaled by the
	// block size actually being seen, so the depth is derived from it rather
	// than configured a second time. One source for "how far ahead may I read"
	// is the point: two would disagree, and the disagreement would show up as a
	// park that grows past the bound one of them believed in.
	if ceiling, limited := sm.lookaheadCeilingLocked(); limited {
		return sm.wantedBlocksLocked(best, int32(ceiling-int64(best))) //nolint:gosec // the ceiling is best plus a block count
	}

	// No read-ahead limit configured. The node-wide download window is then the
	// only honest bound left: nothing past it could be requested in this pass
	// anyway, so naming more of the header list buys nothing. A nil settings is
	// possible in a bare-struct test harness, and Legacy sits past the 4 KB
	// guard page, where an unguarded dereference is a hardware fault rather than
	// a recoverable panic.
	depth := int32(1)
	if sm.settings != nil {
		depth = int32(max(1, sm.settings.Legacy.BlockDownloadWindow)) //nolint:gosec // a block count, not a size
	}

	return sm.wantedBlocksLocked(best, depth)
}

// unownedBlocks keeps the wanted blocks nobody currently owes us, and lets a
// quiet owner off the hook on the way past.
//
// The RequestedWithin test comes FIRST, and that order is the rule rather than
// an accident: a block inside its retry window has an owner who may still
// deliver, and asking for it again spends a peer slot that a block nobody owes
// could have used. Forgiving it as well would be worse still, because
// forgiveness frees the owner's budget and the pass would then hand the same
// peer the same block it is already carrying.
//
// Forgiveness runs before the budgets are read, not after. A peer that has gone
// quiet holding a full slice keeps every one of those slots against its
// per-peer cap until somebody releases them, so a pass that read the budgets
// first would find that peer at zero and hand out nothing — the stall this rule
// exists to break. ForgiveOwners keeps the ownership and drops only the
// obligation, so a copy still on the wire from the quiet peer is admitted when
// it lands.
//
// Takes no lock but the download ledger's own, so it is safe with headerMu
// released and must not be called with it held.
func (sm *SyncManager) unownedBlocks(wanted []wantedBlock) []wantedBlock {
	candidates := make([]wantedBlock, 0, len(wanted))

	for _, block := range wanted {
		if sm.blockDownloads.RequestedWithin(block.hash, blockRequestRetryInterval) {
			continue
		}

		sm.blockDownloads.ForgiveOwners(block.hash, blockRequestRetryInterval)

		candidates = append(candidates, block)
	}

	return candidates
}

// requestBlocks places each candidate with a peer that has budget for it and
// builds that peer's getdata. It stops at the first block no peer can take,
// because the candidates ascend and a peer that cannot serve one cannot serve
// anything above it.
//
// Stopping is the whole of the termination argument: the range is bounded before
// this is reached, every iteration consumes one entry of it, and nothing here
// re-reads the range or asks for another round. A block left unplaced is simply
// picked up by the next pass, which recomputes the range from the best block
// processed — there is no position to lose it from.
//
// A block whose quiet owner has just been forgiven is never re-asked of that
// same owner. The assigner is told to prefer any other peer with budget, and
// when the owner is the only peer left the block is reasserted rather than
// requested a second time. Sending it twice would have the peer answer twice,
// and the second copy arrives after the first discharged the obligation
// (handleBlockMsg calls RemoveOwner on the answering peer), so it looks
// unrequested and costs an honest peer its whole association.
//
// On a node with one peer that means the block is not re-asked at all, which is
// the right answer rather than a gap: there is nobody to help, so the only thing
// a second getdata could achieve is the disconnect above. Recovery is the peer's
// own stall detection, the frontier race, and the ledger's expiry.
func (sm *SyncManager) requestBlocks(assigner *downloadAssigner, candidates []wantedBlock) {
	for _, block := range candidates {
		target, ok := assigner.takeAvoiding(block.height, func(p *peerpkg.Peer) bool {
			return sm.blockDownloads.HasOwner(p, block.hash)
		})
		if !ok {
			return
		}

		// The assigner had nobody but the peer that already holds our request
		// for this block. Re-arm what we hold instead of asking twice: this
		// refreshes the retry window, so the next pass waits another interval
		// before considering the block again, and sends nothing.
		if sm.blockDownloads.ReassertOwner(target.peer, block.hash) {
			// Charged like a request, because that is what it is to the two
			// caps. ReassertOwner clears the forgiven flag, so the block is back
			// in CountForPeer and back in Len from here on, while both budgets
			// were computed with the forgiven records excluded.
			target.budget--
			assigner.remaining--

			continue
		}

		// Record the request before it goes out. A block the ledger will not
		// take is a block we must not ask for: the reply would arrive with
		// nothing vouching for it and cost an honest peer its connection.
		if !sm.blockDownloads.Add(target.peer, block.hash) {
			sm.logger.Warnf("[assignWantedBlocks] block download ledger full at %d blocks, holding off on %s", maxTrackedBlockDownloads, block.hash)

			return
		}

		hash := block.hash
		if err := assigner.recordRequest(target, &hash); err != nil {
			// The ledger was told about a request that is not going to be sent,
			// so take it back. Left in place the hash is owned by a peer that
			// was never asked, which answers RequestedWithin for the whole
			// ownership ceiling and quietly holds every later pass off it.
			sm.blockDownloads.RemoveOwner(target.peer, block.hash)

			sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)

			return
		}
	}
}
