package netsync

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

// wantedBlock is one block the node wants next, named by the header list.
type wantedBlock struct {
	height int32
	hash   chainhash.Hash
}

// wantedBlocksLocked returns the blocks the node wants next: the contiguous run
// above best, up to depth blocks, that the header list can name.
//
// This is the whole of "what do I want". It is derived from the best block
// processed every time it is asked, so it cannot go stale, cannot be left behind
// and cannot be pointed at the wrong place. The design it replaces kept a cursor
// into the header list, which on 2026-09-12 came to rest above the read-ahead
// limit and left the node holding 955,208 headers while requesting nothing.
//
// The run stops at the first height the list cannot name rather than skipping
// it. A block whose parent never arrives cannot commit, so blocks fetched beyond
// a hole only wait in the park, and a park that fills starts refusing the very
// block that would close the hole.
//
// Callers must hold headerMu.
func (sm *SyncManager) wantedBlocksLocked(best int32, depth int32) []wantedBlock {
	if depth < 1 {
		// A depth of zero asks for nothing for ever, which is a stall wearing a
		// setting's clothes. One block at a time is slow; none is stopped.
		depth = 1
	}

	wanted := make([]wantedBlock, 0, depth)

	for i := int32(1); i <= depth; i++ {
		node, ok := sm.headerAtHeightLocked(best + i)
		if !ok || node.hash == nil {
			break
		}

		wanted = append(wanted, wantedBlock{height: node.height, hash: *node.hash})
	}

	return wanted
}
