package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// holdsBlock reports whether this node already has the block on disk, under
// either of the two shapes a receive path writes.
//
// This is the authority for "what do I hold", and it is the filesystem rather
// than any in-memory index. The park's own Has() reads its entry map, which is
// empty on a restarting node until recovery has run, and which says nothing
// about a block the park has already committed and deleted. A map that is the
// only record of a downloaded block makes every bound on that map a decision to
// throw a block away, and every such decision needs a mechanism somewhere else
// to put it back. There is nothing to put back here.
//
// The answer is trustworthy because of when the files are written, not because
// of what they contain. The streaming path writes one file whose name is the
// block's hash; the pipeline path writes a small record naming the block's
// subtree files, and writes it only after the block's merkle root has been
// checked against its header, deleting anything already written if that check
// fails. So a record's existence proves a complete body arrived and verified.
//
// Every write goes to a temporary sibling, is flushed, and is renamed into
// place, so a partial file is never published and a torn read is not possible.
//
// A store error answers false. That is the safe direction: we re-ask for a block
// we may already have, which costs bandwidth, rather than skip one we do not,
// which stops the chain.
func (sm *SyncManager) holdsBlock(ctx context.Context, hash chainhash.Hash) bool {
	if sm.blockPark == nil || sm.blockPark.store == nil {
		return false
	}

	readCtx, cancel := sm.blockPark.storeCtx(ctx)
	defer cancel()

	// The pipeline record first. It is a few hundred bytes against the whole
	// block's gigabytes, it is the shape the new model writes, and on a node
	// running the pipeline path it is the one that will be there.
	if exists, err := sm.blockPark.store.Exists(readCtx, hash[:], fileformat.FileTypeBlock, parkOpts...); err == nil && exists {
		return true
	}

	exists, err := sm.blockPark.store.Exists(readCtx, hash[:], parkFileType, parkOpts...)
	if err != nil {
		sm.logger.Warnf("[holdsBlock][%s] could not ask the store whether the block is on disk, assuming it is not: %v", hash, err)

		return false
	}

	return exists
}
