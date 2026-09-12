package netsync

import (
	"bytes"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// pipelineVerifiedEntry is what pipelineBlockSink records once a block's merkle
// root has been checked against its header.
//
// PLACEHOLDER, and named as one rather than hidden. Nothing downstream reads
// this map yet: the committer still takes its blocks from the park. Replacing
// it with a park record, so a pipelined block reaches block validation by the
// same route a parked one does, is the next plan's first task.
//
// This is NOT simply "convert, verify and drop". installStreamingBlockPath
// installs one gate/sink/delete triple regardless of which sink is active, and
// after ANY sink returns nil the wire layer still dispatches a blockOnDiskMsg
// that handleBlockOnDiskMsg turns into a blockPark.AdoptWritten call
// (streaming_install.go). With the pipeline sink on, no body was ever written
// to the park's store for that hash, so AdoptWritten registers an entry and
// charges its byte budget for a blob that does not exist, and the drain will
// later try to read it and fail. That phantom-entry consequence is exactly why
// the setting must stay off until the committer is wired to reach a pipelined
// block by some route other than the park — see the setting's longdesc in
// settings/legacy_settings.go.
//
// sm.pipelineVerified is also never pruned: every verified block holds its
// full model.Block for the rest of the process's life until that wiring
// lands, on a branch whose entire purpose is bounding memory.
type pipelineVerifiedEntry struct {
	// block is recorded as a model.Block rather than a bare subtree list
	// because that is exactly what the committer needs and exactly what
	// serializes: (*model.Block).Bytes writes the header, counts, subtree
	// list and coinbase, and model.NewBlockFromBytes reads them back. A
	// narrower record would mean inventing a format for it and then
	// converting to this anyway.
	block *model.Block
	// written is every artefact the subtreeWriter put in the store for this
	// block, kept so pipelineBlockDelete can remove exactly what the sink
	// wrote after the writer itself has gone out of scope. Without this, a
	// post-sink failure in the wire layer (readBlockMessage's short-body
	// check or its transaction-count read, services/legacy/peer/wire_streaming.go)
	// had nothing to tell the delete callback which files to remove.
	written []writtenSubtree
}

// pipelineBlockSink converts a block as it streams off the socket.
//
// It is the receive half of the design: the wire layer hands over the parsed
// header and a reader positioned at the transaction count, and this drives the
// builder, the writer and the merkle accumulator without the block ever being a
// whole object in memory. Resident state is one subtree under construction plus
// one 32-byte hash per completed subtree.
//
// Ordering is load-bearing. The merkle root is compared against the header BEFORE
// the block is offered to anything downstream, because below a checkpoint quick
// validation skips script validation and outpoint-only mode skips the check
// binding a spend to its spender, so the root is the only thing asserting that
// this body is the body that header commits to.
func (sm *SyncManager) pipelineBlockSink(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) error {
	// Resolved before anything is read from r, and that ordering is load-
	// bearing: the fallback below hands r to streamingBlockSink untouched, and
	// that only works if nothing — not even the coinbase — has been consumed
	// from it yet.
	height, resolved := sm.pipelineParentHeight(header.PrevBlock)
	if !resolved {
		// Neither the in-flight header list nor the committed chain has this
		// block's parent, which is a genuine miss beyond the ordinary
		// out-of-order case pipelineParentHeight covers (see its doc comment).
		//
		// There is no error return from this sink that the wire layer treats
		// as anything other than a malformed message: peer.shouldHandleReadError
		// (services/legacy/peer/peer.go) disconnects on every error except an
		// exact io.EOF, io.ErrUnexpectedEOF or non-temporary net.OpError, none
		// of which fit "decline this one and let the ordinary path retry it".
		// So this does not error. It defers to streamingBlockSink instead,
		// which writes the untouched body to the park exactly as it would with
		// PipelineReceive off, and lets the existing park/drain machinery
		// decide the block's fate the way it already correctly does for the
		// non-pipeline path.
		return sm.streamingBlockSink(hash, header, r, n)
	}

	stream, err := newBlockTxStream(r, n)
	if err != nil {
		return err
	}

	// The coinbase is the first transaction in the stream, and the builder needs
	// it before any other: it occupies slot zero of the first subtree.
	coinbase, _, err := stream.Next()
	if err != nil {
		return errors.NewBlockInvalidError("[pipelineBlockSink][%s] failed reading the coinbase", hash, err)
	}

	quickValidation := sm.quickValidationAllowed(height)
	writer := newSubtreeWriter(sm.logger, sm.settings, sm.subtreeStore, height, quickValidation)

	// dedup is never sized from stream.TxCount(). See newPipelineDedupMap's
	// doc comment for why: that count is the peer's own declared transaction
	// count, and sizing a map from it is what let a peer make this node
	// allocate roughly 19 GB by declaring a number.
	dedup := newPipelineDedupMap()

	builder, err := newBlockStreamBuilder(int(stream.TxCount()), sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree, coinbase, writer.Emit(sm.ctx), dedup)
	if err != nil {
		// newBlockStreamBuilder itself never calls Emit, so writer has written
		// nothing yet: this call is a no-op today. It is here anyway because the
		// brief's rule is "every failure path calls DeleteAll" without exception,
		// and leaving this one out is a trap for whoever adds work between
		// newSubtreeWriter and here.
		sm.deleteWrittenOnFailure(hash, writer)

		return err
	}

	for {
		tx, txHash, streamErr := stream.Next()
		if streamErr != nil {
			if errors.Is(streamErr, errBlockTxStreamDone) {
				break
			}

			sm.deleteWrittenOnFailure(hash, writer)

			return streamErr
		}

		if addErr := builder.AddTx(tx, txHash); addErr != nil {
			sm.deleteWrittenOnFailure(hash, writer)

			return addErr
		}
	}

	root, subtreeHashes, err := builder.Finish()
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return err
	}

	if !root.IsEqual(&header.MerkleRoot) {
		sm.deleteWrittenOnFailure(hash, writer)

		return errors.NewBlockInvalidError("[pipelineBlockSink][%s] merkle root %s does not match header's %s", hash, root, header.MerkleRoot)
	}

	// Convert the wire header into a teranode header the same way
	// HandleBlockDirect does (handle_block.go:209-217): serialise it and
	// parse it back with model.NewBlockHeaderFromBytes, rather than inventing
	// a second conversion.
	var headerBytes bytes.Buffer
	if err = header.Serialize(&headerBytes); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return errors.NewProcessingError("[pipelineBlockSink][%s] failed to serialize header", hash, err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return errors.NewProcessingError("[pipelineBlockSink][%s] failed to create block header from bytes", hash, err)
	}

	// Finish returns subtree hashes as values; model.NewBlock wants pointers.
	// Take the address of a loop-local copy, never of the range variable —
	// ranging over subtreeHashes would otherwise leave every pointer aliasing
	// the same backing slot.
	subtreeHashPointers := make([]*chainhash.Hash, len(subtreeHashes))

	for i := range subtreeHashes {
		h := subtreeHashes[i]
		subtreeHashPointers[i] = &h
	}

	// Recorded as a model.Block rather than a bare subtree list because that is
	// exactly what the committer needs and exactly what serializes: (*Block).Bytes
	// writes the header, counts, subtree list and coinbase, and NewBlockFromBytes
	// reads them back. Keeping a narrower record would mean inventing a format for
	// it and then converting to this anyway.
	//
	// blockID is 0: on the unified below-checkpoint route the server assigns it
	// inside quickValidateBlock, which is also where the UTXO work happens. See
	// handle_block.go:607.
	verified, err := model.NewBlock(modelHeader, coinbase, subtreeHashPointers, stream.TxCount(), uint64(n), height, 0)
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return errors.NewProcessingError("[pipelineBlockSink][%s] failed to build block model", hash, err)
	}

	// PLACEHOLDER: see pipelineVerifiedEntry's doc comment above for what this
	// is, what it is not (the block is not simply dropped — see the
	// AdoptWritten/park-budget consequence there), and why it is never pruned.
	sm.pipelineVerifiedMu.Lock()
	if sm.pipelineVerified == nil {
		sm.pipelineVerified = make(map[chainhash.Hash]pipelineVerifiedEntry)
	}

	sm.pipelineVerified[hash] = pipelineVerifiedEntry{block: verified, written: writer.Written()}
	sm.pipelineVerifiedMu.Unlock()

	return nil
}

// pipelineBlockDelete is the pipeline path's orphan-delete callback, installed
// alongside pipelineBlockSink for the same reason streamingBlockDelete is
// installed alongside streamingBlockSink: a body can be written successfully
// and only then found unusable by the wire layer's own post-sink checks (the
// short-body check or the transaction-count read in readBlockMessage,
// services/legacy/peer/wire_streaming.go).
//
// Before this existed, installStreamingBlockPath installed streamingBlockDelete
// unconditionally, whichever sink was active. That deletes from the park's
// blob store; the pipeline sink never writes there, so that delete found
// nothing to remove while the subtree files the pipeline sink actually wrote,
// and the in-memory pipelineVerified entry, were left behind forever.
//
// This cleans up both of what a call under this hash can have written:
// the subtree files recorded in pipelineVerified, if pipelineBlockSink
// completed and verified the block itself; and, unconditionally,
// whatever streamingBlockDelete itself would remove, because
// pipelineParentHeight's unresolvable-parent fallback (see pipelineBlockSink)
// hands the block to the raw park-write sink instead of converting it, and
// that body needs the ordinary park delete regardless of which top-level
// sink function is nominally "the pipeline sink" for this call.
func (sm *SyncManager) pipelineBlockDelete(hash chainhash.Hash) error {
	sm.pipelineVerifiedMu.Lock()
	entry, ok := sm.pipelineVerified[hash]
	if ok {
		delete(sm.pipelineVerified, hash)
	}
	sm.pipelineVerifiedMu.Unlock()

	var firstErr error

	if ok {
		for _, w := range entry.written {
			if err := sm.subtreeStore.Del(sm.ctx, w.Hash[:], w.FileType); err != nil && firstErr == nil {
				firstErr = errors.NewStorageError("[pipelineBlockDelete][%s] failed deleting %s for subtree %s", hash, w.FileType, w.Hash, err)
			}
		}
	}

	if err := sm.streamingBlockDelete(hash); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// pipelineParentHeight resolves a block's height from its parent's hash,
// without requiring the parent to be a COMMITTED block.
//
// The ordinary out-of-order case is a parent that is still in the in-flight
// header list (sm.headerIndex) — measured at roughly 91% of blocks on this
// node — because the header round can outrun body delivery. Asking only
// sm.blockchainClient.GetBlockHeader, which answers only for a committed
// block (parentIsInChain in streaming_install.go makes the identical call for
// exactly that meaning), treated that ordinary case as a fault.
//
// The header list is checked first because it is the common case and never
// blocks. The store is checked second, with headerMu already released,
// because a blockchain client call can take an unbounded time and headerMu's
// own invariant (manager.go's "Rule B" comment on the SyncManager struct)
// forbids holding it across one.
func (sm *SyncManager) pipelineParentHeight(parent chainhash.Hash) (uint32, bool) {
	sm.headerMu.Lock()
	e, inList := sm.headerIndex[parent]
	sm.headerMu.Unlock()

	if inList && e != nil {
		if node, ok := e.Value.(*headerNode); ok && node != nil && node.height >= 0 {
			return uint32(node.height) + 1, true
		}
	}

	if sm.blockchainClient == nil {
		return 0, false
	}

	_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)
	if err != nil {
		return 0, false
	}

	return meta.Height + 1, true
}

// dedupInitialCapacity bounds the pipeline dedup map's pre-sizing hint. It is a
// fixed constant rather than the peer's declared transaction count on purpose
// — see newPipelineDedupMap.
//
// ~1,048,576 slots costs roughly 50 MB with NewSplitSwissMapUint64's own 20%
// per-bucket headroom (go-tx-map tx_map.go). A block larger than this still
// dedups correctly: Put is what makes the CVE-2012-2459 check exact, not the
// pre-sizing, so a map that starts smaller than the block just grows through
// its own ordinary rehashing as real transactions arrive, the same as any Go
// map given a low capacity hint.
const dedupInitialCapacity uint32 = 1 << 20

// newPipelineDedupMap returns a fresh duplicate-transaction map for the
// pipeline sink, pre-sized at dedupInitialCapacity regardless of what a peer
// declared for the block's transaction count.
//
// The declared count is never trusted for this because it is checked only
// against the wire payload ceiling (services/legacy/config.go
// maxWireBlockPayload, 4,000,000,000 bytes) against a 10-byte minimum
// transaction size — so a peer may declare up to 400,000,000 transactions in
// a body it then never sends. txmap.NewSplitSwissMapUint64 pre-sizes all 1024
// buckets eagerly from whatever length it is given (go-tx-map tx_map.go
// NewSplitSwissMapUint64), so sizing this map from that declared count would
// allocate roughly 19 GB before a single transaction byte arrives — gated by
// nothing but a hash and a proof-of-work check that any sync peer already
// passes.
func newPipelineDedupMap() txmap.TxMap {
	return txmap.NewSplitSwissMapUint64(dedupInitialCapacity)
}

// deleteWrittenOnFailure calls writer.DeleteAll and, if the delete itself
// fails, logs a single-line warning naming the block hash rather than
// discarding the error silently. subtreeWriter's own doc comment explains why
// an orphaned subtree file is dangerous (it is content-addressed and shared
// with any other block carrying the same run of transactions); a cleanup
// failure that goes unlogged defeats the point of calling DeleteAll at all,
// since nothing else will ever tell an operator those files are still there.
func (sm *SyncManager) deleteWrittenOnFailure(hash chainhash.Hash, writer *subtreeWriter) {
	if delErr := writer.DeleteAll(sm.ctx); delErr != nil {
		sm.logger.Warnf("[pipelineBlockSink][%s] failed to delete subtree files after a failed block: %v", hash, delErr)
	}
}

// pipelineVerifiedBlockFor returns the model.Block pipelineBlockSink recorded
// for hash, or nil if the sink has not verified it.
func (sm *SyncManager) pipelineVerifiedBlockFor(hash chainhash.Hash) *model.Block {
	sm.pipelineVerifiedMu.Lock()
	defer sm.pipelineVerifiedMu.Unlock()

	return sm.pipelineVerified[hash].block
}
