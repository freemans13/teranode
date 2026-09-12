package netsync

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// pipelineVerifiedBlock is what pipelineBlockSink records once a block's merkle
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
// subtree hashes for the rest of the process's life until that wiring lands,
// on a branch whose entire purpose is bounding memory.
type pipelineVerifiedBlock struct {
	height        uint32
	subtreeHashes []chainhash.Hash
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

	// A missing parent is the ordinary out-of-order case, not a local fault: the
	// header round can outrun body delivery, and this is how that shows up here.
	_, parentMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &header.PrevBlock)
	if err != nil {
		return errors.NewBlockInvalidError("[pipelineBlockSink][%s] parent %s not found", hash, header.PrevBlock, err)
	}

	height := parentMeta.Height + 1

	quickValidation := sm.quickValidationAllowed(height)
	writer := newSubtreeWriter(sm.logger, sm.settings, sm.subtreeStore, height, quickValidation)

	// The uint32 conversion is safe because newBlockTxStream refuses any count
	// above maxBlockTxCount (1<<31), well inside uint32's range.
	dedup := txmap.NewSplitSwissMapUint64(uint32(stream.TxCount())) //nolint:gosec // bounded by maxBlockTxCount (1<<31) in newBlockTxStream

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

	// PLACEHOLDER: see pipelineVerifiedBlock's doc comment above for what this
	// is, what it is not (the block is not simply dropped — see the
	// AdoptWritten/park-budget consequence there), and why it is never pruned.
	sm.pipelineVerifiedMu.Lock()
	if sm.pipelineVerified == nil {
		sm.pipelineVerified = make(map[chainhash.Hash]pipelineVerifiedBlock)
	}

	sm.pipelineVerified[hash] = pipelineVerifiedBlock{height: height, subtreeHashes: subtreeHashes}
	sm.pipelineVerifiedMu.Unlock()

	return nil
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

// pipelineSubtreeHashesFor returns the subtree root hashes pipelineBlockSink
// recorded for hash, in block order, or nil if the sink has not verified it.
func (sm *SyncManager) pipelineSubtreeHashesFor(hash chainhash.Hash) []chainhash.Hash {
	sm.pipelineVerifiedMu.Lock()
	defer sm.pipelineVerifiedMu.Unlock()

	return sm.pipelineVerified[hash].subtreeHashes
}
