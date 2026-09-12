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
// same route a parked one does, is the next plan's first task. Until then this
// sink converts and verifies a block and then drops it, which is why the
// setting defaults off.
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
		return err
	}

	for {
		tx, txHash, streamErr := stream.Next()
		if streamErr != nil {
			if errors.Is(streamErr, errBlockTxStreamDone) {
				break
			}

			_ = writer.DeleteAll(sm.ctx)

			return streamErr
		}

		if addErr := builder.AddTx(tx, txHash); addErr != nil {
			_ = writer.DeleteAll(sm.ctx)

			return addErr
		}
	}

	root, subtreeHashes, err := builder.Finish()
	if err != nil {
		_ = writer.DeleteAll(sm.ctx)

		return err
	}

	if !root.IsEqual(&header.MerkleRoot) {
		_ = writer.DeleteAll(sm.ctx)

		return errors.NewBlockInvalidError("[pipelineBlockSink][%s] merkle root %s does not match header's %s", hash, root, header.MerkleRoot)
	}

	// PLACEHOLDER, and named as one rather than hidden. Nothing downstream reads
	// this map yet: the committer still takes its blocks from the park. Replacing
	// it with a park record, so a pipelined block reaches block validation by the
	// same route a parked one does, is the next plan's first task. Until then this
	// sink converts and verifies a block and then drops it, which is why the
	// setting defaults off.
	sm.pipelineVerifiedMu.Lock()
	if sm.pipelineVerified == nil {
		sm.pipelineVerified = make(map[chainhash.Hash]pipelineVerifiedBlock)
	}

	sm.pipelineVerified[hash] = pipelineVerifiedBlock{height: height, subtreeHashes: subtreeHashes}
	sm.pipelineVerifiedMu.Unlock()

	return nil
}

// pipelineSubtreeHashesFor returns the subtree root hashes pipelineBlockSink
// recorded for hash, in block order, or nil if the sink has not verified it.
func (sm *SyncManager) pipelineSubtreeHashesFor(hash chainhash.Hash) []chainhash.Hash {
	sm.pipelineVerifiedMu.Lock()
	defer sm.pipelineVerifiedMu.Unlock()

	return sm.pipelineVerified[hash].subtreeHashes
}
