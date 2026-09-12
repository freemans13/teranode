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

// pipelineSubtreeHashesFor returns the subtree root hashes pipelineBlockSink
// recorded for hash, in block order, or nil if the sink has not verified it.
func (sm *SyncManager) pipelineSubtreeHashesFor(hash chainhash.Hash) []chainhash.Hash {
	sm.pipelineVerifiedMu.Lock()
	defer sm.pipelineVerifiedMu.Unlock()

	return sm.pipelineVerified[hash].subtreeHashes
}
