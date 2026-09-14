package netsync

import (
	"bytes"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

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
//
// The bool it returns is the ONLY honest source of "did this delivery convert
// the block" anywhere in the system: true only on the path that actually wrote
// a converted record, false on every other return including the fallback.
// handleBlockOnDiskMsg reads it off BlockBody.Converted rather than inferring
// conversion by asking whether some blob happens to exist for this hash — see
// that field's doc comment for why a blob's mere existence is the wrong
// question.
func (sm *SyncManager) pipelineBlockSink(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
	// Every block converts. There is no fallback and nothing writes a whole
	// block body to one file any more.
	//
	// An unknown parent height does not stop it. The height feeds exactly two
	// decisions and both have a safe answer without one. The subtree file type
	// takes .subtreeToCheck, which means "still needs validating": conservative,
	// because writing .subtree unearned would skip validation, while writing
	// .subtreeToCheck unnecessarily only costs work. The delete-at-height falls
	// back to the committed tip plus the read-ahead depth plus the retention,
	// which is above any height this block can actually have, so it gives the
	// same guarantee a known height does: pruning is driven by the committed
	// chain, and a block waiting in the park is above it. The committer
	// re-derives the height from the store before committing anything, so a
	// record carrying zero is corrected there.
	//
	// Two refusals used to sit here and both were wrong. One declined to convert
	// above the final checkpoint, on the grounds that a converted record carries
	// block ID zero and only quick validation assigns one: but the ordinary route
	// already passes zero up there, because its assignment sits inside a
	// quick-validation-only branch, and zero is the universal "assign
	// server-side" convention that full validation reads too. The other declined
	// when the parent height would not resolve, which was a constructor argument
	// with no answer for "unknown" rather than a rule about anything.
	height, resolved := sm.pipelineParentHeight(header.PrevBlock)

	stream, err := newBlockTxStream(r, n)
	if err != nil {
		return false, err
	}

	// The coinbase is the first transaction in the stream, and the builder needs
	// it before any other: it occupies slot zero of the first subtree.
	coinbase, _, err := stream.Next()
	if err != nil {
		return false, errors.NewBlockInvalidError("[pipelineBlockSink][%s] failed reading the coinbase", hash, err)
	}

	var writer *subtreeWriter
	if resolved {
		writer = newSubtreeWriter(sm.logger, sm.settings, sm.subtreeStore, height, sm.quickValidationAllowed(height))
	} else {
		writer = newSubtreeWriterUnresolvedHeight(sm.logger, sm.settings, sm.subtreeStore, sm.fallbackSubtreeDAH())
	}

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

		return false, err
	}

	for {
		tx, txHash, streamErr := stream.Next()
		if streamErr != nil {
			if errors.Is(streamErr, errBlockTxStreamDone) {
				break
			}

			sm.deleteWrittenOnFailure(hash, writer)

			return false, streamErr
		}

		if addErr := builder.AddTx(tx, txHash); addErr != nil {
			sm.deleteWrittenOnFailure(hash, writer)

			return false, addErr
		}
	}

	root, subtreeHashes, err := builder.Finish()
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, err
	}

	if !root.IsEqual(&header.MerkleRoot) {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewBlockInvalidError("[pipelineBlockSink][%s] merkle root %s does not match header's %s", hash, root, header.MerkleRoot)
	}

	// Convert the wire header into a teranode header the same way
	// HandleBlockDirect does (handle_block.go:209-217): serialise it and
	// parse it back with model.NewBlockHeaderFromBytes, rather than inventing
	// a second conversion.
	var headerBytes bytes.Buffer
	if err = header.Serialize(&headerBytes); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to serialize header", hash, err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to create block header from bytes", hash, err)
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
	// blockID is 0 on every route, not only below the checkpoint: it is the
	// universal "assign server-side" convention. Below the checkpoint,
	// quickValidateBlock assigns it and does the UTXO create/spend the record
	// carries no other way to do. Above it, buildAddBlockOpts
	// (services/blockvalidation/BlockValidation.go) returns nil for a zero ID
	// and AddBlock behaves exactly as it does for any other peer-fetched block,
	// which is never pre-assigned one either — see Step 1's finding in the task
	// report for the file:line evidence.
	verified, err := model.NewBlock(modelHeader, coinbase, subtreeHashPointers, stream.TxCount(), uint64(n), height, 0)
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to build block model", hash, err)
	}

	// The record is now genuinely on disk: a serialized model.Block under
	// fileformat.FileTypeBlock, a few hundred bytes rather than the gigabytes a
	// decoded block would take. This is what closes the defect this task
	// exists for. installStreamingBlockPath installs one gate/sink/delete
	// triple regardless of which sink is active, and after ANY sink returns
	// nil the wire layer still dispatches a blockOnDiskMsg that
	// handleBlockOnDiskMsg turns into a blockPark.AdoptWritten call
	// (streaming_install.go). Before this write existed, no body had ever been
	// written to the park's store for this hash, so AdoptWritten registered an
	// entry and charged the park's byte budget for a blob that did not exist,
	// and the drain later failed to read it. Writing here, before this
	// function can report success, means that by the time AdoptWritten runs
	// there is something real underneath it.
	if err = sm.blockPark.WriteConvertedBlock(sm.ctx, hash, verified); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewStorageError("[pipelineBlockSink][%s] failed to write the converted record", hash, err)
	}

	return true, nil
}

// pipelineBlockDelete is the pipeline path's orphan-delete callback, installed
// alongside pipelineBlockSink for the same reason streamingBlockDelete is
// installed alongside streamingBlockSink: a body can be written successfully
// and only then found unusable by the wire layer's own post-sink checks (the
// short-body check or the transaction-count read in readBlockMessage,
// services/legacy/peer/wire_streaming.go).
//
// Before FIX 3, installStreamingBlockPath installed streamingBlockDelete
// unconditionally, whichever sink was active. That deletes from the park's
// blob store; the pipeline sink never writes there, so that delete found
// nothing to remove while the subtree files the pipeline sink actually wrote
// were left behind forever.
//
// converted is THIS call's own answer to "did the delivery that just failed
// actually convert anything" — blockBodySink's own return value for that one
// call, threaded through unchanged by the wire layer (BlockBody.Converted,
// deleteOrphanedBody). It is NOT re-derived by asking whether a converted
// record happens to exist for hash right now, which a fix round after this
// one's first cut found was still wrong: a hash can be re-requested and
// re-delivered while an EARLIER delivery for it is still genuinely parked,
// waiting on its own parent — ownership is released as soon as a delivery's
// sink call finishes, and the streaming gate accepts any hash asked for
// within the last hour — so a second, unrelated delivery's own failure
// (a body that ends short, for example) must not read that earlier delivery's
// record back and delete the subtree files it names out from under it. Only
// when THIS call is known, from its own return value, to have written a
// record does this function go looking for what it wrote.
//
// This task also retired the in-memory pipelineVerified map that used to tell
// this function which subtree files to remove. It is not replaced with a
// second map — a second map keyed by hash would have exactly the same
// cross-delivery problem converted exists to avoid. Instead, when converted is
// true, the record ReadConverted hands back names every subtree THIS call's
// own sink wrote (record.Subtrees) — true only because converted being true
// guarantees this call was the one that just wrote whatever sits under hash
// right now, with nothing else able to have raced in between — and the
// structure file type each one was written under — FileTypeSubtree or
// FileTypeSubtreeToCheck — is a pure function of the record's own height via
// quickValidationAllowed, the same test subtreeWriter used to choose it while
// writing, EXCEPT at height 0: that is pipelineParentHeight's own "unresolved"
// sentinel (see its doc comment), never a real height, and subtreeWriter's
// unresolved-height constructor always wrote FileTypeSubtreeToCheck for it
// regardless of what quickValidationAllowed(0) would say (0 reads as below
// every checkpoint, which is exactly the misreading that constructor exists to
// avoid). Reusing quickValidationAllowed here for that case would ask for the
// wrong file type and leave the real one behind.
//
// The converted record itself is NOT deleted here. It is deleted inside
// blockPark.Delete, which streamingBlockDelete below calls unconditionally, so
// that every path that retires a park entry — not only this discard path —
// retires the record with it. Deleting it a second time here would only race
// that call harmlessly, so there is nothing to gain by keeping a second delete
// site, and something to lose: two call sites making the same decision drift
// apart the moment only one of them is updated.
func (sm *SyncManager) pipelineBlockDelete(hash chainhash.Hash, converted bool) error {
	var firstErr error

	if converted {
		record, err := sm.blockPark.ReadConverted(sm.ctx, hash)

		switch {
		case err == nil && record != nil:
			structureType := fileformat.FileTypeSubtreeToCheck
			if record.Height != 0 && sm.quickValidationAllowed(record.Height) {
				structureType = fileformat.FileTypeSubtree
			}

			for _, root := range record.Subtrees {
				for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta, structureType} {
					if delErr := sm.subtreeStore.Del(sm.ctx, root[:], ft); delErr != nil && firstErr == nil {
						firstErr = errors.NewStorageError("[pipelineBlockDelete][%s] failed deleting %s for subtree %s", hash, ft, root, delErr)
					}
				}
			}

		case err != nil && !errors.Is(err, errors.ErrNotFound):
			// converted being true means THIS call's own sink wrote a record,
			// so a not-found error here is not the ordinary case it would be
			// otherwise — it means the write this call just made cannot be
			// read back. Either way, this is a store timeout, a permit-pool
			// wait that ran out, or ReadConverted's own hash-mismatch refusal
			// — every one of which means the subtree files this block's sink
			// actually wrote are NOT being deleted here, exactly the failure
			// mode deleteWrittenOnFailure's own comment warns about for the
			// same reason: an unlogged cleanup failure is indistinguishable
			// from a cleanup that never needed to run.
			sm.logger.Warnf("[pipelineBlockDelete][%s] could not read back the record this delivery just converted, so its subtree files were not deleted: %v", hash, err)
		}
	}

	// Unconditional regardless of converted: on the admission-budget fallback
	// (admitPipelineSink's duplicate-in-flight and acquire-timeout cases) this
	// call's own sink wrote a raw body here (streamingBlockSink, converted
	// false), and on every path this also retires whatever the park holds for
	// hash — see blockPark.Delete's own comment on why attempting both file
	// types is safe even though only one of them is ever this call's own.
	if err := sm.streamingBlockDelete(hash, converted); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// pipelineParentHeight resolves a block's height from its parent's hash,
// without requiring the parent to be a COMMITTED block.
//
// The ordinary out-of-order case is a parent that is still only in the
// header cache (sm.headerCache) — measured at roughly 91% of blocks on this
// node — because the header round can outrun body delivery. Asking only
// sm.blockchainClient.GetBlockHeader, which answers only for a committed
// block (parentIsInChain in streaming_install.go makes the identical call for
// exactly that meaning), treated that ordinary case as a fault.
//
// The cache is checked first because it is the common case and never blocks:
// it holds its own lock, released before the fallback runs, because a
// blockchain client call can take an unbounded time.
func (sm *SyncManager) pipelineParentHeight(parent chainhash.Hash) (uint32, bool) {
	if height, ok := sm.headerCache.HeightOf(parent); ok && height >= 0 {
		return uint32(height) + 1, true
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

// fallbackSubtreeDAH returns the delete-at-height newSubtreeWriterUnresolvedHeight
// stamps every artefact with, for a block whose parent height
// pipelineParentHeight could not resolve.
//
// committedHeight() is the last block this node has actually put into the
// chain — never negative in practice, floored at 0 defensively. Added to it is
// the widest the download walk can read ahead of that tip: legacy_
// blockDownloadWindow, the node-wide ceiling on outstanding block requests
// (lookaheadCeilingLocked, manager.go, scales a narrower bound DOWN from this
// one by block size, never wider), floored at 1 so a misconfigured 0 cannot
// zero the whole sum. No block this node holds — parked, mid-conversion, or
// committed — can have a height above committedHeight()+BlockDownloadWindow,
// so adding the configured retention on top gives a delete-at-height strictly
// above any height this specific block can actually turn out to have, exactly
// the guarantee height + retention gives when the height is known.
func (sm *SyncManager) fallbackSubtreeDAH() uint32 {
	tip := sm.committedHeight()
	if tip < 0 {
		tip = 0
	}

	depth := 1
	if sm.settings != nil && sm.settings.Legacy.BlockDownloadWindow > depth {
		depth = sm.settings.Legacy.BlockDownloadWindow
	}

	return uint32(tip) + uint32(depth) + sm.settings.GetSubtreeValidationBlockHeightRetention()
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
