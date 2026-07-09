package blockvalidation

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// windowSpend captures the outpoint and spending side of one input, snapshotted from
// a batch before batch.Close() frees the bt.Tx objects. The ~72 bytes per input are
// the only data retained across batches in the de-interleaved window pipeline.
//
// Fields mirror the parameters of GetSpendsOutpointOnly in stores/utxo/utils.go:
//
//	Spend{TxID: parentTxHash, Vout: vout, UTXOHash: &zero, SpendingData: spend.NewSpendingData(spendingTxHash, vin)}
type windowSpend struct {
	parentTxHash   chainhash.Hash // creating tx (= input.PreviousTxIDChainHash())
	vout           uint32         // output index within the parent tx
	spendingTxHash chainhash.Hash // tx that spends the output
	vin            uint32         // index of this input in the spending tx
}

// createBlockUTXOs is the CREATE-only pass for the de-interleaved window pipeline.
// It runs the full 3-stage prefetch→extend→process pipeline for a below-checkpoint
// block, creating UTXOs for every non-coinbase tx, writing subtree files, and
// returning a snapshot of every input outpoint so the caller can drive the SPEND
// pass independently once the window is ready.
//
// Constraints:
//   - Only valid for the outpoint-only fast path; returns an error if outpointOnly is false.
//   - Does NOT call Spend — that is the responsibility of spendBlockUTXOs (Task 2).
//   - batch.Close() is called after snapshotting, so bt.Tx objects are not retained.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - block: Block to process (must have Subtrees populated and a valid CoinbaseTx)
//   - outpointOnly: Must be true; guards the fast path invariant
//
// Returns:
//   - []windowSpend: one entry per non-coinbase input across all batches
//   - error: on any failure
func (u *BlockValidation) createBlockUTXOs(ctx context.Context, block *model.Block, outpointOnly bool) ([]windowSpend, error) {
	if !outpointOnly {
		return nil, errors.NewProcessingError("[createBlockUTXOs][%s] createBlockUTXOs called on non-outpoint-only block", block.Hash().String())
	}

	// Invariant I4 (fail-closed): mirror the guard in createAndSpendUTXOsForBatch.
	if block.Height > blockchain.HighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints) {
		return nil, errors.NewProcessingError("[createBlockUTXOs] invariant I4 violated: outpoint-only mode active above checkpoint at height %d", block.Height)
	}

	numSubtrees := len(block.Subtrees)
	if numSubtrees == 0 {
		return nil, errors.NewProcessingError("[createBlockUTXOs][%s] block has no subtrees", block.Hash().String())
	}

	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64
	blockIDSet := false

	prefetchDepth := u.settings.BlockValidation.SubtreeBatchPrefetchDepth
	if prefetchDepth <= 0 {
		prefetchDepth = 2
	}

	// Channel for prefetched batches (subtrees read from disk).
	prefetchChan := make(chan *SubtreeProcessingBatch, prefetchDepth)
	// Channel for extended batches (txs extended, ready for UTXO ops).
	extendedChan := make(chan *SubtreeProcessingBatch, prefetchDepth)

	// allSpends accumulates the outpoint snapshots from every batch.
	// Written only by stage 3 (a single goroutine), read by the caller after g.Wait().
	// No mutex needed: g.Wait() provides the happens-before edge.
	var allSpends []windowSpend

	g, gCtx := errgroup.WithContext(ctx)

	// Drain channels on error to prevent goroutine leaks from large batches stuck in buffers.
	defer func() {
		for range prefetchChan {
		}
		for range extendedChan {
		}
	}()

	// Stage 1: Reader — prefetch batches from disk.
	g.Go(func() error {
		defer close(prefetchChan)
		subtreeBatchSize := u.settings.BlockValidation.SubtreeBatchSize
		for batchStart := 0; batchStart < numSubtrees; batchStart += subtreeBatchSize {
			batchEnd := batchStart + subtreeBatchSize
			if batchEnd > numSubtrees {
				batchEnd = numSubtrees
			}

			start := time.Now()
			batch, err := u.prefetchSubtreeBatch(gCtx, block, batchStart, batchEnd, outpointOnly)
			if err != nil {
				return err
			}
			u.logger.Debugf("[createBlockUTXOs:prefetch][%s] batch %d-%d prefetched in %v", block.Hash().String(), batchStart, batchEnd, time.Since(start))

			select {
			case prefetchChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 2: Extender — extend transactions (sequential to maintain extendedTxs map).
	g.Go(func() error {
		defer close(extendedChan)
		extendedTxs := make(map[chainhash.Hash]*bt.Tx)
		for batch := range prefetchChan {
			start := time.Now()
			if err := u.extendBatch(gCtx, block, batch, extendedTxs); err != nil {
				return err
			}
			u.logger.Debugf("[createBlockUTXOs:extend][%s] batch %d-%d extended (%d txs) in %v", block.Hash().String(), batch.batchStart, batch.batchEnd, len(batch.batchTxs), time.Since(start))

			select {
			case extendedChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 3: Processor — CREATE UTXOs, write subtree files, snapshot outpoints, free batch.
	g.Go(func() error {
		for batch := range extendedChan {
			// (a) Block ID assignment on first batch (idempotent retry-safe).
			if !blockIDSet && len(batch.batchTxs) > 0 {
				existingMeta, err := u.utxoStore.Get(gCtx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
				if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
					existingBlockID = uint64(existingMeta.BlockIDs[0])
					block.ID = existingMeta.BlockIDs[0]
					u.logger.Debugf("[createBlockUTXOs][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
				} else if block.ID == 0 {
					id, err := u.blockchainClient.AssignBlockID(gCtx, block.Hash())
					if err != nil {
						return errors.NewProcessingError("[createBlockUTXOs][%s] failed to assign block ID", block.Hash().String(), err)
					}
					block.ID, err = blockIDToUint32(id, block.Hash().String())
					if err != nil {
						return err
					}
				}
				blockIDSet = true
			}

			// (b) CREATE UTXOs for all txs in the batch (goroutine fan-out, same as createAndSpendUTXOsForBatch Phase 1).
			createG, createCtx := errgroup.WithContext(gCtx)
			util.SafeSetLimit(u.logger, createG, u.settings.UtxoStore.StoreBatcherSize*8)

			var existingTxsMu sync.Mutex
			var existingTxHashes []*chainhash.Hash

			minedBlockInfo := utxo.MinedBlockInfo{
				BlockID:     block.ID,
				BlockHeight: block.Height,
			}

			lockUTXOs := !u.quickValidateSkipsUtxoLock(block)

			batchSize := batch.batchEnd - batch.batchStart
			for i := 0; i < batchSize; i++ {
				globalSubtreeIdx := batch.batchStart + i
				txRange := batch.txRanges[i]
				for txIdx := txRange[0]; txIdx < txRange[1]; txIdx++ {
					tx := batch.batchTxs[txIdx]
					sIdx := globalSubtreeIdx
					createG.Go(func() error {
						_, err := u.utxoStore.Create(createCtx, tx, block.Height, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
							BlockID:     block.ID,
							BlockHeight: block.Height,
							SubtreeIdx:  sIdx,
						}), utxo.WithLocked(lockUTXOs), utxo.WithSkipExtendedInputs(outpointOnly))
						if err != nil {
							if errors.Is(err, errors.ErrTxExists) {
								txHash := tx.TxIDChainHash()
								existingTxsMu.Lock()
								existingTxHashes = append(existingTxHashes, txHash)
								existingTxsMu.Unlock()
								return nil
							}
							return errors.NewProcessingError("[createBlockUTXOs][%s] failed to create UTXO for tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
						}
						return nil
					})
				}
			}

			if err := createG.Wait(); err != nil {
				batch.Close()
				return err
			}

			// (c) Phase 1.5: SetMinedMulti for any ErrTxExists (per batch, same as createAndSpendUTXOsForBatch).
			if len(existingTxHashes) > 0 {
				if err := utxo.SetMinedMultiChunked(gCtx, u.logger, u.utxoStore, existingTxHashes, minedBlockInfo,
					u.settings.UtxoStore.MaxMinedBatchSize, u.settings.UtxoStore.MaxMinedRoutines); err != nil {
					batch.Close()
					return errors.NewProcessingError("[createBlockUTXOs][%s] failed to update mined info for %d existing txs", block.Hash().String(), len(existingTxHashes), err)
				}
			}

			// (d) Write subtree files (sync variant).
			if err := u.writeSubtreeFilesForBatch(gCtx, block, batch); err != nil {
				batch.Close()
				return err
			}

			// (e) Snapshot outpoints: iterate every tx's inputs before freeing the batch.
			var batchSpends []windowSpend
			for _, tx := range batch.batchTxs {
				txHash := *tx.TxIDChainHash()
				for j, input := range tx.Inputs {
					batchSpends = append(batchSpends, windowSpend{
						parentTxHash:   *input.PreviousTxIDChainHash(),
						vout:           input.PreviousTxOutIndex,
						spendingTxHash: txHash,
						vin:            uint32(j), //nolint:gosec
					})
				}
			}

			allSpends = append(allSpends, batchSpends...)

			// (f) Free mmap resources — MUST happen after snapshotting.
			batch.Close()
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// validateSubtrees checks merkle root and subtree sizes; mirrors processBlockSubtreesPipeline.
	if _, err := u.validateSubtrees(ctx, block, existingBlockID); err != nil {
		return nil, err
	}

	return allSpends, nil
}

// spendBlockUTXOs is the SPEND-only pass for the de-interleaved window pipeline.
// It replays the windowSpend snapshots returned by createBlockUTXOs through the same
// hardened spend semantics as spendBatchWithRetry: transient-retry with progress-check,
// fail-closed on ErrTxConflicting / ErrSpent / any non-retryable error.
//
// Implementation: the snapshots are grouped by spendingTxHash and each group is
// assembled into a minimal *bt.Tx (hash pre-set; inputs ordered by vin) so that
// the existing spendBatchWithRetry loop can be reused without duplication.
// The store's outpoint-only spend path (utxo.IgnoreFlags{IgnoreLocked, SkipUTXOHashCheck})
// is engaged via outpointOnly — identical to createAndSpendUTXOsForBatch phase 2.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - block: Block whose UTXOs are being spent (provides Height for the spend call)
//   - spends: Snapshots from createBlockUTXOs; empty slice is a no-op
//   - outpointOnly: Must be true (guards the fast-path invariant; mirrors createBlockUTXOs)
func (u *BlockValidation) spendBlockUTXOs(ctx context.Context, block *model.Block, spends []windowSpend, outpointOnly bool) error {
	// spendBlockUTXOs is exclusively a below-checkpoint outpoint-only operation.
	// The UTXO-hash path requires parent satoshi values read inline during extend,
	// which the de-interleaved pipeline does not retain across the barrier.
	// Fail-closed rather than silently producing wrong spend records.
	if !outpointOnly {
		return errors.NewProcessingError("[spendBlockUTXOs][%s] spendBlockUTXOs called on non-outpoint-only block", block.Hash().String())
	}

	if len(spends) == 0 {
		return nil
	}

	txs := windowSpendsToTxs(spends)
	return u.spendBatchWithRetry(ctx, block, txs, outpointOnly)
}

// windowSpendsToTxs converts a flat []windowSpend into a []*bt.Tx suitable for
// spendBatchWithRetry. Each unique spendingTxHash becomes one minimal *bt.Tx:
//   - TxIDChainHash() returns spendingTxHash (set via SetTxHash — no serialisation).
//   - Inputs are ordered by ascending vin so that GetSpendsOutpointOnly builds
//     SpendingData.Vin values that match the original transaction's input indices.
//
// The resulting txs carry NO outputs and NO scripts — only the fields the
// outpoint-only spend path reads (PreviousTxIDChainHash, PreviousTxOutIndex, TxIDChainHash).
func windowSpendsToTxs(spends []windowSpend) []*bt.Tx {
	// Group by spendingTxHash preserving vin order within each group.
	type group struct {
		hash   chainhash.Hash
		inputs []windowSpend
	}

	seen := make(map[chainhash.Hash]int) // hash → index in groups
	var groups []group

	for _, ws := range spends {
		idx, ok := seen[ws.spendingTxHash]
		if !ok {
			idx = len(groups)
			seen[ws.spendingTxHash] = idx
			groups = append(groups, group{hash: ws.spendingTxHash})
		}
		groups[idx].inputs = append(groups[idx].inputs, ws)
	}

	txs := make([]*bt.Tx, len(groups))
	for i, g := range groups {
		txs[i] = buildMinimalSpendTx(g.hash, g.inputs)
	}
	return txs
}

// buildMinimalSpendTx assembles a minimal *bt.Tx for a single spendingTxHash from its
// windowSpend entries. The inputs are sorted by vin so GetSpendsOutpointOnly produces
// SpendingData.Vin values that match the original indices.
func buildMinimalSpendTx(spendingTxHash chainhash.Hash, inputs []windowSpend) *bt.Tx {
	// Sort by vin to preserve original input ordering.
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].vin < inputs[j].vin })

	tx := bt.NewTx()
	h := spendingTxHash
	tx.SetTxHash(&h)

	for _, ws := range inputs {
		parentH := ws.parentTxHash
		in := &bt.Input{PreviousTxOutIndex: ws.vout}
		_ = in.PreviousTxIDAdd(&parentH) // only fails on zero-hash; outpoints in real txs are non-zero
		tx.Inputs = append(tx.Inputs, in)
	}

	return tx
}

// ProcessBlockWindow processes a batch of K below-checkpoint blocks concurrently
// using the three-fence phased pipeline:
//
//	C1 (parallel): createBlockUTXOs for all K blocks → barrier (g.Wait)
//	C2 (parallel): spendBlockUTXOs for all K blocks → barrier (g.Wait)
//	C3 (serial):   commitBlock for each block in ascending height order
//
// Correctness invariants:
//   - Every block must be below the hardcoded checkpoint AND outpoint-only eligible;
//     if any block fails this guard the window is rejected fail-closed before C1.
//   - C1→C2 barrier: no spend may run until ALL K creates are committed (the
//     g.Wait() call is the only synchronisation point; there is no other path).
//   - Spend order is unordered (safe below checkpoint: disjoint outpoints, no
//     double-spends, CVE-2012-2459 dedup enforced in PREPARE before hand-off).
//   - C3 commits in strict ascending height order via commitBlock; moveForwardBlock
//     inside AddBlock rejects out-of-order commits at the FSM level.
//
// Failure handling:
//   - C1 or C2 error: return immediately; no C3 commits run; creates are
//     idempotent (ErrTxExists→SetMinedMultiChunked) so the caller may retry.
//   - C3 error at height h: return an error that names the last successfully
//     committed height so the caller can resume after that block.
//
// blocks must be sorted ascending by Height and all below the highest hardcoded
// checkpoint with outpointOnly eligibility. An empty slice is a no-op.
func (u *BlockValidation) ProcessBlockWindow(ctx context.Context, blocks []*model.Block, peerID string) error {
	if len(blocks) == 0 {
		return nil
	}

	// --- Gate: every block must be below the hardcoded checkpoint + outpoint-only. ---
	// Fail-closed: a single above-checkpoint block must never enter the concurrent path.
	highest := model.HighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints)
	for _, blk := range blocks {
		if !model.BelowCheckpoint(u.settings.ChainCfgParams.Checkpoints, blk.Height) {
			return errors.NewProcessingError(
				"[ProcessBlockWindow] block at height %d is not below the hardcoded checkpoint (highest=%d); window rejected fail-closed",
				blk.Height, highest,
			)
		}
		if !u.quickValidateOutpointOnly(blk) {
			return errors.NewProcessingError(
				"[ProcessBlockWindow] block at height %d is not outpoint-only eligible; window rejected fail-closed",
				blk.Height,
			)
		}
	}

	k := len(blocks)

	// --- C1: parallel creates. ---
	// spends[i] holds the windowSpend snapshots from createBlockUTXOs for blocks[i].
	spends := make([][]windowSpend, k)

	c1g, c1ctx := errgroup.WithContext(ctx)
	// Concurrency cap: each block's create already fans out internally using
	// StoreBatcherSize*8 goroutines; the batcher — not this errgroup — is the real
	// DB-pool gate. The window-level limit is StoreBatcherSize so that a
	// misconfigured k does not spawn an unbounded number of concurrent intra-block
	// fan-outs. SafeSetLimit clamps to runtime.NumCPU() when StoreBatcherSize < 1.
	util.SafeSetLimit(u.logger, c1g, u.settings.UtxoStore.StoreBatcherSize)
	var mu sync.Mutex
	for i := range k {
		blockIdx := i
		blk := blocks[i]
		c1g.Go(func() error {
			ws, err := u.createBlockUTXOs(c1ctx, blk, true /* outpointOnly */)
			if err != nil {
				return errors.NewProcessingError("[ProcessBlockWindow][C1][%s] createBlockUTXOs failed", blk.Hash().String(), err)
			}
			mu.Lock()
			spends[blockIdx] = ws
			mu.Unlock()
			return nil
		})
	}

	// BARRIER C1→C2: all creates committed before any spend begins.
	if err := c1g.Wait(); err != nil {
		return err
	}

	// --- C2: parallel spends. ---
	c2g, c2ctx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, c2g, u.settings.UtxoStore.SpendBatcherSize)
	for i := range k {
		blk := blocks[i]
		ws := spends[i]
		c2g.Go(func() error {
			if err := u.spendBlockUTXOs(c2ctx, blk, ws, true /* outpointOnly */); err != nil {
				return errors.NewProcessingError("[ProcessBlockWindow][C2][%s] spendBlockUTXOs failed", blk.Hash().String(), err)
			}
			return nil
		})
	}

	// BARRIER C2→C3: all spends done before any commit runs.
	if err := c2g.Wait(); err != nil {
		return err
	}

	// --- C3: serial commits in ascending height order. ---
	// blocks is already sorted ascending by Height (caller contract).
	// commitBlock calls AddBlock+moveForwardBlock which enforces HashPrevBlock==tip,
	// so out-of-order delivery would be rejected by the FSM — C3 serialises this.
	var lastCommittedHeight uint32
	for _, blk := range blocks {
		if err := u.commitBlock(ctx, blk, peerID, "ProcessBlockWindow"); err != nil {
			if lastCommittedHeight > 0 {
				return errors.NewProcessingError(
					"[ProcessBlockWindow][C3][%s] commitBlock failed; last committed height=%d",
					blk.Hash().String(), lastCommittedHeight, err,
				)
			}
			return errors.NewProcessingError("[ProcessBlockWindow][C3][%s] commitBlock failed (no blocks committed yet)", blk.Hash().String(), err)
		}
		lastCommittedHeight = blk.Height
	}

	u.logger.Debugf("[ProcessBlockWindow] completed window of %d blocks (heights %d–%d)", k, blocks[0].Height, blocks[k-1].Height)
	return nil
}
