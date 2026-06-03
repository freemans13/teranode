package netsync

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

func (sm *SyncManager) HandleBlockDirect(ctx context.Context, peer *peer.Peer, blockHash chainhash.Hash, msgBlock *wire.MsgBlock) (err error) {
	sm.logger.Debugf("[HandleBlockDirect][%s] starting handling block", blockHash.String())

	// check whether this block already exists
	blockExists, err := sm.blockchainClient.GetBlockExists(ctx, &blockHash)
	if err != nil {
		sm.logger.Errorf("[HandleBlockDirect][%s] failed to check if block exists: %s", blockHash.String(), err)
		return errors.NewProcessingError("failed to check if block exists", err)
	}

	if blockExists {
		sm.logger.Warnf("[HandleBlockDirect][%s] block already exists", blockHash.String())
		return nil
	}

	block := bsvutil.NewBlock(msgBlock)

	// Determine this block's height. On the quick-validation pipeline path this
	// reads the checkpoint-anchored in-memory header list (no GetBlockHeader read,
	// so it does not depend on the parent block being finalized first); otherwise
	// it falls back to the original blockchain lookup. Side effect: sets block
	// height via SetHeight.
	blockHeight, err := sm.deriveBlockHeight(ctx, block, blockHash)
	if err != nil {
		return err
	}

	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "HandleBlockDirect",
		tracing.WithLogMessage(
			sm.logger,
			"[HandleBlockDirect][%s %d] %d txs, peer %s",
			block.Hash().String(),
			blockHeight,
			len(block.Transactions()),
			peer.String(),
		),
		tracing.WithTag("blockHash", block.Hash().String()),
		tracing.WithTag("peer", peer.String()),
		tracing.WithHistogram(prometheusLegacyNetsyncHandleBlockDirect),
	)
	defer func() {
		// set the block height gauge in the prometheus metrics
		prometheusLegacyNetsyncBlockHeight.Set(float64(blockHeight))

		deferFn(err)
	}()

	// Wait for block assembly to be ready
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, sm.logger, sm.blockAssembly, blockHeight, sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
		// block-assembly is still behind, so we cannot process this block
		return err
	}

	// Block-finalization pipeline (quick-validation catch-up only): when enabled and
	// this block is at/below the highest checkpoint, finalization of the previous
	// block — including its mined_set — runs concurrently on the finalizer goroutine,
	// so tx-work here must NOT block waiting for it. Safe in quick mode: the trusted
	// by-outpoint spend never consults parent block heights, and the parent-BlockID
	// linkage checked at finalization is set at output-creation time, not at mined_set.
	pipelined := sm.settings.Legacy.BlockFinalizationPipeline && sm.quickValidationAllowed(blockHeight)

	// Surface a prior block's finalization failure before ingesting further blocks.
	if pipelined {
		if ferr := sm.finalizeError(); ferr != nil {
			return ferr
		}
	}

	// Wait for the previous block's setTxMined to complete before validating
	// this block's transactions. Ensures BIP68 sequence lock validation can
	// correctly look up parent transaction BlockHeights in the UTXO store. Skipped
	// on the pipelined quick path (see above), where the wait would serialise the
	// pipeline and is not needed.
	if blockHeight > 1 && !pipelined {
		if err = sm.waitForPreviousBlockMined(ctx, &block.MsgBlock().Header.PrevBlock, blockHeight); err != nil {
			return err
		}
	}

	// Mark this height in-flight before its PhaseA begins (inline or on a worker),
	// so the finalizer's gap watchdog can tell it apart from a genuinely missing
	// block while it waits to finalize it.
	if pipelined {
		sm.markDispatched(blockHeight)
	}

	// Concurrent ahead-creation (the throughput lever, legacy_blockPipelineConcurrency
	// > 1): run this block's PhaseA tx-work on a bounded worker pool and finalize via
	// the height-ordered reorder buffer. The consumer establishes the authoritative
	// finalize start height here (it dispatches in arrival order, contiguous with the
	// committed tip), and — critically for the create→fee dependency — caches this
	// block's output satoshis at parse time BEFORE dispatch, so a later block's
	// concurrent fee resolution finds them regardless of createUtxos order. Then it
	// returns so the next block's PhaseA can begin immediately.
	if pipelined && sm.effectivePipelineConcurrency() > 1 {
		sm.ensureFinalizer(blockHeight)
		sm.cacheBlockOutputSatoshis(block)

		if err = sm.acquirePipelineSlot(); err != nil {
			return err
		}

		go sm.runPhaseAWorker(block, blockHash, blockHeight)

		return nil
	}

	// Inline PhaseA: serial path (default / above the checkpoint), or the pipelined
	// path at concurrency 1 (finalization still overlaps the next block's tx-work).
	job, err := sm.runPhaseA(ctx, block, blockHash, blockHeight)
	if err != nil {
		return err
	}

	// Pipelined quick path: hand finalization (ProcessBlock + mined_set + orphan
	// processing) to the in-order finalizer goroutine so it overlaps the next
	// block's tx-work. The bounded handoff channel back-pressures tx-work once the
	// look-ahead horizon is full. Use the manager context (not the per-call tracing
	// context, whose span ends when this function returns).
	if pipelined {
		job.ctx = sm.ctx
		sm.enqueueFinalize(job)

		return nil
	}

	// Serial path (default / above the checkpoint): finalize inline, unchanged.
	return sm.finalizeBlock(job)
}

// runPhaseA performs a block's order-independent tx-work below the checkpoint:
// build the header/coinbase, prepareSubtrees (create all outputs + trusted spend
// + write subtree data), assemble the model block and pre-check proof of work.
// It returns a finalizeJob carrying everything the order-sensitive PhaseB
// (finalizeBlock) needs, decoupled from the wire block so the decode arena can be
// released. Safe to run concurrently across blocks once the satoshi cache is
// pre-built and the cross-block subtree batcher is bypassed (see ensureFinalizer
// and prepareSubtrees).
func (sm *SyncManager) runPhaseA(ctx context.Context, block *bsvutil.Block, blockHash chainhash.Hash, blockHeight uint32) (*finalizeJob, error) {
	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return nil, errors.NewProcessingError("failed to serialize header", err)
	}

	// create the Teranode compatible block header
	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return nil, errors.NewProcessingError("failed to create block header from bytes", err)
	}

	var coinbase bytes.Buffer
	if err = block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return nil, errors.NewProcessingError("failed to serialize coinbase", err)
	}

	// Single coinbase decode per block, retained into the teranodeBlock model
	// for downstream use; stays on the standard heap path. The arena variant
	// would require Put before return, at which point coinbaseTx's scripts
	// would alias soon-to-be-reused memory. Per-block tx loops in legacy
	// ingestion live in bsvutil/subtree-assembly, which works with
	// bsvutil.Tx (not bt.Tx) and never round-trips through go-bt decode.
	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return nil, errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	// validate all subtrees and store all subtree data
	// this also should spend and create all utxos
	subtrees, blockID, err := sm.prepareSubtrees(ctx, block)
	if err != nil {
		return nil, err
	}

	// create valid teranode block, with the subtree hash
	blockSize := block.MsgBlock().SerializeSize()

	blockSizeUint64, err := safeconversion.IntToUint64(blockSize)
	if err != nil {
		return nil, err
	}

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), blockSizeUint64, blockHeight, blockID)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// pre-check that there is enough proof of work on the block, before we do any other processing
	headerValid, _, err := teranodeBlock.Header.HasMetTargetDifficulty()
	if !headerValid {
		return nil, errors.NewBlockInvalidError("invalid block header: %s", teranodeBlock.Header.Hash().String(), err)
	}

	// Pre-extract the tx hashes here, before any (possibly deferred) finalization,
	// so the orphan-processing work does not keep `block` (and therefore the
	// wire.MsgBlock + its decode arena) reachable. Copy the hash *values* (not the
	// *chainhash.Hash pointers returned by tx.Hash(), which alias into the
	// bsvutil.Tx wrapper and would pin it).
	wireTxs := block.Transactions()
	txHashes := make([]chainhash.Hash, len(wireTxs))
	for i, tx := range wireTxs {
		txHashes[i] = *tx.Hash()
	}

	return &finalizeJob{
		ctx:           ctx,
		teranodeBlock: teranodeBlock,
		txHashes:      txHashes,
		blockHashStr:  block.Hash().String(),
		blockHeight:   blockHeight,
	}, nil
}

// effectivePipelineConcurrency returns the PhaseA window size actually used.
// Concurrency > 1 requires the parse-time satoshi cache (ParentSatoshiCacheMB >
// 0): without it every cross-block parent fee would hit the store, which under
// concurrency may not yet hold the parent's outputs (its createUtxos may not have
// run), breaking the create→fee dependency. With the cache disabled we clamp to
// 1 (serial tx-work, still-pipelined finalization), which is always correct.
func (sm *SyncManager) effectivePipelineConcurrency() int {
	c := sm.settings.Legacy.BlockPipelineConcurrency
	if c <= 1 {
		return 1
	}

	if sm.settings.Legacy.ParentSatoshiCacheMB <= 0 {
		return 1
	}

	return c
}

// cacheBlockOutputSatoshis records every output's satoshis from a block into the
// satoshi cache, read straight from wire data (cached tx hashes + TxOut.Value) so
// it is cheap and needs no bt.Tx conversion. This is the design's step (a):
// populating at PARSE time on the in-order blockQueue consumer — before a block's
// PhaseA is dispatched — guarantees that when a later block's concurrent PhaseA
// resolves a fee against this block's outputs, the satoshis are already cached,
// independent of whether this block's createUtxos has run. No-op when the cache
// is disabled.
func (sm *SyncManager) cacheBlockOutputSatoshis(block *bsvutil.Block) {
	if sm.satoshiCache == nil {
		return
	}

	for _, tx := range block.Transactions() {
		txHash := tx.Hash()

		for i, out := range tx.MsgTx().TxOut {
			if out == nil {
				continue
			}

			idx, err := safeconversion.IntToUint32(i)
			if err != nil {
				continue
			}

			sm.satoshiCache.put(txHash, idx, uint64(out.Value))
		}
	}
}

// runPhaseAWorker runs runPhaseA off the blockQueue consumer (concurrent window)
// and hands the resulting job to the in-order finalizer. PhaseA failures halt the
// pipeline via setFinalizeError (surfaced to the consumer before the next block).
// It always releases its window slot. Uses the manager context, since the
// per-call tracing span has already ended by the time this runs.
func (sm *SyncManager) runPhaseAWorker(block *bsvutil.Block, blockHash chainhash.Hash, blockHeight uint32) {
	defer sm.releasePipelineSlot()

	job, err := sm.runPhaseA(sm.ctx, block, blockHash, blockHeight)
	if err != nil {
		sm.setFinalizeError(err)
		return
	}

	sm.enqueueFinalize(job)
}

// finalizeJob carries everything block finalization needs, decoupled from the
// wire block so it can be handed to the finalizer goroutine without pinning the
// decode arena.
type finalizeJob struct {
	ctx           context.Context
	teranodeBlock *model.Block
	txHashes      []chainhash.Hash
	blockHashStr  string
	blockHeight   uint32
}

// finalizeBlock runs the order-sensitive tail of block processing: the
// consensus-checking ProcessBlock (merkle/coinbase/reward, AddBlock, mined_set)
// and background orphan processing. Called inline on the serial path, or in
// height order on the finalizer goroutine when the pipeline is enabled.
func (sm *SyncManager) finalizeBlock(job *finalizeJob) error {
	if err := sm.ProcessBlock(job.ctx, job.teranodeBlock); err != nil {
		return err
	}

	// process any orphan transactions that are now valid in background
	// this will also remove the transactions from the orphan pool.
	go func() {
		acceptedTxs := make([]*TxHashAndFee, 0)
		for i := range job.txHashes {
			sm.processOrphanTransactions(job.ctx, &job.txHashes[i], &acceptedTxs)
		}

		if len(acceptedTxs) > 0 {
			sm.logger.Infof("[finalizeBlock][%s %d] accepted %d orphan transactions", job.blockHashStr, job.blockHeight, len(acceptedTxs))
			sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
		}
	}()

	return nil
}

// finalizeChDepth bounds the block-finalization pipeline's look-ahead: at most
// this many blocks of completed tx-work may sit ahead of finalization before the
// blockQueue consumer back-pressures. Kept small (per the design's 2–4 horizon)
// so the in-flight blocks' state stays bounded.
const finalizeChDepth = 4

// ensureFinalizer lazily starts the finalizer goroutine, its handoff channel and
// the PhaseA window semaphore on the first pipelined block, and records the
// authoritative finalize start height. Called only from the single blockQueue
// consumer goroutine, so sync.Once fully serialises construction. startHeight is
// the first pipelined block's height (contiguous with the committed tip, since
// the consumer dispatches in arrival order during in-order headers-first sync).
func (sm *SyncManager) ensureFinalizer(startHeight uint32) {
	sm.finalizeOnce.Do(func() {
		sm.finalizeStartHeight = startHeight

		if sm.settings.Legacy.BlockPipelineConcurrency > 1 && sm.settings.Legacy.ParentSatoshiCacheMB <= 0 {
			sm.logger.Warnf("[finalizeLoop] legacy_blockPipelineConcurrency>1 ignored: it requires legacy_parentSatoshiCacheMB>0 for fee correctness; running tx-work serially")
		}

		concurrency := sm.effectivePipelineConcurrency()

		sm.pipelineSem = make(chan struct{}, concurrency)

		// Pre-build the off-heap satoshi fee-cache before any concurrent PhaseA
		// worker runs prepareSubtrees, so its lazy init there cannot race. No-op
		// when the cache is disabled; prepareSubtrees still builds it lazily on the
		// serial/single-worker path.
		if concurrency > 1 && sm.satoshiCache == nil && sm.settings.Legacy.ParentSatoshiCacheMB > 0 {
			poc, cacheErr := newSatoshiCache(sm.settings.Legacy.ParentSatoshiCacheMB * 1024 * 1024)
			if cacheErr != nil {
				sm.logger.Errorf("[finalizeLoop] failed to pre-create satoshi cache: %v", cacheErr)
			} else {
				sm.satoshiCache = poc
				sm.logger.Infof("[finalizeLoop] pre-created %d MB off-heap satoshi cache for concurrent pipeline", sm.settings.Legacy.ParentSatoshiCacheMB)
			}
		}

		if concurrency > 1 && sm.settings.Legacy.SubtreeWriteBatchBlocks > 1 {
			sm.logger.Warnf("[finalizeLoop] legacy_subtreeWriteBatchBlocks>1 is ignored under legacy_blockPipelineConcurrency>1 (the cross-block subtree-write batcher is single-goroutine); using per-block direct writes")
		}

		depth := finalizeChDepth
		if concurrency > depth {
			depth = concurrency
		}

		sm.finalizeCh = make(chan *finalizeJob, depth)
		go sm.finalizeLoop()

		sm.logger.Infof("[finalizeLoop] block-finalization pipeline started (concurrency %d, start height %d, look-ahead horizon %d blocks)", concurrency, startHeight, depth)
	})
}

// enqueueFinalize hands a job to the finalizer, back-pressuring the caller when
// the handoff channel is full, and aborting cleanly on shutdown.
func (sm *SyncManager) enqueueFinalize(job *finalizeJob) {
	sm.ensureFinalizer(job.blockHeight)

	select {
	case sm.finalizeCh <- job:
	case <-sm.quit:
	}
}

// acquirePipelineSlot takes a PhaseA window token, blocking the consumer when the
// window is full so it cannot run unboundedly far ahead of finalization. Aborts
// on shutdown.
func (sm *SyncManager) acquirePipelineSlot() error {
	select {
	case sm.pipelineSem <- struct{}{}:
		return nil
	case <-sm.quit:
		return errors.NewProcessingError("[HandleBlockDirect] shutting down")
	}
}

// releasePipelineSlot returns a PhaseA window token once a worker's job has been
// handed to the finalizer (or its PhaseA failed).
func (sm *SyncManager) releasePipelineSlot() {
	<-sm.pipelineSem
}

// finalizeLoop finalizes blocks in strict ascending height order via the reorder
// buffer, regardless of the order their (concurrent) PhaseA tx-work completes.
// Finalizing in order guarantees a block's parent is already on chain when it is
// added. On the first error it records it (halting further ingest via
// finalizeError) and keeps draining so senders never block. On shutdown it exits;
// any not-yet-finalized blocks are simply re-processed on restart
// (createUtxos/spend are idempotent).
func (sm *SyncManager) finalizeLoop() {
	buf := newFinalizeReorderBuffer()
	buf.setStart(sm.finalizeStartHeight)

	// Watchdog: if a block body for the awaited height is genuinely lost, higher
	// blocks complete PhaseA and pile up in the buffer while finalization halts.
	// The pipeline path never trips the stage-1 missing-parent self-heal, so detect
	// the wedge here and re-request the gap from the tip.
	detector := &gapStallDetector{timeout: finalizeStallTimeout}
	ticker := time.NewTicker(finalizeStallCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.quit:
			return
		case job := <-sm.finalizeCh:
			for _, ready := range buf.add(job) {
				if err := sm.finalizeBlock(ready); err != nil {
					sm.setFinalizeError(err)
				}

				sm.markFinalized(ready.blockHeight)
			}
		case <-ticker.C:
			waiting, started := buf.waitingFor()

			// Keep the in-flight set bounded; everything below the awaited height
			// is finalized or stale.
			sm.pruneDispatchedBelow(waiting)

			// Re-request only a genuinely missing block, not one that is simply
			// queued behind slow finalization (the in-flight false-positive).
			if sm.gapShouldResync(detector.observe(waiting, started, buf.len(), time.Now()), waiting) {
				sm.maybeResyncFinalizeGap(waiting)
			}
		}
	}
}

// finalizeError returns the first finalization error seen by finalizeLoop, if any.
func (sm *SyncManager) finalizeError() error {
	sm.finalizeErrMu.Lock()
	defer sm.finalizeErrMu.Unlock()

	return sm.finalizeErrV
}

// setFinalizeError records the first finalization failure.
func (sm *SyncManager) setFinalizeError(err error) {
	sm.finalizeErrMu.Lock()
	defer sm.finalizeErrMu.Unlock()

	if sm.finalizeErrV == nil {
		sm.finalizeErrV = err
		sm.logger.Errorf("[finalizeLoop] block finalization failed, halting pipeline ingest: %v", err)
	}
}

// waitForPreviousBlockMined waits for the previous block to have mined_set=true.
// This ensures setTxMined has completed for the previous block before we validate
// the next block's transactions, which is critical for BIP68 sequence lock validation
// that needs correct BlockHeights from parent transactions in the UTXO store.
func (sm *SyncManager) waitForPreviousBlockMined(ctx context.Context, prevBlockHash *chainhash.Hash, blockHeight uint32) error {
	_, err := retry.Retry(ctx, sm.logger, func() (bool, error) {
		isMined, err := sm.blockchainClient.GetBlockIsMined(ctx, prevBlockHash)
		if err != nil {
			return false, errors.NewServiceError(
				"[waitForPreviousBlockMined][height:%d] parent %s mined status not available yet",
				blockHeight, prevBlockHash.String(), err)
		}
		if !isMined {
			return false, errors.NewBlockParentNotMinedError(
				"[waitForPreviousBlockMined][height:%d] parent %s not mined yet",
				blockHeight, prevBlockHash.String())
		}
		return true, nil
	},
		retry.WithBackoffDurationType(sm.settings.BlockValidation.IsParentMinedRetryBackoffDuration),
		retry.WithBackoffMultiplier(sm.settings.BlockValidation.IsParentMinedRetryBackoffMultiplier),
		retry.WithRetryCount(sm.settings.BlockValidation.IsParentMinedRetryMaxRetry),
		retry.WithMessage("waitForPreviousBlockMined: legacy sync waiting for parent mined_set"),
	)
	return err
}

func (sm *SyncManager) ProcessBlock(ctx context.Context, teranodeBlock *model.Block) (err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "SyncManager:processBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[SyncManager:processBlock][%s %d] processing block",
			teranodeBlock.Hash().String(),
			teranodeBlock.Height,
		),
		tracing.WithHistogram(prometheusLegacyNetsyncProcessBlock),
	)
	defer func() {
		deferFn(err)
	}()

	// send the block to the blockValidation for processing and validation
	// all the block subtrees should have been validated in processSubtrees
	// teranodeBlock.ID was set by model.NewBlock from the pre-assigned ID returned by prepareSubtrees.
	// Read it from the struct here — avoids duplicating it as a parameter. It still has to travel as
	// a separate proto field in the gRPC request because block.Bytes() does not serialize ID.
	if err = sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height, "", "legacy", teranodeBlock.ID); err != nil {
		if errors.Is(err, errors.ErrBlockExists) {
			sm.logger.Infof("[SyncManager:processBlock][%s %d] block already exists", teranodeBlock.Hash().String(), teranodeBlock.Height)
			return nil
		}

		return errors.NewProcessingError("failed to process block", err)
	}

	return nil
}

type TxMapWrapper struct {
	Tx                 *bt.Tx
	SomeParentsInBlock bool
	ChildLevelInBlock  uint32
}

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) (subtrees []*chainhash.Hash, blockID uint32, err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "prepareSubtrees",
		tracing.WithLogMessage(
			sm.logger,
			"[prepareSubtrees][%s] processing subtree for block height %d, tx count %d",
			block.Hash().String(),
			block.Height(),
			len(block.Transactions()),
		),
		tracing.WithHistogram(prometheusLegacyNetsyncPrepareSubtrees),
	)
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("[prepareSubtrees] recovered in prepareSubtrees: %v", r, err)
		}

		deferFn(err)
	}()

	subtrees = make([]*chainhash.Hash, 0)

	txCount := len(block.Transactions())
	if txCount <= 1 {
		return subtrees, blockID, nil
	}

	// Partition the block's transactions into K subtrees so each non-final subtree
	// is exactly subtreeSize leaves and the final subtree's leaf count is in
	// [1, subtreeSize] — matching model.Block.CheckMerkleRoot's Length-based lift
	// rules. The final subtree does not need to be a power of two: the
	// duplicate-when-odd rule applied inside BuildMerkleTreeStoreFromBytes already
	// pads its natural root to height ceil(log2(length)), which is what the lift
	// in CheckMerkleRoot expects. For blocks where txCount ≤
	// MaximumMerkleItemsPerSubtree the partition is the unchanged single-subtree
	// case.
	maxItems := sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree

	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(txCount, maxItems)
	if err != nil {
		return nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to partition block", err)
	}

	subtreeSlices := make([]*subtreepkg.Subtree, numSubtrees)
	subtreeDatas := make([]*subtreepkg.Data, numSubtrees)
	subtreeMetas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && numSubtrees > 1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		if terr != nil {
			return nil, 0, errors.NewSubtreeError("[prepareSubtrees] failed to create subtree %d", i, terr)
		}

		if i == 0 {
			if err = st.AddCoinbaseNode(); err != nil {
				return nil, 0, errors.NewSubtreeError("[prepareSubtrees] failed to add coinbase placeholder", err)
			}
		}

		subtreeSlices[i] = st
		subtreeDatas[i] = subtreepkg.NewSubtreeData(st)
		subtreeMetas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](txCount)

	if err = sm.createTxMap(ctx, block, txMap); err != nil {
		return nil, 0, err
	}

	blockHeight32, convErr := safeconversion.Int32ToUint32(block.Height())
	if convErr != nil {
		return nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to convert block height", convErr)
	}

	// Quick validation is safe whenever the block sits at/below the highest hard-coded
	// checkpoint for the active network. POW (verified upstream by HasMetTargetDifficulty)
	// plus checkpoint-anchored chain linkage make the block canonical regardless of which
	// FSM state drove the catch-up. The checkpoint list is owned by go-chaincfg — see PR
	// #844 for the matching FSM-RUN gate that relies on the same invariant.
	quickValidationMode := sm.quickValidationAllowed(blockHeight32)

	// Lazily create the off-heap satoshi cache on the first quick-validation block
	// (catch-up only). Populated by createUtxos and consulted by resolveQuickFees to
	// resolve fees without a store read. nil/zero overhead when disabled or synced.
	if quickValidationMode && sm.satoshiCache == nil && sm.settings.Legacy.ParentSatoshiCacheMB > 0 {
		poc, cacheErr := newSatoshiCache(sm.settings.Legacy.ParentSatoshiCacheMB * 1024 * 1024)
		if cacheErr != nil {
			return nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to create satoshi cache", cacheErr)
		}

		sm.satoshiCache = poc
		sm.logger.Infof("[prepareSubtrees] created %d MB off-heap satoshi cache for quick-validation fees", sm.settings.Legacy.ParentSatoshiCacheMB)
	}

	// In quickValidationMode the previous-output decorate is skipped entirely: the
	// chain is checkpoint-anchored, so scripts/fees/utxo-hash are not re-checked and
	// inputs are spent trusted (by outpoint) in ValidateTransactionsLegacyMode. Txs
	// are persisted non-extended; subtree-data readers re-extend on demand. The full
	// validation path below the checkpoint still extends up front.
	if !quickValidationMode {
		if err = sm.extendTransactions(ctx, block, txMap); err != nil {
			return nil, 0, err
		}
	}

	if err = sm.createSubtrees(ctx, block, txMap, subtreeSlices, subtreeDatas, subtreeMetas, quickValidationMode); err != nil {
		return nil, 0, err
	}

	// Lazily construct the subtree write batcher on the first catch-up block.
	//
	// The batcher is gated on quickValidationMode for two reasons:
	//   1. Reader durability contract: outside quick mode, CheckBlockSubtrees
	//      reads each subtree file synchronously after writeSubtree returns.
	//      Coalescing would leave the file in the batcher's in-memory buffer
	//      when the reader asks for it. Quick mode skips that reader, so
	//      there is no same-session consumer expecting immediate durability.
	//   2. Memory footprint: writeSubtreeDirect streams payloads straight to
	//      filestorer; the batcher must materialise each blob as []byte so
	//      it can hold items across block boundaries. That's a bounded cost
	//      for checkpoint-anchored historical catch-up, but an unbounded
	//      liability on arbitrary-size tip blocks.
	//
	// Additionally gated on SubtreeWriteBatchBlocks > 1: at the default
	// batchBlocks=1 the batcher flushes after every block anyway (3 items =
	// 1 block * 3), so we gain no cross-block coalescing — but we would
	// still pay the cost of calling subtreeData.Serialize(), which can
	// allocate ~10.9 GB for the largest historical blocks. writeSubtreeDirect
	// streams subtreeData via WriteTransactionsToWriter and avoids that
	// allocation. So at batchBlocks=1 we skip the batcher entirely and let
	// writeSubtree dispatch straight to writeSubtreeDirect. The batcher is
	// only constructed when operators explicitly opt in to cross-block
	// coalescing (batchBlocks>1), in which case they have accepted the
	// materialisation cost for the perf benefit.
	//
	// Safety: prepareSubtrees is only ever called from the blockHandler
	// goroutine (single-threaded block processing path), so no mutex is
	// needed around the batcher pointer.
	// Skip the cross-block batcher under concurrency: it is single-goroutine
	// (shared in-memory buffer across blocks) and would race when multiple PhaseA
	// workers write subtrees at once. With it bypassed, writeSubtree streams each
	// block's subtrees directly to distinct files — concurrency-safe.
	if quickValidationMode && sm.subtreeWriteBatcher == nil && sm.settings.Legacy.SubtreeWriteBatchBlocks > 1 && sm.effectivePipelineConcurrency() <= 1 {
		sm.subtreeWriteBatcher = NewSubtreeWriteBatcher(
			sm.settings.Legacy.SubtreeWriteBatchBlocks,
			sm.settings.Legacy.SubtreeWriteBatchWait,
			sm.logger,
			sm.flushSubtreeWriteBatch,
		)
	}

	if quickValidationMode {
		// Fetch block ID upfront so UTXOs carry mined info from creation. This ID is
		// threaded through to blockvalidation via ProcessBlock so it can call
		// AddBlock(WithID, WithMinedSet(true)) and cause the setMinedChan worker to
		// skip setTxMinedStatus (MinedSet guard in BlockValidation.go).
		//
		// Note on ID gaps: GetNextBlockID advances the sequence atomically. If anything
		// fails after this point (createUtxos error, network error, context cancellation),
		// the ID is consumed and a gap appears in block IDs. This is acceptable — the
		// blockchain store tolerates non-contiguous IDs; the sequence is used only as a
		// monotonic counter, not a contiguous index.
		id, idErr := sm.blockchainClient.GetNextBlockID(ctx)
		if idErr != nil {
			return nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to get next block ID", idErr)
		}

		blockID = uint32(id) // nolint:gosec

		// in quickValidationMode, we can process transactions in a block in parallel, but in reverse order
		// first we create all the utxos, then we spend them
		if err = sm.ValidateTransactionsLegacyMode(ctx, txMap, block, blockID); err != nil {
			return nil, 0, err
		}
	}

	for i := 0; i < numSubtrees; i++ {
		if err = sm.writeSubtree(ctx, block, subtreeSlices[i], subtreeDatas[i], subtreeMetas[i], quickValidationMode); err != nil {
			return nil, 0, err
		}
	}

	// In quickValidationMode the transactions and subtree files have already been
	// produced locally, so we can skip the round-trip through subtreeValidation.
	if !quickValidationMode {
		for i := 0; i < numSubtrees; i++ {
			if err = sm.checkSubtreeFromBlock(ctx, block, subtreeSlices[i]); err != nil {
				return nil, 0, err
			}
		}
	}

	for i := 0; i < numSubtrees; i++ {
		subtrees = append(subtrees, subtreeSlices[i].RootHash())
	}

	return subtrees, blockID, nil
}

// quickValidationAllowed reports whether the given block height is covered by a
// hard-coded checkpoint for the active network. Checkpoint-anchored chain linkage
// combined with the upstream PoW check makes the block canonical, so we can skip
// subtree re-validation and the per-UTXO setTxMined cross-check.
//
// Returns false when the network defines no checkpoints (regtest) or when the
// block height is above the highest checkpoint — those blocks must follow the
// regular validation path.
func (sm *SyncManager) quickValidationAllowed(blockHeight uint32) bool {
	if sm.chainParams == nil {
		return false
	}

	highest := blockchain.HighestCheckpointHeight(sm.chainParams.Checkpoints)
	if highest == 0 {
		return false
	}

	return blockHeight <= highest
}

func (sm *SyncManager) checkSubtreeFromBlock(ctx context.Context, block *bsvutil.Block, subtree *subtreepkg.Subtree) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "checkSubtreeFromBlock",
		tracing.WithLogMessage(sm.logger, "[checkSubtreeFromBlock][%s] checking subtree for block %s height %d", subtree.RootHash().String(), block.Hash().String(), block.Height()),
	)

	defer deferFn()

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return err
	}

	if err := sm.subtreeValidation.CheckSubtreeFromBlock(ctx, *subtree.RootHash(), "legacy", blockHeightUint32, block.Hash(), &block.MsgBlock().Header.PrevBlock); err != nil {
		return errors.NewSubtreeError("failed to check subtree", err)
	}

	return nil
}

// writeSubtree dispatches subtree blob persistence to the batcher when it
// is active, otherwise falls through to the synchronous writeSubtreeDirect
// path.
//
// Only quickValidationMode blocks go through the batcher. In any other mode
// (steady-state RUNNING, or catch-up blocks past the final checkpoint once
// PR #723 lands) CheckBlockSubtrees reads each subtree file immediately
// after this function returns, so the writes must be on disk synchronously;
// and the direct path's streaming semantics avoid materialising
// potentially-huge tip-block payloads in memory. See the comment on the
// batcher construction in prepareSubtrees for the full rationale.
//
// When quickValidationMode flips to false mid-session (e.g. FSM transitions
// out of catch-up and back to RUNNING), drain and retire the batcher so
// subsequent blocks take the synchronous path with no lost items.
func (sm *SyncManager) writeSubtree(ctx context.Context, block *bsvutil.Block, subtree *subtreepkg.Subtree,
	subtreeData *subtreepkg.Data, subtreeMetaData *subtreepkg.Meta, quickValidationMode bool) error {
	if !quickValidationMode && sm.subtreeWriteBatcher != nil {
		err := sm.subtreeWriteBatcher.Stop(ctx)
		sm.subtreeWriteBatcher = nil
		if err != nil {
			// Fail fast: if the drain failed there may be buffered subtree
			// items that never made it to disk, and subsequent validation
			// readers would see missing blobs. Surface the error instead
			// of silently falling through to the direct path.
			return errors.NewStorageError("[writeSubtree] draining SubtreeWriteBatcher on quickValidationMode transition", err)
		}
	}
	if sm.subtreeWriteBatcher == nil {
		return sm.writeSubtreeDirect(ctx, block, subtree, subtreeData, subtreeMetaData, quickValidationMode)
	}

	// Note: the batched path pre-serializes each payload to []byte before submitting.
	// writeSubtreeDirect streams to avoid large intermediate allocations for big blocks
	// (subtreeData.Serialize() can produce a multi-GB buffer on the largest historical
	// blocks), but coalescing requires the full payload in memory so the batcher can
	// submit it across block boundaries. This memory cost is why the batcher is
	// constructed only when SubtreeWriteBatchBlocks > 1 (see prepareSubtrees):
	// operators opting in to cross-block coalescing accept the materialisation cost,
	// while the default configuration (batchBlocks=1) bypasses the batcher entirely
	// and goes through writeSubtreeDirect's streaming path.
	//
	// The batched path must only be reached when batchBlocks>1; if sm.subtreeWriteBatcher
	// is non-nil here, we trust prepareSubtrees' gating.
	dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention //nolint:gosec
	treeRootHash := *subtree.RootHash()
	dataRootHash := *subtreeData.RootHash()

	treeFileType := fileformat.FileTypeSubtreeToCheck
	if quickValidationMode {
		treeFileType = fileformat.FileTypeSubtree
	}

	// Check existence for all three blobs before serialising. When re-processing
	// blocks, skipping Serialize() on blobs that are already on disk saves the
	// worst-case allocation — subtreeData.Serialize() alone can produce a
	// multi-GB buffer on the largest historical blocks. flushSubtreeWriteBatch
	// is still defensive against ErrBlobAlreadyExists inside NewFileStorer, but
	// catching it here avoids the allocation entirely.
	// Context fields for every error in this path — mirrors the
	// writeSubtreeDirect formatting so operators can trace a failing blob
	// from a single log line.
	blockHashStr := block.Hash().String()
	blockHeight := block.Height()
	subtreeRootStr := subtree.RootHash().String()
	dataRootStr := dataRootHash.String()

	treeExists, err := sm.subtreeStore.Exists(ctx, treeRootHash[:], treeFileType)
	if err != nil {
		return errors.NewStorageError("[writeSubtree] check subtree exists: block_hash=%s block_height=%d subtree_root=%s file_type=%s", blockHashStr, blockHeight, subtreeRootStr, treeFileType, err)
	}
	dataExists, err := sm.subtreeStore.Exists(ctx, dataRootHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[writeSubtree] check subtree data exists: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
	}
	metaExists, err := sm.subtreeStore.Exists(ctx, dataRootHash[:], fileformat.FileTypeSubtreeMeta)
	if err != nil {
		return errors.NewStorageError("[writeSubtree] check subtree meta exists: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
	}

	if !treeExists {
		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree] serialize subtree: block_hash=%s block_height=%d subtree_root=%s file_type=%s", blockHashStr, blockHeight, subtreeRootStr, treeFileType, err)
		}
		if err := sm.subtreeWriteBatcher.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindTree, FileType: treeFileType, RootHash: treeRootHash, Bytes: subtreeBytes, DeleteAt: dah}); err != nil {
			return errors.NewStorageError("[writeSubtree] submit subtree: block_hash=%s block_height=%d subtree_root=%s file_type=%s", blockHashStr, blockHeight, subtreeRootStr, treeFileType, err)
		}
	}
	if !dataExists {
		dataBytes, err := subtreeData.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree] serialize subtree data: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
		}
		if err := sm.subtreeWriteBatcher.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData, RootHash: dataRootHash, Bytes: dataBytes, DeleteAt: dah}); err != nil {
			return errors.NewStorageError("[writeSubtree] submit subtree data: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
		}
	}
	if !metaExists {
		metaBytes, err := subtreeMetaData.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree] serialize subtree meta: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
		}
		if err := sm.subtreeWriteBatcher.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindMeta, RootHash: dataRootHash, Bytes: metaBytes, DeleteAt: dah}); err != nil {
			return errors.NewStorageError("[writeSubtree] submit subtree meta: block_hash=%s block_height=%d subtree_root=%s data_root=%s", blockHashStr, blockHeight, subtreeRootStr, dataRootStr, err)
		}
	}
	return nil
}

func (sm *SyncManager) writeSubtreeDirect(ctx context.Context, block *bsvutil.Block, subtree *subtreepkg.Subtree,
	subtreeData *subtreepkg.Data, subtreeMetaData *subtreepkg.Meta, quickValidationMode bool) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "writeSubtree",
		tracing.WithLogMessage(sm.logger, "[writeSubtree][%s] writing subtree for block %s height %d", subtree.RootHash().String(), block.Hash().String(), block.Height()),
	)

	subtreeFileExtension := fileformat.FileTypeSubtreeToCheck
	if quickValidationMode {
		subtreeFileExtension = fileformat.FileTypeSubtree
	}

	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)
	// Limit to 3 concurrent writes (subtree, subtreeData, subtreeMeta)
	util.SafeSetLimit(g, 3)

	g.Go(func() error {
		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree", subtree.RootHash().String(), err)
		}

		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtree.RootHash()[:],
			subtreeFileExtension,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to create subtree file", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - *subtree.RootHash(), uint32(block.Height())

		if _, err = storer.Write(subtreeBytes); err != nil {
			return errors.NewStorageError("error writing subtree to disk", err)
		}

		if err = storer.Close(ctx); err != nil {
			return errors.NewStorageError("error closing subtree file", err)
		}

		writeSucceeded = true

		return nil
	})

	g.Go(func() error {
		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtreeData.RootHash()[:],
			fileformat.FileTypeSubtreeData,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to create subtree data file", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree data %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - , *subtreeData.RootHash(), uint32(block.Height())

		// Stream transactions directly to the file storer instead of serializing
		// into a single large buffer. This eliminates the ~10.9 GB intermediate
		// allocation that Serialize() creates for large blocks.
		if err := subtreeData.WriteTransactionsToWriter(storer, 0, subtreeData.Subtree.Length()); err != nil {
			return errors.NewStorageError("error streaming subtree data to disk", err)
		}

		if err = storer.Close(ctx); err != nil {
			return errors.NewStorageError("error closing subtree data file", err)
		}

		writeSucceeded = true

		return nil
	})

	// Always store subtree meta data - even when not in quickValidationMode, we need to ensure
	// metadata exists because checkSubtreeFromBlock may return early if the subtree already exists
	// (e.g., created by block assembly) without creating the metadata
	g.Go(func() error {
		// Check if metadata already exists (e.g., came in via P2P) to avoid unnecessary work
		if exists, _ := sm.subtreeStore.Exists(gCtx, subtreeData.RootHash()[:], fileformat.FileTypeSubtreeMeta); exists {
			return nil
		}

		subtreeBytes, err := subtreeMetaData.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree data", subtree.RootHash().String(), err)
		}

		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtreeData.RootHash()[:],
			fileformat.FileTypeSubtreeMeta,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to store subtree meta data", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree meta %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - , *subtree.RootHash(), uint32(block.Height())

		if _, err = storer.Write(subtreeBytes); err != nil {
			return errors.NewStorageError("error writing subtree meta to disk", err)
		}

		if err = storer.Close(gCtx); err != nil {
			return errors.NewStorageError("error closing subtree meta file", err)
		}

		writeSucceeded = true

		return nil
	})

	return g.Wait()
}

// flushSubtreeWriteBatch is the SubtreeWriteBatcher flush callback. It writes each item
// to the subtreeStore concurrently, bounded to 8 parallel writes.
func (sm *SyncManager) flushSubtreeWriteBatch(ctx context.Context, items []SubtreeWriteItem) error {
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, 8)

	for _, item := range items {
		item := item
		g.Go(func() error {
			// rootHash is formatted once per item and used as a stable, searchable
			// identifier in every error message so operators can grep logs for the
			// failing blob without cross-referencing batch offsets.
			rootHash := chainhash.Hash(item.RootHash).String()

			var fileType fileformat.FileType
			switch item.Kind {
			case SubtreeKindTree:
				fileType = item.FileType
			case SubtreeKindData:
				fileType = fileformat.FileTypeSubtreeData
			case SubtreeKindMeta:
				// Mirror the existence check from writeSubtreeDirect: skip if already present
				// (e.g., created by block assembly via P2P).
				exists, err := sm.subtreeStore.Exists(gCtx, item.RootHash[:], fileformat.FileTypeSubtreeMeta)
				if err != nil {
					return errors.NewStorageError("flushSubtreeWriteBatch: check meta exists (kind=%d hash=%s)", item.Kind, rootHash, err)
				}
				if exists {
					return nil
				}
				fileType = fileformat.FileTypeSubtreeMeta
			default:
				return errors.NewProcessingError("flushSubtreeWriteBatch: unknown SubtreeKind %d (hash=%s)", item.Kind, rootHash)
			}

			storer, err := filestorer.NewFileStorer(
				gCtx, sm.logger, sm.settings, sm.subtreeStore,
				item.RootHash[:], fileType,
				options.WithDeleteAt(item.DeleteAt),
			)
			if err != nil {
				if errors.Is(err, errors.ErrBlobAlreadyExists) {
					return nil
				}
				return errors.NewStorageError("flushSubtreeWriteBatch: create file (kind=%d type=%s hash=%s)", item.Kind, fileType, rootHash, err)
			}
			var ok bool
			defer func() {
				if !ok {
					storer.Abort(errors.NewProcessingError("flushSubtreeWriteBatch: write aborted (kind=%d type=%s hash=%s)", item.Kind, fileType, rootHash))
				}
			}()
			if _, err := storer.Write(item.Bytes); err != nil {
				return errors.NewStorageError("flushSubtreeWriteBatch: write (kind=%d type=%s hash=%s)", item.Kind, fileType, rootHash, err)
			}
			if err := storer.Close(gCtx); err != nil {
				return errors.NewStorageError("flushSubtreeWriteBatch: close (kind=%d type=%s hash=%s)", item.Kind, fileType, rootHash, err)
			}
			ok = true
			return nil
		})
	}
	return g.Wait()
}

func (sm *SyncManager) ValidateTransactionsLegacyMode(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	block *bsvutil.Block, blockID uint32) (err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactionsLegacyMode",
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactionsLegacyMode),
		tracing.WithLogMessage(sm.logger, "[validateTransactionsLegacyMode] called for block %s, height %d", block.Hash(), block.Height()),
	)

	defer func() {
		deferFn(err)
	}()

	if err = sm.createUtxos(ctx, txMap, block, blockID); err != nil {
		return err
	}

	sm.logger.Infof("[validateTransactionsLegacyMode] created utxos with %d items", txMap.Length())

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		// already wrapped in a processing error
		return err
	}

	candidateBlockTime, candidateParentMedianTime, err := sm.candidateFinalityTimesForBlock(ctx, block, blockHeightUint32)
	if err != nil {
		return errors.NewProcessingError("[validateTransactionsLegacyMode] failed to select finality time sources", err)
	}

	if err = sm.PreValidateTransactions(ctx, txMap, *block.Hash(), blockHeightUint32, candidateBlockTime, candidateParentMedianTime); err != nil {
		return errors.NewProcessingError("[validateTransactionsLegacyMode] failed to pre-validate transactions", err)
	}

	return nil
}

// candidateFinalityTimesForBlock picks the validator finality-time options
// for the given block based on its CSV era. Exactly one return value is
// non-zero on success:
//
//   - Pre-CSV (blockHeight < CSVHeight): returns (block header timestamp, 0).
//     The validator consumes Options.CandidateBlockTime in this era.
//   - Post-CSV (blockHeight >= CSVHeight): returns (0, candidate-parent MTP).
//     The validator consumes Options.CandidateParentMedianTime in this era,
//     and the parent-chain-walk sourcing rule + chain re-anchor + walk
//     fallback live inside candidateParentMedianTimeForBlock.
//
// The other field stays zero so candidateBlockTimePtr /
// candidateParentMedianTimePtr in services/validator can drop it from the
// proto wire. Extracted as a separate method so the era-selection branch
// can be table-tested at the package level without standing up the full
// SyncManager pipeline.
func (sm *SyncManager) candidateFinalityTimesForBlock(ctx context.Context, block *bsvutil.Block, blockHeight uint32) (candidateBlockTime uint32, candidateParentMedianTime uint32, err error) {
	if blockHeight < uint32(sm.chainParams.CSVHeight) {
		candidateBlockTime, err = safeconversion.Int64ToUint32(block.MsgBlock().Header.Timestamp.Unix())
		if err != nil {
			return 0, 0, err
		}

		return candidateBlockTime, 0, nil
	}

	candidateParentMedianTime, err = sm.candidateParentMedianTimeForBlock(ctx, &block.MsgBlock().Header.PrevBlock)
	if err != nil {
		return 0, 0, err
	}

	return 0, candidateParentMedianTime, nil
}

// candidateParentMedianTimeForBlock returns the candidate-parent MTP for the
// post-CSV consensus path, i.e. the equivalent of bitcoin-sv's
// pindexPrev->GetMedianTimePast() for a candidate whose parent is parentHash.
//
// The MTP is computed by fetching 11 block headers walking back from
// parentHash via the blockchain's GetBlockHeaders API and taking the median of
// their timestamps. GetBlockHeaders is fork-aware: its SQL fallback path
// recursively walks parent_id when the start hash is not on the main chain,
// so a candidate building on a side-chain parent receives the MTP of THAT
// parent chain — not the main-chain MTP at the same height (which is what a
// height-based lookup like GetMedianTimePastForHeights would return).
//
// The value is computed and returned unconditionally because the validator's
// post-CSV consensus path now hard-errors on a missing
// Options.CandidateParentMedianTime (no tip-MTP soft-fall). The earlier
// parent==tip optimisation was unsound — the validator reads
// blockState.MedianTime later than the caller's tip check, and the utxo
// store updates that field asynchronously from blockchain notifications, so
// a tip advance / reorg between the two reads would silently swap the
// comparison time source.
func (sm *SyncManager) candidateParentMedianTimeForBlock(ctx context.Context, parentHash *chainhash.Hash) (uint32, error) {
	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	// Try the batched API first — it is cache-friendly and resolves in a
	// single round-trip on the steady-state path. The SQL implementation
	// runs an on_main_chain probe and a SELECT in two statements; if a reorg
	// lands between them, the returned headers may not anchor to parentHash.
	// candidateParentMedianTimeFromHeaders re-anchors the result and returns
	// an error in that case.
	headers, _, err := sm.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: failed to fetch parent-chain headers", parentHash.String(), err)
	}

	mtp, anchorErr := candidateParentMedianTimeFromHeaders(parentHash, headers)
	if anchorErr == nil {
		return mtp, nil
	}

	// Re-anchor failure on the batched path. Retrying the batched API does
	// not help because GetBlockHeaders caches the (parentHash, 11) result —
	// the next call replays the same headers from cache. Fall back to a
	// hash-keyed parent-chain walk: GetBlockHeader's cache is keyed by hash,
	// so each header is uniquely identified and the same race cannot poison
	// this path. Cost on a cold cache is N round-trips (N=11) instead of 1,
	// taken only on the rare reorg-race event.
	walked, walkErr := sm.walkParentChain(ctx, parentHash, blockchain.MedianTimeBlocks)
	if walkErr != nil {
		return 0, errors.NewProcessingError("parent hash %s: batched-API re-anchor failed (%v); fallback walk failed", parentHash.String(), anchorErr, walkErr)
	}

	mtp, err = candidateParentMedianTimeFromHeaders(parentHash, walked)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: re-anchor failed on both batched fetch (%v) and hash-walk fallback", parentHash.String(), anchorErr, err)
	}

	return mtp, nil
}

// walkParentChain fetches exactly depth block headers starting at startHash and
// walking backwards via HashPrevBlock. Each hop uses blockchainClient.GetBlockHeader
// which is keyed by hash in the in-memory cache — so its results are
// deterministic regardless of which block is canonical at any given height
// (block contents are immutable once stored). This makes the walk
// race-safe under reorg, at the cost of N round-trips on a cold cache.
//
// Returned headers are ordered newest-first, matching the contract of
// blockchainClient.GetBlockHeaders' return order so candidateParentMedianTimeFromHeaders
// can re-anchor them with the same logic.
//
// A nil pointer (cur == nil) or a nil header response (header == nil) is
// treated as a hard error. Production callers only invoke this when the
// candidate height is at or above CSVHeight, which is well past the first
// `depth` blocks of the chain — so we never legitimately walk off the
// beginning of the chain. Tolerating short returns would silently produce
// an incomplete MTP on a transient cache miss mid-chain; raising loudly
// instead forces the caller to surface the underlying issue.
func (sm *SyncManager) walkParentChain(ctx context.Context, startHash *chainhash.Hash, depth uint64) ([]*model.BlockHeader, error) {
	headers := make([]*model.BlockHeader, 0, depth)
	cur := startHash

	for i := uint64(0); i < depth; i++ {
		if cur == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil prev-block link at depth %d (walked off the chain)", i)
		}

		header, _, err := sm.blockchainClient.GetBlockHeader(ctx, cur)
		if err != nil {
			return nil, errors.NewProcessingError("walkParentChain: failed at depth %d (hash %s)", i, cur.String(), err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil header at depth %d (hash %s) — possible transient cache miss", i, cur.String())
		}

		headers = append(headers, header)
		cur = header.HashPrevBlock
	}

	return headers, nil
}

// candidateParentMedianTimeFromHeaders verifies that the supplied headers form
// a contiguous chain ending at parentHash, then returns the median of their
// timestamps.
//
// The verification closes a concurrency gap in blockchainClient.GetBlockHeaders:
// its main-chain fast path probes the start hash's on_main_chain status in one
// SQL statement and then runs the SELECT that returns the headers in a second
// statement. A reorg fired between the two statements (READ COMMITTED isolation)
// would return main-chain headers at the same height range that no longer
// correspond to parentHash — silently swapping the timestamp set we compute
// MTP over. Re-anchoring the result locally is O(11) and bulletproof: we check
// that the newest returned header equals parentHash and that each consecutive
// pair is linked via HashPrevBlock → Hash().
//
// Empty input and any verification failure surface as a hard error: silently
// returning 0 would let the caller pass Options.CandidateParentMedianTime=0
// to the validator, which now rejects post-CSV consensus requests with a
// missing parent MTP (no tip-MTP soft-fall) — but the error here gives a
// more precise diagnostic at the source rather than waiting for the
// validator's downstream rejection.
//
// Mirrors bitcoin-sv's CBlockIndex::GetMedianTimePast() for the median
// computation itself: sorts the gathered timestamps and returns
// `pbegin[(pend - pbegin) / 2]` — the upper-middle on even counts.
func candidateParentMedianTimeFromHeaders(parentHash *chainhash.Hash, headers []*model.BlockHeader) (uint32, error) {
	if len(headers) == 0 {
		return 0, errors.NewProcessingError("cannot compute median timestamp from zero headers")
	}

	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	// headers are returned newest-first (ORDER BY height DESC). headers[0] must
	// equal parentHash; each subsequent header must be the parent of the one
	// before it. Each element is guarded against nil — production paths
	// (SQL store, gRPC client) do not emit nil entries, but the helper is
	// meant to hard-fail on bad header data rather than panic.
	if headers[0] == nil {
		return 0, errors.NewProcessingError("nil header at depth 0")
	}

	headHash := headers[0].Hash()
	if headHash == nil || !headHash.IsEqual(parentHash) {
		return 0, errors.NewProcessingError("returned chain head does not match requested parent hash (possible reorg between header probe and fetch)")
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return 0, errors.NewProcessingError("nil header at depth %d", i)
		}

		prev := headers[i-1].HashPrevBlock
		cur := headers[i].Hash()
		if prev == nil || cur == nil || !prev.IsEqual(cur) {
			return 0, errors.NewProcessingError("parent-chain link broken at depth %d (possible reorg between header probe and fetch)", i)
		}
	}

	timestamps := make([]uint32, len(headers))
	for i, h := range headers {
		timestamps[i] = h.Timestamp
	}

	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	return timestamps[len(timestamps)/2], nil
}

// createUtxos creates all the utxos for the transactions in the block in parallel
// before any spending is done. This only occurs in legacy mode when we assume the
// block is valid.
func (sm *SyncManager) createUtxos(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], block *bsvutil.Block, blockID uint32) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createUtxos",
		tracing.WithLogMessage(sm.logger, "[createUtxos] called for block %s / height %d", block.Hash(), block.Height()),
		tracing.WithHistogram(prometheusLegacyNetsyncCreateUtxos),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in createUtxos: %v", r, err)
		}

		deferFn(err)
	}()

	storeBatcherSize := sm.settings.Legacy.StoreBatcherSize
	storeBatcherConcurrency := sm.settings.Legacy.StoreBatcherConcurrency

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, storeBatcherSize*storeBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return errors.NewProcessingError("failed to convert block height to uint32", err)
	}

	// Track txs that already exist in the store so we can merge our blockID into their
	// BlockIDs after the Create pass. The quickValidation fast path skips the async
	// setTxMinedStatus step entirely (AddBlock with MinedSet=true), so any tx that
	// pre-existed without our blockID (propagation, prior crashed attempt, or the
	// pre-fast-path subtreeValidation route) would otherwise stay with empty/wrong
	// BlockIDs and fail descendant blocks with "has no block IDs".
	var (
		existingTxsMu    sync.Mutex
		existingTxHashes []*chainhash.Hash
	)

	// create all the utxos first
	for _, txHash := range txMap.Keys() {
		txHash := txHash

		g.Go(func() error {
			txWrapper, ok := txMap.Get(txHash)
			if !ok {
				return errors.NewProcessingError("transaction %s not found in txMap", txHash.String())
			}

			if _, err := sm.utxoStore.Create(gCtx, txWrapper.Tx, blockHeightUint32, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
				BlockID:     blockID,
				BlockHeight: blockHeightUint32,
				SubtreeIdx:  0, // legacy path produces a single subtree at index 0
			})); err != nil {
				if errors.Is(err, errors.ErrTxExists) {
					existingTxsMu.Lock()
					existingTxHashes = append(existingTxHashes, &txHash)
					existingTxsMu.Unlock()
					// fall through to cache population — the outputs exist in the store
				} else {
					return err
				}
			}

			// Cache this tx's output satoshis for later blocks' fee resolution
			// (nil-safe no-op when the cache is disabled).
			sm.satoshiCache.putTx(txWrapper.Tx)

			return nil
		})
	}

	// wait for all utxos to be created
	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to create utxos", err)
	}

	// Merge our blockID into any tx that already existed. Without this, those txs
	// keep their stale (or empty) BlockIDs and the next block's validOrderAndBlessed
	// check fails in model/Block.go getParentTxMetaBlockIDs.
	//
	// Chunk the merge across a worker pool (#936). A single SetMinedMulti call with
	// every existing tx in the block overruns the aerospike client connection pool
	// on fat blocks (e.g. mainnet 755880 = 2.87M txs). Pattern mirrors
	// stores/utxo/aerospike/longest_chain.go:MarkTransactionsOnLongestChain.
	if len(existingTxHashes) > 0 {
		sm.logger.Debugf("[createUtxos] merging blockID %d into %d pre-existing tx(s)", blockID, len(existingTxHashes))

		batchSize := sm.settings.UtxoStore.MaxMinedBatchSize
		if batchSize < 1 {
			batchSize = 1
		}
		numChunks := (len(existingTxHashes) + batchSize - 1) / batchSize
		numWorkers := min(sm.settings.UtxoStore.MaxMinedRoutines, numChunks)
		if numWorkers < 1 {
			numWorkers = 1
		}

		minedBlockInfo := utxo.MinedBlockInfo{
			BlockID:        blockID,
			BlockHeight:    blockHeightUint32,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}

		rangeSize := (len(existingTxHashes) + numWorkers - 1) / numWorkers

		mergeG, mergeCtx := errgroup.WithContext(ctx)

		for w := 0; w < numWorkers && w*rangeSize < len(existingTxHashes); w++ {
			workerStart := w * rangeSize
			workerEnd := min(workerStart+rangeSize, len(existingTxHashes))
			workerHashes := existingTxHashes[workerStart:workerEnd]

			mergeG.Go(func() error {
				for i := 0; i < len(workerHashes); i += batchSize {
					if mergeCtx.Err() != nil {
						return mergeCtx.Err()
					}
					chunkEnd := min(i+batchSize, len(workerHashes))
					chunk := workerHashes[i:chunkEnd]
					if _, err := sm.utxoStore.SetMinedMulti(mergeCtx, chunk, minedBlockInfo); err != nil {
						return err
					}
				}
				return nil
			})
		}

		if err = mergeG.Wait(); err != nil {
			return errors.NewProcessingError("failed to merge blockID into %d pre-existing txs", len(existingTxHashes), err)
		}
	}

	return nil
}

// PreValidateTransactions pre-validates all the transactions in the block before
// sending them to subtree validation.
//
// candidateBlockTime and candidateParentMedianTime are paired finality-time
// sources for the consensus path inside the validator (SkipPolicyChecks=true):
// the former is consumed only when blockHeight < CSVHeight (bitcoin-sv's pre-
// BIP113 ContextualCheckBlock at src/validation.cpp:6020-6022), the latter
// only when blockHeight >= CSVHeight (bitcoin-sv's post-BIP113 path at
// src/validation.cpp:6001). The caller passes the one matching this block's
// era and zeroes the other.
func (sm *SyncManager) PreValidateTransactions(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	blockHash chainhash.Hash, blockHeight uint32, candidateBlockTime uint32, candidateParentMedianTime uint32) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "PreValidateTransactions",
		tracing.WithLogMessage(sm.logger, "[PreValidateTransactions] called for block %s / height %d", blockHash, blockHeight),
		tracing.WithHistogram(prometheusLegacyNetsyncPreValidateTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in PreValidateTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	spendBatcherSize := sm.settings.Legacy.SpendBatcherSize
	spendBatcherConcurrency := sm.settings.Legacy.SpendBatcherConcurrency
	concurrencyLimit := spendBatcherSize * spendBatcherConcurrency

	// Pre-warm the MTP store once before spawning per-transaction goroutines, so each goroutine
	// can read mtpStore[h] without locking and without making gRPC calls.
	if err = sm.validationClient.EnsureMTPLoaded(ctx, blockHeight); err != nil {
		return err
	}

	// These transactions arrive as part of a block, so they should be treated as valid
	// transactions that all need to be processed. If one fails (e.g. transient Aerospike
	// DEVICE_OVERLOAD), rolling back or cancelling all other independent transactions
	// in the block makes no sense. We retry failed transactions with backoff to adapt
	// to whatever throughput the storage backend can handle.
	const maxRetries = 10
	const retryBackoff = 2 * time.Second

	pendingTxHashes := txMap.Keys()
	totalTxCount := txMap.Length()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return errors.NewProcessingError("[PreValidateTransactions] context cancelled")
		}

		if attempt > 0 {
			sm.logger.Infof("[PreValidateTransactions] retry %d/%d: %d of %d transactions remaining",
				attempt, maxRetries, len(pendingTxHashes), totalTxCount)
			time.Sleep(retryBackoff)
		}

		g, _ := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, concurrencyLimit)

		var (
			mu           sync.Mutex
			retryableTxs []chainhash.Hash
			lastErr      error
			hardFail     error
		)

		for _, txHash := range pendingTxHashes {
			txHash := txHash

			g.Go(func() (err error) {
				timeStart := time.Now()
				defer func() {
					prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
				}()

				txWrapper, ok := txMap.Get(txHash)
				if !ok {
					// Not found in txMap — non-recoverable, fail immediately
					mu.Lock()
					hardFail = errors.NewProcessingError("transaction %s not found in txMap", txHash.String())
					mu.Unlock()
					return nil
				}

				// Trusted-connect spend. PreValidateTransactions is only reached via the
				// quickValidationMode path (prepareSubtrees → ValidateTransactionsLegacyMode),
				// which runs only at/below the highest hard-coded checkpoint. PoW +
				// checkpoint linkage make the block canonical, so we record the spends by
				// outpoint without re-validating (no script execution, no UTXO-hash check,
				// no fee) and without extending the tx — eliminating the previous-output
				// decorate, the dominant per-block read/allocation cost during catch-up.
				// createUtxos already created the outputs (WithSkipUtxoCreation meant the
				// validator's create was a no-op here anyway), so the spend was the
				// validator's only net effect on this path. The store skips the UTXO-hash
				// guard via IgnoreUTXOHash and inserts ON CONFLICT DO NOTHING (idempotent).
				if _, spendErr := sm.utxoStore.Spend(ctx, txWrapper.Tx, blockHeight, utxo.IgnoreFlags{IgnoreUTXOHash: true}); spendErr != nil {
					// ErrTxConflicting can still surface from stale spending data in the
					// store; the confirmed block takes precedence and the conflict is
					// resolved by ProcessConflicting during block acceptance.
					if errors.Is(spendErr, errors.ErrTxConflicting) {
						return nil
					}

					if errors.IsRetryableError(spendErr) {
						mu.Lock()
						retryableTxs = append(retryableTxs, txHash)
						lastErr = spendErr
						mu.Unlock()
					} else {
						mu.Lock()
						hardFail = spendErr
						mu.Unlock()
					}
				}

				return nil
			})
		}

		_ = g.Wait()

		if hardFail != nil {
			return errors.NewProcessingError("[PreValidateTransactions] non-retryable error", hardFail)
		}

		if len(retryableTxs) == 0 {
			if attempt > 0 {
				sm.logger.Infof("[PreValidateTransactions] all transactions succeeded after %d retries", attempt)
			}
			return nil
		}

		// No progress since last attempt — stop retrying
		if attempt > 0 && len(retryableTxs) >= len(pendingTxHashes) {
			return errors.NewProcessingError("[PreValidateTransactions] %d of %d transactions failed with no progress, giving up",
				len(retryableTxs), totalTxCount, lastErr)
		}

		pendingTxHashes = retryableTxs
	}

	return errors.NewProcessingError("[PreValidateTransactions] %d of %d transactions still failing after %d retries",
		len(pendingTxHashes), totalTxCount, maxRetries)
}

// classifyAndCountPrewarmError routes a validator error from the pre-warm path
// (validateTransactions) into the prometheusLegacyNetsyncPrewarmErrors counter
// and emits a log line at the level appropriate for the class. Pre-warm errors
// are intentionally dropped — real subtree validation runs later and catches
// consensus violations on its own — so this helper exists purely to give ops
// observability into a path that previously silently swallowed every error
// (see issue #4590).
func classifyAndCountPrewarmError(logger ulogger.Logger, err error) {
	switch {
	case errors.Is(err, errors.ErrTxInvalid):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("tx_invalid").Inc()
		logger.Errorf("[validateTransactions][prewarm] critical: tx invalid: %v", err)
	case errors.Is(err, errors.ErrServiceError):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("service").Inc()
		logger.Warnf("[validateTransactions][prewarm] service error (transient): %v", err)
	case errors.Is(err, errors.ErrTxConflicting), errors.Is(err, errors.ErrTxExists):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("policy").Inc()
		logger.Debugf("[validateTransactions][prewarm] expected: %v", err)
	case errors.Is(err, errors.ErrProcessing):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("processing").Inc()
		logger.Warnf("[validateTransactions][prewarm] processing error: %v", err)
	default:
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("other").Inc()
		logger.Warnf("[validateTransactions][prewarm] unclassified: %v", err)
	}
}

// validateTransactions validates all the transactions in the block in parallel
// per level. This is done to speed up subtree validation later on.
// The levels indicate the number of parents in the block.
func (sm *SyncManager) validateTransactions(ctx context.Context, maxLevel uint32, blockTxsPerLevel map[uint32][]*bt.Tx, block *bsvutil.Block) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactions",
		tracing.WithLogMessage(sm.logger, "[validateTransactions] called for block %s / height %d", block.Hash(), block.Height()),
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactions),
	)

	// Convert block height once and propagate any failure immediately —
	// silently dropping the candidate finality fields to zero would defeat
	// the always-populate rule and hand the validator back to the tip-MTP
	// race that this code is built to avoid. Mirrors the upfront conversion
	// in ValidateTransactionsLegacyMode.
	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return err
	}

	candidateBlockTime, candidateParentMedianTime, err := sm.candidateFinalityTimesForBlock(ctx, block, blockHeightUint32)
	if err != nil {
		return errors.NewProcessingError("[validateTransactions] failed to select finality time sources", err)
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in validateTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	spendBatcherSize := sm.settings.Legacy.SpendBatcherSize
	spendBatcherConcurrency := sm.settings.Legacy.SpendBatcherConcurrency

	var timeStart time.Time

	if err = sm.validationClient.EnsureMTPLoaded(ctx, blockHeightUint32); err != nil {
		return err
	}

	// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
	// This allows us to process all the transactions in parallel. The levels indicate the number of parents in the block.
	for i := uint32(0); i <= maxLevel; i++ {
		_, _, deferLevelFn := tracing.Tracer("netsync").Start(ctx, fmt.Sprintf("validateTransactions:level:%d", i))

		if len(blockTxsPerLevel[i]) < 10 {
			// if we have less than 10 transactions on a certain level, we can process them immediately by triggering the batcher
			for txIdx := range blockTxsPerLevel[i] {
				blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
				if err != nil {
					return err
				}

				timeStart = time.Now()

				if _, validateErr := sm.validationClient.Validate(ctx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true), validator.WithCandidateBlockTime(candidateBlockTime), validator.WithCandidateParentMedianTime(candidateParentMedianTime)); validateErr != nil {
					classifyAndCountPrewarmError(sm.logger, validateErr)
				}

				prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			}

			sm.validationClient.TriggerBatcher()
		} else {
			// process all the transactions on a certain level in parallel
			g, gCtx := errgroup.WithContext(ctx)
			util.SafeSetLimit(g, spendBatcherSize*spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

			for txIdx := range blockTxsPerLevel[i] {
				txIdx := txIdx

				g.Go(func() error {
					timeStart := time.Now()
					defer func() {
						prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
					}()

					blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
					if err != nil {
						return err
					}

					// send to validation, but only if the parent is not in the same block
					if _, validateErr := sm.validationClient.Validate(gCtx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true), validator.WithCandidateBlockTime(candidateBlockTime), validator.WithCandidateParentMedianTime(candidateParentMedianTime)); validateErr != nil {
						classifyAndCountPrewarmError(sm.logger, validateErr)
					}

					return nil
				})
			}

			// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
			_ = g.Wait()

			deferLevelFn()
		}
	}

	return nil
}

func (sm *SyncManager) extendTransactions(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "extendTransactions",
		tracing.WithLogMessage(sm.logger, "[extendTransactions] called for block %s / height %d", block.Hash(), block.Height()),
		tracing.WithHistogram(prometheusLegacyNetsyncExtendTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in extendTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	outpointBatcherSize := sm.settings.Legacy.OutpointBatcherSize

	// Phase 1: populate inputs whose parents are same-block transactions. These are
	// served directly from the in-memory txMap, so no DB work is needed here. We run
	// per-tx goroutines (bounded by OutpointBatcherSize) because each tx's own inputs
	// are populated independently; this phase reads same-block parent outputs
	// immediately and does not wait for the parent transaction to be extended first.
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, outpointBatcherSize)

	// Blocks always include a coinbase, but guard against 0-tx edge cases
	// (malformed/test blocks) where len-1 would produce a negative capacity.
	txCount := len(block.Transactions())
	txCapacity := 0
	if txCount > 0 {
		txCapacity = txCount - 1
	}
	txs := make([]*bt.Tx, 0, txCapacity)

	for idx, wireTx := range block.Transactions() {
		if idx == 0 {
			// skip the coinbase transaction, as it cannot be extended
			continue
		}

		txHash := *wireTx.Hash()

		// the coinbase transaction is not part of the txMap
		txWrapper, found := txMap.Get(txHash)
		if !found {
			return errors.NewTxError("transaction %s not found in txMap", txHash.String())
		}

		tx := txWrapper.Tx
		txs = append(txs, tx)

		g.Go(func() error {
			if err := sm.extendFromTxMap(gCtx, tx, txMap); err != nil {
				return errors.NewTxError("failed to extend transaction from txMap", err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to extend transactions from txMap", err)
	}

	// Phase 2: for inputs whose parents are NOT same-block, batch the decoration
	// using the store's internal chunking instead of issuing one DB lookup per tx.
	// For a 20k-tx block this collapses ~20k round-trips into roughly O(N / chunkSize).
	//
	// BatchPreviousOutputsDecorate skips inputs that already have PreviousTxScript set,
	// so Phase 1's work is preserved. If it returns a processing/not-found error the
	// most likely cause is a parent that's been pruned (DAH'd) because the child
	// already had a prior processing pass. Fall back to per-tx decoration so the
	// existing recovery path (utxoStore.Get on the child itself) can still kick in.
	if batchErr := sm.utxoStore.BatchPreviousOutputsDecorate(ctx, txs); batchErr != nil {
		if errors.Is(batchErr, errors.ErrProcessing) || errors.Is(batchErr, errors.ErrTxNotFound) {
			return sm.extendPerTxFallback(ctx, txs)
		}
		return errors.NewProcessingError("failed to batch-decorate previous outputs", batchErr)
	}

	return nil
}

// extendFromTxMap populates a transaction's inputs whose parents are in the same
// block (available via txMap). Parent Outputs are populated at wire-parse time
// and never mutated afterwards, so they can be read immediately without waiting
// for the parent's own inputs to be extended.
//
// Inputs whose parents are not in txMap are left for a later bulk DB lookup (see
// extendTransactions phase 2).
func (sm *SyncManager) extendFromTxMap(ctx context.Context, tx *bt.Tx, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	defer func() {
		prometheusLegacyNetsyncBlockTxSize.Observe(float64(tx.Size()))
		prometheusLegacyNetsyncBlockTxNrInputs.Observe(float64(len(tx.Inputs)))
		prometheusLegacyNetsyncBlockTxNrOutputs.Observe(float64(len(tx.Outputs)))
		// NOTE: prometheusLegacyNetsyncBlockTxExtend is intentionally NOT observed here.
		// This function is phase 1 only (same-block parents from txMap); phase 2 (bulk
		// DB decoration via BatchPreviousOutputsDecorate) runs block-wide in
		// extendTransactions. Observing a tx-level duration here would under-report
		// end-to-end extend cost versus the old per-tx DB path. We could revisit by
		// adding a block-level phase-2 histogram if dashboards need it.
	}()

	txWrapper, found := txMap.Get(*tx.TxIDChainHash())
	if !found {
		return errors.NewProcessingError("tx %s not found in txMap", tx.TxIDChainHash())
	}

	// The per-input work here is trivial (bounds check + two field assignments),
	// and extendTransactions already parallelises across transactions up to
	// Legacy.OutpointBatcherSize (default 1024). Spawning another goroutine per
	// input would multiply concurrency into the tens of thousands for large
	// blocks with negligible wall-clock benefit. Process inputs synchronously.
	for i, input := range tx.Inputs {
		// Honour caller-initiated cancellation between inputs.
		if err := ctx.Err(); err != nil {
			return err
		}

		prevTxHash := *input.PreviousTxIDChainHash()

		prevTxWrapper, found := txMap.Get(prevTxHash)
		if !found {
			// Parent lives outside this block — phase 2 will decorate it via the batch DB call.
			continue
		}

		// Flag the child tx as having at least one in-block parent (used by
		// downstream bookkeeping). Safe to set repeatedly from this single
		// goroutine.
		txWrapper.SomeParentsInBlock = true

		// A malformed/hostile block could carry a wrapper without a parsed
		// parent transaction; fail with a TxInvalidError instead of panicking
		// on the dereferences below.
		if prevTxWrapper.Tx == nil {
			return errors.NewTxInvalidError("tx %s input %d references missing previous transaction %s",
				tx.TxIDChainHash(), i, prevTxHash)
		}

		if input.PreviousTxOutIndex >= uint32(len(prevTxWrapper.Tx.Outputs)) {
			return errors.NewTxInvalidError("tx %s input %d references out-of-range output %d on parent %s (has %d outputs)",
				tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash, len(prevTxWrapper.Tx.Outputs))
		}

		prevOutput := prevTxWrapper.Tx.Outputs[input.PreviousTxOutIndex]
		if prevOutput == nil || prevOutput.LockingScript == nil {
			return errors.NewTxInvalidError("tx %s input %d previous output %d is nil or has nil locking script (parent %s)",
				tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash)
		}

		// Parent's Outputs are populated at wire-parse time and never mutated
		// afterwards, so we can read them immediately without waiting for the
		// parent tx itself to finish being extended. The old implementation
		// polled on prevTxWrapper.Tx.IsExtended(); that was unnecessary
		// (IsExtended checks the parent's *inputs*, not its outputs) and caused
		// a deadlock under the two-phase flow in extendTransactions, where a
		// pure-non-local-parent tx only becomes "extended" after phase 2 runs.
		tx.Inputs[i].PreviousTxSatoshis = prevOutput.Satoshis
		tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevOutput.LockingScript)
	}

	return nil
}

// extendPerTxFallback runs the original per-tx decoration path. It is invoked only
// when BatchPreviousOutputsDecorate fails with a missing-parent / processing error;
// the per-tx path additionally tries `utxoStore.Get(txHash, fields.Tx)` to recover
// from DAH'd parents that the child itself has already been processed with.
func (sm *SyncManager) extendPerTxFallback(ctx context.Context, txs []*bt.Tx) error {
	for _, tx := range txs {
		if err := sm.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
			if errors.Is(err, errors.ErrProcessing) || errors.Is(err, errors.ErrTxNotFound) {
				txMeta, metaErr := sm.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
				if metaErr == nil && txMeta != nil && txMeta.Tx != nil {
					if len(txMeta.Tx.Inputs) != len(tx.Inputs) {
						return errors.NewProcessingError("recovered tx %s has %d inputs but expected %d",
							tx.TxIDChainHash(), len(txMeta.Tx.Inputs), len(tx.Inputs))
					}
					for i, input := range txMeta.Tx.Inputs {
						tx.Inputs[i].PreviousTxSatoshis = input.PreviousTxSatoshis
						tx.Inputs[i].PreviousTxScript = input.PreviousTxScript
					}
					continue
				}
			}
			return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
		}
	}
	return nil
}

// createSubtrees fills the supplied subtree slices in order with the block's
// non-coinbase transactions, advancing to the next slice whenever the current
// one is complete. Subtree 0's first node is the coinbase placeholder (added
// by prepareSubtrees) so the per-slice fill count is subtreeSize-1 leaves for
// subtree 0 and subtreeSize for subsequent subtrees (subject to the final
// subtree's smaller capacity).
func (sm *SyncManager) createSubtrees(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	subtreeSlices []*subtreepkg.Subtree, subtreeDatas []*subtreepkg.Data, subtreeMetas []*subtreepkg.Meta, quickValidationMode bool) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createSubtrees",
		tracing.WithLogMessage(sm.logger, "[createSubtrees] called for block %s / height %d", block.Hash(), block.Height()),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in createSubtrees: %v", r, err)
		}

		deferFn(err)
	}()

	currentSubtreeIdx := 0

	// In quickValidationMode the transactions are not extended (scripts are never
	// re-validated below the checkpoint), so the fee cannot be derived from the tx
	// alone. Resolve every tx's fee up front from parent-output satoshis (txMap →
	// satoshi cache → batched store read). Correct fees are consensus-load-bearing:
	// model.Block.checkBlockRewardAndFees sums subtree.Fees against the coinbase.
	var quickFees map[chainhash.Hash]uint64
	if quickValidationMode {
		if quickFees, err = sm.resolveQuickFees(ctx, txMap); err != nil {
			return errors.NewProcessingError("[createSubtrees] failed to resolve quick-validation fees for block %s", block.Hash(), err)
		}
	}

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// the coinbase transaction is not part of the txMap
		txWrapper, found := txMap.Get(txHash)
		if !found {
			continue
		}

		tx := txWrapper.Tx

		// Advance to the next subtree slot if the current one is full.
		for currentSubtreeIdx < len(subtreeSlices) && subtreeSlices[currentSubtreeIdx].IsComplete() {
			currentSubtreeIdx++
		}

		if currentSubtreeIdx >= len(subtreeSlices) {
			return errors.NewSubtreeError("[createSubtrees] no subtree slot remaining for tx %s", txHash.String())
		}

		subtree := subtreeSlices[currentSubtreeIdx]
		subtreeData := subtreeDatas[currentSubtreeIdx]
		subtreeMeta := subtreeMetas[currentSubtreeIdx]

		txSize, err := safeconversion.IntToUint64(tx.Size())
		if err != nil {
			return err
		}

		// In quickValidationMode the tx is not extended (scripts are never fetched),
		// so the fee comes from the satoshi-only resolution computed above. Above the
		// checkpoint the tx is extended and the fee is computed directly.
		var fee uint64
		if quickValidationMode {
			fee = quickFees[txHash]
		} else {
			fee, err = calculateTransactionFee(tx)
			if err != nil {
				return err
			}
		}

		if err = subtree.AddNode(txHash, fee, txSize); err != nil {
			return errors.NewTxError("failed to add node (%s) to subtree", txHash, err)
		}

		nodeIdx := subtree.Length() - 1

		if err = subtreeData.AddTx(tx, nodeIdx); err != nil {
			return errors.NewTxError("failed to add tx to subtree data", err)
		}

		if err = subtreeMeta.SetTxInpointsFromTx(tx); err != nil {
			return errors.NewTxError("failed to add tx to subtree meta data", err)
		}
	}

	sm.logger.Infof("[createSubtrees] created %d subtrees for block %s / height %d", len(subtreeSlices), block.Hash(), block.Height())

	return nil
}

// parentOutpoint identifies a specific previous output (parent tx hash + output
// index) for resolving its satoshis during quick-validation fee computation.
type parentOutpoint struct {
	hash chainhash.Hash
	idx  uint32
}

// resolveQuickFees computes the correct fee for every non-coinbase transaction in
// the block using parent-output satoshis ONLY (no locking scripts), for
// quickValidationMode where the chain is checkpoint-trusted and scripts are never
// re-validated. It returns a map of tx hash → fee.
//
// Satoshi sources, per parent outpoint, in order:
//  1. same-block parent — read directly from the in-memory txMap (output satoshis
//     are populated at wire-parse time, so no wait and no store read);
//  2. recent cross-block parent — the off-heap satoshi cache (populated by
//     createUtxos as earlier blocks' outputs were created);
//  3. cold remainder — a single batched store read (satoshis harvested from a
//     throwaway shell tx so the real block transactions are NEVER mutated and
//     subtree data stays non-extended).
//
// This is the "done right" replacement for the previous fee-0 shortcut: recording
// 0 fees made model.Block.checkBlockRewardAndFees reject any block whose miner
// claimed fees, so quick-validation IBD died at the first fee-bearing block.
func (sm *SyncManager) resolveQuickFees(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) (map[chainhash.Hash]uint64, error) {
	dst := make([]byte, 0, 16)
	resolved := make(map[parentOutpoint]uint64)
	var cold []parentOutpoint

	// Pass 1: resolve every referenced parent outpoint from the txMap or the cache;
	// collect the cold remainder for a single batched store read. Deduped by outpoint.
	for _, txHash := range txMap.Keys() {
		txWrapper, ok := txMap.Get(txHash)
		if !ok || txWrapper.Tx == nil {
			continue
		}

		for _, input := range txWrapper.Tx.Inputs {
			op := parentOutpoint{hash: *input.PreviousTxIDChainHash(), idx: input.PreviousTxOutIndex}
			if _, done := resolved[op]; done {
				continue
			}

			if parent, inBlock := txMap.Get(op.hash); inBlock && parent.Tx != nil {
				if int(op.idx) >= len(parent.Tx.Outputs) || parent.Tx.Outputs[op.idx] == nil {
					return nil, errors.NewTxInvalidError("tx %s references out-of-range output %d on same-block parent %s", txHash, op.idx, op.hash)
				}

				resolved[op] = parent.Tx.Outputs[op.idx].Satoshis
				continue
			}

			if sats, hit := sm.satoshiCache.satoshis(&op.hash, op.idx, &dst); hit {
				resolved[op] = sats
				continue
			}

			resolved[op] = 0 // placeholder; filled by the cold read below
			cold = append(cold, op)
		}
	}

	// Batched satoshi-only store read for the cold remainder. A throwaway shell tx
	// carries one input per cold outpoint; PreviousOutputsDecorate fills its inputs
	// in place, leaving the real block transactions untouched.
	if len(cold) > 0 {
		shell := &bt.Tx{Inputs: make([]*bt.Input, len(cold))}
		for i, op := range cold {
			in := &bt.Input{PreviousTxOutIndex: op.idx}
			_ = in.PreviousTxIDAdd(&op.hash)
			shell.Inputs[i] = in
		}

		if err := sm.utxoStore.PreviousOutputsDecorate(ctx, shell); err != nil {
			return nil, errors.NewProcessingError("[resolveQuickFees] failed to fetch %d cold parent outputs", len(cold), err)
		}

		for i, op := range cold {
			resolved[op] = shell.Inputs[i].PreviousTxSatoshis
		}
	}

	// Pass 2: sum each transaction's fee from its resolved parent satoshis.
	fees := make(map[chainhash.Hash]uint64, txMap.Length())

	for _, txHash := range txMap.Keys() {
		txWrapper, ok := txMap.Get(txHash)
		if !ok || txWrapper.Tx == nil {
			continue
		}

		tx := txWrapper.Tx
		if tx.IsCoinbase() {
			continue
		}

		var inputValue uint64
		for _, input := range tx.Inputs {
			inputValue += resolved[parentOutpoint{hash: *input.PreviousTxIDChainHash(), idx: input.PreviousTxOutIndex}]
		}

		var outputValue uint64
		for _, output := range tx.Outputs {
			outputValue += output.Satoshis
		}

		if inputValue < outputValue {
			return nil, errors.NewTxError("transaction %s has invalid fees: input %d < output %d", txHash, inputValue, outputValue)
		}

		fees[txHash] = inputValue - outputValue
	}

	// Per-block satoshi-cache effectiveness (catch-up visibility). Only cross-block
	// parent lookups touch the cache — same-block parents are served from txMap and
	// never counted here; misses fall to the batched cold store read (len(cold)).
	if hits, misses := sm.satoshiCache.stats(); hits+misses > 0 {
		sm.logger.Infof("[resolveQuickFees] satoshi cache: %d hits, %d misses (%.1f%% hit), %d cold store reads",
			hits, misses, float64(hits)*100/float64(hits+misses), len(cold))
	}

	return fees, nil
}

func calculateTransactionFee(tx *bt.Tx) (uint64, error) {
	// Calculate the fees of this transaction
	// we do this with a signed int, to prevent overflow in case of invalid fees
	inputValue := uint64(0)
	outputValue := uint64(0)

	if tx == nil {
		return 0, errors.NewTxError("transaction is nil")
	}

	// can only calculate fees for extended transactions
	if !tx.IsExtended() { // block height not used
		return 0, errors.NewTxError("transaction %s is not extended", tx.TxIDChainHash())
	}

	// We don't need to check for coinbase transactions, as they have no inputs
	if !tx.IsCoinbase() {
		// Calculate the fees of this transaction
		// We don't need to check for coinbase transactions, as they have no inputs
		for _, input := range tx.Inputs {
			inputValue += input.PreviousTxSatoshis
		}

		for _, output := range tx.Outputs {
			outputValue += output.Satoshis
		}

		if inputValue < outputValue {
			return 0, errors.NewTxError("transaction %s has invalid fees: %d (input: %d, output: %d)", tx.TxIDChainHash(), inputValue-outputValue, inputValue, outputValue)
		}
	}

	return inputValue - outputValue, nil
}

func (sm *SyncManager) createTxMap(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createTxMap",
		tracing.WithDebugLogMessage(
			sm.logger,
			"[createTxMap][%s %d] processing transactions into map for block",
			block.Hash().String(),
			block.Height(),
		),
	)
	defer deferFn()

	for _, wireTx := range block.Transactions() {
		tx := &bt.Tx{}

		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			return errors.NewProcessingError("failed to convert wire.Tx to bt.Tx", err)
		}

		// don't add the coinbase to the txMap, we cannot process it anyway
		if !tx.IsCoinbase() {
			// Copy the hash value out of the bsvutil.Tx wrapper. bt.Tx.SetTxHash
			// stores the pointer, so passing wireTx.Hash() directly would keep
			// the wrapping wire.MsgTx (and its decode arena) alive through this
			// bt.Tx and the TxMapWrapper it lands in.
			hashCopy := *wireTx.Hash()
			tx.SetTxHash(&hashCopy)
			txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
		}
	}

	return nil
}

// prepareTxsPerLevel prepares the transactions per level for processing
// levels are determined by the number of parents in the block
func (sm *SyncManager) prepareTxsPerLevel(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) (uint32, [][]*bt.Tx) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "prepareTxsPerLevel")
	defer deferFn()

	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()
		if txWrapper, found := txMap.Get(txHash); found {
			if txWrapper.SomeParentsInBlock {
				for _, input := range txWrapper.Tx.Inputs {
					parentTxHash := *input.PreviousTxIDChainHash()
					if parentTxWrapper, found := txMap.Get(parentTxHash); found {
						// if the parent from this input is at the same level or higher,
						// we need to increase the child level of this transaction
						if parentTxWrapper.ChildLevelInBlock >= txWrapper.ChildLevelInBlock {
							txWrapper.ChildLevelInBlock = parentTxWrapper.ChildLevelInBlock + 1
						}

						if txWrapper.ChildLevelInBlock > maxLevel {
							maxLevel = txWrapper.ChildLevelInBlock
						}
					}
				}
			}

			sizePerLevel[txWrapper.ChildLevelInBlock] += 1
		}
	}

	blockTxsPerLevel := make([][]*bt.Tx, maxLevel+1)

	// pre-allocation of the blockTxsPerLevel map
	for i := uint32(0); i <= maxLevel; i++ {
		blockTxsPerLevel[i] = make([]*bt.Tx, 0, sizePerLevel[i])
	}

	// put all transactions in a map per level for processing
	for _, txWrapper := range txMap.Range() {
		blockTxsPerLevel[txWrapper.ChildLevelInBlock] = append(blockTxsPerLevel[txWrapper.ChildLevelInBlock], txWrapper.Tx)
	}

	return maxLevel, blockTxsPerLevel
}

func (sm *SyncManager) ExtendTransaction(ctx context.Context, tx *bt.Tx, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	timeStart := time.Now()
	defer func() {
		prometheusLegacyNetsyncBlockTxSize.Observe(float64(tx.Size()))
		prometheusLegacyNetsyncBlockTxNrInputs.Observe(float64(len(tx.Inputs)))
		prometheusLegacyNetsyncBlockTxNrOutputs.Observe(float64(len(tx.Outputs)))
		prometheusLegacyNetsyncBlockTxExtend.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
	}()

	txWrapper, found := txMap.Get(*tx.TxIDChainHash())
	if !found {
		return errors.NewProcessingError("tx %s not found in txMap", tx.TxIDChainHash())
	}

	inputLen := len(tx.Inputs)
	populatedInputs := atomic.Int32{}

	g := errgroup.Group{}
	// Limit goroutines to number of CPU cores to prevent scheduler thrashing
	// This prevents spawning thousands of goroutines for transactions with many inputs
	util.SafeSetLimit(&g, runtime.NumCPU()*2)

	for i, input := range tx.Inputs {
		i := i         // capture the loop variable
		input := input // capture the input variable
		prevTxHash := *input.PreviousTxIDChainHash()

		if prevTxWrapper, found := txMap.Get(prevTxHash); found {
			g.Go(func() error {
				txWrapper.SomeParentsInBlock = true

				// A malformed/hostile block could carry a wrapper without a parsed
				// parent transaction; fail fast instead of panicking on the
				// dereferences below.
				if prevTxWrapper.Tx == nil {
					return errors.NewTxInvalidError("tx %s input %d references missing previous transaction %s",
						tx.TxIDChainHash(), i, prevTxHash)
				}

				// Parent Outputs are populated at wire-parse time and never mutated afterwards,
				// so the bounds check is safe to run before WaitForParent and lets us reject
				// malformed peer blocks without burning up to 120s on the polling loop.
				if input.PreviousTxOutIndex >= uint32(len(prevTxWrapper.Tx.Outputs)) {
					return errors.NewTxInvalidError("tx %s input %d references out-of-range output %d on parent %s (has %d outputs)",
						tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash, len(prevTxWrapper.Tx.Outputs))
				}

				prevOutput := prevTxWrapper.Tx.Outputs[input.PreviousTxOutIndex]
				if prevOutput == nil || prevOutput.LockingScript == nil {
					return errors.NewTxInvalidError("tx %s input %d previous output %d is nil or has nil locking script (parent %s)",
						tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash)
				}

				// we do have a parent, but since everything is happening in parallel, we need to check if the parent has
				// already been extended
				timeOut := time.After(120 * time.Second)

			WaitForParent:
				for {
					select {
					case <-timeOut:
						return errors.NewProcessingError("timed out waiting for parent transaction %s to be extended", prevTxHash.String())
					default:
						if prevTxWrapper.Tx.IsExtended() {
							break WaitForParent
						}

						time.Sleep(10 * time.Millisecond) // wait for the parent transaction to be extended
					}
				}

				// No lock needed - each goroutine writes to a unique index
				tx.Inputs[i].PreviousTxSatoshis = prevOutput.Satoshis
				tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevOutput.LockingScript)

				populatedInputs.Add(1)

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to extend transaction %s", tx.TxIDChainHash(), err)
	}

	if int(populatedInputs.Load()) == inputLen {
		// all inputs were populated, we can return early
		return nil
	}

	if err := sm.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrProcessing) || errors.Is(err, errors.ErrTxNotFound) {
			// we could not decorate the transaction. This could be because the parent transaction has been DAH'd, which
			// can only happen if this transaction has been processed. In that case, we can try getting the transaction
			// itself.
			txMeta, err := sm.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
			if err == nil && txMeta != nil {
				if txMeta.Tx != nil {
					for i, input := range txMeta.Tx.Inputs {
						tx.Inputs[i].PreviousTxSatoshis = input.PreviousTxSatoshis
						tx.Inputs[i].PreviousTxScript = input.PreviousTxScript
					}

					return nil
				}
			}
		}

		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
	}

	return nil
}

// WireTxToGoBtTx converts a wire.Tx to a bt.Tx.
//
// Script bytes are *copied* (not aliased) so the resulting bt.Tx is fully
// independent of the source wire.MsgBlock's decode arena. This is the
// load-bearing fix for legacy-sync GC pressure: aliasing kept the arena's
// 4 MiB chunks reachable for the entire downstream pipeline lifetime
// (subtree prep, validation, persistence, the orphan goroutine below), so
// arenas piled up across all in-flight blocks and dominated the live heap.
// Copying lets the arena and its containing MsgBlock be reclaimed as soon
// as this function (and any other consumers in HandleBlockDirect) returns.
func WireTxToGoBtTx(wireTx *bsvutil.Tx, tx *bt.Tx) error {
	wTx := wireTx.MsgTx()

	tx.Version = uint32(wTx.Version) //nolint:gosec
	tx.LockTime = wTx.LockTime

	tx.Inputs = make([]*bt.Input, len(wTx.TxIn))
	for i, in := range wTx.TxIn {
		tx.Inputs[i] = &bt.Input{
			UnlockingScript:    &bscript.Script{},
			PreviousTxOutIndex: in.PreviousOutPoint.Index,
			SequenceNumber:     in.Sequence,
		}
		_ = tx.Inputs[i].PreviousTxIDAdd(&in.PreviousOutPoint.Hash)
		*tx.Inputs[i].UnlockingScript = bytes.Clone(in.SignatureScript)
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))
	for i, out := range wTx.TxOut {
		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value),
			LockingScript: &bscript.Script{},
		}
		*tx.Outputs[i].LockingScript = bytes.Clone(out.PkScript)
	}

	return nil
}
