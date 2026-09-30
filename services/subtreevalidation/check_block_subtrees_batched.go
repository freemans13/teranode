package subtreevalidation

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// parentOutputsChunkSize is how many outpoints the batch path asks the store for
// in one ParentOutputsForValidation call.
const parentOutputsChunkSize = 8192

// unconfirmedParentHeight mirrors the validator's sentinel for a parent the store
// has no block recorded for. The validator substitutes the candidate height for
// it on the block path (Options.UnconfirmedParentsAtCandidateHeight).
const unconfirmedParentHeight uint32 = 0xFFFFFFFF

// batchChecker returns the local validator's batch checker when CheckBlockSubtrees
// should take the batch path: the node is catching up (CATCHINGBLOCKS), the block
// is above the highest checkpoint, and the validator runs in this process. Every
// other case keeps processTransactionsInLevels. In RUNNING the level path also
// hands transactions to block assembly, which the batch path does not do.
func (u *Server) batchChecker(state blockchain.FSMStateType, blockHeight uint32) (validator.BlockBatchChecker, bool) {
	if state != blockchain.FSMStateCATCHINGBLOCKS {
		return nil, false
	}

	if blockHeight <= blockchain.HighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints) {
		return nil, false
	}

	checker, ok := u.validatorClient.(validator.BlockBatchChecker)

	return checker, ok
}

// checkBlockBodyBound proves, before anything is written, that the block's
// subtree list is the one its header commits to: it loads the first and last
// subtrees' node lists and checks the merkle root with the coinbase substituted.
// Every other subtree is bound to its key when it is loaded. The node lists are
// stored as FileTypeSubtreeToCheck, so the batch load reads them locally.
func (u *Server) checkBlockBodyBound(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest, block *model.Block, peerID string, dah uint32) error {
	if len(block.Subtrees) == 0 {
		return nil
	}

	first, err := u.getSubtreeToCheck(ctx, request, *block.Subtrees[0], peerID, dah)
	if err != nil {
		return err
	}

	last := first
	if len(block.Subtrees) > 1 {
		if last, err = u.getSubtreeToCheck(ctx, request, *block.Subtrees[len(block.Subtrees)-1], peerID, dah); err != nil {
			return err
		}
	}

	return block.CheckBodyBoundToHeader(first, last)
}

// processTransactionsBatched validates one load batch of a block during catch-up
// above the checkpoint. It replaces processTransactionsInLevels there; the caller
// no longer works in dependency levels, the store does.
//
//  1. Resolve: clear every input's previous-output fields, then fill each one
//     from a same-batch parent held in memory (keyed by the txid this node
//     computed while reading the subtree data) or from the store's
//     ParentOutputsForValidation. A spend of an outpoint twice, or of a
//     transaction later in the batch, fails the block.
//  2. Check every transaction's scripts, fees and consensus rules on all cores.
//     With every input resolved no transaction waits on another.
//  3. Write the checked transactions, in block order, with SpendAndCreateMulti.
//  4. Send every transaction that could not take this path, and its
//     descendants, through processTransactionsInLevels, which handles missing
//     parents, conflicts and already-mined records exactly as today.
func (u *Server) processTransactionsBatched(ctx context.Context, checker validator.BlockBatchChecker, allTransactions []*bt.Tx,
	blockHash chainhash.Hash, blockHeight uint32, candidateBlockTime uint32, candidateParentMedianTime uint32, blockIds map[uint32]bool) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsBatched",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processTransactionsBatched] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	txHashes := make([]chainhash.Hash, len(allTransactions))

	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsBatched] transaction is nil at index %d", i)
		}

		txHashes[i] = *tx.TxIDChainHash()
	}

	// Pre-check, as the level path does: drop what is already validated.
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsBatched] Failed to check txMeta cache", err)
	}

	if missed > 0 {
		missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, u.settings.SubtreeValidation.BatchMissingTransactions, false, true)
		if err != nil {
			return errors.NewProcessingError("[processTransactionsBatched] Failed to check txMeta store", err)
		}
	}

	if missed == 0 {
		return nil
	}

	b := &batchState{
		txs:      allTransactions,
		hashes:   txHashes,
		position: make(map[chainhash.Hash]int, len(allTransactions)),
		heights:  make([][]uint32, len(allTransactions)),
		parents:  make([][]int, len(allTransactions)),
		fallback: make([]bool, len(allTransactions)),
	}

	for i, tx := range allTransactions {
		if tx.IsCoinbase() {
			continue
		}

		b.position[txHashes[i]] = i

		if !txMetaSlice[i].isSet {
			b.missing = append(b.missing, i)
		}
	}

	start := time.Now()

	if err = u.resolveBatch(ctx, b, blockHeight); err != nil {
		return err
	}

	prometheusSubtreeValidationBatchStep.WithLabelValues("resolve").Observe(time.Since(start).Seconds())

	validatorOptions := validator.ProcessOptions(
		validator.WithSkipPolicyChecks(true),
		validator.WithInBlock(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
		validator.WithCandidateBlockTime(candidateBlockTime),
		validator.WithCandidateParentMedianTime(candidateParentMedianTime),
		validator.WithUnconfirmedParentsAtCandidateHeight(true),
		validator.WithAddTXToBlockAssembly(false),
	)

	if err = u.validatorClient.EnsureMTPLoaded(ctx, blockHeight); err != nil {
		return errors.NewProcessingError("[processTransactionsBatched] failed to pre-load MTP store: %v", err)
	}

	start = time.Now()

	if err = u.checkBatch(ctx, checker, b, blockHeight, validatorOptions); err != nil {
		return err
	}

	prometheusSubtreeValidationBatchStep.WithLabelValues("check").Observe(time.Since(start).Seconds())

	start = time.Now()

	if err = u.writeBatch(ctx, checker, b, blockHeight); err != nil {
		return err
	}

	prometheusSubtreeValidationBatchStep.WithLabelValues("write").Observe(time.Since(start).Seconds())

	// Everything that did not take the batch path goes through today's path, in
	// block order. It re-reads parents from the store, so the inputs this path
	// resolved are overwritten, never trusted.
	var fallbackTxs []*bt.Tx

	for _, i := range b.missing {
		if b.fallback[i] {
			fallbackTxs = append(fallbackTxs, allTransactions[i])
		}
	}

	prometheusSubtreeValidationBatchTxs.WithLabelValues("fallback").Add(float64(len(fallbackTxs)))

	if len(fallbackTxs) == 0 {
		return nil
	}

	u.logger.Debugf("[processTransactionsBatched] %d of %d transactions go through the per-transaction path", len(fallbackTxs), len(b.missing))

	start = time.Now()
	defer func() {
		prometheusSubtreeValidationBatchStep.WithLabelValues("fallback").Observe(time.Since(start).Seconds())
	}()

	return u.processTransactionsInLevels(ctx, fallbackTxs, blockHash, chainhash.Hash{}, blockHeight, candidateBlockTime, candidateParentMedianTime, blockIds, false)
}

// batchState is one load batch on the batch path. Indices are positions in txs.
type batchState struct {
	txs      []*bt.Tx
	hashes   []chainhash.Hash
	position map[chainhash.Hash]int // non-coinbase transactions of the batch, by locally computed txid
	missing  []int                  // transactions to validate, in block order
	heights  [][]uint32             // per missing transaction, each input's parent height
	parents  [][]int                // per missing transaction, its parents' positions in the batch
	fallback []bool                 // sent through the per-transaction path instead
}

// markDescendantsFallback sends every missing transaction with a parent in the
// batch that falls back through the per-transaction path as well. Parents come
// before children, so one pass in block order suffices.
func (b *batchState) markDescendantsFallback() {
	for _, i := range b.missing {
		if b.fallback[i] {
			continue
		}

		for _, p := range b.parents[i] {
			if b.fallback[p] {
				b.fallback[i] = true
				break
			}
		}
	}
}

type storeInputRef struct {
	tx, input int
}

// resolveBatch extends every input of every missing transaction (step 1).
func (u *Server) resolveBatch(ctx context.Context, b *batchState, blockHeight uint32) error {
	spent := make(map[utxo.Outpoint]int)

	var (
		refs      []storeInputRef
		outpoints []utxo.Outpoint
		fromMem   int
	)

	for _, i := range b.missing {
		tx := b.txs[i]
		b.heights[i] = make([]uint32, len(tx.Inputs))

		for k, in := range tx.Inputs {
			// Never check a transaction with previous-output fields a peer
			// supplied (GHSA-v76m-6vc7-g7c7).
			in.PreviousTxSatoshis = 0
			in.PreviousTxScript = nil

			op := utxo.Outpoint{TxID: *in.PreviousTxIDChainHash(), Vout: in.PreviousTxOutIndex}

			if first, dup := spent[op]; dup {
				return errors.NewBlockInvalidError("[processTransactionsBatched] transactions %s and %s both spend %s:%d", b.hashes[first], b.hashes[i], op.TxID, op.Vout)
			}

			spent[op] = i

			p, inBatch := b.position[op.TxID]
			if !inBatch {
				refs = append(refs, storeInputRef{tx: i, input: k})
				outpoints = append(outpoints, op)

				continue
			}

			if p >= i {
				return errors.NewBlockInvalidError("[processTransactionsBatched] transaction %s spends %s, which is not earlier in the block", b.hashes[i], op.TxID)
			}

			parent := b.txs[p]
			if int(op.Vout) >= len(parent.Outputs) || parent.Outputs[op.Vout] == nil {
				return errors.NewTxInvalidError("[processTransactionsBatched] transaction %s spends output %d of %s, which has %d outputs", b.hashes[i], op.Vout, op.TxID, len(parent.Outputs))
			}

			in.PreviousTxSatoshis = parent.Outputs[op.Vout].Satoshis
			in.PreviousTxScript = parent.Outputs[op.Vout].LockingScript
			// A same-block parent is mined at this block's height.
			b.heights[i][k] = blockHeight
			b.parents[i] = appendUniqueInt(b.parents[i], p)
			fromMem++
		}
	}

	prometheusSubtreeValidationBatchParentOutputs.WithLabelValues("memory").Add(float64(fromMem))
	prometheusSubtreeValidationBatchParentOutputs.WithLabelValues("store").Add(float64(len(outpoints)))

	// Read every other parent output from the store, in chunks.
	answers := make([]utxo.ParentOutput, len(outpoints))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency))

	for chunkStart := 0; chunkStart < len(outpoints); chunkStart += parentOutputsChunkSize {
		chunkEnd := min(chunkStart+parentOutputsChunkSize, len(outpoints))

		g.Go(func() error {
			chunkAnswers, err := u.utxoStore.ParentOutputsForValidation(gCtx, outpoints[chunkStart:chunkEnd])
			if err != nil {
				return errors.NewProcessingError("[processTransactionsBatched] failed to read parent outputs", err)
			}

			if len(chunkAnswers) != chunkEnd-chunkStart {
				return errors.NewProcessingError("[processTransactionsBatched] store returned %d parent outputs for %d outpoints", len(chunkAnswers), chunkEnd-chunkStart)
			}

			copy(answers[chunkStart:chunkEnd], chunkAnswers)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for n, answer := range answers {
		ref := refs[n]
		in := b.txs[ref.tx].Inputs[ref.input]

		switch {
		case answer.Err != nil:
			// A store fault is never a verdict on the block: retry the batch.
			return errors.NewProcessingError("[processTransactionsBatched] failed to read parent output %s:%d", outpoints[n].TxID, outpoints[n].Vout, answer.Err)
		case answer.Status == utxo.ParentOutputTxNotFound:
			// The per-transaction path reports the missing parent and defers it,
			// as today.
			b.fallback[ref.tx] = true
			continue
		case answer.Status == utxo.ParentOutputNoSuchIndex:
			return errors.NewTxInvalidError("[processTransactionsBatched] transaction %s spends output %d of %s, which does not exist", b.hashes[ref.tx], outpoints[n].Vout, outpoints[n].TxID)
		case answer.Status == utxo.ParentOutputMined:
			b.heights[ref.tx][ref.input] = answer.Height
		case answer.Status == utxo.ParentOutputNotMined:
			b.heights[ref.tx][ref.input] = unconfirmedParentHeight
		default:
			return errors.NewProcessingError("[processTransactionsBatched] store gave no answer for parent output %s:%d", outpoints[n].TxID, outpoints[n].Vout)
		}

		in.PreviousTxSatoshis = answer.Satoshis
		in.PreviousTxScript = answer.LockingScript
	}

	b.markDescendantsFallback()

	return nil
}

// checkBatch checks every resolved transaction on all cores (step 2). A
// consensus failure fails the block, as on the level path; any other failure
// sends the transaction through the per-transaction path.
func (u *Server) checkBatch(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, blockHeight uint32, opts *validator.Options) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	var sentBack atomic.Int64

	for _, i := range b.missing {
		if b.fallback[i] {
			continue
		}

		g.Go(func() error {
			err := checker.CheckExtendedTransaction(gCtx, b.txs[i], blockHeight, b.heights[i], opts)
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
				u.logger.Warnf("[processTransactionsBatched] Invalid transaction detected: %s: %v", b.hashes[i], err)
				return err
			}

			if gCtx.Err() != nil {
				return gCtx.Err()
			}

			// Each goroutine writes only its own slot.
			b.fallback[i] = true
			sentBack.Add(1)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[processTransactionsBatched] failed to check transactions", err)
	}

	if sentBack.Load() > 0 {
		b.markDescendantsFallback()
	}

	return nil
}

// writeBatch writes the checked transactions with SpendAndCreateMulti, in block
// order, in lists bounded by subtreevalidation_spendAndCreateMultiMaxTxs and
// ..MaxBytes (step 3). Created records have their txmeta published, as the
// validator publishes them; every other outcome falls back.
func (u *Server) writeBatch(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, blockHeight uint32) error {
	maxTxs := max(1, u.settings.SubtreeValidation.SpendAndCreateMultiMaxTxs)
	maxBytes := u.settings.SubtreeValidation.SpendAndCreateMultiMaxBytes

	var (
		list      []int
		listBytes int
	)

	flush := func() error {
		if len(list) == 0 {
			return nil
		}

		txs := make([]*bt.Tx, len(list))
		txids := make([]chainhash.Hash, len(list))

		for n, i := range list {
			txs[n] = b.txs[i]
			txids[n] = b.hashes[i]
		}

		prometheusSubtreeValidationBatchLists.Inc()

		results, err := u.utxoStore.SpendAndCreateMulti(ctx, txs, blockHeight, utxo.WithTXIDs(txids), utxo.WithIgnoreLocked(true))

		switch {
		case utxo.IsSpendAndCreateMultiRefused(err):
			// The caller already checked what the store refuses, so this is a bug
			// here. Nothing was written; send the list through today's path.
			u.logger.Errorf("[processTransactionsBatched] store refused a list of %d transactions, sending them through the per-transaction path: %v", len(list), err)

			for _, i := range list {
				b.fallback[i] = true
			}
		case err != nil:
			// Retrying the batch is safe: existing records are recognised one by one.
			return errors.NewProcessingError("[processTransactionsBatched] SpendAndCreateMulti failed", err)
		case len(results) != len(list):
			return errors.NewProcessingError("[processTransactionsBatched] SpendAndCreateMulti returned %d results for %d transactions", len(results), len(list))
		default:
			created := 0

			for n, r := range results {
				if r.Status == utxo.MultiTxCreated {
					created++

					checker.PublishTxMeta(r.Meta, &txids[n], true)

					continue
				}

				// Existed, failed or parent failed: today's path decides, including
				// the already-mined-on-our-chain and conflicting checks.
				b.fallback[list[n]] = true
			}

			prometheusSubtreeValidationBatchTxs.WithLabelValues("created").Add(float64(created))
		}

		list = list[:0]
		listBytes = 0

		return nil
	}

	for _, i := range b.missing {
		if b.fallback[i] {
			continue
		}

		// A parent written in an earlier list that did not end up created sends
		// its children back too; parents in the current list are the store's to
		// handle (MultiTxParentFailed).
		parentFellBack := false

		for _, p := range b.parents[i] {
			if b.fallback[p] {
				parentFellBack = true
				break
			}
		}

		if parentFellBack {
			b.fallback[i] = true
			continue
		}

		size := b.txs[i].Size()
		if len(list) > 0 && (len(list) >= maxTxs || (maxBytes > 0 && listBytes+size > maxBytes)) {
			if err := flush(); err != nil {
				return err
			}
		}

		list = append(list, i)
		listBytes += size
	}

	return flush()
}

func appendUniqueInt(s []int, v int) []int {
	for _, x := range s {
		if x == v {
			return s
		}
	}

	return append(s, v)
}
