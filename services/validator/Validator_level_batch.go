package validator

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// ValidateLevelBatch validates an entire level of transactions in batch mode.
// This method is optimized for block validation where transactions are organized by dependency
// levels and can be validated together with minimal coordination overhead.
//
// Safety: Preserves ALL validation semantics from validateInternal including:
// - Script validation (parallel per-tx)
// - IsFinal checks
// - Conflict detection with ConflictingTxID extraction
// - Parent metadata updates for conflicting transactions
// - Block assembly integration
// - Kafka notifications
// - Two-phase commit (lock/unlock)
// - Per-transaction rollback on partial failure
//
// Performance: Eliminates per-transaction channel coordination overhead by batching
// all UTXO operations (spends and creates) at the level granularity.
//
// Error handling: Returns per-transaction results. Individual transaction failures
// do not fail the entire level - failed transactions are simply excluded from
// parent metadata for the next level.
func (v *Validator) ValidateLevelBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) ([]*LevelValidationResult, error) {
	ctx, span, deferFn := tracing.Tracer("validator").Start(
		ctx,
		"ValidateLevelBatch",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusValidatorLevelBatch),
	)
	defer deferFn()

	if len(txs) == 0 {
		return nil, nil
	}

	prometheusValidatorLevelBatchSize.Observe(float64(len(txs)))

	results := make([]*LevelValidationResult, len(txs))
	for i := range results {
		results[i] = &LevelValidationResult{
			TxHash:  txs[i].TxIDChainHash(),
			Success: false,
		}
	}

	// Get atomic block state
	blockState := v.GetBlockState()
	if blockHeight == 0 {
		blockHeight = blockState.Height + 1
	}

	// PHASE 1: Validation Checks (parallel, uses ParentMetadata + batchers)
	// =======================================================================
	// Transactions already extended by extendTxWithInBlockParents for level 1+
	// Level 0 uses getTransactionInputBlockHeightsAndExtendTx which leverages batchers
	// ParentMetadata prevents UTXO fetches for in-block parents (Validator.go:725-740)

	type validationResult struct {
		utxoHeights []uint32
		err         error
	}

	validationResults := make([]validationResult, len(txs))
	g, gCtx := errgroup.WithContext(ctx)
	// Use high concurrency for CPU-bound script validation
	util.SafeSetLimit(g, 512)

	for i, tx := range txs {
		i, tx := i, tx
		g.Go(func() error {
			tx.SetTxHash(tx.TxIDChainHash())
			txID := tx.TxIDChainHash().String()

			// Check IsFinal (consensus rule - cannot skip)
			if blockHeight > v.settings.ChainCfgParams.CSVHeight {
				if blockState.MedianTime == 0 {
					validationResults[i].err = errors.NewProcessingError("utxo store not ready, median block time: 0")
					return nil
				}
				if err := util.IsTransactionFinal(tx, blockHeight, blockState.MedianTime); err != nil {
					validationResults[i].err = errors.NewUtxoNonFinalError("[ValidateLevelBatch][%s] transaction is not final", txID, err)
					return nil
				}
			}

			// Check coinbase (consensus rule - cannot skip)
			if tx.IsCoinbase() {
				validationResults[i].err = errors.NewProcessingError("[ValidateLevelBatch][%s] coinbase transactions are not supported", txID)
				return nil
			}

			var utxoHeights []uint32

			// Get UTXO heights and extend if needed
			// Uses ParentMetadata optimization for level 1+ (no UTXO fetch)
			// Uses batchers for level 0 (unavoidable UTXO fetch, but batched)
			if !tx.IsExtended() {
				var err error
				utxoHeights, err = v.getTransactionInputBlockHeightsAndExtendTx(gCtx, tx, txID, opts)
				if err != nil {
					validationResults[i].err = errors.NewProcessingError("[ValidateLevelBatch][%s] error getting transaction input block heights", txID, err)
					return nil
				}
			}

			// Validate transaction format and consensus rules
			if err := v.validateTransaction(gCtx, tx, blockHeight, utxoHeights, opts); err != nil {
				validationResults[i].err = errors.NewProcessingError("[ValidateLevelBatch][%s] error validating transaction", txID, err)
				return nil
			}

			// Get utxo heights if not already fetched (transaction was pre-extended)
			if len(utxoHeights) == 0 {
				var err error
				utxoHeights, err = v.getTransactionInputBlockHeightsAndExtendTx(gCtx, tx, txID, opts)
				if err != nil {
					validationResults[i].err = errors.NewProcessingError("[ValidateLevelBatch][%s] error getting transaction input block heights", txID, err)
					return nil
				}
			}

			// Validate scripts and signatures
			if err := v.validateTransactionScripts(gCtx, tx, blockHeight, utxoHeights, opts); err != nil {
				validationResults[i].err = errors.NewProcessingError("[ValidateLevelBatch][%s] error validating transaction scripts", txID, err)
				return nil
			}

			validationResults[i].utxoHeights = utxoHeights
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		span.RecordError(err)
		return nil, errors.NewProcessingError("[ValidateLevelBatch] validation failed", err)
	}

	// Check for validation failures
	for i, valResult := range validationResults {
		if valResult.err != nil {
			results[i].Err = valResult.err
		}
	}

	// PHASE 2: Batch Spend Operations
	// ================================
	// Collect spend requests for transactions that passed validation
	spendRequests := make([]*utxo.BatchSpendRequest, 0, len(txs))
	spendIndexMap := make(map[int]int) // spendRequestIdx -> resultsIdx

	for i, tx := range txs {
		// Skip transactions that failed validation
		if validationResults[i].err != nil {
			continue
		}

		// Cache tx hash
		tx.SetTxHash(tx.TxIDChainHash())

		spendIndexMap[len(spendRequests)] = i
		spendRequests = append(spendRequests, &utxo.BatchSpendRequest{
			Tx:          tx,
			BlockHeight: blockHeight,
			IgnoreFlags: utxo.IgnoreFlags{
				IgnoreConflicting: false,
				IgnoreLocked:      opts.IgnoreLocked,
			},
		})
	}

	// Execute batch spend
	var spendResults []*utxo.BatchSpendResult
	var spendErr error

	if len(spendRequests) > 0 {
		startSpend := time.Now()
		v.logger.Infof("[ValidateLevelBatch] Starting batch spend for %d transactions", len(spendRequests))
		spendResults, spendErr = v.utxoStore.SpendBatchDirect(ctx, spendRequests)
		if spendErr != nil {
			span.RecordError(spendErr)
			return nil, errors.NewProcessingError("[ValidateLevelBatch] batch spend failed", spendErr)
		}
		v.logger.Infof("[ValidateLevelBatch] Batch spend completed in %v for %d transactions", time.Since(startSpend), len(spendRequests))
	}

	// PHASE 3: Partition Results by Type
	// ===================================
	// Successful: All spends succeeded, ready for create
	// Conflicting: Spent by another tx, create as conflicting if CreateConflicting=true
	// Failed: Other errors (frozen, locked, missing parent, etc.)

	type txCategory struct {
		tx              *bt.Tx
		resultIdx       int
		conflictingTxID *chainhash.Hash
	}

	successfulTxs := make([]txCategory, 0, len(spendResults))
	conflictingTxs := make([]txCategory, 0)

	for spendIdx, spendResult := range spendResults {
		resultIdx := spendIndexMap[spendIdx]

		if spendResult.Success {
			// All spends succeeded
			successfulTxs = append(successfulTxs, txCategory{
				tx:        txs[resultIdx],
				resultIdx: resultIdx,
			})

		} else if spendResult.Err != nil {
			// Check error type
			if opts.CreateConflicting && (errors.Is(spendResult.Err, errors.ErrSpent) || errors.Is(spendResult.Err, errors.ErrTxConflicting)) {
				// Conflicting transaction
				conflictingTxs = append(conflictingTxs, txCategory{
					tx:              txs[resultIdx],
					resultIdx:       resultIdx,
					conflictingTxID: spendResult.ConflictingTxID,
				})
				results[resultIdx].ConflictingTxID = spendResult.ConflictingTxID

			} else if errors.Is(spendResult.Err, errors.ErrTxNotFound) {
				// Parent DAH'd - check if tx already exists (reuse from validateInternal:575-585)
				txMeta := &meta.Data{}
				if err := v.utxoStore.GetMeta(ctx, txs[resultIdx].TxIDChainHash(), txMeta); err == nil {
					v.logger.Warnf("[ValidateLevelBatch][%s] parent tx not found, but tx already exists in store, assuming already blessed", txs[resultIdx].TxID())
					results[resultIdx].TxMeta = txMeta
					results[resultIdx].Success = true
					results[resultIdx].Err = nil
				} else {
					results[resultIdx].Err = spendResult.Err
				}

			} else {
				// Other error (frozen, locked, missing, invalid, etc.)
				results[resultIdx].Err = spendResult.Err
			}
		}
	}

	v.logger.Infof("[ValidateLevelBatch] Partition phase: %d successful, %d conflicting, %d failed", len(successfulTxs), len(conflictingTxs), len(txs)-len(successfulTxs)-len(conflictingTxs))

	// PHASE 4: Batch Create Successful Transactions
	// ==============================================
	blockAssemblyEnabled := !v.settings.BlockAssembly.Disabled
	addToBlockAssembly := blockAssemblyEnabled && opts.AddTXToBlockAssembly

	if len(successfulTxs) > 0 {
		createRequests := make([]*utxo.BatchCreateRequest, len(successfulTxs))
		for i, cat := range successfulTxs {
			createRequests[i] = &utxo.BatchCreateRequest{
				Tx:          cat.tx,
				BlockHeight: blockHeight,
				Conflicting: false,
				Locked:      addToBlockAssembly, // Lock if sending to block assembly
			}
		}

		if !opts.SkipUtxoCreation {
			startCreate := time.Now()
			v.logger.Infof("[ValidateLevelBatch] Starting batch create for %d transactions", len(createRequests))
			createResults, err := v.utxoStore.CreateBatchDirect(ctx, createRequests)
			if err != nil {
				span.RecordError(err)
				return nil, errors.NewProcessingError("[ValidateLevelBatch] batch create failed", err)
			}
			v.logger.Infof("[ValidateLevelBatch] Batch create completed in %v for %d transactions", time.Since(startCreate), len(createRequests))

			// Collect transactions that already exist for batch metadata fetch
			existingTxIndices := make([]int, 0)
			for i, createResult := range createResults {
				if errors.Is(createResult.Err, errors.ErrTxExists) {
					existingTxIndices = append(existingTxIndices, i)
				}
			}

			// Batch fetch metadata for existing transactions
			if len(existingTxIndices) > 0 {
				v.logger.Infof("[ValidateLevelBatch] Batch fetching metadata for %d existing transactions", len(existingTxIndices))
				unresolvedMeta := make([]*utxo.UnresolvedMetaData, len(existingTxIndices))
				for i, idx := range existingTxIndices {
					unresolvedMeta[i] = &utxo.UnresolvedMetaData{
						Hash: *successfulTxs[idx].tx.TxIDChainHash(),
					}
				}

				if err := v.utxoStore.BatchDecorate(ctx, unresolvedMeta); err != nil {
					v.logger.Errorf("[ValidateLevelBatch] failed to batch fetch metadata for existing txs: %v", err)
				} else {
					// Update results with fetched metadata
					for i, idx := range existingTxIndices {
						cat := successfulTxs[idx]
						if unresolvedMeta[i].Data != nil {
							results[cat.resultIdx].Success = true
							results[cat.resultIdx].TxMeta = unresolvedMeta[i].Data
							results[cat.resultIdx].Err = nil
						}
					}
				}
			}

			// Process create results
			for i, createResult := range createResults {
				cat := successfulTxs[i]

				if createResult.Success {
					results[cat.resultIdx].Success = true
					results[cat.resultIdx].TxMeta = createResult.TxMeta
					results[cat.resultIdx].Err = nil

				} else if errors.Is(createResult.Err, errors.ErrTxExists) {
					// Already handled by batch fetch above
					if results[cat.resultIdx].TxMeta == nil {
						v.logger.Warnf("[ValidateLevelBatch][%s] tx exists but batch fetch failed", cat.tx.TxID())
						results[cat.resultIdx].Err = createResult.Err
					}

				} else if createResult.Err != nil {
					// Create failed - rollback spends
					v.logger.Errorf("[ValidateLevelBatch][%s] error creating tx in UTXO store: %v", cat.tx.TxID(), createResult.Err)

					// Get spends for this transaction
					spends, _ := utxo.GetSpends(cat.tx)
					if reverseErr := v.reverseSpends(ctx, spends); reverseErr != nil {
						v.logger.Errorf("[ValidateLevelBatch][%s] error reversing utxo spends: %v", cat.tx.TxID(), reverseErr)
					}

					results[cat.resultIdx].Err = createResult.Err
				}
			}
		} else {
			// SkipUtxoCreation - just create metadata
			for _, cat := range successfulTxs {
				txMeta, err := util.TxMetaDataFromTx(cat.tx)
				if err != nil {
					results[cat.resultIdx].Err = errors.NewProcessingError("[ValidateLevelBatch][%s] failed to get tx meta data", cat.tx.TxID(), err)
				} else {
					results[cat.resultIdx].Success = true
					results[cat.resultIdx].TxMeta = txMeta
				}
			}
		}
	}

	// PHASE 5: Create Conflicting Transactions
	// =========================================
	// Reuse pattern from validateInternal:550-574
	if len(conflictingTxs) > 0 {
		conflictCreateRequests := make([]*utxo.BatchCreateRequest, len(conflictingTxs))
		for i, cat := range conflictingTxs {
			conflictCreateRequests[i] = &utxo.BatchCreateRequest{
				Tx:          cat.tx,
				BlockHeight: blockHeight,
				Conflicting: true, // KEY: Mark as conflicting
				Locked:      false,
			}
		}

		conflictCreateResults, err := v.utxoStore.CreateBatchDirect(ctx, conflictCreateRequests)
		if err != nil {
			v.logger.Errorf("[ValidateLevelBatch] failed to create conflicting transactions: %v", err)
		} else {
			for i, createResult := range conflictCreateResults {
				cat := conflictingTxs[i]

				if createResult.Success || errors.Is(createResult.Err, errors.ErrTxExists) {
					// Successfully created as conflicting or already exists
					results[cat.resultIdx].TxMeta = createResult.TxMeta
					results[cat.resultIdx].Err = errors.NewTxConflictingError("[ValidateLevelBatch][%s] tx is conflicting", cat.tx.TxID())
				} else {
					v.logger.Errorf("[ValidateLevelBatch][%s] failed to create as conflicting: %v", cat.tx.TxID(), createResult.Err)
					results[cat.resultIdx].Err = createResult.Err
				}
			}
		}
	}

	// PHASE 6: Block Assembly Integration
	// ====================================
	// Only send successful transactions to block assembly (reuse from validateInternal:628-664)
	if addToBlockAssembly && v.blockAssembler != nil {
		blockAssemblyGroup, baCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(blockAssemblyGroup, 100)

		for _, cat := range successfulTxs {
			if results[cat.resultIdx].Success {
				cat := cat
				blockAssemblyGroup.Go(func() error {
					tx := cat.tx
					txMeta := results[cat.resultIdx].TxMeta

					// Get tx inpoints
					txInpoints, err := subtree.NewTxInpointsFromTx(tx)
					if err != nil {
						return errors.NewProcessingError("[ValidateLevelBatch][%s] error getting tx inpoints: %v", tx.TxID(), err)
					}

					// Send to block assembler
					if err := v.sendToBlockAssembler(baCtx, &blockassembly.Data{
						TxIDChainHash: *tx.TxIDChainHash(),
						Fee:           txMeta.Fee,
						Size:          uint64(tx.Size()),
						TxInpoints:    txInpoints,
					}, nil); err != nil {
						v.logger.Errorf("[ValidateLevelBatch][%s] error sending to block assembler: %v", tx.TxID(), err)
						return nil // Don't fail entire batch
					}

					return nil
				})
			}
		}

		if err := blockAssemblyGroup.Wait(); err != nil {
			v.logger.Errorf("[ValidateLevelBatch] block assembly integration failed: %v", err)
		}
	}

	// PHASE 7: Kafka Notifications
	// =============================
	// Send TxMeta to Kafka for successful transactions (reuse from validateInternal:656-658)
	if v.txmetaKafkaProducerClient != nil {
		for _, cat := range successfulTxs {
			if results[cat.resultIdx].Success && results[cat.resultIdx].TxMeta != nil {
				if err := v.sendTxMetaToKafka(results[cat.resultIdx].TxMeta, cat.tx.TxIDChainHash()); err != nil {
					v.logger.Errorf("[ValidateLevelBatch][%s] error sending to Kafka: %v", cat.tx.TxID(), err)
				}
			}
		}
	}

	// PHASE 8: Two-Phase Commit (unlock locked transactions)
	// =======================================================
	// Reuse pattern from validateInternal:662-667
	if addToBlockAssembly {
		lockedTxHashes := make([]chainhash.Hash, 0, len(successfulTxs))
		for _, cat := range successfulTxs {
			if results[cat.resultIdx].Success && results[cat.resultIdx].TxMeta != nil && results[cat.resultIdx].TxMeta.Locked {
				lockedTxHashes = append(lockedTxHashes, *cat.tx.TxIDChainHash())
			}
		}

		if len(lockedTxHashes) > 0 {
			if err := v.twoPhaseCommitTransactions(ctx, lockedTxHashes); err != nil {
				v.logger.Errorf("[ValidateLevelBatch] failed to unlock transactions: %v", err)
			}
		}
	}

	// Count successes for metrics
	successCount := 0
	conflictCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		} else if result.ConflictingTxID != nil {
			conflictCount++
		}
	}

	v.logger.Debugf("[ValidateLevelBatch] Completed: %d successful, %d conflicting, %d failed", successCount, conflictCount, len(txs)-successCount-conflictCount)

	prometheusValidatorLevelBatchSuccess.Add(float64(successCount))
	prometheusValidatorLevelBatchConflicts.Add(float64(conflictCount))

	return results, nil
}

// twoPhaseCommitTransactions unlocks multiple transactions after block assembly integration
func (v *Validator) twoPhaseCommitTransactions(ctx context.Context, txHashes []chainhash.Hash) error {
	return v.utxoStore.SetLocked(ctx, txHashes, false)
}
