package validator

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
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

	// TIMING: Track each phase duration
	phaseStart := time.Now()

	// PHASE 0: Pre-fetch Parent Transactions (Level 0 Only)
	// ======================================================
	// Pre-fetch ALL unique parent transactions for the entire level in a single BatchDecorate call
	// This replaces ~25 individual BatchDecorate calls (via getBatcher) with ONE upfront query
	// Saves ~3.7 seconds per level by reducing Aerospike roundtrips from 25 to 1
	if opts != nil {
		parentMap, err := v.prefetchParentsForLevel(ctx, txs, opts)
		if err != nil {
			span.RecordError(err)
			return nil, errors.NewProcessingError("[ValidateLevelBatch] failed to prefetch parents", err)
		}
		// Store in opts for workers to use (avoids individual Get() calls)
		opts.PrefetchedParents = parentMap
	}
	v.logger.Infof("[ValidateLevelBatch] PHASE 0 (prefetch) completed in %v", time.Since(phaseStart))

	// PHASE 1: Validation Checks (parallel, uses ParentBlockHeights + batchers)
	phaseStart = time.Now()
	// =======================================================================
	// Transactions already extended by extendTxWithInBlockParents for level 1+
	// Level 0 uses getTransactionInputBlockHeightsAndExtendTx which leverages batchers
	// ParentBlockHeights prevents UTXO fetches for in-block parents (Validator.go:725-740)

	// Calculate optimal worker count based on CPU cores and transaction count
	// Default: 2x CPU cores for CPU-bound script validation
	numWorkers := getOptimalWorkerCount(len(txs), opts.WorkerPoolSize, opts)

	// Create worker pool with parent context for proper cancellation/tracing
	pool := newValidationWorkerPool(ctx, v, numWorkers, len(txs), blockHeight, blockState, opts)
	pool.Start()

	// Submit all transactions as jobs to the worker pool
	for i, tx := range txs {
		pool.Submit(validationJob{
			txIndex: i,
			tx:      tx,
		})
	}

	// Wait for all validations to complete
	pool.Close()

	// Get results from the worker pool
	validationResults := pool.results

	// Check for validation failures
	for i, valResult := range validationResults {
		if valResult.err != nil {
			results[i].Err = valResult.err
		}
	}

	// REJECTED TX KAFKA NOTIFICATIONS
	// ================================
	// Publish invalid transactions to rejected tx Kafka topic for monitoring
	// (matches behavior from Validator.go:311-354)

	// Check if we should publish rejected txs (skip during sync/catchup)
	shouldPublishRejectedTxs := false
	if v.rejectedTxKafkaProducerClient != nil && v.blockchainClient != nil {
		state, err := v.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			v.logger.Debugf("[ValidateLevelBatch] failed to get FSM state for rejected tx notifications: %v", err)
		} else if *state != blockchain_api.FSMStateType_CATCHINGBLOCKS && *state != blockchain_api.FSMStateType_LEGACYSYNCING {
			shouldPublishRejectedTxs = true
		}
	}

	// Collect invalid transactions for rejection notifications
	type rejectedTx struct {
		txHash string
		reason string
	}
	rejectedTxs := make([]rejectedTx, 0)

	if shouldPublishRejectedTxs {
		for i, valResult := range validationResults {
			if valResult.err != nil && errors.Is(valResult.err, errors.ErrTxInvalid) {
				rejectedTxs = append(rejectedTxs, rejectedTx{
					txHash: txs[i].TxIDChainHash().String(),
					reason: valResult.err.Error(),
				})
			}
		}
	}

	// Publish rejected transactions to Kafka
	if len(rejectedTxs) > 0 {
		startKafka := time.Now()

		for _, rejected := range rejectedTxs {
			m := &kafkamessage.KafkaRejectedTxTopicMessage{
				TxHash: rejected.txHash,
				Reason: rejected.reason,
				PeerId: "", // Empty peer_id indicates internal rejection
			}

			value, err := proto.Marshal(m)
			if err != nil {
				v.logger.Errorf("[ValidateLevelBatch] failed to marshal rejected tx message for %s: %v", rejected.txHash, err)
				continue
			}

			v.rejectedTxKafkaProducerClient.Publish(&kafka.Message{
				Key:   []byte(rejected.txHash),
				Value: value,
			})
		}

		prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
		v.logger.Debugf("[ValidateLevelBatch] published %d rejected txs to Kafka", len(rejectedTxs))
	}
	v.logger.Infof("[ValidateLevelBatch] PHASE 1 (validation) completed in %v", time.Since(phaseStart))

	// PHASE 2: Batch Spend Operations
	phaseStart = time.Now()
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
		spendResults, spendErr = v.utxoStore.SpendBatchDirect(ctx, spendRequests)
		if spendErr != nil {
			span.RecordError(spendErr)
			return nil, errors.NewProcessingError("[ValidateLevelBatch] batch spend failed", spendErr)
		}
	}
	v.logger.Infof("[ValidateLevelBatch] PHASE 2 (spend) completed in %v", time.Since(phaseStart))

	// PHASE 3: Partition Results by Type
	phaseStart = time.Now()
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

	v.logger.Debugf("[ValidateLevelBatch] Partition phase: %d successful, %d conflicting, %d failed", len(successfulTxs), len(conflictingTxs), len(txs)-len(successfulTxs)-len(conflictingTxs))
	v.logger.Infof("[ValidateLevelBatch] PHASE 3 (partition) completed in %v", time.Since(phaseStart))

	// PHASE 4: Batch Create Successful Transactions
	phaseStart = time.Now()
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
			createResults, err := v.utxoStore.CreateBatchDirect(ctx, createRequests)
			if err != nil {
				span.RecordError(err)
				return nil, errors.NewProcessingError("[ValidateLevelBatch] batch create failed", err)
			}

			// Collect transactions that already exist for batch metadata fetch
			existingTxIndices := make([]int, 0)
			for i, createResult := range createResults {
				if errors.Is(createResult.Err, errors.ErrTxExists) {
					existingTxIndices = append(existingTxIndices, i)
				}
			}

			// Batch fetch metadata for existing transactions
			if len(existingTxIndices) > 0 {
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
	v.logger.Infof("[ValidateLevelBatch] PHASE 4 (create successful) completed in %v (%d txs)", time.Since(phaseStart), len(successfulTxs))

	// PHASE 5: Create Conflicting Transactions
	phaseStart = time.Now()
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
	v.logger.Infof("[ValidateLevelBatch] PHASE 5 (create conflicting) completed in %v (%d txs)", time.Since(phaseStart), len(conflictingTxs))

	// PHASE 6: Block Assembly Integration
	phaseStart = time.Now()
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

	// Unlock transactions (two-phase commit completion)
	if addToBlockAssembly {
		lockedTxHashes := make([]chainhash.Hash, 0, len(successfulTxs))
		for _, cat := range successfulTxs {
			if results[cat.resultIdx].Success {
				lockedTxHashes = append(lockedTxHashes, *cat.tx.TxIDChainHash())
			}
		}

		if len(lockedTxHashes) > 0 {
			if err := v.twoPhaseCommitTransactions(ctx, lockedTxHashes); err != nil {
				v.logger.Errorf("[ValidateLevelBatch] failed to unlock transactions: %v", err)
			}
		}
	}
	v.logger.Infof("[ValidateLevelBatch] PHASE 6 (block assembly + unlock) completed in %v", time.Since(phaseStart))

	// PHASE 7: Kafka Notifications (concurrent worker pool)
	phaseStart = time.Now()
	// ====================================================
	// Send TxMeta to Kafka for successful transactions using worker pool for parallelization
	if v.txmetaKafkaProducerClient != nil {
		// Create lightweight Kafka notification worker pool
		numKafkaWorkers := 100 // Fixed concurrency to prevent overwhelming batcher
		if numKafkaWorkers > len(successfulTxs) {
			numKafkaWorkers = len(successfulTxs)
		}

		kafkaPool := newKafkaNotificationWorkerPool(v, numKafkaWorkers, len(successfulTxs))
		kafkaPool.Start()

		// Submit all Kafka notification jobs
		for _, cat := range successfulTxs {
			if results[cat.resultIdx].Success && results[cat.resultIdx].TxMeta != nil {
				kafkaPool.Submit(kafkaNotificationJob{
					tx:     cat.tx,
					txMeta: results[cat.resultIdx].TxMeta,
				})
			}
		}

		// Wait for all Kafka notifications to complete
		kafkaPool.Close()
	}
	v.logger.Infof("[ValidateLevelBatch] PHASE 7 (kafka notifications) completed in %v", time.Since(phaseStart))

	// PHASE 8: Two-Phase Commit (unlock locked transactions)
	phaseStart = time.Now()
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
	v.logger.Infof("[ValidateLevelBatch] PHASE 8 (unlock) completed in %v", time.Since(phaseStart))

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

// prefetchParentsForLevel pre-fetches parent transaction outputs for a level in a single batch query.
// This replaces ~25 individual BatchDecorate calls (via getBatcher) with ONE upfront call,
// significantly reducing Aerospike roundtrips and improving throughput.
//
// The method:
// 1. Scans all transactions in the level to collect unique parent hashes
// 2. Filters out parents already in ParentBlockHeights (in-block from same ValidateMulti)
// 3. Filters out parents already in previousValidateMultiCache (from previous ValidateMulti)
// 4. Calls BatchDecorate ONCE with all remaining parents (fetches BlockHeights + Outputs + External)
// 5. Returns a map for O(1) lookup by workers
//
// Optimization: Fetches only Outputs (not Inputs), reducing data transfer by ~50%
// Compared to fields.Tx which fetches: Inputs, Outputs, Version, LockTime, External
// We fetch: BlockHeights, Outputs, External (only what's needed for extending transactions)
//
// Performance impact:
// - Reduces 25+ Aerospike roundtrips to 1 roundtrip
// - ~50% less data transfer vs fetching full transactions
// - Saves ~3.7 seconds per level (3.89s → ~0.2s)
// - Increases throughput from 22K to ~42K tx/sec
func (v *Validator) prefetchParentsForLevel(ctx context.Context, txs []*bt.Tx, opts *Options) (map[chainhash.Hash]*meta.Data, error) {
	// Step 1: Collect ALL unique parent hashes for the entire level
	// Pre-allocate with estimated capacity (avg 2 inputs per tx) to reduce map growth and GC
	estimatedParents := len(txs) * 2
	uniqueParents := make(map[chainhash.Hash]bool, estimatedParents)

	for _, tx := range txs {
		if tx == nil {
			continue
		}

		for _, input := range tx.Inputs {
			if input == nil {
				continue
			}

			parentHash := input.PreviousTxIDChainHash()
			if parentHash == nil {
				continue
			}

			// Skip if in ParentBlockHeights (in-block parent from same ValidateMulti call)
			if opts != nil && opts.ParentBlockHeights != nil {
				if _, found := opts.ParentBlockHeights[*parentHash]; found {
					continue // Already have this parent's block height
				}
			}

			// Skip if in previousValidateMultiCache (from previous ValidateMulti call)
			// Simple O(1) map lookup
			v.previousValidateMultiCacheMu.RLock()
			_, foundInCache := v.previousValidateMultiCache[*parentHash]
			v.previousValidateMultiCacheMu.RUnlock()

			if foundInCache {
				continue // Already have this parent cached
			}

			uniqueParents[*parentHash] = true
		}
	}

	// If no parents need fetching, return empty map
	if len(uniqueParents) == 0 {
		v.logger.Debugf("[prefetchParentsForLevel] No external parents to fetch (all in cache or ParentBlockHeights)")
		return make(map[chainhash.Hash]*meta.Data), nil
	}

	v.logger.Debugf("[prefetchParentsForLevel] Pre-fetching %d unique parent transactions for level", len(uniqueParents))

	// Step 2: Build UnresolvedMetaData items for BatchDecorate
	// Pre-allocate with exact size to avoid slice growth
	items := make([]*utxo.UnresolvedMetaData, 0, len(uniqueParents))
	for parentHash := range uniqueParents {
		parentHashCopy := parentHash
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   parentHashCopy,
			Fields: []fields.FieldName{fields.BlockHeights, fields.Outputs, fields.External},
		})
	}

	// Step 3: Call BatchDecorate with chunking for large batches
	startBatch := time.Now()

	// For large parent sets, chunk the BatchDecorate call to prevent overwhelming Aerospike
	// Use same batch size as Create/Spend operations for consistency
	maxBatchSize := 5000 // Match settings.conf utxostore_maxAerospikeBatchSize

	if len(items) <= maxBatchSize {
		// Small batch - single call
		err := v.utxoStore.BatchDecorate(ctx, items)
		if err != nil {
			return nil, errors.NewProcessingError("[prefetchParentsForLevel] failed to batch fetch parents", err)
		}
	} else {
		// Large batch - chunk and parallelize
		g, _ := errgroup.WithContext(ctx)
		// g.SetLimit(1)
		g.SetLimit(64)

		for i := 0; i < len(items); i += maxBatchSize {
			i := i
			end := i + maxBatchSize
			if end > len(items) {
				end = len(items)
			}

			chunk := items[i:end]

			g.Go(func() error {
				return v.utxoStore.BatchDecorate(ctx, chunk)
			})
		}

		if err := g.Wait(); err != nil {
			return nil, errors.NewProcessingError("[prefetchParentsForLevel] failed to batch fetch parents", err)
		}
	}

	v.logger.Debugf("[prefetchParentsForLevel] BatchDecorate completed in %v for %d parents", time.Since(startBatch), len(items))

	// Step 4: Build result map for O(1) lookup by workers
	parentMap := make(map[chainhash.Hash]*meta.Data, len(items))
	fetchedCount := 0
	errorCount := 0

	for _, item := range items {
		if item.Err == nil && item.Data != nil {
			parentMap[item.Hash] = item.Data
			fetchedCount++
		} else if item.Err != nil {
			errorCount++
			// Don't fail the entire level - let individual transactions handle missing parents
			v.logger.Debugf("[prefetchParentsForLevel] Failed to fetch parent %s: %v", item.Hash.String(), item.Err)
		}
	}

	v.logger.Debugf("[prefetchParentsForLevel] Pre-fetched %d/%d parents successfully (%d errors)",
		fetchedCount, len(uniqueParents), errorCount)

	return parentMap, nil
}
