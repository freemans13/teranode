package validator

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// ValidateMulti validates multiple transactions with automatic dependency ordering and batch processing.
// This method organizes transactions by dependency levels (DAG) and processes each level in sequence,
// enabling efficient validation of transaction sets with complex dependencies.
//
// The validation process follows these steps:
// 1. Organize transactions by dependency level (level 0 = no in-batch parents)
// 2. For each level sequentially:
//    a. Build parent metadata from successfully validated transactions in previous level
//    b. Optionally extend transactions with in-block parent outputs (if AutoExtendTransactions)
//    c. Validate entire level using ValidateLevelBatch
//    d. Track successful validations for next level's parent metadata
//    e. Release grandparent level memory (keep only 2 levels in memory)
//    f. Check for context cancellation before starting next level
// 3. Update previousLevelCache with successful transactions from this ValidateMulti call
//
// Performance optimizations:
// - Single UTXO batch operation per level (not per transaction)
// - Parent metadata optimization skips ~500MB+ Aerospike fetches
// - Transaction extension eliminates UTXO store lookups for in-block parents
// - Memory-efficient: releases grandparent levels, optional MaxBatchSize batching
//
// Safety guarantees:
// - Parent metadata only includes successfully validated transactions
// - Failed parent validation causes child validation to fail
// - Per-transaction error tracking with conflict detection
// - Maintains all validation semantics from single-transaction path
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txs: Slice of transactions to validate (can have interdependencies)
//   - blockHeight: Current block height for validation
//   - opts: Validation options (AutoExtendTransactions, MaxBatchSize, ParentBlockHeights, etc.)
//
// Returns:
//   - *MultiResult: Per-transaction results with success, metadata, conflicts, errors
//   - error: Critical errors preventing validation (not per-transaction failures)
func (v *Validator) ValidateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) (*MultiResult, error) {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "ValidateMulti")
	defer deferFn()

	if len(txs) == 0 {
		return &MultiResult{Results: make(map[chainhash.Hash]*TxValidationResult)}, nil
	}

	// Handle nil options
	if opts == nil {
		opts = NewDefaultOptions()
	}

	// Initialize ParentBlockHeights if not provided
	if opts.ParentBlockHeights == nil {
		opts.ParentBlockHeights = make(map[chainhash.Hash]uint32)
	}

	// OPTIMIZATION: Skip level organization if flag is set
	// Process all transactions as a single level (no DAG construction)
	if opts.SkipLevelOrganization {
		// Just validate everything as one batch
		levelResults, err := v.ValidateLevelBatch(ctx, txs, blockHeight, opts)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Convert to MultiResult format
		results := make(map[chainhash.Hash]*TxValidationResult, len(levelResults))
		for _, levelResult := range levelResults {
			txHash := *levelResult.TxHash
			results[txHash] = &TxValidationResult{
				Success:         levelResult.Success,
				TxMeta:          levelResult.TxMeta,
				ConflictingTxID: levelResult.ConflictingTxID,
				Err:             levelResult.Err,
			}
		}

		return &MultiResult{Results: results}, nil
	}

	// Step 1: Organize transactions by dependency level
	// Use ordered algorithm if we can assume topological ordering (typical for blocks)
	// Otherwise use general algorithm that handles any ordering
	txsPerLevel, err := organizeTxsByLevelOrdered(ctx, txs)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Initialize tracking structures
	results := make(map[chainhash.Hash]*TxValidationResult)
	successfulTxsByLevel := make(map[uint32]map[chainhash.Hash]bool) // level -> txHash -> success
	var resultsMutex sync.RWMutex

	// Determine concurrency level
	concurrentLevels := 1
	if opts != nil && opts.ConcurrentLevels > 1 {
		concurrentLevels = opts.ConcurrentLevels
	}

	// Determine chunk size for concurrent processing within levels
	chunkSize := 0
	if opts != nil && opts.ChunkSize > 0 {
		chunkSize = opts.ChunkSize
	}

	// Step 2: Process levels with controlled concurrency
	if concurrentLevels == 1 {
		// Sequential processing (original behavior)
		for level := uint32(0); level < uint32(len(txsPerLevel)); level++ {
			levelTxs := txsPerLevel[level]
			if len(levelTxs) == 0 {
				continue
			}

			// Initialize successful txs map for this level
			successfulTxsByLevel[level] = make(map[chainhash.Hash]bool)

		// Step 2a: Build parent metadata from previous level's successful validations
		if level > 0 {
			prevLevel := level - 1
			if successfulTxs, exists := successfulTxsByLevel[prevLevel]; exists && len(successfulTxs) > 0 {
				parentBlockHeights := buildParentMetadata(txsPerLevel[prevLevel], blockHeight, successfulTxs)
				// Merge with existing parent block heights
				for hash, height := range parentBlockHeights {
					opts.ParentBlockHeights[hash] = height
				}
			}
		}

		// Step 2b: Optionally extend transactions with in-block parent outputs
		if opts.AutoExtendTransactions && level > 0 {
			parentMap := buildParentMap(txsPerLevel[level-1])
			if len(parentMap) > 0 {
				// Extend all transactions at this level
				for _, txWithIdx := range levelTxs {
					extendTxWithParentMap(txWithIdx.tx, parentMap)
				}
			}
		}

		// Step 2c: Validate level - chunked or whole
		if chunkSize > 0 && len(levelTxs) > chunkSize {
			// CHUNKED PROCESSING - Split level into chunks and process concurrently
			// CRITICAL: Limit concurrent chunks to prevent Aerospike connection pool exhaustion
			//
			// Each chunk runs SpendBatchDirect and CreateBatchDirect, both using connection pool
			// With unlimited chunks, we can create thousands of concurrent operations:
			//   Example: 229K txs / 75 chunk size = 3,058 chunks
			//   Each chunk can use up to ConnectionQueueSize (64) connections
			//   Total: 3,058 * 64 = 195,712 connection attempts!
			//   Available: 64 connections → EXHAUSTION
			//
			// Solution: Limit concurrent chunks based on connection pool capacity
			//   ConnectionQueueSize = 64 (typical)
			//   Safe limit: 64 / 8 = 8 concurrent chunks
			//   Moderate: 64 / 4 = 16 concurrent chunks
			//   Aggressive: 64 / 2 = 32 concurrent chunks

			maxConcurrent := 8 // Safe default: allows 8 chunks with 64 connections each
			if opts != nil && opts.MaxConcurrentChunks > 0 {
				maxConcurrent = opts.MaxConcurrentChunks
			}

			numChunks := (len(levelTxs) + chunkSize - 1) / chunkSize
			chunkResults := make([][]*LevelValidationResult, numChunks)

			// Use errgroup with concurrency limit
			g := errgroup.Group{}
			g.SetLimit(maxConcurrent) // CRITICAL: Prevents connection pool exhaustion!

			for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
				start := chunkIdx * chunkSize
				end := start + chunkSize
				if end > len(levelTxs) {
					end = len(levelTxs)
				}

				chunkTxs := make([]*bt.Tx, end-start)
				for i := start; i < end; i++ {
					chunkTxs[i-start] = levelTxs[i].tx
				}

				// Capture loop variables for closure
				idx := chunkIdx
				chunk := chunkTxs

				g.Go(func() error {
					chunkLevelResults, err := v.ValidateLevelBatch(ctx, chunk, blockHeight, opts)
					if err != nil {
						return errors.NewProcessingError("error validating level %d chunk %d: %v", level, idx, err)
					}
					chunkResults[idx] = chunkLevelResults
					return nil
				})
			}

			// Wait for all chunks to complete
			if err := g.Wait(); err != nil {
				span.RecordError(err)
				return nil, err
			}

			// Combine chunk results
			for _, chunkRes := range chunkResults {
				for _, levelResult := range chunkRes {
					txHash := *levelResult.TxHash
					result := &TxValidationResult{
						Success:         levelResult.Success,
						TxMeta:          levelResult.TxMeta,
						ConflictingTxID: levelResult.ConflictingTxID,
						Err:             levelResult.Err,
					}
					results[txHash] = result

					if levelResult.Success {
						successfulTxsByLevel[level][txHash] = true
					}
				}
			}
		} else {
			// WHOLE LEVEL PROCESSING - Original behavior
			levelTxsSlice := make([]*bt.Tx, len(levelTxs))
			for i, txWithIdx := range levelTxs {
				levelTxsSlice[i] = txWithIdx.tx
			}

			levelResults, err := v.ValidateLevelBatch(ctx, levelTxsSlice, blockHeight, opts)
			if err != nil {
				span.RecordError(err)
				return nil, errors.NewProcessingError("error validating level %d: %v", level, err)
			}

			// Step 2d: Process level results
			for _, levelResult := range levelResults {
				txHash := *levelResult.TxHash

				// Create result entry
				result := &TxValidationResult{
					Success:         levelResult.Success,
					TxMeta:          levelResult.TxMeta,
					ConflictingTxID: levelResult.ConflictingTxID,
					Err:             levelResult.Err,
				}
				results[txHash] = result

				// Track successful validations for next level's parent metadata
				if levelResult.Success {
					successfulTxsByLevel[level][txHash] = true
				}
			}
		}

		// Step 2e: Memory management - release grandparent level (level-2)
		// Keep only current level and parent level in memory
		if level >= 2 {
			grandparentLevel := level - 2
			delete(successfulTxsByLevel, grandparentLevel)
			// Note: txsPerLevel is read-only so we don't need to clear it
		}

		// Step 2f: Check for context cancellation before starting next level
		// This allows graceful exit between levels without leaving partial state
		select {
		case <-ctx.Done():
			// Context cancelled - return partial results processed so far
			v.logger.Infof("[ValidateMulti] Context cancelled after completing level %d of %d, returning partial results (%d transactions processed)",
				level, len(txsPerLevel)-1, len(results))

			// Update cache with partial results before returning
			v.updatePreviousLevelCache(txs, results)

			// Return partial results with context error
			span.RecordError(ctx.Err())
			return nil, errors.NewProcessingError("context cancelled after level %d: %w", level, ctx.Err())
		default:
			// Context still active, continue to next level
		}
		}
	} else {
		// CONCURRENT LEVEL PROCESSING - Pipeline multiple levels
		// Key: Use channels to ensure Level N finishes UTXO creation before Level N+1 starts spending
		v.logger.Infof("[ValidateMulti] Processing %d levels with concurrency=%d", len(txsPerLevel), concurrentLevels)

		// Completion channels - signal when each level's UTXOs are created
		levelComplete := make([]chan struct{}, len(txsPerLevel))
		for i := range levelComplete {
			levelComplete[i] = make(chan struct{})
		}

		pipeline := make(chan int, concurrentLevels)
		var wg sync.WaitGroup
		errChan := make(chan error, len(txsPerLevel))

		for level := uint32(0); level < uint32(len(txsPerLevel)); level++ {
			levelTxs := txsPerLevel[level]
			if len(levelTxs) == 0 {
				close(levelComplete[level]) // No work, mark as complete
				continue
			}

			// Block if pipeline is full
			pipeline <- int(level)
			wg.Add(1)

			go func(lvl uint32, txsAtLevel []txWithIndex) {
				defer wg.Done()
				defer func() { <-pipeline }()
				defer close(levelComplete[lvl]) // Signal completion

				// CRITICAL: Wait for previous level to complete UTXO creation
				if lvl > 0 {
					<-levelComplete[lvl-1] // Block until parent level finishes
				}

				// Initialize successful txs map for this level
				resultsMutex.Lock()
				successfulTxsByLevel[lvl] = make(map[chainhash.Hash]bool)
				resultsMutex.Unlock()

				// Build parent metadata from previous level
				if lvl > 0 {
					prevLevel := lvl - 1
					resultsMutex.RLock()
					if successfulTxs, exists := successfulTxsByLevel[prevLevel]; exists && len(successfulTxs) > 0 {
						parentBlockHeights := buildParentMetadata(txsPerLevel[prevLevel], blockHeight, successfulTxs)
						for hash, height := range parentBlockHeights {
							opts.ParentBlockHeights[hash] = height
						}
					}
					resultsMutex.RUnlock()
				}

				// Extend transactions with in-block parent outputs
				if opts.AutoExtendTransactions && lvl > 0 {
					parentMap := buildParentMap(txsPerLevel[lvl-1])
					if len(parentMap) > 0 {
						for _, txWithIdx := range txsAtLevel {
							extendTxWithParentMap(txWithIdx.tx, parentMap)
						}
					}
				}

				// Validate entire level
				levelTxsSlice := make([]*bt.Tx, len(txsAtLevel))
				for i, txWithIdx := range txsAtLevel {
					levelTxsSlice[i] = txWithIdx.tx
				}

				levelResults, err := v.ValidateLevelBatch(ctx, levelTxsSlice, blockHeight, opts)
				if err != nil {
					errChan <- errors.NewProcessingError("error validating level %d: %v", lvl, err)
					return
				}

				// Store results (with locking)
				resultsMutex.Lock()
				for _, levelResult := range levelResults {
					txHash := *levelResult.TxHash
					results[txHash] = &TxValidationResult{
						Success:         levelResult.Success,
						TxMeta:          levelResult.TxMeta,
						ConflictingTxID: levelResult.ConflictingTxID,
						Err:             levelResult.Err,
					}
					if levelResult.Success {
						successfulTxsByLevel[lvl][txHash] = true
					}
				}
				resultsMutex.Unlock()

				// Level complete - UTXOs are now created and available for next level
			}(level, levelTxs)
		}

		// Wait for all levels to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		if err := <-errChan; err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	// Step 3: Update previousLevelCache with successful transactions from this ValidateMulti call
	// This allows the next ValidateMulti call to look up these transactions without UTXO store access
	v.updatePreviousLevelCache(txs, results)

	return &MultiResult{Results: results}, nil
}

// updatePreviousLevelCache updates the cache with successful transactions from the current ValidateMulti call
// Simple replacement strategy: entire cache replaced with current successful transactions
// No eviction logic needed - keeps only the previous call's transactions
// OPTIMIZATION: Heavy work done outside lock, only pointer swap under lock
func (v *Validator) updatePreviousLevelCache(txs []*bt.Tx, results map[chainhash.Hash]*TxValidationResult) {
	// Build a txHash -> tx map first for O(1) lookups (avoid O(N²) nested loop)
	// Done OUTSIDE lock to avoid blocking readers
	txMap := make(map[chainhash.Hash]*bt.Tx, len(txs))
	for _, tx := range txs {
		if tx != nil {
			txMap[*tx.TxIDChainHash()] = tx
		}
	}

	// Build new cache with current successful transactions (OUTSIDE lock)
	newCache := make(map[chainhash.Hash]*bt.Tx, len(results))
	for txHash, result := range results {
		if result.Success && result.TxMeta != nil {
			// O(1) lookup instead of O(N) scan
			if tx, found := txMap[txHash]; found {
				newCache[txHash] = tx
			}
		}
	}

	// ONLY hold lock for pointer swap (microseconds, not milliseconds)
	v.previousValidateMultiCacheMu.Lock()
	v.previousValidateMultiCache = newCache
	v.previousValidateMultiCacheMu.Unlock()

	v.logger.Debugf("[updatePreviousLevelCache] Replaced cache with %d successful transactions", len(newCache))
}
