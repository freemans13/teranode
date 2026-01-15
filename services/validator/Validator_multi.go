package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// ValidateMultiple validates multiple transactions with automatic dependency ordering and batch processing.
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
//   - opts: Validation options (AutoExtendTransactions, MaxBatchSize, ParentMetadata, etc.)
//
// Returns:
//   - *MultiValidationResult: Per-transaction results with success, metadata, conflicts, errors
//   - error: Critical errors preventing validation (not per-transaction failures)
func (v *Validator) ValidateMultiple(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) (*MultiValidationResult, error) {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "ValidateMultiple")
	defer deferFn()

	if len(txs) == 0 {
		return &MultiValidationResult{Results: make(map[chainhash.Hash]*TxValidationResult)}, nil
	}

	// Handle nil options
	if opts == nil {
		opts = NewDefaultOptions()
	}

	// Initialize ParentMetadata if not provided
	if opts.ParentMetadata == nil {
		opts.ParentMetadata = make(map[chainhash.Hash]*ParentTxMetadata)
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

	// Step 2: Process each level sequentially
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
				parentMetadata := buildParentMetadata(txsPerLevel[prevLevel], blockHeight, successfulTxs)
				// Merge with existing parent metadata
				for hash, meta := range parentMetadata {
					opts.ParentMetadata[hash] = meta
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

		// Step 2c: Validate entire level using batch validation
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

		// Step 2e: Memory management - release grandparent level (level-2)
		// Keep only current level and parent level in memory
		if level >= 2 {
			grandparentLevel := level - 2
			delete(successfulTxsByLevel, grandparentLevel)
			// Note: txsPerLevel is read-only so we don't need to clear it
		}
	}

	return &MultiValidationResult{Results: results}, nil
}
