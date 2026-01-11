package subtreevalidation

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// prefetchLevel0Parents batch-fetches parent output data for all Level 0 transactions.
// Level 0 transactions have NO parents in the current block, so ALL inputs require
// UTXO store lookups. This function extends all Level 0 transactions upfront using
// concurrent calls to PreviousOutputsDecorate, which internally batches the requests.
//
// The prefetching works by launching concurrent goroutines (limited by semaphore) that
// call PreviousOutputsDecorate for each transaction. All these calls feed outpoints into
// the UTXO store's internal outpointBatcher simultaneously, resulting in a single
// Aerospike BatchOperate call with automatic deduplication of parent transaction fetches.
//
// Performance characteristics:
//   - Computational: O(n + m) where n = level 0 tx count, m = unique parent txs
//   - Database: O(1) Aerospike BatchOperate call (vs O(n) sequential calls)
//   - Memory: < 1MB additional (goroutine stacks + batcher queue)
//   - Expected speedup: 10-100x for network-bound scenarios
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - level0Txs: Slice of Level 0 transactions (those with NO in-block parents)
//   - utxoStore: UTXO store interface for fetching parent output data
//   - settings: Settings containing Level0PrefetchConcurrency configuration
//
// Returns:
//   - error: nil on success, or wrapped error if any transaction extension fails
func prefetchLevel0Parents(
	ctx context.Context,
	level0Txs []missingTx,
	utxoStore utxo.Store,
	settings *settings.Settings,
) error {
	if len(level0Txs) == 0 {
		return nil // No level 0 transactions to prefetch
	}

	// Determine concurrency limit from configuration
	// Higher values allow more parallel PreviousOutputsDecorate calls,
	// feeding more outpoints into the batcher simultaneously.
	// Recommended: 128-256 for I/O-bound operations
	maxConcurrency := settings.SubtreeValidation.Level0PrefetchConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 128 // Default: reasonable limit for most systems
	}

	sem := semaphore.NewWeighted(int64(maxConcurrency))
	errGroup, ctx := errgroup.WithContext(ctx)

	for _, mTx := range level0Txs {
		if mTx.tx == nil {
			continue
		}

		// Skip if already extended (shouldn't happen for level 0, but defensive)
		if mTx.tx.IsExtended() {
			continue
		}

		// Acquire semaphore slot to limit concurrent goroutines
		if err := sem.Acquire(ctx, 1); err != nil {
			return errors.NewProcessingError("[prefetchLevel0Parents] Failed to acquire semaphore", err)
		}

		mTx := mTx // Capture loop variable for goroutine
		errGroup.Go(func() error {
			defer sem.Release(1)

			// PreviousOutputsDecorate fetches parent output data and populates inputs.
			// For each input, it:
			//   1. Puts outpoint in internal batcher (if not already extended)
			//   2. Batcher deduplicates by parent TXID
			//   3. Single Aerospike BatchOperate fetches all parent txs
			//   4. Extracts specific outputs (PreviousTxSatoshis + PreviousTxScript)
			//   5. Populates input fields
			//
			// All concurrent calls to this method feed into the same batcher,
			// resulting in optimal batching and deduplication.
			err := utxoStore.PreviousOutputsDecorate(ctx, mTx.tx)
			if err != nil {
				return errors.NewProcessingError(
					fmt.Sprintf("[prefetchLevel0Parents] Failed to decorate transaction %s", mTx.tx.TxIDChainHash().String()),
					err)
			}

			// Mark as extended to skip redundant extension attempts
			mTx.tx.SetExtended(true)
			return nil
		})
	}

	// Wait for all prefetch operations to complete
	// If any goroutine returns an error, errGroup.Wait() returns that error
	if err := errGroup.Wait(); err != nil {
		return errors.NewProcessingError("[prefetchLevel0Parents] Level 0 parent prefetch failed", err)
	}

	return nil
}
