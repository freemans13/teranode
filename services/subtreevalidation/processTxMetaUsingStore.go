// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// processTxMetaUsingStore attempts to retrieve transaction metadata from the underlying store
// for a batch of transactions. It supports both batched and individual transaction retrieval.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - batched: If true, uses batch operations for retrieval
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//
// Returns:
//   - int: Number of transactions missing from store
//   - error: Any error encountered during processing
//
// The function uses BatchDecorate when batched is true, otherwise falls back to
// individual GetMeta calls. It will return a ThresholdExceededError if failFast
// is true and the number of missing transactions exceeds the configured threshold.
func (u *Server) processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data,
	blockIds map[uint32]bool, batched bool, failFast bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewInvalidArgumentError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTxMetaUsingStore")
	defer deferFn()

	batchSize := u.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	validateSubtreeInternalConcurrency := u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency
	missingTxThreshold := u.settings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, validateSubtreeInternalConcurrency)

	var missed atomic.Int32

	if batched {
		for i := 0; i < len(txHashes); i += batchSize {
			i := i // capture range variable for goroutine

			g.Go(func() error {
				end := subtree.Min(i+batchSize, len(txHashes))

				missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, end-i)

				for j := 0; j < subtree.Min(batchSize, len(txHashes)-i); j++ {
					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						// Return the error that caused the cancellation
						return errors.NewContextCanceledError("[processTxMetaUsingStore] context done", gCtx.Err())

					default:
						if txHashes[i+j].Equal(*subtree.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						if txMetaSlice[i+j] == nil {
							missingTxHashesCompacted = append(missingTxHashesCompacted, &utxo.UnresolvedMetaData{
								Hash: txHashes[i+j],
								Idx:  i + j,
							})
						}
					}
				}

				if err := u.utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.Conflicting, fields.BlockIDs); err != nil {
					return errors.NewStorageError("error running batch decorate on utxo store for missing transactions", err)
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					// Return the error that caused the cancellation
					return errors.NewContextCanceledError("[processTxMetaUsingStore] context done", gCtx.Err())
				default:
					missingTxThresholdInt32, err := safeconversion.IntToInt32(missingTxThreshold)
					if err != nil {
						return err
					}

					for _, data := range missingTxHashesCompacted {
						if data.Data == nil || data.Err != nil {
							newMissed := missed.Add(1)

							if failFast && missingTxThresholdInt32 > 0 && newMissed > missingTxThresholdInt32 {
								return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
							}

							continue
						}

						txMetaSlice[data.Idx] = data.Data

						// if the tx is conflicting, we need to check if it is conflicting on the current chain
						if txMetaSlice[data.Idx].Conflicting {
							if err = u.checkCounterConflictingOnCurrentChain(ctx, data.Hash, blockIds); err != nil {
								return errors.NewProcessingError("[processTxMetaUsingStore][%s] failed to check counter conflicting tx on current chain", data.Hash.String(), err)
							}
						}

						// check whether this transaction has already been mined into a block on our chain
						if len(txMetaSlice[data.Idx].BlockIDs) > 0 && blockIds != nil {
							for _, blockID := range txMetaSlice[data.Idx].BlockIDs {
								if _, exists := blockIds[blockID]; exists {
									// this transaction has already been mined into a block on our chain
									return errors.NewProcessingError("transaction %s has already been mined into block ID %d on our chain", data.Hash.String(), blockID)
								}
							}
						}
					}

					return nil
				}
			})
		}

		if err := g.Wait(); err != nil {
			return int(missed.Load()), errors.NewContextCanceledError("[processTxMetaUsingStore]", err)
		}

		return int(missed.Load()), nil
	} else {
		for i := 0; i < len(txHashes); i += batchSize {
			i := i

			g.Go(func() error {
				// cycle through the batch size, making sure not to go over the length of the txHashes
				for j := 0; j < subtree.Min(batchSize, len(txHashes)-i); j++ {
					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						// Return the error that caused the cancellation
						return errors.NewContextCanceledError("[processTxMetaUsingStore] context done", gCtx.Err())

					default:
						txHash := txHashes[i+j]

						missingTxThresholdInt32, err := safeconversion.IntToInt32(missingTxThreshold)
						if err != nil {
							return err
						}

						if txHash.Equal(*subtree.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						if txMetaSlice[i+j] == nil {
							txMeta, err := u.utxoStore.GetMeta(gCtx, &txHash)
							if err != nil {
								return errors.NewStorageError("error getting tx meta from utxo store", err)
							}

							if txMeta != nil {
								txMetaSlice[i+j] = txMeta
								continue
							}
						}

						newMissed := missed.Add(1)

						if failFast && missingTxThreshold > 0 && newMissed > missingTxThresholdInt32 {
							return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
						}
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return int(missed.Load()), errors.NewContextCanceledError("[processTxMetaUsingStore] context done", err)
		}

		return int(missed.Load()), nil
	}
}
