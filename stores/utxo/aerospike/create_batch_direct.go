// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CreateBatchDirect performs batch creation for multiple transactions in a single operation.
// This method bypasses the batcher queue and executes a direct Aerospike BatchOperate,
// providing significant performance improvements for level-based block validation.
//
// Safety: Preserves all creation semantics including:
// - Conflicting flag with automatic DAH (DeleteAtHeight) for cleanup
// - Locked flag for block assembly two-phase commit
// - Parent conflictingChildren metadata updates
// - Multi-record pagination for large transactions (uses existing StoreTransactionExternally)
//
// Performance: Eliminates per-transaction channel coordination overhead, batching entire
// level's creates together.
//
// Error handling: Returns per-transaction results. KEY_EXISTS_ERROR is converted to ErrTxExists.
func (s *Store) CreateBatchDirect(ctx context.Context, requests []*utxo.BatchCreateRequest) ([]*utxo.BatchCreateResult, error) {
	ctx, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "CreateBatchDirect",
		tracing.WithHistogram(prometheusUtxoCreateBatchDirect),
	)
	defer deferFn()

	if len(requests) == 0 {
		return nil, nil
	}

	// Track batch size for monitoring
	prometheusUtxoCreateBatchDirectSize.Observe(float64(len(requests)))

	// Initialize results slice
	results := make([]*utxo.BatchCreateResult, len(requests))
	for i := range results {
		results[i] = &utxo.BatchCreateResult{
			TxHash:  requests[i].Tx.TxIDChainHash(),
			Success: false,
		}
	}

	// PHASE 1: Update parent conflictingChildren metadata for conflicting transactions
	// This must happen BEFORE creating the transaction record (same as create.go:174-178)
	for _, req := range requests {
		if req.Conflicting {
			if err := s.updateParentConflictingChildren(req.Tx); err != nil {
				return nil, errors.NewProcessingError("[CREATE_BATCH_DIRECT] failed to update parent conflicting children", err)
			}
		}
	}

	// PHASE 2: Prepare batch records
	batchRecords := make([]aerospike.BatchRecordIfc, len(requests))
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	for i, req := range requests {
		if req.Tx == nil {
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT] transaction is nil")
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil) // NOOP
			continue
		}

		// Get bins to store - reuse existing logic from create.go:333
		external := s.settings.UtxoStore.ExternalizeAllTransactions

		// Check if transaction size requires external storage
		var extendedSize int
		if len(req.Tx.Inputs) == 0 {
			// Partial transaction - only outputs
			for _, output := range req.Tx.Outputs {
				if output != nil {
					extendedSize += len(output.Bytes())
				}
			}
		} else {
			extendedSize = len(req.Tx.ExtendedBytes())
		}

		if extendedSize > MaxTxSizeInStoreInBytes {
			external = true
		}

		bins, binsErr := s.GetBinsToStore(
			req.Tx,
			req.BlockHeight,
			req.BlockIDs,
			req.BlockHeights,
			req.SubtreeIdxs,
			external,
			req.Tx.TxIDChainHash(),
			req.Tx.IsCoinbase(),
			req.Conflicting,
			req.Locked,
		)
		if binsErr != nil {
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] failed to get bins", req.Tx.TxID(), binsErr)
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil) // NOOP
			continue
		}

		// Calculate Aerospike key
		key, keyErr := aerospike.NewKey(s.namespace, s.setName, req.Tx.TxIDChainHash()[:])
		if keyErr != nil {
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] failed to create key", req.Tx.TxID(), keyErr)
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil) // NOOP
			continue
		}

		// Handle pagination - large transactions use existing two-phase commit path
		if len(bins) > 1 {
			// Multi-record transaction - delegate to existing StoreTransactionExternally
			// This preserves the two-phase commit protocol with creating flag
			item := &BatchStoreItem{
				txHash:       req.Tx.TxIDChainHash(),
				isCoinbase:   req.Tx.IsCoinbase(),
				tx:           req.Tx,
				blockHeight:  req.BlockHeight,
				lockTime:     req.Tx.LockTime,
				blockIDs:     req.BlockIDs,
				blockHeights: req.BlockHeights,
				subtreeIdxs:  req.SubtreeIdxs,
				conflicting:  req.Conflicting,
				locked:       req.Locked,
				done:         make(chan error, 1),
			}

			if len(req.Tx.Inputs) == 0 {
				go s.StorePartialTransactionExternally(ctx, item, bins)
			} else {
				go s.StoreTransactionExternally(ctx, item, bins)
			}

			// Mark as NOOP in this batch
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		// Single-record transaction - add to batch
		putOps := make([]*aerospike.Operation, len(bins[0]))
		for j, bin := range bins[0] {
			putOps[j] = aerospike.PutOp(bin)
		}

		// Add DeleteAtHeight for conflicting transactions
		// Reuse pattern from create.go:432-434
		if req.Conflicting {
			dah := req.BlockHeight + s.settings.GetUtxoStoreBlockHeightRetention()
			putOps = append(putOps, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), dah)))
		}

		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
	}

	// PHASE 3: Execute single Aerospike batch operation
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return nil, errors.NewStorageError("[CREATE_BATCH_DIRECT] failed to batch create", err)
	}

	// PHASE 4: Process results
	for i, record := range batchRecords {
		batchErr := record.BatchRec().Err
		if batchErr != nil {
			aErr, ok := batchErr.(*aerospike.AerospikeError)
			if ok && aErr.ResultCode == types.KEY_EXISTS_ERROR {
				// Transaction already exists - not an error in block validation context
				results[i].Err = errors.NewTxExistsError("[CREATE_BATCH_DIRECT] transaction exists", requests[i].Tx.TxIDChainHash())
				results[i].Success = false
			} else if ok && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
				// This is a NOOP record (pagination handled externally) - skip
				results[i].Success = false
			} else {
				results[i].Err = errors.NewStorageError("[CREATE_BATCH_DIRECT][%s] failed to create", requests[i].Tx.TxID(), batchErr)
				results[i].Success = false
			}
		} else {
			// Success
			results[i].Success = true

			// Create metadata from transaction
			// Reuse pattern from Validator.go:622
			txMeta, err := util.TxMetaDataFromTx(requests[i].Tx)
			if err != nil {
				results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] failed to create metadata", requests[i].Tx.TxID(), err)
				results[i].Success = false
				continue
			}

			txMeta.Conflicting = requests[i].Conflicting
			txMeta.Locked = requests[i].Locked
			results[i].TxMeta = txMeta

			prometheusUtxostoreCreate.Inc()
		}
	}

	return results, nil
}
