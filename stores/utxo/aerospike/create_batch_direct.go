// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
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

	// Track async operations for multi-record transactions
	asyncOps := make(map[int]chan error)

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

		// Get bins to store - reuse existing logic from create.go:313-331
		external := s.settings.UtxoStore.ExternalizeAllTransactions

		// Check if transaction size requires external storage (same logic as create.go:313-330)
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
			// NOTE: We'll launch async but track the done channel to wait for completion
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

			// Store the done channel for later waiting
			asyncOps[i] = item.done

			// Mark as NOOP in this batch
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		// Single-record transaction - check if it needs blob storage (like create.go:358-410)
		if external {
			// Single-record transaction but too large for inline storage
			// Must write to blob storage synchronously (like create.go:358-410)
			// This handles large single-record transactions (extendedSize > 32KB but outputs < 4096)

			// Write to blob storage
			var blobData []byte
			if len(req.Tx.Inputs) == 0 {
				// Partial transaction - create UTXO wrapper
				nonNilOutputs := utxopersister.UnpadSlice(req.Tx.Outputs)
				wrapper := utxopersister.UTXOWrapper{
					TxID:     *req.Tx.TxIDChainHash(),
					Height:   req.BlockHeight,
					Coinbase: req.Tx.IsCoinbase(),
					UTXOs:    make([]*utxopersister.UTXO, 0, len(nonNilOutputs)),
				}
				for idx, output := range req.Tx.Outputs {
					if output != nil {
						wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
							Index:  uint32(idx),
							Value:  output.Satoshis,
							Script: *output.LockingScript,
						})
					}
				}
				blobData = wrapper.Bytes()
			} else {
				blobData = req.Tx.ExtendedBytes()
			}

			// Write to external store
			fileType := fileformat.FileTypeTx
			if len(req.Tx.Inputs) == 0 {
				fileType = fileformat.FileTypeOutputs
			}

			if err := s.externalStore.Set(ctx, req.Tx.TxIDChainHash()[:], fileType, blobData, options.WithDeleteAt(0)); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
				results[i].Err = errors.NewStorageError("[CREATE_BATCH_DIRECT][%s] failed to write to external storage", req.Tx.TxID(), err)
				batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil) // NOOP
				continue
			}
		}

		// Create Aerospike record (either inline or with External=true marker)
		putOps := make([]*aerospike.Operation, len(bins[0]))
		for j, bin := range bins[0] {
			putOps[j] = aerospike.PutOp(bin)
		}

		// Add DeleteAtHeight for conflicting transactions
		if req.Conflicting {
			dah := req.BlockHeight + s.settings.GetUtxoStoreBlockHeightRetention()
			putOps = append(putOps, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), dah)))
		}

		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
	}

	// PHASE 3: Execute Aerospike batch operations (split into chunks if needed)
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	maxBatchSize := s.settings.UtxoStore.MaxAerospikeBatchSize

	numChunks := (len(batchRecords) + maxBatchSize - 1) / maxBatchSize
	s.logger.Debugf("[CREATE_BATCH_DIRECT] Executing Aerospike BatchOperate with %d operations (max %d per batch, %d chunks)", len(batchRecords), maxBatchSize, numChunks)

	// Log connection pool usage before starting
	connsBefore := s.client.GetActiveConnectionCount()
	s.logger.Infof("[CREATE_BATCH_DIRECT] Aerospike connections before chunks: %d (pool size: %d)", connsBefore, s.client.GetConnectionQueueSize())

	// PHASE 2 OPTIMIZATION: Parallelize chunk processing for high throughput
	// Split into chunks and execute them in parallel using errgroup
	// Limit concurrency to ConnectionQueueSize to prevent overwhelming Aerospike
	// This is safe because each chunk operates on disjoint transaction keys
	g, _ := errgroup.WithContext(ctx)
	// g.SetLimit(1)
	g.SetLimit(s.client.GetConnectionQueueSize())

	for i := 0; i < len(batchRecords); i += maxBatchSize {
		i := i // Capture loop variable for goroutine
		end := i + maxBatchSize
		if end > len(batchRecords) {
			end = len(batchRecords)
		}

		chunk := batchRecords[i:end]

		g.Go(func() error {
			err := s.client.BatchOperate(batchPolicy, chunk)
			if err != nil {
				// Check if this is KEY_EXISTS_ERROR - this happens when ANY record in the batch
				// already exists with CREATE_ONLY policy. This is not a fatal error - individual
				// records will have their own errors set which we handle in Phase 4.
				aErr, ok := err.(*aerospike.AerospikeError)
				if !ok || aErr.ResultCode != types.KEY_EXISTS_ERROR {
					// True batch-level failure (connection error, etc.)
					return errors.NewStorageError("[CREATE_BATCH_DIRECT] failed to batch create (chunk %d-%d)", i, end, err)
				}
				// KEY_EXISTS_ERROR - continue to Phase 4 to handle per-record results
				s.logger.Debugf("[CREATE_BATCH_DIRECT] Batch chunk %d-%d contains existing keys, will handle per-record in Phase 4", i, end)
			}
			return nil
		})
	}

	// Give goroutines a moment to all launch and make their requests
	time.Sleep(10 * time.Millisecond)
	connsPeak := s.client.GetActiveConnectionCount()
	s.logger.Infof("[CREATE_BATCH_DIRECT] Aerospike connections during chunk execution (peak): %d", connsPeak)

	// Wait for all chunks to complete
	if err := g.Wait(); err != nil {
		return nil, err
	}

	connsAfter := s.client.GetActiveConnectionCount()
	s.logger.Infof("[CREATE_BATCH_DIRECT] Aerospike connections after chunks complete: %d", connsAfter)

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
				// DEBUG: Log if target parent fails to create
				targetParent := "b4d259564fe04d69f4e3a5be2d38045820c2daedccc612ce24224717c68577e7"
				if requests[i].Tx.TxID() == targetParent {
					s.logger.Errorf("[CREATE_BATCH_DIRECT][DEBUG] Target parent %s: BatchWrite FAILED with error: %v", targetParent, batchErr)
				}

				results[i].Err = errors.NewStorageError("[CREATE_BATCH_DIRECT][%s] failed to create", requests[i].Tx.TxID(), batchErr)
				results[i].Success = false
			}
		} else {
			// Success
			results[i].Success = true

			// // DEBUG: Log when target parent succeeds
			// targetParent := "b4d259564fe04d69f4e3a5be2d38045820c2daedccc612ce24224717c68577e7"
			// if requests[i].Tx.TxID() == targetParent {
			// 	s.logger.Infof("[CREATE_BATCH_DIRECT][DEBUG] Target parent %s: BatchWrite succeeded, verifying record exists...", targetParent)

			// 	// CRITICAL DEBUG: Verify record actually exists in Aerospike
			// 	key, _ := aerospike.NewKey(s.namespace, s.setName, requests[i].Tx.TxIDChainHash()[:])
			// 	verifyRecord, verifyErr := s.client.Get(nil, key)
			// 	if verifyErr != nil || verifyRecord == nil {
			// 		s.logger.Errorf("[CREATE_BATCH_DIRECT][DEBUG] Target parent %s: VERIFICATION FAILED - record not found in Aerospike after BatchWrite success! Error: %v", targetParent, verifyErr)
			// 	} else {
			// 		s.logger.Infof("[CREATE_BATCH_DIRECT][DEBUG] Target parent %s: Verification OK - record exists with %d bins", targetParent, len(verifyRecord.Bins))
			// 	}
			// }

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

	// PHASE 5: Wait for async operations to complete (multi-record transactions)
	// This prevents TX_CREATING errors when next level tries to spend
	for i, doneChan := range asyncOps {
		err := <-doneChan

		// Handle errors
		if err != nil && !errors.Is(err, errors.ErrTxExists) {
			s.logger.Errorf("[CREATE_BATCH_DIRECT][DEBUG] Transaction %s async create returned error: %v", requests[i].Tx.TxID(), err)
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] async create failed", requests[i].Tx.TxID(), err)
			results[i].Success = false
			continue
		}

		// Transaction created successfully (or already exists)
		// Now verify creating flag is actually cleared to prevent TX_CREATING errors
		// StoreTransactionExternally may return success even if clearCreatingFlag failed
		// (by design for recovery), but for immediate level-based processing we need
		// the flag actually cleared

		txHash := requests[i].Tx.TxIDChainHash()
		cleared := false
		maxRetries := 3
		retryDelay := 10 * time.Millisecond

		for retry := 0; retry < maxRetries; retry++ {
			// Check if creating flag is set on master record
			key, keyErr := aerospike.NewKey(s.namespace, s.setName, txHash[:])
			if keyErr != nil {
				break
			}

			record, getErr := s.client.Get(nil, key, fields.Creating.String())
			if getErr != nil || record == nil {
				// CRITICAL BUG FIX: Transaction doesn't exist - async create FAILED!
				// This should NOT be treated as "cleared" - the tx was never created
				// Treating this as success causes children to fail with "parent not found"
				s.logger.Errorf("[CREATE_BATCH_DIRECT][%s] async create verification failed - transaction not found in store (async create likely failed)", txHash)
				// Leave cleared=false to trigger error reporting below
				break
			}

			// Check if creating bin exists and is true
			if creating, exists := record.Bins[fields.Creating.String()]; !exists || creating != true {
				// Creating flag not set or false - cleared!
				cleared = true
				break
			}

			// Creating flag still set, retry after delay
			if retry < maxRetries-1 {
				time.Sleep(retryDelay)
				retryDelay *= 2 // Exponential backoff
			}
		}

		if !cleared {
			// CRITICAL: Either creating flag still set OR transaction doesn't exist
			// Both cases mean the transaction is not accessible and should be treated as failure
			// Returning success here causes children to fail with "parent not found" errors
			s.logger.Errorf("[CREATE_BATCH_DIRECT][%s] async create FAILED: transaction not accessible after %d retries", txHash, maxRetries)
			results[i].Success = false
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] async create failed - transaction not accessible", requests[i].Tx.TxID())
			continue
		}

		// Create metadata
		results[i].Success = true
		txMeta, metaErr := util.TxMetaDataFromTx(requests[i].Tx)
		if metaErr != nil {
			results[i].Err = errors.NewProcessingError("[CREATE_BATCH_DIRECT][%s] failed to create metadata", requests[i].Tx.TxID(), metaErr)
			results[i].Success = false
		} else {
			txMeta.Conflicting = requests[i].Conflicting
			txMeta.Locked = requests[i].Locked
			results[i].TxMeta = txMeta
			prometheusUtxostoreCreate.Inc()
		}
	}

	return results, nil
}
