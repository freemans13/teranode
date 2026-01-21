// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// spendBatchDirectItem tracks a single spend operation within the batch
// Maps back to the original transaction and spend index for error distribution
type spendBatchDirectItem struct {
	spend             *utxo.Spend
	requestIdx        int // Index in original requests slice
	spendIdx          int // Index within transaction's spends
	ignoreConflicting bool
	ignoreLocked      bool
}

// SpendBatchDirect performs batch spending for multiple transactions in a single operation.
// This method bypasses the batcher queue and executes a direct Aerospike BatchOperate,
// providing significant performance improvements for level-based block validation.
//
// Safety: ALL Lua safety checks are preserved (frozen, locked, conflicting, creating, coinbase maturity).
// The method calls the same spendMulti() Lua function used by the regular Spend() method.
//
// Performance: Eliminates per-transaction channel coordination overhead, reducing latency from
// 50-100ms per transaction to a single batch operation for the entire level.
//
// Error handling: Returns per-transaction results with per-UTXO error details. Failed transactions
// have their successful spends rolled back to maintain atomicity guarantees.
func (s *Store) SpendBatchDirect(ctx context.Context, requests []*utxo.BatchSpendRequest) ([]*utxo.BatchSpendResult, error) {
	ctx, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "SpendBatchDirect",
		tracing.WithHistogram(prometheusUtxoSpendBatchDirect),
	)
	defer deferFn()

	if len(requests) == 0 {
		return nil, nil
	}

	// Circuit breaker check - fail fast if circuit is open
	if s.spendCircuitBreaker != nil && !s.spendCircuitBreaker.Allow() {
		return nil, errors.NewServiceUnavailableError("[SPEND_BATCH_DIRECT] circuit breaker open, rejecting request")
	}

	// Track batch size for monitoring
	prometheusUtxoSpendBatchDirectSize.Observe(float64(len(requests)))

	// Initialize results slice
	results := make([]*utxo.BatchSpendResult, len(requests))
	for i := range results {
		results[i] = &utxo.BatchSpendResult{
			TxHash:  requests[i].Tx.TxIDChainHash(),
			Spends:  make([]*utxo.Spend, 0),
			Success: false,
		}
	}

	// PHASE 1: Collect and group all spends by parent transaction key + flags
	// This reuses the grouping logic from the existing Spend() implementation
	type groupKey struct {
		keyStr            string
		key               *aerospike.Key
		blockHeight       uint32
		ignoreConflicting bool
		ignoreLocked      bool
	}

	groups := make(map[groupKey][]*spendBatchDirectItem)
	aeroKeyMap := make(map[string]*aerospike.Key)

	// Collect all spends from all transactions
	for reqIdx, req := range requests {
		if req.Tx == nil {
			results[reqIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT] transaction is nil")
			continue
		}

		spends, err := utxo.GetSpends(req.Tx)
		if err != nil {
			results[reqIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT][%s] failed to get spends", req.Tx.TxID(), err)
			continue
		}

		// Store spends in result for later processing
		results[reqIdx].Spends = spends

		// Group each spend by its parent transaction key
		for spendIdx, spend := range spends {
			if spend == nil {
				results[reqIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT][%s] spend is nil at index %d", req.Tx.TxID(), spendIdx)
				continue
			}

			if spend.SpendingData == nil {
				results[reqIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT][%s] spending data is nil for vout %d", req.Tx.TxID(), spend.Vout)
				continue
			}

			// Calculate Aerospike key for the parent transaction
			// Reuse logic from spend.go:505
			keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, s.utxoBatchSize)
			keySourceStr := string(keySource)

			key, ok := aeroKeyMap[keySourceStr]
			if !ok {
				key, err = aerospike.NewKey(s.namespace, s.setName, keySource)
				if err != nil {
					results[reqIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT][%s] failed to create aerospike key", req.Tx.TxID(), err)
					continue
				}
				aeroKeyMap[keySourceStr] = key
			}

			// Group by parent tx key + block height + flags
			gKey := groupKey{
				keyStr:            keySourceStr,
				key:               key,
				blockHeight:       req.BlockHeight,
				ignoreConflicting: req.IgnoreFlags.IgnoreConflicting,
				ignoreLocked:      req.IgnoreFlags.IgnoreLocked,
			}

			groups[gKey] = append(groups[gKey], &spendBatchDirectItem{
				spend:             spend,
				requestIdx:        reqIdx,
				spendIdx:          spendIdx,
				ignoreConflicting: req.IgnoreFlags.IgnoreConflicting,
				ignoreLocked:      req.IgnoreFlags.IgnoreLocked,
			})
		}
	}

	if len(groups) == 0 {
		return results, nil
	}

	// s.logger.Debugf("[SPEND_BATCH_DIRECT] Grouped %d requests into %d parent transaction groups", len(requests), len(groups))

	// PHASE 2: Create Aerospike batch operations
	// Reuse pattern from spend.go:540-564
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(groups))
	batchGroupKeys := make([]groupKey, 0, len(groups))
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for gKey, groupItems := range groups {
		// Create map values for Lua spendMulti() function
		mapValues := make([]aerospike.MapValue, len(groupItems))
		for i, item := range groupItems {
			mapValues[i] = aerospike.NewMapValue(map[any]any{
				"idx":          i, // Index within this group for error mapping
				"offset":       s.calculateOffsetForOutput(item.spend.Vout),
				"vOut":         item.spend.Vout,
				"utxoHash":     item.spend.UTXOHash[:],
				"spendingData": item.spend.SpendingData.Bytes(),
			})
		}

		// Create batch UDF operation - calls same spendMulti() Lua function
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(
			batchUDFPolicy,
			gKey.key,
			LuaPackage,
			"spendMulti",
			aerospike.NewValue(mapValues),
			aerospike.NewValue(gKey.ignoreConflicting),
			aerospike.NewValue(gKey.ignoreLocked),
			aerospike.NewValue(gKey.blockHeight),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
		))

		batchGroupKeys = append(batchGroupKeys, gKey)
	}

	// PHASE 3: Execute Aerospike batch operation
	// Caller controls batch size - no internal chunking needed
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		// Batch-level failure - record for circuit breaker
		if s.spendCircuitBreaker != nil {
			s.spendCircuitBreaker.RecordFailure()
		}
		return nil, errors.NewStorageError("[SPEND_BATCH_DIRECT] failed to batch spend aerospike", err)
	}

	// s.logger.Debugf("[SPEND_BATCH_DIRECT] Aerospike BatchOperate completed in %v", time.Since(startBatch))

	// PHASE 4: Parse Lua responses and distribute errors
	// Reuse error parsing logic from spend.go:580-790
	hasFailures := false

	for batchIdx, batchRecord := range batchRecords {
		gKey := batchGroupKeys[batchIdx]
		groupItems := groups[gKey]

		batchErr := batchRecord.BatchRec().Err
		if batchErr != nil {
			// Aerospike-level error for entire group
			for _, item := range groupItems {
				results[item.requestIdx].Err = errors.NewStorageError("[SPEND_BATCH_DIRECT] aerospike batch error", batchErr)
				results[item.requestIdx].Spends[item.spendIdx].Err = batchErr
			}
			hasFailures = true
			continue
		}

		response := batchRecord.BatchRec().Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			for _, item := range groupItems {
				results[item.requestIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT] no response from Lua")
				results[item.requestIdx].Spends[item.spendIdx].Err = errors.NewProcessingError("no Lua response")
			}
			hasFailures = true
			continue
		}

		// Parse Lua response
		luaResp, parseErr := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
		if parseErr != nil {
			for _, item := range groupItems {
				results[item.requestIdx].Err = errors.NewProcessingError("[SPEND_BATCH_DIRECT] failed to parse Lua response", parseErr)
				results[item.requestIdx].Spends[item.spendIdx].Err = parseErr
			}
			hasFailures = true
			continue
		}

		// Process Lua response
		if luaResp.Status == LuaStatusOK {
			// All spends in this group succeeded
			for _, item := range groupItems {
				results[item.requestIdx].Spends[item.spendIdx].Err = nil
			}

		} else if luaResp.Status == LuaStatusError {
			hasFailures = true

			if luaResp.Message != "" {
				// General error for entire group - applies to all spends
				generalErr := s.createGeneralError(luaResp.ErrorCode, groupItems[0].spend.TxID, gKey.blockHeight, 0, luaResp.Message)

				for _, item := range groupItems {
					results[item.requestIdx].Err = generalErr
					results[item.requestIdx].Spends[item.spendIdx].Err = generalErr
				}

			} else if luaResp.Errors != nil {
				// Individual errors for specific spends within the group
				for _, item := range groupItems {
					if errInfo, hasErr := luaResp.Errors[item.spendIdx]; hasErr {
						spendErr := s.createSpendError(errInfo, &batchSpend{spend: item.spend}, item.spend.TxID)
						results[item.requestIdx].Spends[item.spendIdx].Err = spendErr

						// Extract ConflictingTxID from double-spend error
						// This is critical for conflict detection
						if errInfo.ErrorCode == LuaErrorCodeSpent && errInfo.SpendingData != "" {
							spendingData, parseErr := spendpkg.NewSpendingDataFromString(errInfo.SpendingData)
							if parseErr == nil {
								results[item.requestIdx].ConflictingTxID = spendingData.TxID
								results[item.requestIdx].Spends[item.spendIdx].ConflictingTxID = spendingData.TxID
							}
						}
					} else {
						// This spend succeeded
						results[item.requestIdx].Spends[item.spendIdx].Err = nil
					}
				}
			}
		}
	}

	// PHASE 5: Determine per-transaction success and handle rollback
	// Each transaction succeeds only if ALL its spends succeeded
	for _, result := range results {
		if result.Err != nil {
			// Already marked as failed due to pre-processing error
			continue
		}

		allSpendsSucceeded := true
		for _, spend := range result.Spends {
			if spend.Err != nil {
				allSpendsSucceeded = false
				break
			}
		}

		if allSpendsSucceeded {
			result.Success = true
		} else {
			// Transaction failed - collect successful spends for rollback
			successfulSpends := make([]*utxo.Spend, 0, len(result.Spends))
			var firstErr error

			for _, spend := range result.Spends {
				if spend.Err == nil {
					successfulSpends = append(successfulSpends, spend)
				} else if firstErr == nil {
					firstErr = spend.Err
				}
			}

			// Rollback successful spends (maintains atomicity)
			if len(successfulSpends) > 0 {
				if unspendErr := s.Unspend(ctx, successfulSpends); unspendErr != nil {
					s.logger.Debugf("[SPEND_BATCH_DIRECT][%s] failed to rollback spends: %v", result.TxHash.String(), unspendErr)
				}
			}

			result.Success = false
			result.Err = firstErr
		}
	}

	// Circuit breaker tracking
	if s.spendCircuitBreaker != nil {
		if hasFailures {
			s.spendCircuitBreaker.RecordFailure()
		} else {
			s.spendCircuitBreaker.RecordSuccess()
		}
	}

	// Count successful spends for metrics
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount += len(result.Spends)
		}
	}
	prometheusUtxoMapSpend.Add(float64(successCount))

	return results, nil
}
