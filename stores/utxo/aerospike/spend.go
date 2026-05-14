// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Spend operations in the Aerospike UTXO store handle spending UTXOs through
// batched Lua operations with automatic DAH management and error handling.
//
// # Architecture
//
// The spend process uses a multi-layered approach:
//   1. Batch collection of spend requests
//   2. Grouping of spends by transaction
//   3. Atomic Lua scripts for spending
//   4. DAH management for cleanup
//   5. External storage synchronization
//
// # Main Types

// batchSpend represents a single UTXO spend request in a batch
type batchSpend struct {
	spend             *utxo.Spend // UTXO to spend
	blockHeight       uint32      // Current block height
	errCh             chan error  // Channel for completion notification
	ignoreConflicting bool
	ignoreLocked      bool
}

// IncrementSpentRecordsMulti performs a single BatchOperate to increment spent-extra-records for many txids.
// This avoids enqueueing each increment through the batcher and waiting per-item.
func (s *Store) IncrementSpentRecordsMulti(txids []*chainhash.Hash, increment int) error {
	if len(txids) == 0 {
		return nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	currentBlockHeight := s.blockHeight.Load()

	batchRecordsPtr := getBatchRecordsSlice(len(txids))
	batchRecords := (*batchRecordsPtr)[:0]

	for _, txid := range txids {
		key, err := aerospike.NewKey(s.namespace, s.setName, txid[:])
		if err != nil {
			*batchRecordsPtr = batchRecords
			putBatchRecordsSlice(batchRecordsPtr)
			return errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(
			batchUDFPolicy, LuaPackage, key, subOpIncrementSpentExtraRec, "incrementSpentExtraRecs",
			increment,
			int(currentBlockHeight),
			s.settings.GetUtxoStoreBlockHeightRetention(),
		))
	}

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		*batchRecordsPtr = batchRecords
		putBatchRecordsSlice(batchRecordsPtr)
		return errors.NewStorageError("[IncrementSpentRecordsMulti] error in aerospike batch with %d records: %s", len(batchRecords), err.Error(), err)
	}

	// Inspect per-record errors
	var aggErr error
	for i := range batchRecords {
		batchRecord := batchRecords[i]
		if batchRecord == nil {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] missing batch record; %s", describeChainHashAt(txids, i), describeAerospikeBatchRecord(batchRecord)))
			continue
		}

		batchRec := batchRecord.BatchRec()
		if batchRec == nil {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] missing batch record; %s", describeChainHashAt(txids, i), describeAerospikeBatchRecord(batchRecord)))
			continue
		}

		if recErr := batchRec.Err; recErr != nil {
			if s.logger != nil {
				s.logger.Errorf("[IncrementSpentRecordsMulti][%s] batch record failed; %s: %s", describeChainHashAt(txids, i), describeAerospikeBatchRecord(batchRecord), recErr.Error())
			}
			aggErr = errors.Join(aggErr, recErr)
			continue
		}

		response := batchRec.Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] missing expected response bin %q; %s", describeChainHashAt(txids, i), LuaSuccess.String(), describeAerospikeBatchRecord(batchRecord)))
			continue
		}

		rawResponse := response.Bins[LuaSuccess.String()]
		parsed, err := s.ParseLuaMapResponse(rawResponse)
		if err != nil {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] failed to parse response bin %q (value %s); %s: %s", describeChainHashAt(txids, i), LuaSuccess.String(), describeAerospikeValue(rawResponse), describeAerospikeBatchRecord(batchRecord), err.Error(), err))
			continue
		}

		if parsed.Status != LuaStatusOK {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] incrementSpentExtraRecs returned %s: %s", describeChainHashAt(txids, i), parsed.Status, parsed.Message))
		}
	}

	*batchRecordsPtr = batchRecords
	putBatchRecordsSlice(batchRecordsPtr)

	return aggErr
}

// SetDAHForChildRecordsMulti expands childCount per tx and performs a single BatchOperate
// to set/unset DeleteAtHeight across all child pagination records.
func (s *Store) SetDAHForChildRecordsMulti(items []struct {
	TxID           *chainhash.Hash
	ChildCount     int
	DeleteAtHeight uint32
}) error {
	// Expand into individual child records
	total := 0
	for _, it := range items {
		if it.ChildCount > 0 {
			total += it.ChildCount
		}
	}
	if total == 0 {
		return nil
	}

	batchRecords := make([]aerospike.BatchRecordIfc, 0, total)
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	dahBinName := fields.DeleteAtHeight.String()
	// Pre-create the "unset" operation since it's identical for all unset cases
	unsetOp := aerospike.PutOp(aerospike.NewBin(dahBinName, nil))

	for _, it := range items {
		for i := uint32(1); i <= uint32(it.ChildCount); i++ { // nolint: gosec
			keySource := uaerospike.CalculateKeySourceInternal(it.TxID, i) // children start at 1
			key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				return errors.NewProcessingError("[SetDAHForChildRecordsMulti][%s] failed to create key for pagination record %d: %v", it.TxID.String(), i, err)
			}

			if it.DeleteAtHeight > 0 {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(dahBinName, it.DeleteAtHeight))))
			} else {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, unsetOp))
			}
		}
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("[SetDAHForChildRecordsMulti] failed to set DAH for %d records: %s", len(batchRecords), err.Error(), err)
	}

	var aggErr error
	for _, br := range batchRecords {
		if br == nil {
			aggErr = errors.Join(aggErr, errors.NewStorageError("[SetDAHForChildRecordsMulti] missing batch record; %s", describeAerospikeBatchRecord(br)))
			continue
		}

		batchRec := br.BatchRec()
		if batchRec == nil {
			aggErr = errors.Join(aggErr, errors.NewStorageError("[SetDAHForChildRecordsMulti] missing batch record; %s", describeAerospikeBatchRecord(br)))
			continue
		}

		if recErr := batchRec.Err; recErr != nil {
			if s.logger != nil {
				s.logger.Errorf("[SetDAHForChildRecordsMulti] batch record failed; %s: %s", describeAerospikeBatchRecord(br), recErr.Error())
			}
			aggErr = errors.Join(aggErr, recErr)
		}
	}

	return aggErr
}

// batchIncrement handles record count updates for paginated transactions
type batchIncrement struct {
	txID      *chainhash.Hash               // Transaction hash
	increment int                           // Count adjustment
	res       chan incrementSpentRecordsRes // Result channel
}

type batchDAH struct {
	txID           *chainhash.Hash // Transaction hash
	childIdx       uint32          // Child record index
	deleteAtHeight uint32          // DeleteAtHeight (0 = no delete)
	errCh          chan error      // Error Result channel
}

// handleSpendPanic processes a recovered value from Spend's deferred recover
// and propagates it as an error. Without this, a panic during Spend would be
// logged but the caller would observe (nil, nil) — a silent failure that can
// mask UTXO state corruption.
//
// Uses ERR_UNKNOWN rather than ERR_PROCESSING so the block-validation retry
// classifier (services/blockvalidation/BlockValidation.go) does not treat a
// recovered panic as a transient infrastructure error and retry indefinitely
// against a broken path.
func handleSpendPanic(recovered any, err *error, logger ulogger.Logger) {
	if recovered == nil {
		return
	}

	prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
	logger.Errorf("ERROR panic in aerospike Spend: %v\n%s", recovered, debug.Stack())

	if *err == nil {
		*err = errors.NewUnknownError("panic in Spend: %v", recovered)
	}
}

// Spend marks UTXOs as spent in a batch operation.
// The function:
//  1. Validates inputs
//  2. Batches spend requests
//  3. Handles responses
//  4. Manages rollback on failure
//
// Parameters:
//   - ctx: Context for cancellation
//   - tx: tx to spend
//
// Error handling:
//   - Rolls back successful spends on partial failure
//   - Handles panic recovery
//   - Reports metrics for failures
//
// Example return value:
//
//	spends := []*utxo.Spend{
//	    {
//	        TxID: txHash,
//	        Vout: 0,
//	        UTXOHash: utxoHash,
//	        SpendingTxID: spendingTxHash,
//	    },
//	}
//
//	doubleSpendConflicts := []*chainhash.Hash{
//	    &spendingTxHash,
//	}
//
//	err := store.Spend(ctx, tx)
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) (spends []*utxo.Spend, err error) {
	defer func() {
		handleSpendPanic(recover(), &err, s.logger)
	}()

	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	spends, err = utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	var (
		mu sync.Mutex
		g  = errgroup.Group{}

		spentSpends     = make([]*utxo.Spend, 0, len(spends))
		txAlreadyExists bool
	)

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		idx := idx
		spend := spend

		g.Go(func() error {
			// Per-worker panic recovery. The parent's defer only catches panics in the
			// parent goroutine — errgroup propagates errors but does not recover panics
			// inside g.Go bodies, so without this a worker panic would crash the process.
			defer func() {
				handleSpendPanic(recover(), &spends[idx].Err, s.logger)
			}()

			// Fast-fail check: if circuit breaker is already open, reject immediately
			if s.spendCircuitBreaker != nil && !s.spendCircuitBreaker.Allow() {
				spends[idx].Err = errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request")
				return nil
			}

			errCh := make(chan error, 1)
			s.spendBatcher.PutCtx(ctx, &batchSpend{
				spend:             spend,
				blockHeight:       blockHeight,
				errCh:             errCh,
				ignoreConflicting: useIgnoreConflicting,
				ignoreLocked:      useIgnoreLocked,
			})

			// Wait for batch response with timeout to prevent indefinite blocking
			var batchErr error
			spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
			if spendTimeout <= 0 {
				spendTimeout = 30 * time.Second
			}

			timer := time.NewTimer(spendTimeout)
			defer timer.Stop()

			select {
			case batchErr = <-errCh:
				// Batch completed successfully or with error
			case <-ctx.Done():
				spends[idx].Err = errors.NewContextCanceledError("[SPEND][%s:%d] context canceled while waiting for batch response", spend.TxID.String(), spend.Vout)
				return nil
			case <-timer.C:
				if prometheusUtxoMapErrors != nil {
					prometheusUtxoMapErrors.WithLabelValues("Spend", "BatchTimeout").Inc()
				}
				spends[idx].Err = errors.NewServiceUnavailableError("[SPEND][%s:%d] batch operation timed out after %s", spend.TxID.String(), spend.Vout, spendTimeout)
				return nil
			}

			if batchErr != nil && errors.Is(batchErr, errors.ErrTxNotFound) {
				mu.Lock()
				exists := txAlreadyExists
				mu.Unlock()
				// the parent transaction was not found, this can happen when the parent tx has been DAH'd and removed from
				// the utxo store. We can check whether the tx already exists, which means it has been validated and
				// blessed. In this case we can just return early.
				if exists {
					// we've previously validated that this tx already exists, no point doing a lookup again or logging anything
					batchErr = nil
				} else if _, batchErr = s.Get(ctx, tx.TxIDChainHash()); batchErr == nil {
					s.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", tx.TxID())

					batchErr = nil

					mu.Lock()
					txAlreadyExists = true
					mu.Unlock()
				}
			}

			if batchErr != nil {
				spends[idx].Err = batchErr

				s.logger.Debugf("[SPEND][%s:%d] error in aerospike spend: %+v", spend.TxID.String(), spend.Vout, spend.Err)

				var errSpent *errors.UtxoSpentErrData
				if errors.AsData(batchErr, &errSpent) {
					spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
				}

				// s.logger.Errorf("error in aerospike spend (batched mode) %s: %v\n", spends[idx].TxID.String(), spends[idx].Err)

				// don't stop processing the rest of the batch, we want to see all errors
				return nil
			}

			mu.Lock()
			spentSpends = append(spentSpends, spend)
			mu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewError("error in aerospike spend (batched mode)", err)
	}

	if len(spends) != len(spentSpends) { // there must have been failures
		// Only rollback successful spends when the transaction is genuinely invalid
		// (double-spend, frozen, conflicting, hash mismatch). For transient infrastructure
		// errors (DEVICE_OVERLOAD, timeout, etc.), skip the rollback — the Lua spend
		// script is idempotent for the same spender, so successful spends can safely
		// remain and will be silently skipped on retry.
		if needsSpendRollback(spends) {
			unspendErr := s.Unspend(context.Background(), spentSpends)
			if unspendErr != nil {
				s.logger.Errorf("error in aerospike unspend (batched mode): %v", unspendErr)
			}
		}

		var spendErrors error

		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}

		// return the errors found
		return spends, errors.NewUtxoError("error in aerospike spend (batched mode) - errors", spendErrors)
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return spends, nil
}

// needsSpendRollback returns true if any spend failed due to a validation error
// that indicates the transaction is genuinely invalid. Only explicit Lua-level
// validation failures trigger rollback — infrastructure errors (DEVICE_OVERLOAD,
// timeout, etc.) do not, because the Lua spend script is idempotent for the
// same spender and successful spends will be silently skipped on retry.
func needsSpendRollback(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend.Err == nil {
			continue
		}
		if errors.Is(spend.Err, errors.ErrSpent) ||
			errors.Is(spend.Err, errors.ErrTxConflicting) ||
			errors.Is(spend.Err, errors.ErrFrozen) ||
			errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}
	return false
}

type keyIgnoreLocked struct {
	key               *aerospike.Key
	hash              *chainhash.Hash
	blockHeight       uint32
	ignoreConflicting bool
	ignoreLocked      bool
}

// useExpressionSpend returns true when the expression-based spend path is safe for
// the configured store. Multi-UTXO records (utxoBatchSize > 1) require Lua because
// Aerospike expressions cannot byte-compare list elements, so the offset alone cannot
// uniquely identify the target UTXO and ListSetOp would mutate the wrong slot.
func (s *Store) useExpressionSpend() bool {
	return s.settings.Aerospike.EnableSpendFilterExpressions && s.utxoBatchSize == 1
}

// sendSpendBatchLua processes a batch of spend requests via Lua scripts or expressions.
// The function:
//  1. Groups spends by transaction
//  2. Creates batch UDF operations or expression-based operations
//  3. Executes Lua scripts or expressions
//  4. Handles responses and errors
//  5. Manages DAH settings
//  6. Updates external storage
func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	// Use expression-based implementation only when each Aerospike record holds a single
	// UTXO (utxoBatchSize == 1). With multiple UTXOs per record, the expression cannot
	// byte-compare the specific UTXO hash at a list offset, so we fall back to Lua which
	// performs the strict precondition check inside the UDF.
	if s.useExpressionSpend() {
		s.SpendMultiWithExpressions(s.ctx, batch)
		return
	}

	start := time.Now()
	stat := gocore.NewStat("sendSpendBatchLua")

	ctx, _, deferFn := tracing.Tracer("aerospike").Start(s.ctx, "sendSpendBatchLua",
		tracing.WithParentStat(stat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)
	s.logSpendBatchStart(batchID, len(batch))

	// Prepare and execute batch
	batchesByKey, err := s.prepareSpendBatches(batch, batchID)
	if err != nil {
		return
	}

	batchRecords, batchRecordKeys := s.createBatchRecords(batchesByKey)

	if err := s.executeSpendBatch(batchRecords, batch, batchID); err != nil {
		return
	}

	// Process results
	s.processSpendBatchResults(ctx, batchRecords, batchRecordKeys, batchesByKey, batch, batchID)
	stat.NewStat("postBatchOperate").AddTime(start)
}

// logSpendBatchStart logs the start of a spend batch if verbose debug is enabled
func (s *Store) logSpendBatchStart(batchID uint64, batchSize int) {
	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[spendMulti] sending batch %d of %d spends", batchID, batchSize)
	}
}

// prepareSpendBatches groups spends by key and validates them
func (s *Store) prepareSpendBatches(batch []*batchSpend, batchID uint64) (map[keyIgnoreLocked][]aerospike.MapValue, error) {
	aeroKeyMap := make(map[string]*aerospike.Key)
	batchesByKey := make(map[keyIgnoreLocked][]aerospike.MapValue, len(batch))

	for idx, bItem := range batch {
		if err := s.validateSpendItem(bItem); err != nil {
			s.sendSpendBatchItemError(batch, idx, err)
			continue
		}

		key, err := s.getOrCreateAerospikeKey(bItem, s.utxoBatchSize, aeroKeyMap)
		if err != nil {
			s.sendSpendBatchItemError(batch, idx, err)
			continue
		}

		mapValue := s.createSpendMapValue(idx, bItem)
		useKey := keyIgnoreLocked{
			key:               key,
			hash:              bItem.spend.TxID,
			blockHeight:       bItem.blockHeight,
			ignoreConflicting: bItem.ignoreConflicting,
			ignoreLocked:      bItem.ignoreLocked,
		}

		batchesByKey[useKey] = append(batchesByKey[useKey], mapValue)
	}

	return batchesByKey, nil
}

// getOrCreateAerospikeKey gets or creates an Aerospike key for the spend
func (s *Store) getOrCreateAerospikeKey(bItem *batchSpend, utxoBatchSize int, keyMap map[string]*aerospike.Key) (*aerospike.Key, error) {
	keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout, utxoBatchSize)
	keySourceStr := string(keySource)

	if key, ok := keyMap[keySourceStr]; ok {
		return key, nil
	}

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		return nil, errors.NewProcessingError("[spendMulti][%s] failed to init new aerospike key for spend", describeBatchSpend(bItem), err)
	}

	keyMap[keySourceStr] = key
	return key, nil
}

// validateSpendItem validates that the spend item has all required data
func (s *Store) validateSpendItem(bItem *batchSpend) error {
	if bItem == nil {
		return errors.NewProcessingError("[spendMulti][<nil>] batch item is nil")
	}
	if bItem.spend == nil {
		return errors.NewProcessingError("[spendMulti][%s] spend is nil", describeBatchSpend(bItem))
	}
	if bItem.spend.TxID == nil {
		return errors.NewProcessingError("[spendMulti][%s] txid is nil", describeBatchSpend(bItem))
	}
	if bItem.spend.UTXOHash == nil {
		return errors.NewProcessingError("[spendMulti][%s] utxo hash is nil", describeBatchSpend(bItem))
	}
	if bItem.spend.SpendingData == nil {
		return errors.NewProcessingError("[spendMulti][%s] spending data is nil", describeBatchSpend(bItem))
	}
	return nil
}

// createSpendMapValue creates the map value for a spend item
func (s *Store) createSpendMapValue(idx int, bItem *batchSpend) aerospike.MapValue {
	return aerospike.NewMapValue(map[any]any{
		"idx":          idx,
		"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
		"vOut":         bItem.spend.Vout,
		"utxoHash":     bItem.spend.UTXOHash[:],
		"spendingData": bItem.spend.SpendingData.Bytes(),
	})
}

// createBatchRecords creates the batch records for Aerospike operations
func (s *Store) createBatchRecords(batchesByKey map[keyIgnoreLocked][]aerospike.MapValue) ([]aerospike.BatchRecordIfc, []keyIgnoreLocked) {
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batchesByKey))
	batchRecordKeys := make([]keyIgnoreLocked, 0, len(batchesByKey))
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for batchKey, batchItems := range batchesByKey {
		useLuaPackage := LuaPackage
		if s.settings.Aerospike.SeparateSpendUDFModuleCount > 0 {
			// determine which lua package to use for spends, based on the first byte of the tx id, there will be N packages (0 to N-1)
			useLuaPackage = s.spendLuaPackages[batchKey.hash[0]%uint8(s.settings.Aerospike.SeparateSpendUDFModuleCount)]
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(
			batchUDFPolicy, useLuaPackage, batchKey.key, subOpSpendMulti, "spendMulti",
			batchItems,
			batchKey.ignoreConflicting,
			batchKey.ignoreLocked,
			batchKey.blockHeight,
			s.settings.GetUtxoStoreBlockHeightRetention(),
		))
		batchRecordKeys = append(batchRecordKeys, batchKey)
	}

	return batchRecords, batchRecordKeys
}

// executeSpendBatch executes the batch operation
func (s *Store) executeSpendBatch(batchRecords []aerospike.BatchRecordIfc, batch []*batchSpend, batchID uint64) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for idx, bItem := range batch {
			s.sendSpendBatchItemError(batch, idx, errors.NewStorageError("[spendMulti][%s] BatchOperate failed for batchId %d itemIdx %d: %s", describeBatchSpend(bItem), batchID, idx, err.Error(), err))
		}
		return err
	}
	return nil
}

// processSpendBatchResults processes the results of the batch operation
func (s *Store) processSpendBatchResults(ctx context.Context, batchRecords []aerospike.BatchRecordIfc, batchRecordKeys []keyIgnoreLocked, batchesByKey map[keyIgnoreLocked][]aerospike.MapValue, batch []*batchSpend, batchID uint64) {
	for batchIdx, batchRecord := range batchRecords {
		key := batchRecordKeys[batchIdx]
		batchByKey, ok := batchesByKey[key]
		if !ok {
			s.logger.Errorf("[spendMulti] could not find batch key for batchIdx %d", batchIdx)
			continue
		}

		txID := spendBatchGroupTxID(batchByKey, batch)
		s.processSingleBatchResult(ctx, batchRecord, batchByKey, batch, txID, key.blockHeight, batchID)
	}

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[spendMulti] sending batch %d of %d spends DONE", batchID, len(batch))
	}
}

// processSingleBatchResult processes a single batch record result
func (s *Store) processSingleBatchResult(ctx context.Context, batchRecord aerospike.BatchRecordIfc, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	if batchRecord == nil {
		s.handleMissingResponse(batchRecord, batchByKey, batch, txID, thisBlockHeight, batchID)
		return
	}

	batchRec := batchRecord.BatchRec()
	if batchRec == nil {
		s.handleMissingResponse(batchRecord, batchByKey, batch, txID, thisBlockHeight, batchID)
		return
	}

	batchErr := batchRec.Err
	if batchErr != nil {
		s.handleBatchError(batchRecord, batchByKey, batch, thisBlockHeight, batchID, batchErr)
		return
	}

	response := batchRec.Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		s.handleMissingResponse(batchRecord, batchByKey, batch, txID, thisBlockHeight, batchID)
		return
	}

	res, parseErr := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
	if parseErr != nil {
		s.handleParseError(batchRecord, response.Bins[LuaSuccess.String()], batchByKey, batch, txID, thisBlockHeight, batchID, parseErr)
		return
	}

	// Handle signals
	if res.Signal != "" {
		s.handleSpendSignal(ctx, res.Signal, txID, res.ChildCount, thisBlockHeight)
	}

	// Process based on status
	switch res.Status {
	case LuaStatusOK:
		s.handleSuccessfulSpends(batchByKey, batch)
	case LuaStatusError:
		s.handleErrorSpends(res, batchByKey, batch, txID, thisBlockHeight, batchID)
	}
}

// handleBatchError handles errors from batch operations
func (s *Store) handleBatchError(batchRecord aerospike.BatchRecordIfc, batchByKey []aerospike.MapValue, batch []*batchSpend, thisBlockHeight uint32, batchID uint64, err error) {
	diagnostics := describeAerospikeBatchRecord(batchRecord)
	group := describeSpendBatchGroup(batchByKey, batch)
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			continue
		}
		s.sendSpendBatchItemError(batch, idx, errors.NewStorageError("[spendMulti][%s] aerospike spend batch record failed, batchId %d blockHeight %d; %s; %s: %s", describeBatchSpendAt(batch, idx), batchID, thisBlockHeight, diagnostics, group, err.Error(), err))
	}
	// Record batch-level failure for circuit breaker
	if s.spendCircuitBreaker != nil {
		s.spendCircuitBreaker.RecordFailure()
	}
}

// handleMissingResponse handles missing response from batch operation
func (s *Store) handleMissingResponse(batchRecord aerospike.BatchRecordIfc, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	diagnostics := describeAerospikeBatchRecord(batchRecord)
	group := describeSpendBatchGroup(batchByKey, batch)
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			continue
		}
		s.sendSpendBatchItemError(batch, idx, errors.NewProcessingError("[spendMulti][%s] missing expected response bin %q, batchId %d blockHeight %d; %s; %s", describeChainHash(txID), LuaSuccess.String(), batchID, thisBlockHeight, diagnostics, group))
	}
}

// handleParseError handles parse errors from response
func (s *Store) handleParseError(batchRecord aerospike.BatchRecordIfc, rawResponse interface{}, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64, err error) {
	diagnostics := describeAerospikeBatchRecord(batchRecord)
	group := describeSpendBatchGroup(batchByKey, batch)
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			continue
		}
		s.sendSpendBatchItemError(batch, idx, errors.NewProcessingError("[spendMulti][%s] failed to parse response bin %q (value %s), batchId %d blockHeight %d; %s; %s: %s", describeChainHash(txID), LuaSuccess.String(), describeAerospikeValue(rawResponse), batchID, thisBlockHeight, diagnostics, group, err.Error(), err))
	}
}

func spendBatchItemIndex(batchItem aerospike.MapValue) (int, bool) {
	if batchItem == nil {
		return 0, false
	}

	idx, ok := batchItem["idx"].(int)
	return idx, ok
}

func spendBatchGroupTxID(batchByKey []aerospike.MapValue, batch []*batchSpend) *chainhash.Hash {
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok || idx < 0 || idx >= len(batch) {
			continue
		}
		if batch[idx] == nil || batch[idx].spend == nil {
			continue
		}
		return batch[idx].spend.TxID
	}
	return nil
}

func (s *Store) sendSpendBatchItemError(batch []*batchSpend, idx int, err error) {
	if idx < 0 || idx >= len(batch) || batch[idx] == nil {
		if s.logger != nil {
			s.logger.Errorf("[spendMulti] unable to send batch item result for idx=%d: %v", idx, err)
		}
		return
	}
	s.sendSpendBatchError(batch[idx], err)
}

func (s *Store) sendSpendBatchError(bItem *batchSpend, err error) {
	if bItem == nil {
		if s.logger != nil {
			s.logger.Errorf("[spendMulti] unable to send batch item result for nil item: %v", err)
		}
		return
	}
	if bItem.errCh == nil {
		if s.logger != nil {
			s.logger.Errorf("[spendMulti][%s] unable to send batch item result because errCh is nil: %v", describeBatchSpend(bItem), err)
		}
		return
	}

	bItem.errCh <- err
}

func describeBatchSpendAt(batch []*batchSpend, idx int) string {
	if idx < 0 || idx >= len(batch) {
		return "<unknown>"
	}
	return describeBatchSpend(batch[idx])
}

func describeBatchSpend(bItem *batchSpend) string {
	if bItem == nil {
		return "<nil>"
	}
	if bItem.spend == nil {
		return "spend=<nil>"
	}
	return describeUTXOSpend(bItem.spend)
}

func describeSpendBatchGroup(batchByKey []aerospike.MapValue, batch []*batchSpend) string {
	if len(batchByKey) == 0 {
		return "group=[]"
	}

	const maxItems = 6
	parts := make([]string, 0, min(len(batchByKey), maxItems))
	for itemIdx, batchItem := range batchByKey {
		if itemIdx == maxItems {
			parts = append(parts, fmt.Sprintf("...+%d more", len(batchByKey)-itemIdx))
			break
		}

		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			parts = append(parts, fmt.Sprintf("idx=<invalid> offset=%v", batchItem["offset"]))
			continue
		}

		spendDesc := "<unknown>"
		vout := batchItem["vOut"]
		if idx >= 0 && idx < len(batch) && batch[idx] != nil {
			spendDesc = describeBatchSpend(batch[idx])
			if batch[idx].spend != nil {
				vout = batch[idx].spend.Vout
			}
		}

		parts = append(parts, fmt.Sprintf("idx=%d spend=%s vout=%v offset=%v", idx, spendDesc, vout, batchItem["offset"]))
	}

	return "group=[" + strings.Join(parts, "; ") + "]"
}

// handleSpendSignal handles signals from spend operations
func (s *Store) handleSpendSignal(ctx context.Context, signal LuaSignal, txID *chainhash.Hash, childCount int, thisBlockHeight uint32) {
	if txID == nil {
		s.logger.Errorf("[spendMulti] cannot handle signal %s with nil txid", signal)
		return
	}

	switch signal {
	case LuaSignalAllSpent:
		if err := s.handleExtraRecords(ctx, txID, 1); err != nil {
			s.logger.Errorf("Failed to handle extra records: %v", err)
		}

	case LuaSignalDAHSet:
		// Only set DAH if BlockHeightRetention is configured (> 0)
		// When retention is 0, it means "don't use automatic retention"
		if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
			dahHeight := thisBlockHeight + retention

			if err := s.SetDAHForChildRecords(txID, childCount, dahHeight); err != nil {
				s.logger.Errorf("Failed to set DAH for child records: %v", err)
			}
			// External store DAH is disabled - lifecycle managed by pruner service
		}

	case LuaSignalDAHUnset:
		if err := s.SetDAHForChildRecords(txID, childCount, aerospike.TTLDontExpire); err != nil {
			s.logger.Errorf("Failed to unset DAH for child records: %v", err)
		}
		// External store DAH is disabled - lifecycle managed by pruner service
	}
}

// handleSuccessfulSpends handles successful spend operations
func (s *Store) handleSuccessfulSpends(batchByKey []aerospike.MapValue, batch []*batchSpend) {
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			continue
		}
		s.sendSpendBatchItemError(batch, idx, nil)
	}
	// Record successful batch operation for circuit breaker
	if s.spendCircuitBreaker != nil {
		s.spendCircuitBreaker.RecordSuccess()
	}
}

// handleErrorSpends handles error responses from spend operations
func (s *Store) handleErrorSpends(res *LuaMapResponse, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	if res == nil {
		for _, batchItem := range batchByKey {
			idx, ok := spendBatchItemIndex(batchItem)
			if !ok {
				continue
			}
			s.sendSpendBatchItemError(batch, idx, errors.NewStorageError("[spendMulti][%s] nil error response, blockHeight %d: %d", describeChainHash(txID), thisBlockHeight, batchID))
		}
		return
	}

	if res.Message != "" {
		// General error for all spends
		generalErr := s.createGeneralError(res.ErrorCode, txID, thisBlockHeight, batchID, res.Message)
		for _, batchItem := range batchByKey {
			idx, ok := spendBatchItemIndex(batchItem)
			if !ok {
				continue
			}
			s.sendSpendBatchItemError(batch, idx, generalErr)
		}
	} else if res.Errors != nil {
		// Individual errors for specific spends
		s.handleIndividualErrors(res.Errors, batchByKey, batch, txID)
	} else {
		// ERROR status but no message or errors
		for _, batchItem := range batchByKey {
			idx, ok := spendBatchItemIndex(batchItem)
			if !ok {
				continue
			}
			s.sendSpendBatchItemError(batch, idx, errors.NewStorageError("[spendMulti][%s] error in spendMulti batch record, blockHeight %d: %d - %v", describeChainHash(txID), thisBlockHeight, batchID, res))
		}
	}
}

// createGeneralError creates a general error based on error code
func (s *Store) createGeneralError(errorCode LuaErrorCode, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64, message string) error {
	switch errorCode {
	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[spendMulti][%s] transaction is frozen, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	case LuaErrorCodeConflicting:
		return errors.NewTxConflictingError("[spendMulti][%s] transaction is conflicting, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	case LuaErrorCodeLocked:
		return errors.NewTxLockedError("[spendMulti][%s] transaction is locked, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	case LuaErrorCodeCreating:
		return errors.NewTxCreatingError("[spendMulti][%s] transaction is creating, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	case LuaErrorCodeCoinbaseImmature:
		return errors.NewTxCoinbaseImmatureError("[spendMulti][%s] coinbase is locked, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	case LuaErrorCodeTxNotFound:
		return errors.NewTxNotFoundError("[spendMulti][%s] transaction not found, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	default:
		return errors.NewStorageError("[spendMulti][%s] error in spendMulti batch record, blockHeight %d: %d - %s", describeChainHash(txID), thisBlockHeight, batchID, message)
	}
}

// handleIndividualErrors handles individual errors for specific spends
func (s *Store) handleIndividualErrors(errors map[int]LuaErrorInfo, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash) {
	for _, batchItem := range batchByKey {
		idx, ok := spendBatchItemIndex(batchItem)
		if !ok {
			continue
		}

		if errMsg, hasError := errors[idx]; hasError {
			s.sendSpendBatchItemError(batch, idx, s.createSpendError(errMsg, batch[idx], txID))
		} else {
			s.sendSpendBatchItemError(batch, idx, nil)
		}
	}
}

// createSpendError creates an error for a specific spend
func (s *Store) createSpendError(errMsg LuaErrorInfo, batchItem *batchSpend, txID *chainhash.Hash) error {
	if batchItem == nil || batchItem.spend == nil {
		return errors.NewStorageError("[spendMulti][%s] cannot create spend error for nil batch item: %s", describeChainHash(txID), errMsg.Message)
	}

	switch errMsg.ErrorCode {
	case LuaErrorCodeSpent:
		if errMsg.SpendingData != "" {
			spendingData, parseErr := spendpkg.NewSpendingDataFromString(errMsg.SpendingData)
			if parseErr != nil {
				return errors.NewStorageError("[spendMulti][%s] invalid spending data in error: %s", describeChainHash(txID), errMsg.SpendingData)
			}

			if batchItem.spend.TxID == nil || batchItem.spend.UTXOHash == nil {
				return errors.NewStorageError("[spendMulti][%s] cannot create spent error with nil txid or utxo hash for %s", describeChainHash(txID), describeBatchSpend(batchItem))
			}
			return errors.NewUtxoSpentError(*batchItem.spend.TxID, batchItem.spend.Vout, *batchItem.spend.UTXOHash, spendingData)
		}

		return errors.NewStorageError("[spendMulti][%s] UTXO already spent but no spending data provided", describeChainHash(txID))

	case LuaErrorCodeInvalidSpend:
		return errors.NewUtxoError("[spendMulti][%s] invalid spend for vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[spendMulti][%s] UTXO is frozen, vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozenUntil:
		return errors.NewUtxoFrozenError("[spendMulti][%s] UTXO frozen until block, vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoNotFound:
		return errors.NewTxNotFoundError("[spendMulti][%s] UTXO not found for vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoHashMismatch:
		return errors.NewUtxoHashMismatchError("[spendMulti][%s] UTXO hash mismatch for vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoInvalidSize:
		return errors.NewUtxoInvalidSize("[spendMulti][%s] UTXO invalid size for vout %d: %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.Message)

	default:
		return errors.NewStorageError("[spendMulti][%s] error for vout %d (code: %s): %s", describeChainHash(txID), batchItem.spend.Vout, errMsg.ErrorCode, errMsg.Message)
	}
}

// SetDAHForChildRecords sets DAH for all child records of a transaction
func (s *Store) SetDAHForChildRecords(txID *chainhash.Hash, childCount int, dah uint32) error {
	errs := make([]error, childCount)

	for i := uint32(0); i < uint32(childCount); i++ { // nolint: gosec
		errCh := make(chan error)

		go func() {
			s.setDAHBatcher.Put(&batchDAH{
				txID:           txID,
				childIdx:       i + 1, // We want to set DAH for child record i+1
				deleteAtHeight: dah,
				errCh:          errCh,
			})
		}()

		errs[i] = <-errCh
		if errs[i] != nil {
			s.logger.Errorf("[setDAHForChildRecords][%s] failed to set DAH for child record %d: %v", describeChainHash(txID), i, errs[i])
		}
	}

	var errorsFound bool

	for _, err := range errs {
		if err != nil {
			errorsFound = true
			break
		}
	}

	if errorsFound {
		return errors.NewStorageError("[setDAHForChildRecords][%s] failed to set DAH for one or more child records", describeChainHash(txID))
	}

	return nil
}

// handleExtraRecords manages the record count for paginated transactions when UTXOs are spent.
// This function is called when spending operations affect transactions with multiple records
// to maintain accurate pagination counts for cleanup operations.
//
// Parameters:
//   - ctx: Context for cancellation
//   - txID: Transaction ID whose record count needs updating
//   - increment: Amount to increment (can be negative for decrement)
//
// Returns:
//   - error: Any error encountered during the record count update
func (s *Store) handleExtraRecords(ctx context.Context, txID *chainhash.Hash, increment int) error {
	res, err := s.IncrementSpentRecords(txID, increment) // This is a batch operation
	if err != nil {
		return err
	}

	// Parse the map response
	ret, err := s.ParseLuaMapResponse(res)
	if err != nil {
		wrappedErr := errors.NewProcessingError("[spendMulti][%s] failed to parse IncrementSpentRecords response (value %s): %s", describeChainHash(txID), describeAerospikeValue(res), err.Error(), err)
		s.logger.Errorf("%v", wrappedErr)
		return wrappedErr
	}

	if ret.Status == LuaStatusOK {
		if ret.Signal != "" {
			switch ret.Signal {
			case LuaSignalDAHSet:
				// Only set DAH if BlockHeightRetention is configured (> 0)
				// When retention is 0, it means "don't use automatic retention"
				if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
					// Sanity check: verify all children are actually spent before
					// setting DAH. The spentExtraRecs counter can drift due to
					// interrupted rollbacks, so we don't trust it blindly.
					if ret.ChildCount > 0 {
						allSpent, verifyErr := s.verifyAllChildrenSpent(ctx, txID, ret.ChildCount)
						if verifyErr != nil {
							s.logger.Errorf("[handleExtraRecords][%s] failed to verify children: %v", describeChainHash(txID), verifyErr)
							return verifyErr
						}
						if !allSpent {
							s.logger.Warnf("[handleExtraRecords][%s] spentExtraRecs triggered DAH but not all children are spent — counter drift detected, clearing master DAH", describeChainHash(txID))
							// Lua already set DAH on the master record inline.
							// Clear it since children aren't actually all-spent.
							errCh := make(chan error, 1)
							s.setDAHBatcher.PutCtx(ctx, &batchDAH{
								txID:           txID,
								childIdx:       0, // master record
								deleteAtHeight: 0, // clear DAH
								errCh:          errCh,
							})
							if dahErr := <-errCh; dahErr != nil {
								s.logger.Errorf("[handleExtraRecords][%s] failed to clear drifted master DAH: %v", describeChainHash(txID), dahErr)
							}
							return nil
						}
					}

					thisBlockHeight := s.blockHeight.Load()
					dah := thisBlockHeight + retention

					if err := s.SetDAHForChildRecords(txID, ret.ChildCount, dah); err != nil {
						return err
					}
					// External store DAH is disabled - lifecycle managed by pruner service
				}

			case LuaSignalDAHUnset:
				if err := s.SetDAHForChildRecords(txID, ret.ChildCount, 0); err != nil {
					return err
				}
				// External store DAH is disabled - lifecycle managed by pruner service
			}
		}
	} else if ret.Status == LuaStatusError {
		return errors.NewStorageError("[spendMulti][%s] failed to handleExtraRecords: %v", describeChainHash(txID), ret.Message)
	}

	return nil
}

// verifyAllChildrenSpent batch-reads all child records and checks if every
// child has spentUtxos == recordUtxos. Used as a sanity check before setting
// DAH — the spentExtraRecs counter can drift during interrupted rollbacks,
// so we verify the actual child state before trusting it.
func (s *Store) verifyAllChildrenSpent(ctx context.Context, txID *chainhash.Hash, childCount int) (bool, error) {
	if txID == nil {
		return false, errors.NewProcessingError("[verifyAllChildrenSpent][<nil>] txid is nil")
	}
	if childCount == 0 {
		return true, nil
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	readPolicy := aerospike.NewBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, 0, childCount)

	for i := uint32(1); i <= uint32(childCount); i++ { // nolint: gosec
		keySource := uaerospike.CalculateKeySourceInternal(txID, i)
		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return false, errors.NewProcessingError("[verifyAllChildrenSpent][%s] failed to create key for child %d", describeChainHash(txID), i, err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchRead(
			readPolicy,
			key,
			[]string{fields.SpentUtxos.String(), fields.RecordUtxos.String()},
		))
	}

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] batch read failed", describeChainHash(txID), err)
	}

	for i, br := range batchRecords {
		if br == nil {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] child %d read returned nil batch record", describeChainHash(txID), i+1)
		}
		rec := br.BatchRec()
		if rec == nil {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] child %d read returned nil batch rec; %s", describeChainHash(txID), i+1, describeAerospikeBatchRecord(br))
		}
		if rec.Err != nil {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] child %d read failed", describeChainHash(txID), i+1, rec.Err)
		}
		if rec.Record == nil || rec.Record.Bins == nil {
			return false, nil
		}

		spentUtxos, ok := rec.Record.Bins[fields.SpentUtxos.String()].(int)
		if !ok {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] invalid type for spentUtxos in child %d", describeChainHash(txID), i+1)
		}
		recordUtxos, ok := rec.Record.Bins[fields.RecordUtxos.String()].(int)
		if !ok {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] invalid type for recordUtxos in child %d", describeChainHash(txID), i+1)
		}

		if spentUtxos != recordUtxos {
			return false, nil
		}
	}

	return true, nil
}

type incrementSpentRecordsRes struct {
	res interface{}
	err error
}

// IncrementSpentRecords updates the record count for paginated transactions.
// Used for cleanup management of large transactions.
func (s *Store) IncrementSpentRecords(txid *chainhash.Hash, increment int) (interface{}, error) {
	res := make(chan incrementSpentRecordsRes, 1)

	go func() {
		s.incrementBatcher.Put(&batchIncrement{
			txID:      txid,
			increment: increment,
			res:       res,
		})
	}()

	spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
	if spendTimeout <= 0 {
		spendTimeout = 30 * time.Second
	}

	timer := time.NewTimer(spendTimeout)
	defer timer.Stop()

	select {
	case response := <-res:
		return response.res, response.err
	case <-timer.C:
		if prometheusUtxoMapErrors != nil {
			prometheusUtxoMapErrors.WithLabelValues("IncrementSpentRecords", "BatchTimeout").Inc()
		}
		return nil, errors.NewServiceUnavailableError("[IncrementSpentRecords][%s] batch operation timed out after %s", describeChainHash(txid), spendTimeout)
	}
}

func (s *Store) sendIncrementBatch(batch []*batchIncrement) {
	var err error

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	// Create a batch of records to read, with a max size of the batch
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchItems := make([]*batchIncrement, 0, len(batch))

	currentBlockHeight := s.blockHeight.Load()

	// Create a batch of records to read from the txHashes
	for _, item := range batch {
		if item == nil {
			if s.logger != nil {
				s.logger.Errorf("[IncrementSpentRecords] nil batch item")
			}
			continue
		}
		if item.txID == nil {
			s.sendIncrementBatchResult(item, nil, errors.NewProcessingError("failed to init new aerospike key for txMeta: txid is nil"))
			continue
		}

		aeroKey, err := aerospike.NewKey(s.namespace, s.setName, item.txID[:])
		if err != nil {
			s.sendIncrementBatchResult(item, nil, errors.NewProcessingError("failed to init new aerospike key for txMeta", err))
			continue
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(
			batchUDFPolicy, LuaPackage, aeroKey, subOpIncrementSpentExtraRec, "incrementSpentExtraRecs",
			item.increment,
			int(currentBlockHeight),
			s.settings.GetUtxoStoreBlockHeightRetention(),
		))
		batchItems = append(batchItems, item)
	}

	// send the batch to aerospike
	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			s.sendIncrementBatchResult(item, nil, errors.NewStorageError("[IncrementSpentRecords][%s] BatchOperate failed: %s", describeBatchIncrement(item), err.Error(), err))
		}

		return
	}

	// Process the batch records
	for idx, batchRecordIfc := range batchRecords {
		item := batchIncrementAt(batchItems, idx)
		if batchRecordIfc == nil {
			s.sendIncrementBatchResult(item, nil, errors.NewStorageError("[IncrementSpentRecords][%s] missing batch record; %s", describeBatchIncrement(item), describeAerospikeBatchRecord(batchRecordIfc)))
			continue
		}

		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord == nil {
			s.sendIncrementBatchResult(item, nil, errors.NewStorageError("[IncrementSpentRecords][%s] missing batch record; %s", describeBatchIncrement(item), describeAerospikeBatchRecord(batchRecordIfc)))
			continue
		}

		if batchRecord.Err != nil {
			s.sendIncrementBatchResult(item, nil, errors.NewStorageError("[IncrementSpentRecords][%s] batch record failed; %s: %s", describeBatchIncrement(item), describeAerospikeBatchRecord(batchRecordIfc), batchRecord.Err.Error(), batchRecord.Err))
			continue
		}

		// Get the raw response from Lua
		if batchRecord.Record == nil || batchRecord.Record.Bins == nil {
			s.sendIncrementBatchResult(item, nil, errors.NewProcessingError("[IncrementSpentRecords][%s] missing expected response bin %q; %s", describeBatchIncrement(item), LuaSuccess.String(), describeAerospikeBatchRecord(batchRecordIfc)))
			continue
		}

		rawResponse := batchRecord.Record.Bins[LuaSuccess.String()]
		if rawResponse == nil {
			s.sendIncrementBatchResult(item, nil, errors.NewProcessingError("[IncrementSpentRecords][%s] missing expected response bin %q; %s", describeBatchIncrement(item), LuaSuccess.String(), describeAerospikeBatchRecord(batchRecordIfc)))
			continue
		}

		// Pass through the raw response - let the caller handle parsing
		s.sendIncrementBatchResult(item, rawResponse, nil)
	}
}

func batchIncrementAt(batch []*batchIncrement, idx int) *batchIncrement {
	if idx < 0 || idx >= len(batch) {
		return nil
	}
	return batch[idx]
}

func describeBatchIncrement(item *batchIncrement) string {
	if item == nil {
		return "<nil>"
	}
	return describeChainHash(item.txID)
}

func (s *Store) sendIncrementBatchResult(item *batchIncrement, res interface{}, err error) {
	if item == nil {
		if s.logger != nil {
			s.logger.Errorf("[IncrementSpentRecords] unable to send batch result for nil item: %v", err)
		}
		return
	}
	if item.res == nil {
		if s.logger != nil {
			s.logger.Errorf("[IncrementSpentRecords][%s] unable to send batch result because result channel is nil: %v", describeBatchIncrement(item), err)
		}
		return
	}

	item.res <- incrementSpentRecordsRes{res: res, err: err}
}

func (s *Store) sendSetDAHBatch(batch []*batchDAH) {
	var err error

	// Create batch records with individual TTLs
	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	dahBinName := fields.DeleteAtHeight.String()
	unsetOp := aerospike.PutOp(aerospike.NewBin(dahBinName, nil))

	for i, b := range batch {
		if b == nil {
			if s.logger != nil {
				s.logger.Errorf("[SetDAHBatch] nil batch item at idx=%d", i)
			}
			continue
		}
		if b.txID == nil {
			s.sendDAHBatchError(batch, i, errors.NewProcessingError("[SetDAHBatch][%s] failed to create key for pagination record %d: txid is nil", describeBatchDAH(b), b.childIdx))
			continue
		}

		keySource := uaerospike.CalculateKeySourceInternal(b.txID, b.childIdx)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			s.logger.Errorf("[SetDAHBatch][%s] failed to create key for pagination record %d: %v", b.txID.String(), b.childIdx, err)
			s.sendDAHBatchError(batch, i, errors.NewProcessingError("[SetDAHBatch][%s] failed to create key for pagination record %d", describeBatchDAH(b), b.childIdx, err))
			continue
		}

		if b.deleteAtHeight > 0 {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(dahBinName, b.deleteAtHeight)))
		} else {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, unsetOp)
		}
	}

	// Execute batch operation
	err = s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords)
	if err != nil {
		for idx, bItem := range batch {
			s.sendDAHBatchError(batch, idx, errors.NewStorageError("[SetDAHBatch][%s] failed to set DAH: %s", describeBatchDAH(bItem), err.Error(), err))
		}

		return
	}

	// batchOperate may have no errors, but some of the records may have failed
	for batchIdx, batchRecord := range batchRecords {
		bItem := batchDAHAt(batch, batchIdx)
		if batchRecord == nil {
			s.sendDAHBatchError(batch, batchIdx, errors.NewStorageError("[SetDAHBatch][%s] missing batch record; %s", describeBatchDAH(bItem), describeAerospikeBatchRecord(batchRecord)))
			continue
		}

		batchRec := batchRecord.BatchRec()
		if batchRec == nil {
			s.sendDAHBatchError(batch, batchIdx, errors.NewStorageError("[SetDAHBatch][%s] missing batch record; %s", describeBatchDAH(bItem), describeAerospikeBatchRecord(batchRecord)))
			continue
		}

		err = batchRec.Err

		if err != nil {
			if s.logger != nil {
				s.logger.Errorf("[SetDAHBatch][%s] batch record failed; %s: %s", describeBatchDAH(bItem), describeAerospikeBatchRecord(batchRecord), err.Error())
			}
			s.sendDAHBatchError(batch, batchIdx, err)
			continue
		}

		s.sendDAHBatchError(batch, batchIdx, nil)
	}
}

func batchDAHAt(batch []*batchDAH, idx int) *batchDAH {
	if idx < 0 || idx >= len(batch) {
		return nil
	}
	return batch[idx]
}

func describeBatchDAH(bItem *batchDAH) string {
	if bItem == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s:%d", describeChainHash(bItem.txID), bItem.childIdx)
}

func (s *Store) sendDAHBatchError(batch []*batchDAH, idx int, err error) {
	bItem := batchDAHAt(batch, idx)
	if bItem == nil {
		if s.logger != nil {
			s.logger.Errorf("[SetDAHBatch] unable to send batch item result for idx=%d: %v", idx, err)
		}
		return
	}
	if bItem.errCh == nil {
		if s.logger != nil {
			s.logger.Errorf("[SetDAHBatch][%s] unable to send batch item result because errCh is nil: %v", describeBatchDAH(bItem), err)
		}
		return
	}
	bItem.errCh <- err
}
