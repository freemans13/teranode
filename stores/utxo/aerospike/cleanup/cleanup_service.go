package cleanup

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ensure Store implements the Cleanup Service interface
var _ cleanup.Service = (*Service)(nil)

var IndexName, _ = gocore.Config().Get("cleanup_IndexName", "cleanup_dah_index")

// batcherIfc defines the interface for batching operations
type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
}

// Constants for the cleanup service
const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 4

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

var (
	prometheusMetricsInitOnce  sync.Once
	prometheusUtxoCleanupBatch prometheus.Histogram
)

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// Ctx is the context to use to signal shutdown
	Ctx context.Context

	// IndexWaiter is used to wait for Aerospike indexes to be built
	IndexWaiter IndexWaiter

	// Client is the Aerospike client to use
	Client *uaerospike.Client

	// ExternalStore is the external blob store to use for external transactions
	ExternalStore blob.Store

	// Namespace is the Aerospike namespace to use
	Namespace string

	// Set is the Aerospike set to use
	Set string

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int

	// GetPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	GetPersistedHeight func() uint32
}

// Service manages background jobs for cleaning up records based on block height
type Service struct {
	logger      ulogger.Logger
	settings    *settings.Settings
	client      *uaerospike.Client
	external    blob.Store
	namespace   string
	set         string
	jobManager  *cleanup.JobManager
	ctx         context.Context
	indexWaiter IndexWaiter

	// internally reused variables
	queryPolicy      *aerospike.QueryPolicy
	writePolicy      *aerospike.WritePolicy
	batchWritePolicy *aerospike.BatchWritePolicy
	batchPolicy      *aerospike.BatchPolicy

	// separate batchers for background batch processing
	parentUpdateBatcher batcherIfc[batchParentUpdate]
	deleteBatcher       batcherIfc[batchDelete]

	// getPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	getPersistedHeight func() uint32

	// maxConcurrentOperations limits concurrent operations during cleanup processing
	// Auto-detected from Aerospike client connection queue size
	maxConcurrentOperations int
}

// parentUpdateInfo holds accumulated parent update information for batching
type parentUpdateInfo struct {
	key         *aerospike.Key
	childHashes []*chainhash.Hash // Child transactions being deleted
}

// batchParentUpdate represents a parent record update operation in a batch
type batchParentUpdate struct {
	txHash *chainhash.Hash
	inputs []*bt.Input
	errCh  chan error
}

// batchDelete represents a record deletion operation in a batch
type batchDelete struct {
	key    *aerospike.Key
	txHash *chainhash.Hash
	errCh  chan error
}

// NewService creates a new cleanup service
func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if opts.Client == nil {
		return nil, errors.NewProcessingError("client is required")
	}

	if opts.IndexWaiter == nil {
		return nil, errors.NewProcessingError("index waiter is required")
	}

	if opts.Namespace == "" {
		return nil, errors.NewProcessingError("namespace is required")
	}

	if opts.Set == "" {
		return nil, errors.NewProcessingError("set is required")
	}

	if opts.ExternalStore == nil {
		return nil, errors.NewProcessingError("external store is required")
	}

	// Initialize prometheus metrics if not already initialized
	prometheusMetricsInitOnce.Do(func() {
		prometheusUtxoCleanupBatch = promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "utxo_cleanup_batch_duration_seconds",
			Help:    "Time taken to process a batch of cleanup jobs",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		})
	})

	// Use the configured query policy from settings (configured via aerospike_queryPolicy URL)
	queryPolicy := util.GetAerospikeQueryPolicy(tSettings)
	queryPolicy.IncludeBinData = true // Need to include bin data for cleanup processing

	// Use the configured write policy from settings
	writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)

	// Use the configured batch policies from settings
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(tSettings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	// Use the configured batch policy from settings (configured via aerospike_batchPolicy URL)
	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)

	// Determine max concurrent operations:
	// - Use connection queue size as the upper bound (to prevent connection exhaustion)
	// - If setting is configured (non-zero), use the minimum of setting and connection queue size
	// - If setting is 0 or unset, use connection queue size
	connectionQueueSize := opts.Client.GetConnectionQueueSize()
	maxConcurrentOps := connectionQueueSize
	if tSettings.UtxoStore.CleanupMaxConcurrentOperations > 0 {
		if tSettings.UtxoStore.CleanupMaxConcurrentOperations < maxConcurrentOps {
			maxConcurrentOps = tSettings.UtxoStore.CleanupMaxConcurrentOperations
		}
	}

	service := &Service{
		logger:                  opts.Logger,
		settings:                tSettings,
		client:                  opts.Client,
		external:                opts.ExternalStore,
		namespace:               opts.Namespace,
		set:                     opts.Set,
		ctx:                     opts.Ctx,
		indexWaiter:             opts.IndexWaiter,
		queryPolicy:             queryPolicy,
		writePolicy:             writePolicy,
		batchWritePolicy:        batchWritePolicy,
		batchPolicy:             batchPolicy,
		getPersistedHeight:      opts.GetPersistedHeight,
		maxConcurrentOperations: maxConcurrentOps,
	}

	// Create the job processor function
	jobProcessor := func(job *cleanup.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := cleanup.NewJobManager(cleanup.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    opts.WorkerCount,
		MaxJobsHistory: opts.MaxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

	// Initialize cleanup batchers using dedicated cleanup settings
	parentUpdateBatchSize := tSettings.UtxoStore.CleanupParentUpdateBatcherSize
	parentUpdateBatchDuration := time.Duration(tSettings.UtxoStore.CleanupParentUpdateBatcherDurationMillis) * time.Millisecond
	service.parentUpdateBatcher = batcher.New[batchParentUpdate](parentUpdateBatchSize, parentUpdateBatchDuration, service.sendParentUpdateBatch, true)

	deleteBatchSize := tSettings.UtxoStore.CleanupDeleteBatcherSize
	deleteBatchDuration := time.Duration(tSettings.UtxoStore.CleanupDeleteBatcherDurationMillis) * time.Millisecond
	service.deleteBatcher = batcher.New[batchDelete](deleteBatchSize, deleteBatchDuration, service.sendDeleteBatch, true)

	return service, nil
}

// Start starts the cleanup service and creates the required index if this has not been done already.  This method
// will return immediately but will not start the workers until the initialization is complete.
//
// The service will start a goroutine to initialize the service and create the required index if it does not exist.
// Once the initialization is complete, the service will start the worker goroutines to process cleanup jobs.
//
// The service will also create a rotating queue of cleanup jobs, which will be processed as the block height
// becomes available.  The rotating queue will always keep the most recent jobs and will drop older
// jobs if the queue is full or there is a job with a higher height.
func (s *Service) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	go func() {
		// All processes wait for the index to be built
		if err := s.indexWaiter.WaitForIndexReady(ctx, IndexName); err != nil {
			s.logger.Errorf("Timeout or error waiting for index to be built: %v", err)
		}

		// Only start job manager after index is built
		s.jobManager.Start(ctx)

		s.logger.Infof("[AerospikeCleanupService] started cleanup service")
	}()
}

// Stop stops the cleanup service and waits for all workers to exit.
// This ensures all goroutines are properly terminated before returning.
func (s *Service) Stop(ctx context.Context) error {
	// Stop the job manager
	s.jobManager.Stop()

	s.logger.Infof("[AerospikeCleanupService] stopped cleanup service")

	return nil
}

// SetPersistedHeightGetter sets the function used to get block persister progress.
// This should be called after service creation to wire up coordination with block persister.
func (s *Service) SetPersistedHeightGetter(getter func() uint32) {
	s.getPersistedHeight = getter
}

// UpdateBlockHeight updates the block height and triggers a cleanup job
func (s *Service) UpdateBlockHeight(blockHeight uint32, done ...chan string) error {
	if blockHeight == 0 {
		return errors.NewProcessingError("block height cannot be zero")
	}

	s.logger.Debugf("[AerospikeCleanupService] Updating block height to %d", blockHeight)

	// Pass the done channel to the job manager
	var doneChan chan string

	if len(done) > 0 {
		doneChan = done[0]
	}

	return s.jobManager.UpdateBlockHeight(blockHeight, doneChan)
}

// processCleanupJob processes a cleanup job
func (s *Service) processCleanupJob(job *cleanup.Job, workerID int) {
	// Update job status to running
	job.SetStatus(cleanup.JobStatusRunning)
	job.Started = time.Now()

	// Get the job's context for cancellation support
	jobCtx := job.Context()

	s.logger.Infof("Worker %d starting cleanup job for block height %d", workerID, job.BlockHeight)

	// BLOCK PERSISTER COORDINATION: Calculate safe cleanup height
	//
	// PROBLEM: Block persister creates .subtree_data files after a delay (BlockPersisterPersistAge blocks).
	// If we delete transactions before block persister creates these files, catchup will fail with
	// "subtree length does not match tx data length" (actually missing transactions).
	//
	// SOLUTION: Limit cleanup to transactions that block persister has already processed:
	//   safe_height = min(requested_cleanup_height, persisted_height + retention)
	//
	// EXAMPLE with retention=288, persisted=100, requested=200:
	//   - Block persister has processed blocks up to height 100
	//   - Those blocks' transactions are in .subtree_data files (safe to delete after retention)
	//   - Safe deletion height = 100 + 288 = 388... but wait, we want to clean height 200
	//   - Since 200 < 388, we can safely proceed with cleaning up to 200
	//
	// EXAMPLE where cleanup would be limited (persisted=50, requested=200, retention=100):
	//   - Block persister only processed up to height 50
	//   - Safe deletion = 50 + 100 = 150
	//   - Requested cleanup of 200 is LIMITED to 150 to protect unpersisted blocks 51-200
	//
	// HEIGHT=0 SPECIAL CASE: If persistedHeight=0, block persister isn't running or hasn't
	// processed any blocks yet. Proceed with normal cleanup without coordination.
	safeCleanupHeight := job.BlockHeight

	if s.getPersistedHeight != nil {
		persistedHeight := s.getPersistedHeight()

		// Only apply limitation if block persister has actually processed blocks (height > 0)
		if persistedHeight > 0 {
			retention := s.settings.GetUtxoStoreBlockHeightRetention()

			// Calculate max safe height: persisted_height + retention
			// Block persister at height N means blocks 0 to N are persisted in .subtree_data files.
			// Those transactions can be safely deleted after retention blocks.
			maxSafeHeight := persistedHeight + retention
			if maxSafeHeight < safeCleanupHeight {
				s.logger.Infof("Worker %d: Limiting cleanup from height %d to %d (persisted: %d, retention: %d)",
					workerID, job.BlockHeight, maxSafeHeight, persistedHeight, retention)
				safeCleanupHeight = maxSafeHeight
			}
		}
	}

	// Create a query statement
	stmt := aerospike.NewStatement(s.namespace, s.set)
	stmt.BinNames = []string{fields.TxID.String(), fields.DeleteAtHeight.String(), fields.Inputs.String(), fields.External.String()}

	// Set the filter to find records with a delete_at_height less than or equal to the safe cleanup height
	// This will automatically use the index since the filter is on the indexed bin
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.DeleteAtHeight.String(), 1, int64(safeCleanupHeight)))
	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("Worker %d: failed to set filter for cleanup job %d: %v", workerID, job.BlockHeight, err)

		return
	}

	// iterate through the results, process each record individually using batchers
	recordset, err := s.client.Query(s.queryPolicy, stmt)
	if err != nil {
		s.logger.Errorf("Worker %d: failed to execute query for cleanup job %d: %v", workerID, job.BlockHeight, err)
		s.markJobAsFailed(job, err)
		return
	}

	defer recordset.Close()

	result := recordset.Results()
	recordCount := int64(0)
	lastProgressLog := time.Now()
	progressLogInterval := 30 * time.Second

	// Log initial start
	s.logger.Infof("Worker %d: starting cleanup scan for height %d (delete_at_height <= %d)",
		workerID, job.BlockHeight, safeCleanupHeight)

	// Batch accumulation slices
	batchSize := s.settings.UtxoStore.CleanupDeleteBatcherSize
	parentUpdates := make(map[string]*parentUpdateInfo) // keyed by parent txid
	deletions := make([]*aerospike.Key, 0, batchSize)

	// Process records and accumulate into batches
	for {
		// Check for cancellation before processing next record
		select {
		case <-jobCtx.Done():
			s.logger.Infof("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight)
			recordset.Close()
			// Flush any accumulated operations before exiting
			s.flushCleanupBatches(jobCtx, workerID, job.BlockHeight, parentUpdates, deletions)
			s.markJobAsFailed(job, errors.NewProcessingError("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight))
			return
		default:
		}

		rec, ok := <-result
		if !ok || rec == nil {
			break // No more records
		}

		if rec.Err != nil {
			s.logger.Errorf("Worker %d: error reading record: %v", workerID, rec.Err)
			continue
		}

		// Extract record data
		bins := rec.Record.Bins
		txHash, err := s.extractTxHash(bins)
		if err != nil {
			s.logger.Errorf("Worker %d: %v", workerID, err)
			continue
		}

		inputs, err := s.extractInputs(job, workerID, bins, txHash)
		if err != nil {
			s.logger.Errorf("Worker %d: %v", workerID, err)
			continue
		}

		// Accumulate parent updates
		for _, input := range inputs {
			// Calculate the parent key for this input
			keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.settings.UtxoStore.UtxoBatchSize)
			parentKeyStr := string(keySource)

			if existing, ok := parentUpdates[parentKeyStr]; ok {
				// Add this transaction to the list of deleted children for this parent
				existing.childHashes = append(existing.childHashes, txHash)
			} else {
				parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
				if err != nil {
					s.logger.Errorf("Worker %d: failed to create parent key: %v", workerID, err)
					continue
				}
				parentUpdates[parentKeyStr] = &parentUpdateInfo{
					key:         parentKey,
					childHashes: []*chainhash.Hash{txHash},
				}
			}
		}

		// Accumulate deletion
		deletions = append(deletions, rec.Record.Key)
		recordCount++

		// Log progress
		if recordCount%10000 == 0 || time.Since(lastProgressLog) > progressLogInterval {
			s.logger.Infof("Worker %d: cleanup progress for height %d - processed %d records so far",
				workerID, job.BlockHeight, recordCount)
			lastProgressLog = time.Now()
		}

		// Execute batch when full
		if len(deletions) >= batchSize {
			s.flushCleanupBatches(jobCtx, workerID, job.BlockHeight, parentUpdates, deletions)
			parentUpdates = make(map[string]*parentUpdateInfo)
			deletions = make([]*aerospike.Key, 0, batchSize)
		}
	}

	// Flush any remaining operations
	s.flushCleanupBatches(jobCtx, workerID, job.BlockHeight, parentUpdates, deletions)

	// Check if we were cancelled
	wasCancelled := false
	select {
	case <-jobCtx.Done():
		wasCancelled = true
		s.logger.Infof("Worker %d: cleanup job for height %d was cancelled", workerID, job.BlockHeight)
	default:
		// Not cancelled
	}

	s.logger.Infof("Worker %d: processed %d records for cleanup job %d", workerID, recordCount, job.BlockHeight)

	// Set appropriate status based on cancellation
	if wasCancelled {
		job.SetStatus(cleanup.JobStatusCancelled)
		s.logger.Infof("Worker %d cancelled cleanup job for block height %d after %v, processed %d records before cancellation",
			workerID, job.BlockHeight, time.Since(job.Started), recordCount)
	} else {
		job.SetStatus(cleanup.JobStatusCompleted)
		s.logger.Infof("Worker %d completed cleanup job for block height %d in %v, processed %d records",
			workerID, job.BlockHeight, time.Since(job.Started), recordCount)
	}
	job.Ended = time.Now()

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)
}

func (s *Service) getTxInputsFromBins(job *cleanup.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	var inputs []*bt.Input

	external, ok := bins[fields.External.String()].(bool)
	if ok && external {
		// transaction is external, we need to get the data from the external store
		txBytes, err := s.external.Get(s.ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Check if outputs exist (sometimes only outputs are stored)
				exists, err := s.external.Exists(s.ctx, txHash.CloneBytes(), fileformat.FileTypeOutputs)
				if err != nil {
					return nil, errors.NewProcessingError("Worker %d: error checking existence of outputs for external tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
				}

				if exists {
					// Only outputs exist, no inputs needed for cleanup
					return nil, nil
				}

				// External blob already deleted (by LocalDAH or previous cleanup), just need to delete Aerospike record
				s.logger.Debugf("Worker %d: external tx %s already deleted from blob store for cleanup job %d, proceeding to delete Aerospike record",
					workerID, txHash.String(), job.BlockHeight)
				return []*bt.Input{}, nil
			}
			// Other errors should still be reported
			return nil, errors.NewProcessingError("Worker %d: error getting external tx %s for cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewProcessingError("Worker %d: invalid tx bytes for external tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
		}

		inputs = tx.Inputs
	} else {
		// get the inputs from the record directly
		inputsValue := bins[fields.Inputs.String()]
		if inputsValue == nil {
			// Inputs field might be nil for certain records (e.g., coinbase)
			return []*bt.Input{}, nil
		}

		inputInterfaces, ok := inputsValue.([]interface{})
		if !ok {
			// Log more helpful error with actual type
			return nil, errors.NewProcessingError("Worker %d: inputs field has unexpected type %T (expected []interface{}) for record in cleanup job %d",
				workerID, inputsValue, job.BlockHeight)
		}

		inputs = make([]*bt.Input, len(inputInterfaces))

		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			inputs[i] = &bt.Input{}

			if _, err := inputs[i].ReadFrom(bytes.NewReader(input)); err != nil {
				return nil, errors.NewProcessingError("Worker %d: invalid input for record in cleanup job %d", workerID, job.BlockHeight, err)
			}
		}
	}

	return inputs, nil
}

func (s *Service) markJobAsFailed(job *cleanup.Job, err error) {
	job.SetStatus(cleanup.JobStatusFailed)
	job.Error = err
	job.Ended = time.Now()
}

// sendParentUpdateBatch processes a batch of parent update operations
func (s *Service) sendParentUpdateBatch(batch []*batchParentUpdate) {
	if len(batch) == 0 {
		return
	}

	// Track which batch items are associated with which parent keys
	type parentInfo struct {
		key         *aerospike.Key
		childHashes []*chainhash.Hash
		batchItems  []*batchParentUpdate // Track which items depend on this parent
	}

	parentMap := make(map[string]*parentInfo)                   // Use key string for deduplication
	itemToParents := make(map[*batchParentUpdate][]*parentInfo) // Track parents per item

	// First pass: collect all parent updates and track relationships
	for _, item := range batch {
		if len(item.inputs) == 0 {
			// No inputs, send success immediately
			item.errCh <- nil
			continue
		}

		var itemParents []*parentInfo
		for _, input := range item.inputs {
			keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.settings.UtxoStore.UtxoBatchSize)
			parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
			if err != nil {
				// Send error to this batch item immediately
				item.errCh <- errors.NewProcessingError("error creating parent key for input in external tx %s", item.txHash.String(), err)
				continue
			}

			keyStr := parentKey.String()
			parent, exists := parentMap[keyStr]
			if !exists {
				parent = &parentInfo{
					key:         parentKey,
					childHashes: make([]*chainhash.Hash, 0),
					batchItems:  make([]*batchParentUpdate, 0),
				}
				parentMap[keyStr] = parent
			}

			// Add this child tx hash if not already present
			found := false
			for _, existingHash := range parent.childHashes {
				if existingHash.IsEqual(item.txHash) {
					found = true
					break
				}
			}
			if !found {
				parent.childHashes = append(parent.childHashes, item.txHash)
			}

			// Add this item to the parent's dependent items if not already there
			found = false
			for _, existingItem := range parent.batchItems {
				if existingItem == item {
					found = true
					break
				}
			}
			if !found {
				parent.batchItems = append(parent.batchItems, item)
			}

			itemParents = append(itemParents, parent)
		}
		itemToParents[item] = itemParents
	}

	if len(parentMap) == 0 {
		return // All items already handled
	}

	// Create batch operations
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(parentMap))
	parentsList := make([]*parentInfo, 0, len(parentMap))

	for _, parent := range parentMap {
		// Create multiple MapPutOp operations for this parent
		ops := make([]*aerospike.Operation, 0, len(parent.childHashes))
		for _, childTxHash := range parent.childHashes {
			ops = append(ops, aerospike.MapPutOp(mapPolicy, fields.DeletedChildren.String(), aerospike.NewStringValue(childTxHash.String()), aerospike.BoolValue(true)))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(
			s.batchWritePolicy,
			parent.key,
			ops...,
		))
		parentsList = append(parentsList, parent)
	}

	// Execute the batch operation with custom batch policy
	err := s.client.BatchOperate(s.batchPolicy, batchRecords)
	if err != nil {
		// Send error to all batch items that have parent updates
		for _, item := range batch {
			if len(itemToParents[item]) > 0 {
				item.errCh <- errors.NewProcessingError("error batch updating parent records", err)
			}
		}
		return
	}

	// Check individual batch results and send responses to affected items
	for i, batchRec := range batchRecords {
		parent := parentsList[i]
		var itemErr error

		if batchRec.BatchRec().Err != nil {
			if !errors.Is(batchRec.BatchRec().Err, aerospike.ErrKeyNotFound) {
				// Real error occurred
				itemErr = errors.NewProcessingError("error updating parent record", batchRec.BatchRec().Err)
			}
			// For ErrKeyNotFound, itemErr remains nil (success)
		}

		// Send response to all items that depend on this parent
		for _, item := range parent.batchItems {
			item.errCh <- itemErr
		}
	}
}

// sendDeleteBatch processes a batch of deletion operations
func (s *Service) sendDeleteBatch(batch []*batchDelete) {
	if len(batch) == 0 {
		return
	}

	// Log batch deletion for verification
	// s.logger.Debugf("Sending delete batch of %d records to Aerospike", len(batch))

	// Create batch delete records
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchDeletePolicy := aerospike.NewBatchDeletePolicy()

	for _, item := range batch {
		batchRecords = append(batchRecords, aerospike.NewBatchDelete(batchDeletePolicy, item.key))
	}

	// Execute the batch delete operation with custom batch policy
	err := s.client.BatchOperate(s.batchPolicy, batchRecords)
	if err != nil {
		// Send error to all batch items
		for _, item := range batch {
			item.errCh <- errors.NewProcessingError("error batch deleting records", err)
		}

		return
	}

	// Check individual batch results and send individual responses
	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for i, batchRec := range batchRecords {
		if batchRec.BatchRec().Err != nil {
			if errors.Is(batchRec.BatchRec().Err, aerospike.ErrKeyNotFound) {
				// Record not found, treat as success (already deleted)
				batch[i].errCh <- nil
				alreadyDeletedCount++
			} else {
				// Real error occurred
				batch[i].errCh <- errors.NewProcessingError("error deleting record for tx %s", batch[i].txHash.String(), batchRec.BatchRec().Err)
				errorCount++
			}
		} else {
			// Success
			batch[i].errCh <- nil
			successCount++
		}
	}

	// Log batch results for verification
	// s.logger.Debugf("Delete batch completed: %d successful, %d already deleted, %d errors (total: %d)",
	// 	successCount, alreadyDeletedCount, errorCount, len(batch))
}

func (s *Service) ProcessSingleRecord(txid *chainhash.Hash, inputs []*bt.Input) error {
	errCh := make(chan error)

	s.parentUpdateBatcher.Put(&batchParentUpdate{
		txHash: txid,
		inputs: inputs,
		errCh:  errCh,
	})

	return <-errCh
}

// flushCleanupBatches flushes accumulated parent updates and deletions
func (s *Service) flushCleanupBatches(ctx context.Context, workerID int, blockHeight uint32, parentUpdates map[string]*parentUpdateInfo, deletions []*aerospike.Key) {
	if len(parentUpdates) > 0 {
		s.executeBatchParentUpdates(ctx, workerID, blockHeight, parentUpdates)
	}
	if len(deletions) > 0 {
		s.executeBatchDeletions(ctx, workerID, blockHeight, deletions)
	}
}

// extractTxHash extracts the transaction hash from record bins
func (s *Service) extractTxHash(bins aerospike.BinMap) (*chainhash.Hash, error) {
	txIDBytes, ok := bins[fields.TxID.String()].([]byte)
	if !ok || len(txIDBytes) != 32 {
		return nil, errors.NewProcessingError("invalid or missing txid")
	}

	txHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return nil, errors.NewProcessingError("invalid txid bytes: %v", err)
	}

	return txHash, nil
}

// extractInputs extracts the transaction inputs from record bins
func (s *Service) extractInputs(job *cleanup.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	return s.getTxInputsFromBins(job, workerID, bins, txHash)
}

// flushBatches flushes any accumulated parent updates and deletions
func (s *Service) flushBatches(ctx context.Context, workerID int, blockHeight uint32, parentUpdates map[string]*parentUpdateInfo, deletions []*aerospike.Key) {
	if len(parentUpdates) > 0 {
		s.executeBatchParentUpdates(ctx, workerID, blockHeight, parentUpdates)
	}
	if len(deletions) > 0 {
		s.executeBatchDeletions(ctx, workerID, blockHeight, deletions)
	}
}

// executeBatchParentUpdates executes a batch of parent update operations
func (s *Service) executeBatchParentUpdates(ctx context.Context, workerID int, blockHeight uint32, updates map[string]*parentUpdateInfo) {
	if len(updates) == 0 {
		return
	}

	// Convert map to batch operations
	// Track deleted children by adding child tx hashes to the DeletedChildren map
	// This matches the approach used in sendParentUpdateBatch (async batcher)
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))

	for _, info := range updates {
		// For each child transaction being deleted, add it to the DeletedChildren map
		ops := make([]*aerospike.Operation, len(info.childHashes))
		for i, childHash := range info.childHashes {
			ops[i] = aerospike.MapPutOp(mapPolicy, fields.DeletedChildren.String(),
				aerospike.NewStringValue(childHash.String()), aerospike.BoolValue(true))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(s.batchWritePolicy, info.key, ops...))
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch parent update failed: %v", workerID, err)
		return
	}

	// Check for errors
	successCount := 0
	notFoundCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			// Ignore KEY_NOT_FOUND - parent may have been deleted already
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				notFoundCount++
				continue
			}
			// Log other errors
			s.logger.Errorf("Worker %d: parent update error for key %v: %v", workerID, rec.BatchRec().Key, rec.BatchRec().Err)
			errorCount++
		} else {
			successCount++
		}
	}

	// if successCount > 0 || notFoundCount > 0 || errorCount > 0 {
	// 	s.logger.Infof("Worker %d: parent updates - %d successful, %d not found, %d errors",
	// 		workerID, successCount, notFoundCount, errorCount)
	// }
}

// executeBatchDeletions executes a batch of deletion operations
func (s *Service) executeBatchDeletions(ctx context.Context, workerID int, blockHeight uint32, keys []*aerospike.Key) {
	if len(keys) == 0 {
		return
	}

	// Create batch delete records
	batchDeletePolicy := aerospike.NewBatchDeletePolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))
	for i, key := range keys {
		batchRecords[i] = aerospike.NewBatchDelete(batchDeletePolicy, key)
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch deletion failed for %d records: %v", workerID, len(keys), err)
		return
	}

	// Check for errors and count successes
	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				// Already deleted
				alreadyDeletedCount++
			} else {
				s.logger.Errorf("Worker %d: deletion error for key %v: %v", workerID, rec.BatchRec().Key, rec.BatchRec().Err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	// s.logger.Infof("Worker %d: deleted %d records (%d already deleted, %d errors)",
	// 	workerID, successCount, alreadyDeletedCount, errorCount)
}

// processRecordCleanup processes a single record for cleanup using batchers
func (s *Service) processRecordCleanup(ctx context.Context, job *cleanup.Job, workerID int, rec *aerospike.Result) error {
	if rec.Err != nil {
		return errors.NewProcessingError("Worker %d: error reading record for cleanup job %d", workerID, job.BlockHeight, rec.Err)
	}

	// get all the unique parent records of the record being deleted
	bins := rec.Record.Bins
	if bins == nil {
		return errors.NewProcessingError("Worker %d: missing bins for record in cleanup job %d", workerID, job.BlockHeight)
	}

	txIDBytes, ok := bins[fields.TxID.String()].([]byte)
	if !ok || len(txIDBytes) != 32 {
		return errors.NewProcessingError("Worker %d: invalid or missing txid for record in cleanup job %d", workerID, job.BlockHeight)
	}

	txHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return errors.NewProcessingError("Worker %d: invalid txid bytes for record in cleanup job %d", workerID, job.BlockHeight)
	}

	inputs, err := s.getTxInputsFromBins(job, workerID, bins, txHash)
	if err != nil {
		return err
	}

	// inputs could be empty on seeded records, in which case we just delete the record
	// and do not need to update any parents
	if len(inputs) > 0 {
		// Update parents using batcher in goroutine
		parentErrCh := make(chan error)

		s.parentUpdateBatcher.Put(&batchParentUpdate{
			txHash: txHash,
			inputs: inputs,
			errCh:  parentErrCh,
		})

		// Wait for parent update to complete
		if err = <-parentErrCh; err != nil {
			return errors.NewProcessingError("Worker %d: error updating parents for tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
		}
	}

	// Delete the record using batcher in goroutine
	deleteErrCh := make(chan error)

	s.deleteBatcher.Put(&batchDelete{
		key:    rec.Record.Key,
		txHash: txHash,
		errCh:  deleteErrCh,
	})

	// Wait for deletion to complete
	if err = <-deleteErrCh; err != nil {
		return errors.NewProcessingError("Worker %d: error deleting record for tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
	}

	// Wait for all operations to complete
	return nil
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}
