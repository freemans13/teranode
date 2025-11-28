package pruner

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

var IndexName, _ = gocore.Config().Get("pruner_IndexName", "pruner_dah_index")

// Constants for the pruner service
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
// Service implements the pruner.Service interface for Aerospike-backed UTXO stores.
// This service extracts configuration values as fields during initialization rather than
// storing the settings object, optimizing for performance in hot paths where settings
// are accessed millions of times (e.g., utxoBatchSize in per-record processing loops).
type Service struct {
	logger      ulogger.Logger
	client      *uaerospike.Client
	external    blob.Store
	namespace   string
	set         string
	jobManager  *pruner.JobManager
	ctx         context.Context
	indexWaiter IndexWaiter

	// Configuration values extracted from settings for performance
	utxoBatchSize          int
	blockHeightRetention   uint32
	defensiveEnabled       bool
	defensiveBatchReadSize int

	// Cached field names (avoid repeated String() allocations in hot paths)
	fieldTxID, fieldUtxos, fieldInputs, fieldDeletedChildren, fieldExternal        string
	fieldDeleteAtHeight, fieldTotalExtraRecs, fieldUnminedSince, fieldBlockHeights string

	// Internally reused variables
	queryPolicy      *aerospike.QueryPolicy
	writePolicy      *aerospike.WritePolicy
	batchWritePolicy *aerospike.BatchWritePolicy
	batchPolicy      *aerospike.BatchPolicy

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

// externalFileInfo holds information about external files to delete
type externalFileInfo struct {
	txHash   *chainhash.Hash
	fileType fileformat.FileType
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

	service := &Service{
		logger:                  opts.Logger,
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
		maxConcurrentOperations: tSettings.Pruner.UTXOMaxConcurrentOperations,
		utxoBatchSize:           tSettings.UtxoStore.UtxoBatchSize,
		blockHeightRetention:    tSettings.GetUtxoStoreBlockHeightRetention(),
		defensiveEnabled:        tSettings.Pruner.UTXODefensiveEnabled,
		defensiveBatchReadSize:  tSettings.Pruner.UTXODefensiveBatchReadSize,
		fieldTxID:               fields.TxID.String(),
		fieldUtxos:              fields.Utxos.String(),
		fieldInputs:             fields.Inputs.String(),
		fieldDeletedChildren:    fields.DeletedChildren.String(),
		fieldExternal:           fields.External.String(),
		fieldDeleteAtHeight:     fields.DeleteAtHeight.String(),
		fieldTotalExtraRecs:     fields.TotalExtraRecs.String(),
		fieldUnminedSince:       fields.UnminedSince.String(),
		fieldBlockHeights:       fields.BlockHeights.String(),
	}

	// Create the job processor function
	jobProcessor := func(job *pruner.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := pruner.NewJobManager(pruner.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    opts.WorkerCount,
		MaxJobsHistory: opts.MaxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

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
func (s *Service) processCleanupJob(job *pruner.Job, workerID int) {
	// Update job status to running
	job.SetStatus(pruner.JobStatusRunning)
	job.Started = time.Now()

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
			retention := s.blockHeightRetention

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
	// Conditionally fetch Utxos and DeletedChildren bins only when defensive mode is enabled (for child verification)
	binNames := []string{s.fieldTxID, s.fieldDeleteAtHeight, s.fieldInputs, s.fieldExternal, s.fieldTotalExtraRecs}
	if s.defensiveEnabled {
		binNames = append(binNames, s.fieldUtxos, s.fieldDeletedChildren)
	}
	stmt.BinNames = binNames

	// Set the filter to find records with a delete_at_height less than or equal to the safe cleanup height
	// This will automatically use the index since the filter is on the indexed bin
	err := stmt.SetFilter(aerospike.NewRangeFilter(s.fieldDeleteAtHeight, 1, int64(safeCleanupHeight)))
	if err != nil {
		job.SetStatus(pruner.JobStatusFailed)
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
	recordCount := atomic.Int64{}

	// Process records in chunks for efficient batch verification of children
	const chunkSize = 1000
	chunk := make([]*aerospike.Result, 0, chunkSize)

	// Use errgroup to process chunks in parallel with controlled concurrency
	chunkGroup := &errgroup.Group{}
	// Limit parallel chunk processing to avoid overwhelming the system
	// Allow up to 10 chunks in parallel (10,000 parent records being processed at once)
	util.SafeSetLimit(chunkGroup, 10)

	// Log initial start
	s.logger.Infof("Worker %d: starting cleanup scan for height %d (delete_at_height <= %d)",
		workerID, job.BlockHeight, safeCleanupHeight)

	// Helper to submit a chunk for processing
	submitChunk := func(chunkToProcess []*aerospike.Result) {
		// Copy chunk for goroutine to avoid race
		chunkCopy := make([]*aerospike.Result, len(chunkToProcess))
		copy(chunkCopy, chunkToProcess)

		chunkGroup.Go(func() error {
			processed, err := s.processRecordChunk(job, workerID, chunkCopy)
			if err != nil {
				return err
			}
			recordCount.Add(int64(processed))
			return nil
		})
	}

	// Get job context for cancellation support
	jobCtx := job.Context()

	// Process records and accumulate into chunks
	for {
		// Check for cancellation before processing next chunk
		select {
		case <-jobCtx.Done():
			s.logger.Infof("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight)
			recordset.Close()
			// Process any accumulated chunk before exiting
			if len(chunk) > 0 {
				submitChunk(chunk)
			}
			// Wait for submitted chunks to complete
			if err := chunkGroup.Wait(); err != nil {
				s.logger.Errorf("Worker %d: error in chunks during cancellation: %v", workerID, err)
			}
			s.markJobAsFailed(job, errors.NewProcessingError("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight))
			return
		default:
		}

		rec, ok := <-result
		if !ok || rec == nil {
			// Process final chunk if any
			if len(chunk) > 0 {
				submitChunk(chunk)
			}
			break
		}

		chunk = append(chunk, rec)

		// Process chunk when full (in parallel)
		if len(chunk) >= chunkSize {
			submitChunk(chunk)
			chunk = chunk[:0] // Reset chunk
		}
	}

	// Wait for all parallel chunks to complete
	if err := chunkGroup.Wait(); err != nil {
		s.logger.Errorf("Worker %d: error processing chunks: %v", workerID, err)
		s.markJobAsFailed(job, err)
		return
	}

	finalRecordCount := recordCount.Load()

	// Set job status and record count
	job.SetStatus(pruner.JobStatusCompleted)
	job.RecordsProcessed.Store(finalRecordCount)
	job.Ended = time.Now()

	s.logger.Infof("Worker %d completed cleanup job for block height %d in %v, processed %d records",
		workerID, job.BlockHeight, job.Ended.Sub(job.Started), finalRecordCount)

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)
}

// processRecordChunk processes a chunk of parent records with batched child verification
func (s *Service) processRecordChunk(job *pruner.Job, workerID int, chunk []*aerospike.Result) (int, error) {
	if len(chunk) == 0 {
		return 0, nil
	}

	// Defensive child verification is conditional on the UTXODefensiveEnabled setting
	// When disabled, parents are deleted without verifying children are stable
	var safetyMap map[string]bool
	var parentToChildren map[string][]string

	if !s.defensiveEnabled {
		// Defensive mode disabled - allow all deletions without child verification
		safetyMap = make(map[string]bool)
		parentToChildren = make(map[string][]string)
	} else {
		// Step 1: Extract ALL unique spending children from chunk
		// For each parent record, we extract all spending child TX hashes from spent UTXOs
		// We must verify EVERY child is stable before deleting the parent
		uniqueSpendingChildren := make(map[string][]byte, 100)   // hex hash -> bytes (typical: ~50-100 children per chunk)
		parentToChildren = make(map[string][]string, len(chunk)) // parent record key -> child hashes
		deletedChildren := make(map[string]bool, 20)             // child hash -> already deleted (typical: 0-20)

		for _, rec := range chunk {
			if rec.Err != nil || rec.Record == nil || rec.Record.Bins == nil {
				continue
			}

			// Extract deletedChildren map from parent record
			// If a child is in this map, it means it was already pruned and shouldn't block parent deletion
			if deletedChildrenRaw, hasDeleted := rec.Record.Bins[s.fieldDeletedChildren]; hasDeleted {
				if deletedMap, ok := deletedChildrenRaw.(map[interface{}]interface{}); ok {
					for childHashIface := range deletedMap {
						if childHashStr, ok := childHashIface.(string); ok {
							deletedChildren[childHashStr] = true
							// s.logger.Debugf("Worker %d: Found deleted child in parent record: %s", workerID, childHashStr[:8])
						}
					}
				} else {
					s.logger.Debugf("Worker %d: deletedChildren bin wrong type: %T", workerID, deletedChildrenRaw)
				}
			}

			// Extract all spending children from this parent's UTXOs
			utxosRaw, hasUtxos := rec.Record.Bins[s.fieldUtxos]
			if !hasUtxos {
				continue
			}

			utxosList, ok := utxosRaw.([]interface{})
			if !ok {
				continue
			}

			parentKey := rec.Record.Key.String()
			childrenForThisParent := make([]string, 0, 16) // Pre-allocate for typical ~10 spent UTXOs per tx

			// Scan all UTXOs for spending data
			for _, utxoRaw := range utxosList {
				utxoBytes, ok := utxoRaw.([]byte)
				if !ok || len(utxoBytes) < 68 { // 32 (utxo hash) + 36 (spending data)
					continue
				}

				// spending_data starts at byte 32, first 32 bytes of spending_data is child TX hash
				childTxHashBytes := utxoBytes[32:64]

				// Check if this is actual spending data (not all zeros)
				hasSpendingData := false
				for _, b := range childTxHashBytes {
					if b != 0 {
						hasSpendingData = true
						break
					}
				}

				if hasSpendingData {
					hexHash := chainhash.Hash(childTxHashBytes).String()
					uniqueSpendingChildren[hexHash] = childTxHashBytes
					childrenForThisParent = append(childrenForThisParent, hexHash)
					// s.logger.Debugf("Worker %d: Extracted spending child from UTXO: %s", workerID, hexHash[:8])
				}
			}

			if len(childrenForThisParent) > 0 {
				parentToChildren[parentKey] = childrenForThisParent
			}
		}

		// Step 2: Batch verify all unique children (single BatchGet call for entire chunk)
		if len(uniqueSpendingChildren) > 0 {
			safetyMap = s.batchVerifyChildrenSafety(uniqueSpendingChildren, job.BlockHeight, deletedChildren)
			s.logger.Debugf("Worker %d: batch verified %d unique children from chunk of %d records (%d already deleted)", workerID, len(uniqueSpendingChildren), len(chunk), len(deletedChildren))
		} else {
			safetyMap = make(map[string]bool)
		}
	}

	// Step 3: Process deletions using the safety map
	g := &errgroup.Group{}
	// Limit concurrent operations within each chunk to avoid overwhelming Aerospike
	// Use pre-calculated limit from service initialization
	util.SafeSetLimit(g, s.maxConcurrentOperations)

	processedCount := atomic.Int64{}

	for _, rec := range chunk {
		currentRec := rec // capture for goroutine
		g.Go(func() error {
			if err := s.processRecordCleanupWithSafetyMap(job, workerID, currentRec, safetyMap, parentToChildren); err != nil {
				return err
			}

			processedCount.Add(1)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return int(processedCount.Load()), nil
}

// processRecordCleanupWithSafetyMap processes a single record using pre-computed safety map
func (s *Service) processRecordCleanupWithSafetyMap(job *pruner.Job, workerID int, rec *aerospike.Result, safetyMap map[string]bool, parentToChildren map[string][]string) error {
	if rec.Err != nil {
		return errors.NewProcessingError("Worker %d: error reading record for pruner job %d: %v", workerID, job.BlockHeight, rec.Err)
	}

	bins := rec.Record.Bins
	if bins == nil {
		return errors.NewProcessingError("Worker %d: missing bins for record in pruner job %d", workerID, job.BlockHeight)
	}

	txIDBytes, ok := bins[s.fieldTxID].([]byte)
	if !ok || len(txIDBytes) != 32 {
		return errors.NewProcessingError("Worker %d: invalid or missing txid for record in pruner job %d", workerID, job.BlockHeight)
	}

	txHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return errors.NewProcessingError("Worker %d: invalid txid bytes for record in pruner job %d", workerID, job.BlockHeight)
	}

	// Cache txHash string conversion (used multiple times in this function)
	txHashStr := txHash.String()

	// Verify ALL spending children are stable before deleting parent
	// We extract all children from the parent's spent UTXOs and verify EVERY one is stable
	// If even ONE child is unmined or recently mined, we must keep the parent
	parentKey := rec.Record.Key.String()
	childrenHashes, hasChildren := parentToChildren[parentKey]

	if hasChildren && len(childrenHashes) > 0 {
		// Check if ALL children are safe
		for _, childHash := range childrenHashes {
			if !safetyMap[childHash] {
				// At least one child not yet stable - skip deletion for now
				// Parent will be reconsidered in future cleanup passes
				s.logger.Debugf("Worker %d: skipping deletion of parent %s - child %s not yet safe (%d children total)",
					workerID, txHashStr, childHash[:8], len(childrenHashes))
				return nil
			}
		}
	}

	// Safe to delete - get inputs for parent update
	inputs, err := s.getTxInputsFromBins(job, workerID, bins, txHash)
	if err != nil {
		return err
	}

	// Build parent updates and deletions using main's batch accumulation pattern
	parentUpdates := make(map[string]*parentUpdateInfo, len(inputs)) // One parent per input (worst case)

	// Accumulate parent updates
	for _, input := range inputs {
		keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.utxoBatchSize)
		parentKeyStr := string(keySource)

		if existing, ok := parentUpdates[parentKeyStr]; ok {
			existing.childHashes = append(existing.childHashes, txHash)
		} else {
			parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
			if err != nil {
				return errors.NewProcessingError("Worker %d: failed to create parent key: %v", workerID, err)
			}
			parentUpdates[parentKeyStr] = &parentUpdateInfo{
				key:         parentKey,
				childHashes: []*chainhash.Hash{txHash},
			}
		}
	}

	// Handle external transactions: add file for deletion
	externalFiles := make([]*externalFileInfo, 0)
	external, isExternal := bins[s.fieldExternal].(bool)
	if isExternal && external {
		// Determine file type: if we found inputs, it's a .tx file, otherwise it's .outputs
		fileType := fileformat.FileTypeOutputs
		if len(inputs) > 0 {
			fileType = fileformat.FileTypeTx
		}
		externalFiles = append(externalFiles, &externalFileInfo{
			txHash:   txHash,
			fileType: fileType,
		})
	}

	// Accumulate deletions: master record + any child records
	deletions := []*aerospike.Key{rec.Record.Key}

	// If this is a multi-record transaction, delete all child records
	totalExtraRecs, hasExtraRecs := bins[s.fieldTotalExtraRecs].(int)
	if hasExtraRecs && totalExtraRecs > 0 {
		// Generate keys for all child records: txid_1, txid_2, ..., txid_N
		for i := 1; i <= totalExtraRecs; i++ {
			childKeySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
			childKey, err := aerospike.NewKey(s.namespace, s.set, childKeySource)
			if err != nil {
				s.logger.Errorf("Worker %d: failed to create child key for %s_%d: %v", workerID, txHashStr, i, err)
				continue
			}
			deletions = append(deletions, childKey)
		}
		s.logger.Debugf("Worker %d: deleting external tx %s with %d child records", workerID, txHashStr, totalExtraRecs)
	}

	// Execute parent updates and deletion
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.flushCleanupBatches(ctx, workerID, job.BlockHeight, parentUpdates, deletions, externalFiles); err != nil {
		return errors.NewProcessingError("Worker %d: error flushing operations for tx %s: %v", workerID, txHashStr, err)
	}

	return nil
}

// batchVerifyChildrenSafety checks multiple child transactions at once to determine if their parents
// can be safely deleted. This is much more efficient than checking each child individually.
//
// Safety guarantee: A parent can only be deleted if ALL spending children have been mined and stable
// for at least 288 blocks. This prevents orphaning children by ensuring we never delete a parent while
// ANY of its spending children might still be reorganized out of the chain.
//
// The spending children are extracted from the parent's UTXO spending_data (embedded in each spent UTXO).
// This ensures we verify EVERY child that spent any output, not just one representative child.
//
// Parameters:
//   - spendingChildrenHashes: Map of child TX hashes to verify (32 bytes each) - ALL unique children
//   - currentBlockHeight: Current block height for safety window calculation
//
// Returns:
//   - map[string]bool: Map of childHash (hex string) -> isSafe (true = this child is stable)
func (s *Service) batchVerifyChildrenSafety(lastSpenderHashes map[string][]byte, currentBlockHeight uint32, deletedChildren map[string]bool) map[string]bool {
	if len(lastSpenderHashes) == 0 {
		return make(map[string]bool)
	}

	safetyMap := make(map[string]bool, len(lastSpenderHashes))

	// Mark already-deleted children as safe immediately
	// If a child is in deletedChildren, it means it was already pruned successfully
	// and shouldn't block the parent from being pruned
	markedSafeCount := 0
	for hexHash := range deletedChildren {
		if _, exists := lastSpenderHashes[hexHash]; exists {
			safetyMap[hexHash] = true
			markedSafeCount++
		} else {
			s.logger.Debugf("[batchVerifyChildrenSafety] Deleted child %s not in lastSpenderHashes (not a child of any parent in this chunk)", hexHash[:8])
		}
	}
	if markedSafeCount > 0 {
		s.logger.Infof("[batchVerifyChildrenSafety] Marked %d already-deleted children as safe", markedSafeCount)
	}

	// Process children in batches to avoid overwhelming Aerospike
	batchSize := s.defensiveBatchReadSize
	if batchSize <= 0 {
		batchSize = 1024 // Default batch size if not configured
	}

	// Convert map to slice for batching, skipping already-deleted children
	// Children in deletedChildren are already marked as safe, no need to query Aerospike
	hashEntries := make([]childHashEntry, 0, len(lastSpenderHashes))
	for hexHash, hashBytes := range lastSpenderHashes {
		// Skip children that are already marked as safe (deleted)
		if safetyMap[hexHash] {
			continue
		}
		hashEntries = append(hashEntries, childHashEntry{hexHash: hexHash, hashBytes: hashBytes})
	}

	// Process in batches
	for i := 0; i < len(hashEntries); i += batchSize {
		end := i + batchSize
		if end > len(hashEntries) {
			end = len(hashEntries)
		}
		batch := hashEntries[i:end]

		s.processBatchOfChildren(batch, safetyMap, currentBlockHeight)
	}

	s.logger.Debugf("[batchVerifyChildrenSafety] Verified %d children: %d safe, %d not safe",
		len(safetyMap), countTrue(safetyMap), countFalse(safetyMap))

	return safetyMap
}

// childHashEntry holds a child transaction hash for batch processing
type childHashEntry struct {
	hexHash   string
	hashBytes []byte
}

// processBatchOfChildren verifies a batch of child transactions
func (s *Service) processBatchOfChildren(batch []childHashEntry, safetyMap map[string]bool, currentBlockHeight uint32) {
	// Create batch read operations
	batchPolicy := aerospike.NewBatchPolicy()
	batchPolicy.MaxRetries = 3
	batchPolicy.TotalTimeout = 120 * time.Second

	readPolicy := aerospike.NewBatchReadPolicy()
	readPolicy.ReadModeSC = aerospike.ReadModeSCSession

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	hashToKey := make(map[string]string, len(batch)) // hex hash -> key for mapping

	for _, entry := range batch {
		hexHash := entry.hexHash
		hashBytes := entry.hashBytes
		if len(hashBytes) != 32 {
			s.logger.Warnf("[batchVerifyChildrenSafety] Invalid hash length for %s", hexHash)
			safetyMap[hexHash] = false
			continue
		}

		childHash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			s.logger.Warnf("[batchVerifyChildrenSafety] Failed to create hash: %v", err)
			safetyMap[hexHash] = false
			continue
		}

		key, err := aerospike.NewKey(s.namespace, s.set, childHash[:])
		if err != nil {
			s.logger.Warnf("[batchVerifyChildrenSafety] Failed to create key for child %s: %v", childHash.String(), err)
			safetyMap[hexHash] = false
			continue
		}

		batchRecords = append(batchRecords, aerospike.NewBatchRead(
			readPolicy,
			key,
			[]string{s.fieldUnminedSince, s.fieldBlockHeights},
		))
		hashToKey[hexHash] = key.String()
	}

	if len(batchRecords) == 0 {
		return
	}

	// Execute batch operation
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[processBatchOfChildren] Batch operation failed: %v", err)
		// Mark all in this batch as unsafe on batch error
		for hexHash := range hashToKey {
			safetyMap[hexHash] = false
		}
		return
	}

	// Process results - use configured retention setting as safety window
	safetyWindow := s.blockHeightRetention

	// Build reverse map for O(1) lookup instead of O(n²) nested loop
	// This avoids scanning all batch records for each child hash
	keyToRecord := make(map[string]*aerospike.BatchRecord, len(batchRecords))
	for _, batchRec := range batchRecords {
		keyToRecord[batchRec.BatchRec().Key.String()] = batchRec.BatchRec()
	}

	for hexHash, keyStr := range hashToKey {
		// O(1) map lookup instead of O(n) scan
		record := keyToRecord[keyStr]
		if record == nil {
			safetyMap[hexHash] = false
			continue
		}

		if record.Err != nil {
			// Check if this is a "key not found" error - child was already deleted
			// This can happen due to race conditions when processing chunks in parallel:
			// - Chunk 1 deletes child C and updates parent A's deletedChildren
			// - Chunk 2 already loaded parent A (before the update) and now queries child C
			// - Child C is gone, so we get KEY_NOT_FOUND_ERROR
			// In this case, the child is ALREADY deleted, so it's safe to consider it stable
			if aerospikeErr, ok := record.Err.(*aerospike.AerospikeError); ok {
				if aerospikeErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// Child already deleted by another chunk - safe to proceed with parent deletion
					safetyMap[hexHash] = true
					continue
				}
			}
			// Any other error → be conservative, don't delete parent
			s.logger.Warnf("[batchVerifyChildrenSafety] Unexpected error for child %s: %v", hexHash[:8], record.Err)
			safetyMap[hexHash] = false
			continue
		}

		if record.Record == nil || record.Record.Bins == nil {
			safetyMap[hexHash] = false
			continue
		}

		bins := record.Record.Bins

		// Check unmined status
		unminedSince, hasUnminedSince := bins[s.fieldUnminedSince]
		if hasUnminedSince && unminedSince != nil {
			// Child is unmined, not safe
			safetyMap[hexHash] = false
			continue
		}

		// Check block heights
		blockHeightsRaw, hasBlockHeights := bins[s.fieldBlockHeights]
		if !hasBlockHeights {
			// No block heights, treat as not safe
			safetyMap[hexHash] = false
			continue
		}

		blockHeightsList, ok := blockHeightsRaw.([]interface{})
		if !ok || len(blockHeightsList) == 0 {
			safetyMap[hexHash] = false
			continue
		}

		// Find maximum block height
		var maxChildBlockHeight uint32
		for _, heightRaw := range blockHeightsList {
			height, ok := heightRaw.(int)
			if ok && uint32(height) > maxChildBlockHeight {
				maxChildBlockHeight = uint32(height)
			}
		}

		if maxChildBlockHeight == 0 {
			safetyMap[hexHash] = false
			continue
		}

		// Check if child has been stable long enough
		if currentBlockHeight < maxChildBlockHeight+safetyWindow {
			safetyMap[hexHash] = false
		} else {
			safetyMap[hexHash] = true
		}
	}
}

// Helper to count true values in map
func countTrue(m map[string]bool) int {
	count := 0
	for _, v := range m {
		if v {
			count++
		}
	}
	return count
}

// Helper to count false values in map
func countFalse(m map[string]bool) int {
	count := 0
	for _, v := range m {
		if !v {
			count++
		}
	}
	return count
}

func (s *Service) getTxInputsFromBins(job *pruner.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	var inputs []*bt.Input

	external, ok := bins[s.fieldExternal].(bool)
	if ok && external {
		// transaction is external, we need to get the data from the external store
		txBytes, err := s.external.Get(s.ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Check if outputs exist (sometimes only outputs are stored)
				exists, err := s.external.Exists(s.ctx, txHash.CloneBytes(), fileformat.FileTypeOutputs)
				if err != nil {
					return nil, errors.NewProcessingError("Worker %d: error checking existence of outputs for external tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
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
			return nil, errors.NewProcessingError("Worker %d: error getting external tx %s for cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewProcessingError("Worker %d: invalid tx bytes for external tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
		}

		inputs = tx.Inputs
	} else {
		// get the inputs from the record directly
		inputsValue := bins[s.fieldInputs]
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
				return nil, errors.NewProcessingError("Worker %d: invalid input for record in cleanup job %d: %v", workerID, job.BlockHeight, err)
			}
		}
	}

	return inputs, nil
}

func (s *Service) markJobAsFailed(job *pruner.Job, err error) {
	job.SetStatus(pruner.JobStatusFailed)
	job.Error = err
	job.Ended = time.Now()
}

// flushCleanupBatches flushes accumulated parent updates, external file deletions, and Aerospike deletions
func (s *Service) flushCleanupBatches(ctx context.Context, workerID int, blockHeight uint32, parentUpdates map[string]*parentUpdateInfo, deletions []*aerospike.Key, externalFiles []*externalFileInfo) error {
	// Execute parent updates first
	if len(parentUpdates) > 0 {
		if err := s.executeBatchParentUpdates(ctx, workerID, blockHeight, parentUpdates); err != nil {
			return err
		}
	}

	// Delete external files before Aerospike records (fail-safe: if file deletion fails, we keep the record)
	if len(externalFiles) > 0 {
		if err := s.executeBatchExternalFileDeletions(ctx, workerID, blockHeight, externalFiles); err != nil {
			return err
		}
	}

	// Delete Aerospike records last
	if len(deletions) > 0 {
		if err := s.executeBatchDeletions(ctx, workerID, blockHeight, deletions); err != nil {
			return err
		}
	}

	return nil
}

// extractTxHash extracts the transaction hash from record bins
func (s *Service) extractTxHash(bins aerospike.BinMap) (*chainhash.Hash, error) {
	txIDBytes, ok := bins[s.fieldTxID].([]byte)
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
func (s *Service) extractInputs(job *pruner.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	return s.getTxInputsFromBins(job, workerID, bins, txHash)
}

// executeBatchParentUpdates executes a batch of parent update operations
func (s *Service) executeBatchParentUpdates(ctx context.Context, workerID int, blockHeight uint32, updates map[string]*parentUpdateInfo) error {
	if len(updates) == 0 {
		return nil
	}

	// Convert map to batch operations
	// Track deleted children by adding child tx hashes to the DeletedChildren map
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))

	for _, info := range updates {
		// For each child transaction being deleted, add it to the DeletedChildren map
		ops := make([]*aerospike.Operation, len(info.childHashes))
		for i, childHash := range info.childHashes {
			ops[i] = aerospike.MapPutOp(mapPolicy, s.fieldDeletedChildren,
				aerospike.NewStringValue(childHash.String()), aerospike.BoolValue(true))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(s.batchWritePolicy, info.key, ops...))
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Worker %d: context cancelled, skipping parent update batch", workerID)
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch parent update failed: %v", workerID, err)
		return errors.NewStorageError("batch parent update failed", err)
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

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d parent update operations failed", workerID, errorCount)
	}

	return nil
}

// executeBatchDeletions executes a batch of deletion operations
func (s *Service) executeBatchDeletions(ctx context.Context, workerID int, blockHeight uint32, keys []*aerospike.Key) error {
	if len(keys) == 0 {
		return nil
	}

	// Create batch delete records
	batchDeletePolicy := aerospike.NewBatchDeletePolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))
	for i, key := range keys {
		batchRecords[i] = aerospike.NewBatchDelete(batchDeletePolicy, key)
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Worker %d: context cancelled, skipping deletion batch", workerID)
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch deletion failed for %d records: %v", workerID, len(keys), err)
		return errors.NewStorageError("batch deletion failed", err)
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

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d deletion operations failed", workerID, errorCount)
	}

	return nil
}

// executeBatchExternalFileDeletions deletes external blob files for transactions being pruned
func (s *Service) executeBatchExternalFileDeletions(ctx context.Context, workerID int, blockHeight uint32, files []*externalFileInfo) error {
	if len(files) == 0 {
		return nil
	}

	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for _, fileInfo := range files {
		// Check context before each deletion
		select {
		case <-ctx.Done():
			s.logger.Infof("Worker %d: context cancelled, stopping external file deletions", workerID)
			return ctx.Err()
		default:
		}

		// Delete the external file
		err := s.external.Del(ctx, fileInfo.txHash.CloneBytes(), fileInfo.fileType)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Already deleted (by LocalDAH cleanup or previous pruning)
				alreadyDeletedCount++
				s.logger.Debugf("Worker %d: external file for tx %s (type %d) already deleted", workerID, fileInfo.txHash.String(), fileInfo.fileType)
			} else {
				s.logger.Errorf("Worker %d: failed to delete external file for tx %s (type %d): %v", workerID, fileInfo.txHash.String(), fileInfo.fileType, err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	s.logger.Debugf("Worker %d: external file deletion batch - success: %d, already deleted: %d, errors: %d", workerID, successCount, alreadyDeletedCount, errorCount)

	// Return error if any deletions failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d external file deletions failed", workerID, errorCount)
	}

	return nil
}

// ProcessSingleRecord processes a single transaction for cleanup (for testing/manual cleanup)
// This is a simplified wrapper around the batch operations for single-record processing
func (s *Service) ProcessSingleRecord(txHash *chainhash.Hash, inputs []*bt.Input) error {
	if len(inputs) == 0 {
		return nil // No parents to update
	}

	// Build parent updates map
	parentUpdates := make(map[string]*parentUpdateInfo, len(inputs)) // One parent per input (worst case)
	for _, input := range inputs {
		keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.utxoBatchSize)
		parentKeyStr := string(keySource)

		if existing, ok := parentUpdates[parentKeyStr]; ok {
			existing.childHashes = append(existing.childHashes, txHash)
		} else {
			parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
			if err != nil {
				return errors.NewProcessingError("failed to create parent key", err)
			}
			parentUpdates[parentKeyStr] = &parentUpdateInfo{
				key:         parentKey,
				childHashes: []*chainhash.Hash{txHash},
			}
		}
	}

	// Execute parent updates synchronously
	return s.executeBatchParentUpdates(s.ctx, 0, 0, parentUpdates)
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*pruner.Job {
	return s.jobManager.GetJobs()
}

// GetJobByHeight returns a job for the specified block height
func (s *Service) GetJobByHeight(blockHeight uint32) *pruner.Job {
	return s.jobManager.GetJobByHeight(blockHeight)
}
