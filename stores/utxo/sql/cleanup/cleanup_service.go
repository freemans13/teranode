package cleanup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/ordishs/gocore"
)

// Ensure Store implements the Cleanup Service interface
var _ cleanup.Service = (*Service)(nil)

const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 2

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	logger             ulogger.Logger
	settings           *settings.Settings
	db                 *usql.DB
	jobManager         *cleanup.JobManager
	ctx                context.Context
	getPersistedHeight func() uint32
}

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// DB is the SQL database connection
	DB *usql.DB

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int

	// Ctx is the context to use to signal shutdown
	Ctx context.Context
}

// NewService creates a new cleanup service for the SQL store
func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if tSettings == nil {
		return nil, errors.NewProcessingError("settings is required")
	}

	if opts.DB == nil {
		return nil, errors.NewProcessingError("db is required")
	}

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount, _ = gocore.Config().GetInt("sql_cleanup_worker_count", DefaultWorkerCount)
	}

	maxJobsHistory := opts.MaxJobsHistory
	if maxJobsHistory <= 0 {
		maxJobsHistory = DefaultMaxJobsHistory
	}

	service := &Service{
		logger:   opts.Logger,
		settings: tSettings,
		db:       opts.DB,
		ctx:      opts.Ctx,
	}

	// Create the job processor function
	jobProcessor := func(job *cleanup.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := cleanup.NewJobManager(cleanup.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    workerCount,
		MaxJobsHistory: maxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

	return service, nil
}

// Start starts the cleanup service
func (s *Service) Start(ctx context.Context) {
	s.logger.Infof("[SQLCleanupService] starting cleanup service")
	s.jobManager.Start(ctx)
}

// UpdateBlockHeight updates the current block height and triggers cleanup if needed
func (s *Service) UpdateBlockHeight(blockHeight uint32, doneCh ...chan string) error {
	if blockHeight == 0 {
		return errors.NewProcessingError("Cannot update block height to 0")
	}

	return s.jobManager.TriggerCleanup(blockHeight, doneCh...)
}

// SetPersistedHeightGetter sets the function used to get block persister progress.
// This allows cleanup to coordinate with block persister to avoid premature deletion.
func (s *Service) SetPersistedHeightGetter(getter func() uint32) {
	s.getPersistedHeight = getter
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}

// processCleanupJob executes the cleanup for a specific job
func (s *Service) processCleanupJob(job *cleanup.Job, workerID int) {
	s.logger.Debugf("[SQLCleanupService %d] running cleanup job for block height %d", workerID, job.BlockHeight)

	job.Started = time.Now()

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
				s.logger.Infof("[SQLCleanupService %d] Limiting cleanup from height %d to %d (persisted: %d, retention: %d)",
					workerID, job.BlockHeight, maxSafeHeight, persistedHeight, retention)
				safeCleanupHeight = maxSafeHeight
			}
		}
	}

	// Execute the cleanup with safe height
	err := deleteTombstoned(s.db, s.settings, s.logger, safeCleanupHeight, workerID)

	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("[SQLCleanupService %d] cleanup job failed for block height %d: %v", workerID, job.BlockHeight, err)

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusFailed.String()
			close(job.DoneCh)
		}
	} else {
		job.SetStatus(cleanup.JobStatusCompleted)
		job.Ended = time.Now()

		s.logger.Debugf("[SQLCleanupService %d] cleanup job completed for block height %d in %v",
			workerID, job.BlockHeight, time.Since(job.Started))

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusCompleted.String()
			close(job.DoneCh)
		}
	}
}

// ChildVerificationResult contains the result of verifying a transaction's children
type ChildVerificationResult struct {
	CanDelete           bool
	Reason              string
	ProblematicChildren []ChildInfo
}

// ChildInfo contains information about a child transaction
type ChildInfo struct {
	Hash        string
	IsMined     bool
	BlockHeight *uint32
}

// batchVerifyChildrenBeforeDeletion checks all transactions' children in a single query
// Returns a map of tx hash -> verification result
// NOTE: Transactions with no outputs at all will NOT be in the returned map - caller must handle this
func batchVerifyChildrenBeforeDeletion(db *usql.DB, currentHeight uint32, retention uint32) (map[string]*ChildVerificationResult, error) {
	// Single query to get ALL outputs (spent and unspent) for transactions to be deleted
	// This replaces N queries with 1 query, massively improving performance
	//
	// IMPORTANT: We return ALL transactions that have outputs. For defensive verification:
	// 1. Check that ALL outputs are spent (no unspent UTXOs remaining)
	// 2. Check that all children of spent outputs are mined and confirmed deep enough
	query := `
		WITH transactions_to_check AS (
			SELECT id, hash
			FROM transactions
			WHERE delete_at_height <= $1
		)
		SELECT
			ttc.hash as parent_hash,
			o.idx as output_index,
			o.spending_data,
			t_child.hash as child_hash,
			t_child.block_height as child_block_height
		FROM transactions_to_check ttc
		INNER JOIN outputs o ON ttc.id = o.transaction_id
		LEFT JOIN inputs i ON o.spending_data IS NOT NULL
			AND i.previous_transaction_hash = ttc.hash
			AND i.previous_tx_idx = o.idx
		LEFT JOIN transactions t_child ON i.transaction_id = t_child.id
		ORDER BY ttc.hash, o.idx
	`

	rows, err := db.Query(query, currentHeight)
	if err != nil {
		return nil, errors.NewStorageError("failed to batch query children", err)
	}
	defer rows.Close()

	// Build results map
	results := make(map[string]*ChildVerificationResult)

	for rows.Next() {
		var parentHash string
		var outputIndex uint32
		var spendingData *[]byte
		var childHash *string
		var childBlockHeight *uint32

		if err := rows.Scan(&parentHash, &outputIndex, &spendingData, &childHash, &childBlockHeight); err != nil {
			return nil, errors.NewStorageError("failed to scan batch child row", err)
		}

		// Initialize result for this parent if not exists
		if _, exists := results[parentHash]; !exists {
			results[parentHash] = &ChildVerificationResult{
				CanDelete:           true,
				ProblematicChildren: []ChildInfo{},
			}
		}

		result := results[parentHash]

		// First check: Is this an unspent output?
		if spendingData == nil {
			// UNSPENT OUTPUT - Transaction should NOT be deleted (has UTXOs still in use)
			result.CanDelete = false
			result.Reason = "has unspent outputs (UTXOs still in use)"
			result.ProblematicChildren = append(result.ProblematicChildren, ChildInfo{
				Hash:        fmt.Sprintf("output_%d_unspent", outputIndex),
				IsMined:     false,
				BlockHeight: nil,
			})
			continue
		}

		// Output is spent - analyze the child transaction
		// Child transaction should exist if output is spent
		if childHash == nil {
			// Spent output but no child found - data inconsistency
			result.CanDelete = false
			result.Reason = "has spent outputs with missing child transactions"
			result.ProblematicChildren = append(result.ProblematicChildren, ChildInfo{
				Hash:        fmt.Sprintf("output_%d", outputIndex),
				IsMined:     false,
				BlockHeight: nil,
			})
			continue
		}

		child := ChildInfo{
			Hash:        *childHash,
			IsMined:     childBlockHeight != nil,
			BlockHeight: childBlockHeight,
		}

		// Check if child is mined
		if !child.IsMined {
			result.CanDelete = false
			result.Reason = "has unmined children"
			result.ProblematicChildren = append(result.ProblematicChildren, child)
			continue
		}

		// Check if child is confirmed deep enough (current height - child height > retention)
		confirmationDepth := currentHeight - *childBlockHeight
		if confirmationDepth <= retention {
			result.CanDelete = false
			result.Reason = "has children not confirmed deep enough"
			result.ProblematicChildren = append(result.ProblematicChildren, child)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating batch child rows", err)
	}

	return results, nil
}

// deleteTombstoned removes transactions that have passed their expiration time.
func deleteTombstoned(db *usql.DB, tSettings *settings.Settings, logger ulogger.Logger, blockHeight uint32, workerID int) error {
	// If defensive cleanup is disabled, use the simple delete approach
	if !tSettings.UtxoStore.DefensiveCleanupEnabled {
		// Delete transactions that have passed their expiration time
		// this will cascade to inputs, outputs, block_ids and conflicting_children
		if _, err := db.Exec("DELETE FROM transactions WHERE delete_at_height <= $1", blockHeight); err != nil {
			return errors.NewStorageError("failed to delete transactions", err)
		}
		return nil
	}

	// Defensive cleanup: verify children before deletion
	logger.Debugf("[SQLCleanupService %d] defensive cleanup enabled, verifying children before deletion", workerID)

	retention := tSettings.GetUtxoStoreBlockHeightRetention()

	// Batch verify all transactions and their children in a single query
	verificationResults, err := batchVerifyChildrenBeforeDeletion(db, blockHeight, retention)
	if err != nil {
		return errors.NewStorageError("failed to batch verify children", err)
	}

	// Query all transactions that would be deleted
	query := "SELECT hash FROM transactions WHERE delete_at_height <= $1"
	rows, err := db.Query(query, blockHeight)
	if err != nil {
		return errors.NewStorageError("failed to query transactions for deletion", err)
	}
	defer rows.Close()

	var txHashesToDelete []string
	skippedCount := 0

	for rows.Next() {
		var txHash string
		if err := rows.Scan(&txHash); err != nil {
			return errors.NewStorageError("failed to scan transaction hash", err)
		}

		// Check verification result from batch query
		verificationResult, hasSpentOutputs := verificationResults[txHash]

		// DEFENSIVE: If transaction is not in verification results, it means we found no spent outputs
		// This could mean:
		// 1. Transaction has unspent outputs (should NOT delete - UTXOs still in use!)
		// 2. Transaction has no outputs at all (rare edge case)
		// 3. Data inconsistency (delete_at_height set but outputs not properly tracked)
		//
		// In defensive mode, we SKIP deletion if we don't have positive confirmation
		// that all children are safe. This is the conservative/safe approach.
		if !hasSpentOutputs {
			skippedCount++
			logger.Warnf("[SQLCleanupService %d] skipping deletion of tx %s: no spent outputs found (may have unspent UTXOs or data inconsistency)",
				workerID, txHash[:16])
			continue
		}

		// Transaction has spent outputs, check if all their children are safe
		if verificationResult.CanDelete {
			txHashesToDelete = append(txHashesToDelete, txHash)
		} else {
			skippedCount++

			// Log warning with details
			problematicChildrenInfo := ""
			for i, child := range verificationResult.ProblematicChildren {
				if i > 0 {
					problematicChildrenInfo += ", "
				}
				if child.IsMined {
					problematicChildrenInfo += fmt.Sprintf("%s... (mined at height %d)", child.Hash[:8], *child.BlockHeight)
				} else {
					problematicChildrenInfo += child.Hash[:8] + "... (unmined)"
				}
			}

			logger.Warnf("[SQLCleanupService %d] skipping deletion of tx %s: %s [children: %s]",
				workerID, txHash[:16], verificationResult.Reason, problematicChildrenInfo)
		}
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("error iterating transactions", err)
	}

	// Delete transactions that passed verification
	if len(txHashesToDelete) > 0 {
		logger.Infof("[SQLCleanupService %d] deleting %d transactions (skipped %d)", workerID, len(txHashesToDelete), skippedCount)

		// Delete in batches to avoid exceeding database parameter limits
		// PostgreSQL limit is ~32,767 parameters, we use 1000 for safety margin
		const maxBatchSize = 1000
		totalDeleted := 0

		for i := 0; i < len(txHashesToDelete); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(txHashesToDelete) {
				end = len(txHashesToDelete)
			}
			batch := txHashesToDelete[i:end]

			// Build parameterized IN clause for cross-database compatibility
			placeholders := make([]string, len(batch))
			args := make([]interface{}, len(batch))
			for j, hash := range batch {
				placeholders[j] = fmt.Sprintf("$%d", j+1)
				args[j] = hash
			}

			// This will cascade to inputs, outputs, block_ids and conflicting_children
			deleteQuery := fmt.Sprintf("DELETE FROM transactions WHERE hash IN (%s)", strings.Join(placeholders, ","))
			result, err := db.Exec(deleteQuery, args...)
			if err != nil {
				return errors.NewStorageError(fmt.Sprintf("failed to delete batch of %d transactions", len(batch)), err)
			}

			if result != nil {
				if rowsAffected, err := result.RowsAffected(); err == nil {
					totalDeleted += int(rowsAffected)
				}
			}

			logger.Debugf("[SQLCleanupService %d] deleted batch %d-%d (%d transactions)", workerID, i, end, len(batch))
		}

		logger.Infof("[SQLCleanupService %d] completed deletion of %d transactions", workerID, totalDeleted)
	} else {
		logger.Debugf("[SQLCleanupService %d] no transactions to delete (skipped %d)", workerID, skippedCount)
	}

	return nil
}
