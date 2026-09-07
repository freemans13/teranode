package pruner

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	safetyWindow     uint32 // Block height retention for child stability verification
	defensiveEnabled bool   // Enable defensive checks before deleting UTXO transactions
	logger           ulogger.Logger
	settings         *settings.Settings
	db               *usql.DB
	ctx              context.Context
}

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// DB is the SQL database connection
	DB *usql.DB

	// Ctx is the context to use to signal shutdown
	Ctx context.Context

	// SafetyWindow is the number of blocks a child must be stable before parent deletion
	// If not specified, defaults to global_blockHeightRetention (288 blocks)
	SafetyWindow uint32
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

	safetyWindow := opts.SafetyWindow
	if safetyWindow == 0 {
		// Default to global retention setting (288 blocks)
		safetyWindow = tSettings.GlobalBlockHeightRetention
	}

	service := &Service{
		safetyWindow:     safetyWindow,
		defensiveEnabled: tSettings.Pruner.UTXODefensiveEnabled,
		logger:           opts.Logger,
		settings:         tSettings,
		db:               opts.DB,
		ctx:              opts.Ctx,
	}

	return service, nil
}

// Start starts the cleanup service
func (s *Service) Start(ctx context.Context) {
	s.logger.Infof("[SQLCleanupService] service ready")
}

// AddObserver adds an observer to be notified when pruning completes.
// This is a no-op for the SQL pruner service as it doesn't support observers yet.
func (s *Service) AddObserver(observer pruner.Observer) {
	// No-op: SQL pruner doesn't support observers yet
}

// Prune removes transactions marked for deletion at or before the specified height.
// Returns the number of records processed and any error encountered.
// This method is synchronous and blocks until pruning completes or context is cancelled.
func (s *Service) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("Cannot prune at block height 0")
	}

	startTime := time.Now()

	// Log start of cleanup
	s.logger.Infof("[pruner][%s:%d] phase 2: starting cleanup scan (delete_at_height <= %d)",
		blockHashStr, blockHeight, blockHeight)

	// Execute the cleanup
	deletedCount, err := s.deleteTombstoned(ctx, blockHeight)
	if err != nil {
		s.logger.Errorf("[pruner][%s:%d] phase 2: cleanup failed: %v", blockHashStr, blockHeight, err)
		return 0, err
	}

	// Calculate throughput
	elapsed := time.Since(startTime)
	tps := float64(deletedCount) / elapsed.Seconds()

	// Format TPS for readability
	var tpsStr string
	if tps >= 1_000_000 {
		tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
	} else if tps >= 1_000 {
		tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
	} else {
		tpsStr = fmt.Sprintf("%.2f records/sec", tps)
	}

	s.logger.Infof("[pruner][%s:%d] phase 2: completed cleanup in %v: deleted %s records (%s)",
		blockHashStr, blockHeight, elapsed, util.FormatComma(deletedCount), tpsStr)

	return deletedCount, nil
}

// deleteTombstoned removes transactions that have passed their expiration time.
// Only deletes parent transactions if their last spending child is mined and stable.
func (s *Service) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	// Use configured safety window from settings
	safetyWindow := s.safetyWindow

	// Defensive child verification is conditional on the UTXODefensiveEnabled setting
	// When disabled, parents are deleted without verifying children are stable
	// Each branch is a complete literal statement rather than a fragment
	// concatenated at runtime, so no part of the SQL is ever built from data.
	var createCandidates string

	args := []interface{}{blockHeight}

	if !s.defensiveEnabled {
		// Defensive mode disabled - delete all transactions past their expiration
		createCandidates = `
			CREATE TEMP TABLE utxo_prune_candidates AS
			SELECT id FROM transactions
			WHERE delete_at_height IS NOT NULL
			  AND delete_at_height <= $1
		`
	} else {
		// Defensive mode enabled - verify ALL spending children are stable before deletion
		// This prevents orphaning any child transaction
		createCandidates = `
			CREATE TEMP TABLE utxo_prune_candidates AS
			SELECT id FROM transactions
			WHERE id IN (
				SELECT t.id
				FROM transactions t
				WHERE t.delete_at_height IS NOT NULL
				  AND t.delete_at_height <= $1
				  AND NOT EXISTS (
				    -- Find ANY unstable child - if found, parent cannot be deleted
				    -- This ensures ALL children must be stable before parent deletion
				    SELECT 1
				    FROM outputs o
				    WHERE o.transaction_id = t.id
				      AND o.spending_data IS NOT NULL
				      AND NOT EXISTS (
				          SELECT 1 FROM deleted_children d
				          WHERE d.parent_id = t.id
				            AND d.child_hash = substr(o.spending_data, 1, 32)
				      )
				      AND (
				        -- Extract child TX hash from spending_data (first 32 bytes)
				        -- Check if this child is NOT stable
				        NOT EXISTS (
				          SELECT 1
				          FROM transactions child
				          INNER JOIN block_ids child_blocks ON child.id = child_blocks.transaction_id
				          WHERE child.hash = substr(o.spending_data, 1, 32)
				            AND child.unmined_since IS NULL  -- Child must be mined
				            AND child_blocks.block_height <= ($1 - $2)  -- Child must be stable
				        )
				      )
				  )
			)
		`
		args = append(args, safetyWindow)
	}

	// Use one serializable snapshot for candidate selection, parent markers and
	// deletion. Cross-record atomicity is required even with defensive mode off.
	txn, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, errors.NewStorageError("failed to begin pruning transaction", err)
	}
	defer func() { _ = txn.Rollback() }()

	// Materialize the candidates once: newly inserted markers can make another
	// parent eligible in defensive mode, but that parent needs its own markers
	// before a later prune can delete it. Temporary tables are connection-local
	// and this transaction owns the connection until commit or rollback.
	if _, err := txn.ExecContext(ctx, createCandidates, args...); err != nil {
		return 0, errors.NewStorageError("failed to select pruning candidates", err)
	}
	deleteQuery := "DELETE FROM transactions WHERE id IN (SELECT id FROM utxo_prune_candidates)"
	markerQuery := `INSERT INTO deleted_children (parent_id, child_hash)
  SELECT DISTINCT parent.id, child.hash
  FROM utxo_prune_candidates candidate
  JOIN transactions child ON child.id = candidate.id
  JOIN inputs i ON i.transaction_id = child.id
  JOIN transactions parent ON parent.hash = i.previous_transaction_hash
  WHERE true
  ON CONFLICT (parent_id, child_hash) DO NOTHING`
	if _, err := txn.ExecContext(ctx, markerQuery); err != nil {
		return 0, errors.NewStorageError("failed to mark pruned children", err)
	}
	result, err := txn.ExecContext(ctx, deleteQuery)

	if err != nil {
		return 0, errors.NewStorageError("failed to delete transactions", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, errors.NewStorageError("failed to get rows affected", err)
	}

	if _, err := txn.ExecContext(ctx, "DROP TABLE utxo_prune_candidates"); err != nil {
		return 0, errors.NewStorageError("failed to clear pruning candidates", err)
	}

	if err := txn.Commit(); err != nil {
		return 0, errors.NewStorageError("failed to commit pruning transaction", err)
	}

	return count, nil
}
