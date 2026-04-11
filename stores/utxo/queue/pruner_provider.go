package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util"
)

// Ensure Store implements the pruner.PrunerServiceProvider interface.
var _ pruner.PrunerServiceProvider = (*Store)(nil)

var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
)

// GetPrunerService returns a pruner service for the queue store.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	if prunerServiceInstance != nil {
		return prunerServiceInstance, nil
	}

	svc := &queuePrunerService{
		store:  s,
		logger: s.logger,
	}

	prunerServiceInstance = svc
	return prunerServiceInstance, nil
}

// queuePrunerService implements pruner.Service for the v5 queue store.
type queuePrunerService struct {
	store     *Store
	logger    interface{ Infof(string, ...interface{}) }
	observers []pruner.Observer
	mu        sync.Mutex
}

var _ pruner.Service = (*queuePrunerService)(nil)

// Start starts the pruner service. No background goroutines needed.
func (s *queuePrunerService) Start(_ context.Context) {
	s.logger.Infof("[QueuePrunerService] service ready")
}

// AddObserver adds an observer to be notified when pruning completes.
func (s *queuePrunerService) AddObserver(observer pruner.Observer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, observer)
}

// Prune removes transactions marked for deletion at or before the specified height.
func (s *queuePrunerService) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("cannot prune at block height 0")
	}

	startTime := time.Now()

	s.logger.Infof("[pruner][%s:%d] starting cleanup scan (delete_at_height <= %d)",
		blockHashStr, blockHeight, blockHeight)

	deletedCount, err := s.deleteTombstoned(ctx, blockHeight)
	if err != nil {
		return 0, err
	}

	elapsed := time.Since(startTime)
	tps := float64(deletedCount) / elapsed.Seconds()

	var tpsStr string
	if tps >= 1_000_000 {
		tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
	} else if tps >= 1_000 {
		tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
	} else {
		tpsStr = fmt.Sprintf("%.2f records/sec", tps)
	}

	s.logger.Infof("[pruner][%s:%d] completed cleanup in %v: deleted %s records (%s)",
		blockHashStr, blockHeight, elapsed, util.FormatComma(deletedCount), tpsStr)

	// Notify observers.
	s.mu.Lock()
	observers := make([]pruner.Observer, len(s.observers))
	copy(observers, s.observers)
	s.mu.Unlock()

	for _, obs := range observers {
		obs.OnPruneComplete(blockHeight, deletedCount)
	}

	return deletedCount, nil
}

// deleteTombstoned finds transactions with delete_at_height <= blockHeight and
// cascade-deletes them from all 3 tables.
func (s *queuePrunerService) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	// Find tombstoned tx hashes.
	rows, err := s.store.pool.Query(ctx, `
		SELECT hash FROM txs
		WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1
	`, int64(blockHeight))
	if err != nil {
		return 0, errors.NewStorageError("failed to query tombstoned transactions", err)
	}

	var txHashes [][]byte
	for rows.Next() {
		var hashBytes []byte
		if err := rows.Scan(&hashBytes); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("failed to scan tombstoned tx hash", err)
		}
		txHashes = append(txHashes, hashBytes)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("error iterating tombstoned transactions", err)
	}

	if len(txHashes) == 0 {
		return 0, nil
	}

	// Delete in batches of 500 within a single transaction per batch.
	const batchSize = 500
	var totalDeleted int64

	for i := 0; i < len(txHashes); i += batchSize {
		end := i + batchSize
		if end > len(txHashes) {
			end = len(txHashes)
		}
		batch := txHashes[i:end]

		select {
		case <-ctx.Done():
			return totalDeleted, ctx.Err()
		default:
		}

		pgxTx, err := s.store.pool.Begin(ctx)
		if err != nil {
			return totalDeleted, errors.NewStorageError("[pruner] begin: %v", err)
		}

		for _, hashBytes := range batch {
			deleteStatements := []string{
				`DELETE FROM spends WHERE prev_tx_hash = $1`,
				`DELETE FROM outputs WHERE tx_hash = $1`,
				`DELETE FROM txs WHERE hash = $1`,
			}

			for _, stmt := range deleteStatements {
				if _, err := pgxTx.Exec(ctx, stmt, hashBytes); err != nil {
					pgxTx.Rollback(ctx) //nolint:errcheck
					return totalDeleted, errors.NewStorageError("[pruner] delete failed: %v", err)
				}
			}

			totalDeleted++
		}

		if err := pgxTx.Commit(ctx); err != nil {
			return totalDeleted, errors.NewStorageError("[pruner] commit: %v", err)
		}
	}

	return totalDeleted, nil
}
