package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// Ensure Store implements the pruner.PrunerServiceProvider interface.
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// GetPrunerService returns a pruner service scoped to this store instance,
// creating it lazily on first call. The service holds a reference to the store,
// so keeping it per-instance (rather than a package global) avoids leaking a
// closed store's pool into a later store with the same process lifetime.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	s.prunerServiceMu.Lock()
	defer s.prunerServiceMu.Unlock()

	if s.prunerService != nil {
		return s.prunerService, nil
	}

	s.prunerService = &postgresPrunerService{
		store:  s,
		logger: s.logger,
	}

	return s.prunerService, nil
}

// postgresPrunerService implements pruner.Service for the postgres store.
type postgresPrunerService struct {
	store         *Store
	logger        interface{ Infof(string, ...interface{}) }
	observers     []pruner.Observer
	mu            sync.Mutex
	cursorStarted bool
}

var _ pruner.Service = (*postgresPrunerService)(nil)

// Start starts the pruner service and launches the continuous DAH cursor (Worker 2).
// It is idempotent — subsequent calls are no-ops.
func (s *postgresPrunerService) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cursorStarted {
		s.mu.Unlock()
		return
	}
	s.cursorStarted = true
	s.mu.Unlock()
	s.logger.Infof("[PostgresPrunerService] starting DAH cursor (Worker 2)")
	go s.runDAHCursor(ctx)
}

// AddObserver adds an observer to be notified when pruning completes.
func (s *postgresPrunerService) AddObserver(observer pruner.Observer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, observer)
}

// Prune removes transactions marked for deletion at or before the specified height.
func (s *postgresPrunerService) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("cannot prune at block height 0")
	}

	startTime := time.Now()

	s.logger.Infof("[pruner][%s:%d] starting cleanup scan (delete_at_height <= %d)",
		blockHashStr, blockHeight, blockHeight)

	// Catch-up DAH sweep up to a committed safe-tip before deleting. Worker 2
	// normally keeps this current; this guarantees Prune never misses a tx that
	// completed just before pruning. The stamped DAH is a FUTURE height
	// (completion+retention), so it is disjoint from what this prune deletes.
	lag := int64(s.store.settings.UtxoStore.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}
	if _, err := s.store.sweepDAHUpTo(ctx, s.store.dahSafeTip(lag), 100000); err != nil {
		s.logger.Infof("[pruner][%s:%d] DAH catch-up sweep error (continuing): %v", blockHashStr, blockHeight, err)
	}

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

// deleteTombstoned cascade-deletes every tombstoned tx (delete_at_height <=
// blockHeight) across all partitions IN PARALLEL — one goroutine per partition.
//
// The two tables hash-partition on the SAME tx hash with the SAME modulus, so
// txs_pNN and spends_pNN are ALIGNED: a tx in txs_pNN has all of its
// spend-markers in spends_pNN. That lets each goroutine scan and cascade-delete
// entirely within one partition pair, so no two goroutines ever touch the same
// heap/index pages — zero cross-worker lock/buffer contention. Crucially, the
// per-partition tombstone scan is 1/numPartitions the size and rides its OWN
// leaf index instead of fighting the concurrent-INSERT B-tree splits on the
// shared partitioned-parent index, which is what collapsed concurrent reclaim
// ~13x vs the isolated drain rate.
func (s *postgresPrunerService) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	var totalDeleted atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(pruneDeleteWorkers)
	for p := 0; p < numPartitions; p++ {
		p := p
		g.Go(func() error {
			n, err := s.deleteTombstonedPartition(gctx, p, blockHeight)
			totalDeleted.Add(n)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return totalDeleted.Load(), err
	}

	return totalDeleted.Load(), nil
}

// pruneDeleteWorkers / pruneDeleteBatchSize tune the partition-parallel cascade
// delete. pruneDeleteWorkers caps concurrent partition goroutines (= numPartitions
// when they match, leaving ample headroom in the 100-conn pool for the write
// path); a 10000-hash batch keeps each set-based cascade one bounded leaf index
// scan while minimising round trips.
const (
	pruneDeleteWorkers   = 8
	pruneDeleteBatchSize = 10000
)

// deleteTombstonedPartition cascade-deletes the tombstoned txs of ONE partition.
// It scans the concrete leaf (txs_pNN) for tombstoned hashes, then cascade-deletes
// them from the ALIGNED spends_pNN → txs_pNN leaves in batched, single-statement
// CTEs. One round-trip per batch, and the statement is implicitly atomic: a
// mid-batch failure rolls back both table deletes together, so no orphans are
// left behind. Returns the number of txs rows removed. Leaf table names are
// derived from numPartitions (same package as the schema DDL), so the coupling
// stays in lock-step with partition creation.
func (s *postgresPrunerService) deleteTombstonedPartition(ctx context.Context, partIdx int, blockHeight uint32) (int64, error) {
	txsLeaf := fmt.Sprintf("txs_p%02d", partIdx)
	spendsLeaf := fmt.Sprintf("spends_p%02d", partIdx)

	rows, err := s.store.pool.Query(ctx, fmt.Sprintf(
		`SELECT hash FROM %s WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1`, txsLeaf),
		int64(blockHeight))
	if err != nil {
		return 0, errors.NewStorageError("[pruner] query tombstoned %s: %v", txsLeaf, err)
	}

	var hashes [][]byte
	for rows.Next() {
		var hashBytes []byte
		if scanErr := rows.Scan(&hashBytes); scanErr != nil {
			rows.Close()
			return 0, errors.NewStorageError("[pruner] scan tombstoned %s: %v", txsLeaf, scanErr)
		}
		hashes = append(hashes, hashBytes)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[pruner] iterate tombstoned %s: %v", txsLeaf, err)
	}
	if len(hashes) == 0 {
		return 0, nil
	}

	// spends and txs deleted in ONE statement, scoped to this partition's aligned
	// leaves (same MODULUS). The data-modifying CTE always runs to completion; the
	// top-level DELETE returns the txs parent row count.
	cascadeSQL := fmt.Sprintf(`
		WITH del_spends AS (DELETE FROM %s WHERE prev_tx_hash = ANY($1::bytea[]) RETURNING 1)
		DELETE FROM %s WHERE hash = ANY($1::bytea[])`, spendsLeaf, txsLeaf)

	var deleted int64
	for i := 0; i < len(hashes); i += pruneDeleteBatchSize {
		end := i + pruneDeleteBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}

		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		tag, err := s.store.pool.Exec(ctx, cascadeSQL, hashes[i:end])
		if err != nil {
			return deleted, errors.NewStorageError("[pruner] cascade delete %s: %v", txsLeaf, err)
		}
		// Count txs actually removed (the canonical parent row), not input hashes.
		deleted += tag.RowsAffected()
	}

	return deleted, nil
}
