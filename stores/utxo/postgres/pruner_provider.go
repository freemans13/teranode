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
	cursorCancel  context.CancelFunc
}

var _ pruner.Service = (*postgresPrunerService)(nil)

// Start starts the pruner service and launches the continuous DAH cursor (Worker 2).
// It is idempotent — subsequent calls are no-ops.
//
// The background cursor runs under a store-owned context derived from
// context.Background(), NOT the caller's ctx. The cursor is a long-lived worker
// tied to the store's lifetime; binding it to a request- or startup-scoped ctx
// would silently stop it (permanently, given the cursorStarted guard) when that
// ctx is cancelled. It is stopped explicitly via stop() from Store.Stop/Close.
func (s *postgresPrunerService) Start(_ context.Context) {
	s.mu.Lock()
	if s.cursorStarted {
		s.mu.Unlock()
		return
	}
	s.cursorStarted = true
	cursorCtx, cancel := context.WithCancel(context.Background())
	s.cursorCancel = cancel
	s.mu.Unlock()
	s.logger.Infof("[PostgresPrunerService] starting DAH cursor (Worker 2)")
	go s.runDAHCursor(cursorCtx)
}

// stop cancels the background DAH cursor if it is running. Idempotent.
func (s *postgresPrunerService) stop() {
	s.mu.Lock()
	cancel := s.cursorCancel
	s.cursorCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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

	// Bounded catch-up DAH sweep SLICE, not a sweep-to-tip monolith. A stamped
	// DAH is always a FUTURE height (completion+1+retention), so Prune does not
	// need the watermark at the tip before deleting — it needs stamping and
	// deleting to make progress TOGETHER. The previous unconditional full sweep
	// serialised the two phases: under load each Prune call became a 10-20s
	// sweep followed by a multi-second drain, deletes arrived in rare bursts,
	// the live table outgrew cache, and the run collapsed into a slow regime
	// (bimodal ~88K vs ~65K TPS on identical code). A bounded step keeps each
	// Prune call short so the caller's loop interleaves stamp slices with delete
	// slices continuously; Worker 2's cursor still does the bulk stamping in
	// parallel. For unit-test-sized datasets one 4096-height window covers the
	// whole range, so a single Prune call still stamps+deletes everything.
	lag := int64(s.store.settings.UtxoStore.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}
	if _, err := s.store.sweepDAHStep(ctx, s.store.dahSafeTip(lag), 100000, 2); err != nil {
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
	// pruneDeleteMaxBatchesPerCall bounds one Prune call's delete work per
	// partition, for the same reason the catch-up sweep is bounded (see Prune):
	// a large stamped backlog must not turn one call into a multi-second drain
	// that blocks the caller's loop from interleaving stamping. Callers loop, so
	// the backlog still drains — in bounded slices. Unit-test datasets fit well
	// inside one slice (8 x 10K x 8 partitions = 640K rows per call).
	pruneDeleteMaxBatchesPerCall = 8
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

	// Delete tombstoned txs in bounded batches WITHOUT first loading every matching
	// hash into Go memory. A single statement selects up to pruneDeleteBatchSize
	// doomed hashes and cascade-deletes them from the aligned spends/txs leaves
	// (same MODULUS); we loop until a batch comes back short. Previously the initial
	// SELECT accumulated EVERY tombstoned hash into a [][]byte before any delete —
	// millions of rows per partition (×8 partitions) could OOM the node.
	//
	// The doomed CTE applies the delete_at_height predicate in the SAME statement
	// and snapshot as the DELETE, so a concurrent reorg Unspend that clears the
	// stamp is honoured: a revived tx is simply not selected and keeps its spends.
	// Each loop iteration re-evaluates the predicate from scratch, so revived rows
	// are naturally excluded from later batches.
	cascadeSQL := fmt.Sprintf(`
		WITH doomed AS (
			SELECT hash FROM %[1]s
			WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1
			LIMIT $2
		),
		del_spends AS (DELETE FROM %[2]s WHERE prev_tx_hash IN (SELECT hash FROM doomed) RETURNING 1)
		DELETE FROM %[1]s WHERE hash IN (SELECT hash FROM doomed)`, txsLeaf, spendsLeaf)

	var deleted int64
	for batches := 0; batches < pruneDeleteMaxBatchesPerCall; batches++ {
		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		tag, err := s.store.pool.Exec(ctx, cascadeSQL, int64(blockHeight), int64(pruneDeleteBatchSize))
		if err != nil {
			return deleted, errors.NewStorageError("[pruner] cascade delete %s: %v", txsLeaf, err)
		}

		// RowsAffected is the count of txs parent rows removed this batch, which
		// equals the number of doomed hashes selected. A short batch means the
		// partition is drained.
		n := tag.RowsAffected()
		deleted += n
		if n < int64(pruneDeleteBatchSize) {
			break
		}
	}

	return deleted, nil
}
