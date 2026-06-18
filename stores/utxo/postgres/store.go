package postgres

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ utxo.Store = (*Store)(nil)

// Store implements the utxo.Store interface using direct writes to
// append-only PostgreSQL tables with optional batching for throughput.
type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	storeURL *url.URL

	// pgxpool for all pgx-native operations.
	pool *pgxpool.Pool

	// block state
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32

	// batchers — nil until Start() is called.
	createBatcher *batcher.Batcher[batchCreateItem]
	spendBatcher  *batcher.Batcher[batchSpendItem]
	getBatcher    *batcher.Batcher[batchGetItem]
	unlockBatcher *batcher.Batcher[batchUnlockItem]

	// achieved batch-size instrumentation (items/batches per batcher).
	batchStats batchSizeStats

	// pruner service — lazily created per store instance (not a package global).
	prunerService   pruner.Service
	prunerServiceMu sync.Mutex
}

// batchSizeStats accumulates the real (post-trigger) batch sizes each batcher
// flushes, so a config's effective batch size can be measured rather than assumed.
type batchSizeStats struct {
	createItems, createBatches atomic.Int64
	spendItems, spendBatches   atomic.Int64
	getItems, getBatches       atomic.Int64
	unlockItems, unlockBatches atomic.Int64
}

// BatchSizeSnapshot returns the mean achieved batch size per batcher since the
// previous call and resets the counters. A config that flushes 500-item batches
// is size-cap-bound; a much smaller mean means the timeout/tick/drain trigger is
// firing before the size cap.
func (s *Store) BatchSizeSnapshot() map[string]float64 {
	avg := func(items, batches *atomic.Int64) float64 {
		b := batches.Swap(0)
		it := items.Swap(0)
		if b == 0 {
			return 0
		}
		return float64(it) / float64(b)
	}
	return map[string]float64{
		"create": avg(&s.batchStats.createItems, &s.batchStats.createBatches),
		"spend":  avg(&s.batchStats.spendItems, &s.batchStats.spendBatches),
		"get":    avg(&s.batchStats.getItems, &s.batchStats.getBatches),
		"unlock": avg(&s.batchStats.unlockItems, &s.batchStats.unlockBatches),
	}
}

// New creates a new direct-write UTXO store.
// The storeURL scheme should be "postgres".
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	pgxURL := *storeURL
	pgxURL.Scheme = "postgres"

	pgxConfig, err := pgxpool.ParseConfig(pgxURL.String())
	if err != nil {
		return nil, err
	}

	// Respect pool_max_conns from the connection URL when supplied; otherwise
	// default to 100 (the batchers keep many connections busy under load).
	if !pgxURL.Query().Has("pool_max_conns") {
		pgxConfig.MaxConns = 100
	}

	// Full durability — financial data requires synchronous_commit = on. Enforce it
	// on every connection rather than relying on the server default. Group commit
	// (commit_delay + commit_siblings) is the safe way to recover throughput, but
	// those are server-level GUCs and must be configured on the PostgreSQL instance.
	if pgxConfig.ConnConfig.RuntimeParams == nil {
		pgxConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	pgxConfig.ConnConfig.RuntimeParams["synchronous_commit"] = "on"

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}

	initPrometheusMetrics()

	s := &Store{
		logger:   logger,
		settings: tSettings,
		storeURL: storeURL,
		pool:     pool,
	}

	if err := s.createSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return s, nil
}

// Start initializes the create and spend batchers for throughput optimization.
// Without calling Start(), Create() and Spend() work in direct (unbatched) mode.
func (s *Store) Start(_ context.Context) {
	// Instrument batchers like the SQL store so batch size / duration / dispatch
	// reasons are exported via teranode_batcher_* metrics (label batcher="postgres_*").
	otelTracer := tracing.Tracer("utxo").OTelTracer()
	batcherOpts := func(name string) []batcher.Option {
		return []batcher.Option{
			batcher.WithName(name),
			batcher.WithLogger(s.logger),
			batcher.WithMetrics(batchermetrics.Provider()),
			batcher.WithTracer(otelTracer),
		}
	}
	// Per-batcher drain: the global BatcherDrainMode forces drain on every batcher;
	// the per-batcher *BatcherDrainMode settings (matching the aerospike store and the
	// dev-scale-1 config) enable it selectively by arrival pattern. maxConc caps the
	// number of in-flight batch callbacks (go-batcher SetMaxConcurrent).
	globalDrain := s.settings.BatcherDrainMode
	maxConc := s.settings.UtxoStore.BatcherMaxConcurrent

	// Create batcher — pipelines N creates via COPY to staging + INSERT...SELECT.
	storeBatchSize := s.settings.UtxoStore.StoreBatcherSize
	if storeBatchSize <= 0 {
		storeBatchSize = 100
	}
	storeBatchDuration := time.Duration(s.settings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	if storeBatchDuration <= 0 {
		storeBatchDuration = 10 * time.Millisecond
	}
	s.createBatcher = batcher.NewWithPool(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true, batcherOpts("postgres_create")...)
	configureBatcher(s.createBatcher, maxConc, globalDrain || s.settings.UtxoStore.StoreBatcherDrainMode, s.settings.UtxoStore.StoreBatcherTickerIntervalMillis)

	// Spend batcher — pipelines N validation CTEs via SendBatch (no transaction needed).
	// background=true is safe because pipelined CTEs don't hold row locks across batches.
	spendBatchSize := s.settings.UtxoStore.SpendBatcherSize
	if spendBatchSize <= 0 {
		spendBatchSize = 100
	}
	spendBatchDuration := time.Duration(s.settings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	if spendBatchDuration <= 0 {
		spendBatchDuration = 10 * time.Millisecond
	}
	s.spendBatcher = batcher.NewWithPool(spendBatchSize, spendBatchDuration, s.sendSpendBatch, true, batcherOpts("postgres_spend")...)
	configureBatcher(s.spendBatcher, maxConc, globalDrain || s.settings.UtxoStore.SpendBatcherDrainMode, s.settings.UtxoStore.SpendBatcherTickerIntervalMillis)

	// Get batcher — pipelines N SELECTs via SendBatch.
	getBatchSize := s.settings.UtxoStore.GetBatcherSize
	if getBatchSize <= 0 {
		getBatchSize = 500
	}
	getBatchDuration := storeBatchDuration
	s.getBatcher = batcher.NewWithPool(getBatchSize, getBatchDuration, s.sendGetBatch, true, batcherOpts("postgres_get")...)
	configureBatcher(s.getBatcher, maxConc, globalDrain || s.settings.UtxoStore.GetBatcherDrainMode, s.settings.UtxoStore.GetBatcherTickerIntervalMillis)

	// Unlock batcher — pipelines N UPDATEs via SendBatch.
	unlockBatchSize := s.settings.UtxoStore.LockedBatcherSize
	if unlockBatchSize <= 0 {
		unlockBatchSize = 500
	}
	unlockBatchDuration := storeBatchDuration
	s.unlockBatcher = batcher.NewWithPool(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true, batcherOpts("postgres_unlock")...)
	configureBatcher(s.unlockBatcher, maxConc, globalDrain || s.settings.UtxoStore.LockedBatcherDrainMode, s.settings.UtxoStore.LockedBatcherTickerIntervalMillis)
}

// configureBatcher applies the optional max-concurrency cap, drain mode, and tick
// interval to a batcher, in the order go-batcher requires: SetMaxConcurrent and
// SetDrainMode before the first Put, and SetTickInterval after SetDrainMode so the
// drain guard wins on conflict (tick is a no-op + warning when drain is enabled).
func configureBatcher[T any](b *batcher.Batcher[T], maxConcurrent int, drain bool, tickMs int) {
	if maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}
	if drain {
		b.SetDrainMode(true)
	}
	if tickMs > 0 {
		b.SetTickInterval(time.Duration(tickMs) * time.Millisecond)
	}
}

// Stop closes batchers and database connections.
//
// It does NOT nil the batcher pointer fields. Those are written exactly once, in
// Start(), which happens-before any Create/Spend/Get/SetLocked call — so the
// unsynchronised nil-checks in those hot paths are race-free as long as nothing
// writes the pointers again. Nilling them here (concurrently with an in-flight
// operation reading them) was a data race; go-batcher Close is idempotent, so
// closing without nilling is safe and matches the sql store's Stop().
func (s *Store) Stop() {
	s.stopPrunerCursor()
	if s.createBatcher != nil {
		s.createBatcher.Close()
	}
	if s.spendBatcher != nil {
		s.spendBatcher.Close()
	}
	if s.getBatcher != nil {
		s.getBatcher.Close()
	}
	if s.unlockBatcher != nil {
		s.unlockBatcher.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}

// Close drains any in-flight batched writes and releases the connection pool,
// honouring the supplied context as a deadline. It mirrors the SQL store's
// contract (see utxo.Store): batchers are drained in dependency order with the
// state-mutating writers (spend, create) last so they have the best chance of
// committing before the deadline, and the pool is always closed once the drain
// goroutine finishes so connections are not leaked even if ctx expired first.
func (s *Store) Close(ctx context.Context) error {
	s.stopPrunerCursor()

	done := make(chan struct{})

	go func() {
		defer close(done)
		// Drain in dependency order: state-mutating writers last.
		if s.unlockBatcher != nil {
			s.unlockBatcher.Close()
		}
		if s.getBatcher != nil {
			s.getBatcher.Close()
		}
		if s.spendBatcher != nil {
			s.spendBatcher.Close()
		}
		if s.createBatcher != nil {
			s.createBatcher.Close()
		}
		// Always close the pool after the batchers drain, even if ctx has
		// already expired, so the connection pool is not leaked.
		if s.pool != nil {
			s.pool.Close()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopPrunerCursor cancels the background DAH cursor (Worker 2) if a pruner
// service was started, so it does not outlive the store's connection pool.
func (s *Store) stopPrunerCursor() {
	s.prunerServiceMu.Lock()
	ps := s.prunerService
	s.prunerServiceMu.Unlock()
	if pgps, ok := ps.(*postgresPrunerService); ok {
		pgps.stop()
	}
}

// Health checks the database connection.
func (s *Store) Health(ctx context.Context, _ bool) (int, string, error) {
	var num int
	err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&num)
	if err != nil {
		return 503, "Postgres UTXO Store", err
	}
	return 200, "Postgres UTXO Store", nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.blockHeight.Store(blockHeight)
	return nil
}

func (s *Store) GetBlockHeight() uint32 {
	return s.blockHeight.Load()
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	s.medianBlockTime.Store(medianTime)
	return nil
}

func (s *Store) GetMedianBlockTime() uint32 {
	return s.medianBlockTime.Load()
}

func (s *Store) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     s.blockHeight.Load(),
		MedianTime: s.medianBlockTime.Load(),
	}
}
