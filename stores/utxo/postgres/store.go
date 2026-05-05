package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx stdlib driver
)

var _ utxo.Store = (*Store)(nil)

const txCacheMaxSize = 100_000

// batcherMaxConcurrent is the number of concurrent in-flight callback
// goroutines per batcher. The batcher's internal pool dispatches up to
// this many flushes in parallel — replaces the K-workers pattern from
// the previous shardSlot[T] design.
const batcherMaxConcurrent = 8

// txCache is a simple bounded in-process cache for recently created transactions.
type txCache struct {
	mu      sync.RWMutex
	entries map[chainhash.Hash]*meta.Data
	maxSize int
}

func newTxCache(maxSize int) *txCache {
	return &txCache{
		entries: make(map[chainhash.Hash]*meta.Data, maxSize),
		maxSize: maxSize,
	}
}

func (c *txCache) Get(hash chainhash.Hash) *meta.Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.entries[hash]
}

func (c *txCache) Add(hash chainhash.Hash, data *meta.Data) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Evict randomly when full (simple strategy — LRU not needed for this use case).
	if len(c.entries) >= c.maxSize {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[hash] = data
}

func (c *txCache) Remove(hash chainhash.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, hash)
}

// Store implements the utxo.Store interface using direct writes to
// append-only PostgreSQL tables with optional batching for throughput.
type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	storeURL *url.URL

	// pgxpool for all pgx-native operations.
	pool *pgxpool.Pool

	// database/sql.DB for compatibility with code using the standard interface.
	db *sql.DB

	// block state
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32

	// One go-batcher v2 batcher per op. The batcher's internal pool
	// dispatches flushes to a configurable number of concurrent callback
	// goroutines (see SetMaxConcurrent). Each callback acquires its own
	// pgxpool.Conn for the duration of the batch.
	createBatcher *batcher.Batcher[batchCreateItem]
	spendBatcher  *batcher.Batcher[batchSpendItem]
	getBatcher    *batcher.Batcher[batchGetItem]
	unlockBatcher *batcher.Batcher[batchUnlockItem]

	// in-process cache for recently created transactions.
	cache *txCache
}

// workersStarted reports whether the batchers are constructed.
// Public-API methods (Get, Spend, Create, SetLocked) check this to decide
// whether to dispatch through batchers or fall back to direct paths.
func (s *Store) workersStarted() bool {
	return s.getBatcher != nil
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
	// Pool sized for in-flight batcher callbacks plus headroom for
	// direct-path queries (Get with body, PreviousOutputsDecorate, mined
	// updates, etc.). 4 batchers × MaxConcurrent callbacks each, every
	// callback acquires its own conn for the duration of the batch.
	// 4*8 = 32 + 64 headroom = 96. Below the 1000 max_connections postgres
	// is configured for. Pre-warmed so the dispatch path sees no
	// connection-open latency.
	poolSize := int32(4*batcherMaxConcurrent + 64)
	pgxConfig.MaxConns = poolSize
	pgxConfig.MinConns = poolSize
	pgxConfig.MaxConnIdleTime = 30 * time.Minute
	pgxConfig.MaxConnLifetime = 0

	// Full durability — financial data requires synchronous_commit = on (the default).
	// Group commit (commit_delay + commit_siblings) is the safe way to get throughput.

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}

	// Open a database/sql connection using the pgx stdlib driver.
	db, err := sql.Open("pgx", pgxURL.String())
	if err != nil {
		pool.Close()
		return nil, err
	}

	initPrometheusMetrics()

	s := &Store{
		logger:   logger,
		settings: tSettings,
		storeURL: storeURL,
		pool:     pool,
		db:       db,
		cache:    newTxCache(txCacheMaxSize),
	}

	if err := s.createSchema(ctx); err != nil {
		pool.Close()
		db.Close()
		return nil, err
	}

	return s, nil
}

// Start constructs the per-op batchers via go-batcher v2 NewWithPool.
// Each batcher accumulates items up to a size or duration cap, then
// dispatches the batch to its callback through a pool of up to
// batcherMaxConcurrent goroutines. Each callback acquires its own
// pgxpool.Conn for the duration of the batch. Without Start(),
// Get/Spend/Create/SetLocked fall back to direct (unbatched) paths.
func (s *Store) Start(_ context.Context) {
	storeBatchSize := s.settings.UtxoStore.StoreBatcherSize
	if storeBatchSize <= 0 {
		storeBatchSize = 100
	}
	storeBatchDuration := time.Duration(s.settings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	if storeBatchDuration <= 0 {
		storeBatchDuration = 10 * time.Millisecond
	}
	spendBatchSize := s.settings.UtxoStore.SpendBatcherSize
	if spendBatchSize <= 0 {
		spendBatchSize = 100
	}
	spendBatchDuration := time.Duration(s.settings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	if spendBatchDuration <= 0 {
		spendBatchDuration = 10 * time.Millisecond
	}

	const getBatchSize = 500
	const unlockBatchSize = 500

	// background=true: batcher runs flushes asynchronously and Put returns
	// immediately. Same semantics as the previous shardSlot input channel.
	s.createBatcher = batcher.NewWithPool(storeBatchSize, storeBatchDuration, s.runCreateBatch, true)
	s.createBatcher.SetMaxConcurrent(batcherMaxConcurrent)

	s.spendBatcher = batcher.NewWithPool(spendBatchSize, spendBatchDuration, s.runSpendBatch, true)
	s.spendBatcher.SetMaxConcurrent(batcherMaxConcurrent)

	s.getBatcher = batcher.NewWithPool(getBatchSize, storeBatchDuration, s.runGetBatch, true)
	s.getBatcher.SetMaxConcurrent(batcherMaxConcurrent)

	s.unlockBatcher = batcher.NewWithPool(unlockBatchSize, storeBatchDuration, s.runUnlockBatch, true)
	s.unlockBatcher.SetMaxConcurrent(batcherMaxConcurrent)
}

// Stop closes each batcher (allowing pending items to drain), then closes
// the pool and DB.
func (s *Store) Stop() {
	if s.createBatcher != nil {
		s.createBatcher.Close()
		s.createBatcher = nil
	}
	if s.spendBatcher != nil {
		s.spendBatcher.Close()
		s.spendBatcher = nil
	}
	if s.getBatcher != nil {
		s.getBatcher.Close()
		s.getBatcher = nil
	}
	if s.unlockBatcher != nil {
		s.unlockBatcher.Close()
		s.unlockBatcher = nil
	}
	if s.pool != nil {
		s.pool.Close()
	}
	if s.db != nil {
		s.db.Close()
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
