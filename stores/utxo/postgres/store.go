package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher"
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

	// batchers — nil until Start() is called.
	createBatcher *batcher.Batcher[batchCreateItem]
	spendBatcher  *batcher.Batcher[batchSpendItem]
	getBatcher    *batcher.Batcher[batchGetItem]
	unlockBatcher *batcher.Batcher[batchUnlockItem]

	// in-process cache for recently created transactions.
	cache *txCache
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
	pgxConfig.MaxConns = 80
	// Pre-warm the pool. With 4 batchers × N partition goroutines per batch,
	// burst connection demand is high; opening connections lazily costs a TCP
	// handshake + auth round-trip (5-50ms each), which (per runtime trace
	// 2026-04-29) was burning >50% of total network wait time. Holding the
	// full set of connections open from startup eliminates this entirely.
	pgxConfig.MinConns = 80
	pgxConfig.MaxConnIdleTime = 30 * time.Minute
	pgxConfig.MaxConnLifetime = 0 // never reap — let the pool stay warm

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

// Start initializes the create and spend batchers for throughput optimization.
// Without calling Start(), Create() and Spend() work in direct (unbatched) mode.
func (s *Store) Start(_ context.Context) {
	// Create batcher — pipelines N creates via COPY to staging + INSERT...SELECT.
	storeBatchSize := s.settings.UtxoStore.StoreBatcherSize
	if storeBatchSize <= 0 {
		storeBatchSize = 100
	}
	storeBatchDuration := time.Duration(s.settings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	if storeBatchDuration <= 0 {
		storeBatchDuration = 10 * time.Millisecond
	}
	s.createBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true)

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
	s.spendBatcher = batcher.New(spendBatchSize, spendBatchDuration, s.sendSpendBatch, true)

	// Get batcher — pipelines N SELECTs via SendBatch.
	// Duration matches spend/create batcher (configured via settings).
	getBatchSize := 500
	getBatchDuration := storeBatchDuration
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, true)

	// Unlock batcher — pipelines N UPDATEs via SendBatch.
	unlockBatchSize := 500
	unlockBatchDuration := storeBatchDuration
	s.unlockBatcher = batcher.New(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true)
}

// Stop closes batchers and database connections.
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
