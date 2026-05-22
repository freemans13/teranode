package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
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
	pgxConfig.MaxConns = 100

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
	maxConcurrent := s.settings.UtxoStore.BatcherMaxConcurrent

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
	tuneBatcher(s.createBatcher, s.settings.UtxoStore.StoreBatcherDrainMode, maxConcurrent)

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
	tuneBatcher(s.spendBatcher, s.settings.UtxoStore.SpendBatcherDrainMode, maxConcurrent)

	// Get batcher — pipelines N SELECTs via SendBatch.
	getBatchSize := s.settings.UtxoStore.GetBatcherSize
	if getBatchSize <= 0 {
		getBatchSize = 500
	}
	getBatchDuration := time.Duration(s.settings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
	if getBatchDuration <= 0 {
		getBatchDuration = storeBatchDuration
	}
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, true)
	tuneBatcher(s.getBatcher, s.settings.UtxoStore.GetBatcherDrainMode, maxConcurrent)

	// Unlock batcher — pipelines N UPDATEs via SendBatch.
	// (The settings key is named LockedBatcher* to match the aerospike store.)
	unlockBatchSize := s.settings.UtxoStore.LockedBatcherSize
	if unlockBatchSize <= 0 {
		unlockBatchSize = 500
	}
	unlockBatchDuration := time.Duration(s.settings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
	if unlockBatchDuration <= 0 {
		unlockBatchDuration = storeBatchDuration
	}
	s.unlockBatcher = batcher.New(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true)
	tuneBatcher(s.unlockBatcher, s.settings.UtxoStore.LockedBatcherDrainMode, maxConcurrent)
}

// tuneBatcher applies optional drain mode and max-concurrent caps after a
// batcher is constructed. No-op for zero / false values so existing
// deployments see identical behaviour until they opt in.
func tuneBatcher[T any](b *batcher.Batcher[T], drainMode bool, maxConcurrent int) {
	if drainMode {
		b.SetDrainMode(true)
	}
	if maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}
}

// Close drains the batched-write workers and releases the connection pool.
// See utxo.Store.Close for the durability contract.
func (s *Store) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if s.unlockBatcher != nil {
			s.unlockBatcher.Close()
			s.unlockBatcher = nil
		}
		if s.getBatcher != nil {
			s.getBatcher.Close()
			s.getBatcher = nil
		}
		if s.spendBatcher != nil {
			s.spendBatcher.Close()
			s.spendBatcher = nil
		}
		if s.createBatcher != nil {
			s.createBatcher.Close()
			s.createBatcher = nil
		}
	}()

	select {
	case <-done:
		if s.pool != nil {
			s.pool.Close()
		}
		if s.db != nil {
			_ = s.db.Close()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
