package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

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

// Per-op worker counts for shard slots. Each worker holds its own pgxpool
// connection and runs its own pgx.Batch stream — so K=N means N parallel
// postgres backends serving that op out of the same shared input channel
// (the shared-channel multi-reader pattern). Reads scale with K; for writes
// the WAL group-commit (commit_delay + commit_siblings) bundles concurrent
// fsyncs across connections, so multi-worker still helps. Tuned per-op:
//
//	getWorkersPerShard    = 8  (reads scale linearly until WAL/locks bite)
//	spendWorkersPerShard  = 8  (write — bench will tell us if WAL caps it)
//	createWorkersPerShard = 4  (UNNEST batches already bigger; less return)
//	unlockWorkersPerShard = 4  (rarely called per-tx)
const (
	getWorkersPerShard    = 8
	spendWorkersPerShard  = 8
	createWorkersPerShard = 4
	unlockWorkersPerShard = 4
)

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

	// Per-(shard × op) slots. Each slot is one independent pipeline:
	// input channel + one or more worker goroutines, each holding one
	// pgxpool connection for life. Items are routed to a shard by byte 0
	// of tx_hash; in-shard partition selection is left to postgres via
	// PARTITION BY LIST. Each op has its own K (see *WorkersPerShard
	// constants) — multi-worker for both reads and writes.
	createSlots [NumShards]*shardSlot[*batchCreateItem]
	spendSlots  [NumShards]*shardSlot[*batchSpendItem]
	getSlots    [NumShards]*shardSlot[*batchGetItem]
	unlockSlots [NumShards]*shardSlot[*batchUnlockItem]
	workersWG   sync.WaitGroup

	// in-process cache for recently created transactions.
	cache *txCache
}

// workersStarted reports whether the per-shard slot array is up.
// Public-API methods (Get, Spend, Create, SetLocked) check this to decide
// whether to dispatch through workers or fall back to direct paths.
func (s *Store) workersStarted() bool {
	return s.getSlots[0] != nil
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
	// Pool sized for the long-held worker connections plus headroom for
	// direct-path queries (Get with body, PreviousOutputsDecorate, mined
	// updates, etc.). Held conns per shard = sum of K across all four ops.
	// At NumShards=1 with (8+8+4+4) that's 24 held conns + 64 headroom = 88.
	// Below the 1000 max_connections postgres is configured for. Pre-warmed
	// so the dispatch path sees no connection-open latency.
	poolSize := int32(NumShards*(getWorkersPerShard+spendWorkersPerShard+createWorkersPerShard+unlockWorkersPerShard) + 64)
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

// Start spawns the per-shard worker slots (4 ops × NumShards). Each
// worker holds a pgxpool connection for its lifetime and dispatches
// micro-batches of items routed to its shard. Without Start(),
// Get/Spend/Create/SetLocked fall back to direct (unbatched) paths.
func (s *Store) Start(ctx context.Context) {
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

	// Input channel buffer per worker. Generous so callers don't block on
	// `<- input` under bursty load.
	const inBuf = 4096

	// One slot per (shard × op). Each shard is an independent pipeline;
	// items routed to shard A do not touch shard B's worker or its
	// in-flight queries.
	for sh := 0; sh < NumShards; sh++ {
		cs, err := newShardSlot[*batchCreateItem](ctx, s.logger, s.pool, sh, createWorkersPerShard, storeBatchSize, storeBatchDuration, inBuf, s.runCreateBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start create slot sh=%d: %v", sh, err)
			continue
		}
		s.createSlots[sh] = cs

		ss, err := newShardSlot[*batchSpendItem](ctx, s.logger, s.pool, sh, spendWorkersPerShard, spendBatchSize, spendBatchDuration, inBuf, s.runSpendBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start spend slot sh=%d: %v", sh, err)
			continue
		}
		s.spendSlots[sh] = ss

		gs, err := newShardSlot[*batchGetItem](ctx, s.logger, s.pool, sh, getWorkersPerShard, getBatchSize, storeBatchDuration, inBuf, s.runGetBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start get slot sh=%d: %v", sh, err)
			continue
		}
		s.getSlots[sh] = gs

		us, err := newShardSlot[*batchUnlockItem](ctx, s.logger, s.pool, sh, unlockWorkersPerShard, unlockBatchSize, storeBatchDuration, inBuf, s.runUnlockBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start unlock slot sh=%d: %v", sh, err)
			continue
		}
		s.unlockSlots[sh] = us
	}
}

// Stop signals all slot workers to exit and waits for them to drain &
// release their connections, then closes the pool and DB.
func (s *Store) Stop() {
	for sh := 0; sh < NumShards; sh++ {
		if s.createSlots[sh] != nil {
			s.createSlots[sh].Stop()
			s.createSlots[sh] = nil
		}
		if s.spendSlots[sh] != nil {
			s.spendSlots[sh].Stop()
			s.spendSlots[sh] = nil
		}
		if s.getSlots[sh] != nil {
			s.getSlots[sh].Stop()
			s.getSlots[sh] = nil
		}
		if s.unlockSlots[sh] != nil {
			s.unlockSlots[sh].Stop()
			s.unlockSlots[sh] = nil
		}
	}
	s.workersWG.Wait()
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
