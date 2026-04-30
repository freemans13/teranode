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

	// Per-(op × partition) workers. One persistent goroutine per slot, each
	// owns a pgxpool connection for life. Nil until Start() is called.
	createWorkers [NumPartitions]*partitionWorker[*batchCreateItem]
	spendWorkers  [NumPartitions]*partitionWorker[*batchSpendItem]
	getWorkers    [NumPartitions]*partitionWorker[*batchGetItem]
	unlockWorkers [NumPartitions]*partitionWorker[*batchUnlockItem]
	workersWG     sync.WaitGroup

	// in-process cache for recently created transactions.
	cache *txCache
}

// workersStarted reports whether the per-partition worker grid is up.
// Public-API methods (Get, Spend, Create, SetLocked) check this to decide
// whether to dispatch through workers or fall back to direct paths.
func (s *Store) workersStarted() bool {
	return s.getWorkers[0] != nil
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
	pgxConfig.MaxConns = 250
	pgxConfig.MinConns = 250
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

// Start spawns the per-partition worker grid (4 ops × NumPartitions
// workers). Each worker holds a pgxpool connection for its lifetime and
// dispatches micro-batches of items routed to its partition. Without
// Start(), Get/Spend/Create/SetLocked fall back to direct (unbatched) paths.
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

	// Per-partition batch sizes scale down because items are split across
	// partitions. With NumPartitions=8, each partition sees ~1/8 of the
	// global rate, so a per-partition batch of (global/N) keeps the same
	// dispatch cadence as the old global batchers.
	perPartCreate := storeBatchSize / NumPartitions
	if perPartCreate < 1 {
		perPartCreate = 1
	}
	perPartSpend := spendBatchSize / NumPartitions
	if perPartSpend < 1 {
		perPartSpend = 1
	}
	perPartGet := 500 / NumPartitions
	perPartUnlock := 500 / NumPartitions

	// Input channel buffer per worker. Generous so callers don't block on
	// `<- input` under bursty load.
	const inBuf = 4096

	for p := 0; p < NumPartitions; p++ {
		cw, err := newPartitionWorker[*batchCreateItem](ctx, s.logger, s.pool, p, perPartCreate, storeBatchDuration, inBuf, s.runCreateBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start create worker p=%d: %v", p, err)
			continue
		}
		s.createWorkers[p] = cw

		sw, err := newPartitionWorker[*batchSpendItem](ctx, s.logger, s.pool, p, perPartSpend, spendBatchDuration, inBuf, s.runSpendBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start spend worker p=%d: %v", p, err)
			continue
		}
		s.spendWorkers[p] = sw

		gw, err := newPartitionWorker[*batchGetItem](ctx, s.logger, s.pool, p, perPartGet, storeBatchDuration, inBuf, s.runGetBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start get worker p=%d: %v", p, err)
			continue
		}
		s.getWorkers[p] = gw

		uw, err := newPartitionWorker[*batchUnlockItem](ctx, s.logger, s.pool, p, perPartUnlock, storeBatchDuration, inBuf, s.runUnlockBatch, &s.workersWG)
		if err != nil {
			s.logger.Errorf("[Start] failed to start unlock worker p=%d: %v", p, err)
			continue
		}
		s.unlockWorkers[p] = uw
	}
}

// Stop signals all workers to exit and waits for them to drain & release
// their connections, then closes the pool and DB.
func (s *Store) Stop() {
	for p := 0; p < NumPartitions; p++ {
		if s.createWorkers[p] != nil {
			s.createWorkers[p].Stop()
			s.createWorkers[p] = nil
		}
		if s.spendWorkers[p] != nil {
			s.spendWorkers[p].Stop()
			s.spendWorkers[p] = nil
		}
		if s.getWorkers[p] != nil {
			s.getWorkers[p].Stop()
			s.getWorkers[p] = nil
		}
		if s.unlockWorkers[p] != nil {
			s.unlockWorkers[p].Stop()
			s.unlockWorkers[p] = nil
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
