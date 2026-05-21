# Append-Only Direct-Write UTXO Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the v3 queue+materializer UTXO store with an all-direct, append-only architecture that eliminates queue tables, stored procedures, pg_cron, and pg_notify.

**Architecture:** Every operation writes directly to snapshot tables. Spends are append-only INSERTs into a new `spends` table (no UPDATE on outputs). All mutable state lives in a narrow `tx_state` table with HOT updates. Creates use unnest arrays for multi-row inserts in a single transaction.

**Tech Stack:** Go, pgx/v5 (pgxpool), PostgreSQL 17 (64-way hash partitioning, fillfactor tuning, HOT updates)

**Spec:** `docs/superpowers/specs/2026-04-10-append-only-utxo-store-v4-design.md`

**Existing v3 code:** `stores/utxo/queue/` — all files are untracked (new to this branch). We rewrite in-place.

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `stores/utxo/queue/schema.go` | DDL: table creation, partitioning, indexes | Rewrite |
| `stores/utxo/queue/store.go` | Store struct, New(), Start(), Stop(), Health, block state | Rewrite (simplified — no batcher/listener/materializer) |
| `stores/utxo/queue/create.go` | Create() — direct INSERT via unnest | Rewrite |
| `stores/utxo/queue/spend.go` | Spend() — direct INSERT into spends with validation CTE | Rewrite |
| `stores/utxo/queue/get.go` | Get(), GetSpend(), GetMeta(), BatchDecorate(), PreviousOutputsDecorate() | Rewrite (JOINs for spends + tx_state) |
| `stores/utxo/queue/mined.go` | SetMinedMulti() — direct INSERT block_ids + UPDATE tx_state | Rewrite |
| `stores/utxo/queue/conflicting.go` | SetConflicting(), SetLocked(), MarkTransactionsOnLongestChain(), GetConflictingChildren() | Rewrite (UPDATE tx_state instead of transactions) |
| `stores/utxo/queue/delete.go` | Delete(), Unspend() — adds spends + tx_state cleanup | Rewrite |
| `stores/utxo/queue/iterators.go` | Unmined iterators — JOIN tx_state | Rewrite |
| `stores/utxo/queue/alert_system.go` | FreezeUTXOs(), UnFreezeUTXOs(), ReAssignUTXO() | Rewrite (freeze on tx_state) |
| `stores/utxo/queue/preservation.go` | PreserveTransactions(), ProcessExpiredPreservations() | Rewrite (tx_state columns) |
| `stores/utxo/queue/pruner_provider.go` | Pruner interface — delete from tx_state + spends | Rewrite |
| `stores/utxo/queue/metrics.go` | Prometheus metrics | Rewrite (remove queue/batch metrics, add direct op metrics) |
| `stores/utxo/queue/store_test.go` | Unit tests | Rewrite (no StartTestMaterializer) |
| `stores/utxo/throughput_test.go` | Benchmark: SQL vs Queue | Update |
| `stores/utxo/factory/queue.go` | Factory registration | Update (remove Start() call — no background goroutines needed) |

**Files to delete:**

| File | Reason |
|------|--------|
| `stores/utxo/queue/batcher.go` | No COPY batching |
| `stores/utxo/queue/listener.go` | No pg_notify |
| `stores/utxo/queue/buffer.go` | No in-memory buffers |

---

## Task 1: Schema — New Table DDL

**Files:**

- Rewrite: `stores/utxo/queue/schema.go`
- Test: `stores/utxo/queue/store_test.go` (TestSchemaCreation)

- [ ] **Step 1: Write schema test**

```go
// store_test.go
package queue

import (
    "context"
    "net/url"
    "testing"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/stretchr/testify/require"
)

const testDSN = "postgresql://teranode:teranode@localhost:5432/teranode_test"

func cleanDB(t *testing.T) {
    t.Helper()
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, testDSN)
    if err != nil {
        t.Skipf("Skipping: cannot connect to postgres: %v", err)
    }
    defer pool.Close()
    pool.Exec(ctx, `
        DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs, tx_state, transactions CASCADE;
    `)
}

func setupTestStore(t *testing.T) (*Store, context.Context) {
    t.Helper()
    cleanDB(t)
    ctx := context.Background()

    pool, err := pgxpool.New(ctx, testDSN)
    if err != nil {
        t.Skipf("Skipping: cannot connect to postgres: %v", err)
    }

    storeURL, err := url.Parse(testDSN)
    require.NoError(t, err)
    storeURL.Scheme = "postgresqueue"

    tSettings := test.CreateBaseTestSettings(t)
    store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(ctx)
    t.Cleanup(func() {
        cancel()
        store.Stop()
    })

    return store, ctx
}

func TestSchemaCreation(t *testing.T) {
    store, ctx := setupTestStore(t)

    // Verify all snapshot tables exist
    tables := []string{"transactions", "inputs", "outputs", "spends", "tx_state", "block_ids", "conflicting_children"}
    for _, table := range tables {
        var count int
        err := store.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
        require.NoError(t, err, "table %s should exist", table)
    }

    // Verify partitioning (64 partitions per table)
    for _, table := range tables {
        var partCount int
        err := store.pool.QueryRow(ctx, `
            SELECT COUNT(*) FROM pg_inherits
            WHERE inhparent = $1::regclass`, table).Scan(&partCount)
        require.NoError(t, err)
        require.Equal(t, 64, partCount, "table %s should have 64 partitions", table)
    }
}
```

- [ ] **Step 2: Rewrite schema.go with new DDL**

Replace the entire `schema.go` with the new table design from the spec. Key changes:

- Remove all queue table DDL (create_queue, input_queue, output_queue, spend_queue, mined_queue)
- Remove process_batch, materialize_loop, process_delete_at_height stored procedures
- Remove pg_cron extension and schedule
- Remove batch_notifications table
- Add `spends` table (UNIQUE on prev_tx_hash, prev_output_idx, fillfactor=100)
- Add `tx_state` table (PK on tx_hash, fillfactor=50, partial indexes)
- Remove `spending_data` from outputs table
- Remove `frozen`, `conflicting`, `locked`, `unmined_since`, `delete_at_height`, `preserve_until` from transactions table
- All tables: `PARTITION BY HASH` with 64 partitions
- Append-only tables: `fillfactor = 100`
- tx_state: `fillfactor = 50`

The DDL constants should be:

```go
package queue

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5/pgxpool"
)

func (s *Store) createSchema(ctx context.Context) error {
    return createSchemaWithPool(ctx, s.pool)
}

func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
    ddlStatements := []string{
        transactionsDDL,
        txStateDDL,
        inputsDDL,
        outputsDDL,
        spendsDDL,
        blockIDsDDL,
        conflictingChildrenDDL,
    }

    for _, ddl := range ddlStatements {
        if _, err := pool.Exec(ctx, ddl); err != nil {
            return fmt.Errorf("schema creation failed: %w", err)
        }
    }

    // Create 64 hash partitions for each table
    for _, table := range []string{"transactions", "tx_state", "inputs", "outputs", "spends", "block_ids", "conflicting_children"} {
        for i := 0; i < 64; i++ {
            partDDL := fmt.Sprintf(
                `CREATE TABLE IF NOT EXISTS %s_p%d PARTITION OF %s FOR VALUES WITH (MODULUS 64, REMAINDER %d)`,
                table, i, table, i,
            )
            if _, err := pool.Exec(ctx, partDDL); err != nil {
                return fmt.Errorf("partition creation failed for %s_p%d: %w", table, i, err)
            }
        }
    }

    return nil
}

const transactionsDDL = `
CREATE TABLE IF NOT EXISTS transactions (
    hash          BYTEA PRIMARY KEY,
    version       BIGINT NOT NULL,
    lock_time     BIGINT NOT NULL,
    fee           BIGINT NOT NULL,
    size_in_bytes BIGINT NOT NULL,
    coinbase      BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH(hash) WITH (fillfactor = 100);`

const txStateDDL = `
CREATE TABLE IF NOT EXISTS tx_state (
    tx_hash          BYTEA PRIMARY KEY,
    locked           BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting      BOOLEAN NOT NULL DEFAULT FALSE,
    frozen           BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since    BIGINT,
    delete_at_height BIGINT,
    preserve_until   BIGINT
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 50);`

const inputsDDL = `
CREATE TABLE IF NOT EXISTS inputs (
    tx_hash                   BYTEA  NOT NULL,
    idx                       BIGINT NOT NULL,
    previous_transaction_hash BYTEA  NOT NULL,
    previous_tx_idx           BIGINT NOT NULL,
    previous_tx_satoshis      BIGINT NOT NULL,
    previous_tx_script        BYTEA,
    unlocking_script          BYTEA  NOT NULL,
    sequence_number           BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);`

const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                  BYTEA   NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    coinbase_spending_height BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);`

const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH(prev_tx_hash) WITH (fillfactor = 100);`

const blockIDsDDL = `
CREATE TABLE IF NOT EXISTS block_ids (
    tx_hash      BYTEA  NOT NULL,
    block_id     BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    subtree_idx  BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, block_id)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);`

const conflictingChildrenDDL = `
CREATE TABLE IF NOT EXISTS conflicting_children (
    tx_hash       BYTEA NOT NULL,
    child_tx_hash BYTEA NOT NULL,
    PRIMARY KEY (tx_hash, child_tx_hash)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);`
```

Note: the tx_state partial indexes (`px_unmined_since`, `px_delete_at_height`) should be created AFTER partitions, added as separate DDL statements at the end of `createSchemaWithPool`.

- [ ] **Step 3: Run test to verify schema creation**

Run: `go test -v -run TestSchemaCreation -timeout 30s ./stores/utxo/queue/`
Expected: PASS — all 7 tables exist with 64 partitions each

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/schema.go stores/utxo/queue/store_test.go
git commit -m "feat(queue): v4 schema — append-only tables, spends, tx_state"
```

---

## Task 2: Store Struct — Simplified Lifecycle

**Files:**

- Rewrite: `stores/utxo/queue/store.go`
- Delete: `stores/utxo/queue/batcher.go`, `stores/utxo/queue/listener.go`, `stores/utxo/queue/buffer.go`
- Update: `stores/utxo/factory/queue.go`

- [ ] **Step 1: Rewrite store.go**

The v4 Store is dramatically simpler — no batcher, listener, materializer, buffers, or pending batches:

```go
package queue

import (
    "context"
    "database/sql"
    "fmt"
    "net/url"
    "sync/atomic"

    "github.com/bsv-blockchain/teranode/settings"
    "github.com/bsv-blockchain/teranode/stores/utxo"
    "github.com/bsv-blockchain/teranode/ulogger"
    "github.com/jackc/pgx/v5/pgxpool"
)

var _ utxo.Store = (*Store)(nil)

type Store struct {
    logger   ulogger.Logger
    settings *settings.Settings
    storeURL *url.URL
    pool     *pgxpool.Pool
    db       *sql.DB // for database/sql compatibility (iterators, existing helpers)

    blockHeight     atomic.Uint32
    medianBlockTime atomic.Uint32
}

func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
    connStr := *storeURL
    connStr.Scheme = "postgres"

    pgxConfig, err := pgxpool.ParseConfig(connStr.String())
    if err != nil {
        return nil, err
    }
    pgxConfig.MaxConns = 20

    pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
    if err != nil {
        return nil, err
    }

    // Also open a database/sql connection for compatibility
    db, err := sql.Open("pgx", connStr.String())
    if err != nil {
        pool.Close()
        return nil, err
    }

    s := &Store{
        logger:   logger,
        settings: tSettings,
        storeURL: storeURL,
        pool:     pool,
        db:       db,
    }

    if err := s.createSchema(ctx); err != nil {
        pool.Close()
        db.Close()
        return nil, err
    }

    return s, nil
}

func (s *Store) Start(ctx context.Context) {
    // No background goroutines needed — all operations are direct.
}

func (s *Store) Stop() {
    if s.pool != nil {
        s.pool.Close()
    }
    if s.db != nil {
        s.db.Close()
    }
}

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
    var num int
    err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&num)
    if err != nil {
        return 503, "Queue UTXO Store", err
    }
    return 200, "Queue UTXO Store", nil
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
```

- [ ] **Step 2: Delete obsolete files**

```bash
rm stores/utxo/queue/batcher.go stores/utxo/queue/listener.go stores/utxo/queue/buffer.go
```

- [ ] **Step 3: Update factory/queue.go — remove Start() call**

The factory currently calls `store.Start(ctx)` after construction. Since `Start()` is now a no-op, keep the call for interface compatibility but it does nothing.

Check if factory needs updating — read `stores/utxo/factory/queue.go` and keep `store.Start(ctx)` call as-is (it's a no-op now).

- [ ] **Step 4: Verify build compiles**

Run: `go build ./stores/utxo/queue/`

This will fail because other files (create.go, spend.go, etc.) still reference deleted types. That's expected — we'll fix them in subsequent tasks. For now, temporarily stub them with `// TODO: rewrite in Task N` comments or create minimal files that satisfy the compiler.

Actually, since all queue files are untracked, we should rewrite them all before expecting compilation. The approach: write all files with minimal stubs first (Tasks 1-2), then flesh out each operation (Tasks 3+). Each task adds the real implementation for one operation.

- [ ] **Step 5: Create minimal stubs for all operations**

Create stub files that satisfy the `utxo.Store` interface with `panic("not implemented")` for all methods not yet implemented. This lets the package compile while we implement operations one at a time.

Files to stub: `create.go`, `spend.go`, `get.go`, `mined.go`, `conflicting.go`, `delete.go`, `iterators.go`, `alert_system.go`, `preservation.go`, `pruner_provider.go`, `metrics.go`

Each stub file has the method signatures matching the interface, returning `panic("not implemented")`.

- [ ] **Step 6: Verify build compiles**

Run: `go build ./stores/utxo/queue/`
Expected: Success

- [ ] **Step 7: Run schema test**

Run: `go test -v -run TestSchemaCreation -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add stores/utxo/queue/ stores/utxo/factory/queue.go
git commit -m "feat(queue): v4 store struct — direct writes, no materializer"
```

---

## Task 3: Create — Direct INSERT via Unnest

**Files:**

- Rewrite: `stores/utxo/queue/create.go`
- Test: `stores/utxo/queue/store_test.go` (TestCreateAndGet)

- [ ] **Step 1: Write create + get test**

```go
func TestCreateAndGet(t *testing.T) {
    store, ctx := setupTestStore(t)
    tx := createTestTx(t) // reuse existing test helper

    blockHeight := uint32(12345)

    txMeta, err := store.Create(ctx, tx, blockHeight)
    require.NoError(t, err)
    require.NotNil(t, txMeta)
    require.Equal(t, uint64(259), txMeta.SizeInBytes)

    // Get the transaction back
    got, err := store.Get(ctx, tx.TxIDChainHash(), fields.Tx, fields.Inputs, fields.Outputs, fields.BlockIDs)
    require.NoError(t, err)
    require.NotNil(t, got)
    require.Equal(t, uint64(259), got.SizeInBytes)
    require.NotNil(t, got.Tx)
    require.Len(t, got.Tx.Inputs, 1)
    require.Len(t, got.Tx.Outputs, 2)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestCreateAndGet -timeout 30s ./stores/utxo/queue/`
Expected: FAIL (panic: not implemented)

- [ ] **Step 3: Implement Create()**

Replace the stub with the real implementation. Key pattern: single pgx transaction with 4 INSERTs:

1. INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, coinbase) ON CONFLICT DO NOTHING
2. INSERT INTO tx_state (tx_hash, locked, frozen, unmined_since) ON CONFLICT DO NOTHING
3. INSERT INTO inputs via unnest arrays ON CONFLICT DO NOTHING
4. INSERT INTO outputs via unnest arrays ON CONFLICT DO NOTHING
5. If mined: INSERT INTO block_ids ON CONFLICT DO NOTHING

Use `unnest($1::bytea[], $2::bigint[], ...)` for multi-row inputs/outputs in a single statement. Each array parameter is a Go slice.

Also implement the minimal Get() needed to verify (Task 4 does the full Get, but we need basic tx lookup here).

Follow the existing v3 `create.go` for how to:

- Process `CreateOptions` (MinedBlockInfos, Locked, Frozen, etc.)
- Compute fee, sizeInBytes, isCoinbase
- Build utxo hashes via `util.UTXOHash()`
- Determine unmined_since (nil if mined, blockHeight if unmined)
- Compute coinbaseSpendHeight

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestCreateAndGet -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add stores/utxo/queue/create.go stores/utxo/queue/store_test.go
git commit -m "feat(queue): v4 Create — direct INSERT via unnest"
```

---

## Task 4: Get — JOINs for Spends + tx_state

**Files:**

- Rewrite: `stores/utxo/queue/get.go`

- [ ] **Step 1: Write comprehensive get test**

Test Get with all field combinations: Tx, Inputs, Outputs, BlockIDs, Utxos (spend status).

```go
func TestGetWithSpendStatus(t *testing.T) {
    store, ctx := setupTestStore(t)
    // Create parent tx, spend it, then Get with Utxos field
    // Verify spending_data comes from spends table LEFT JOIN
}
```

- [ ] **Step 2: Implement get.go**

Key changes from v3:

- Transaction metadata query JOINs tx_state: `SELECT t.*, ts.locked, ts.conflicting, ts.frozen, ts.unmined_since FROM transactions t JOIN tx_state ts ON ts.tx_hash = t.hash WHERE t.hash = $1`
- Outputs with spend status: `SELECT o.*, sp.spending_data FROM outputs o LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx WHERE o.tx_hash = $1`
- GetSpend: `SELECT o.utxo_hash, o.coinbase_spending_height, sp.spending_data, o.frozen OR ts.frozen, o.spendable_in, ts.conflicting, ts.locked FROM outputs o JOIN tx_state ts ON ts.tx_hash = o.tx_hash LEFT JOIN spends sp ON ... WHERE o.tx_hash = $1 AND o.idx = $2`
- BatchPreviousOutputsDecorate: unchanged (reads only from outputs, no spend status needed)
- BatchDecorate, PreviousOutputsDecorate: delegate to Get()

- [ ] **Step 3: Run all get tests**

Run: `go test -v -run "TestCreateAndGet|TestGetNonExistent|TestGetWithSpendStatus" -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/get.go stores/utxo/queue/store_test.go
git commit -m "feat(queue): v4 Get — JOINs spends + tx_state"
```

---

## Task 5: Spend — Direct INSERT with Validation CTE

**Files:**

- Rewrite: `stores/utxo/queue/spend.go`

This is the most critical operation. The spend SQL from the spec validates and inserts atomically.

- [ ] **Step 1: Write spend tests**

```go
func TestSpendOutput(t *testing.T) {
    store, ctx := setupTestStore(t)
    // Create parent, create spending tx, spend parent output
    // Verify output is now spent via Get
}

func TestDoubleSpend(t *testing.T) {
    store, ctx := setupTestStore(t)
    // Create parent, spend output with txA, then try to spend with txB
    // Verify second spend returns ErrSpent with ConflictingTxID
}
```

- [ ] **Step 2: Implement Spend()**

Key pattern: for each input, execute the validation CTE + INSERT INTO spends:

```sql
WITH validation AS (
    SELECT o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
           o.coinbase_spending_height,
           ts.locked AS tx_locked, ts.conflicting AS tx_conflicting,
           ts.frozen AS tx_frozen,
           sp.spending_data AS existing_spend
    FROM outputs o
    JOIN tx_state ts ON ts.tx_hash = o.tx_hash
    LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
    WHERE o.tx_hash = $1 AND o.idx = $2
)
INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data)
SELECT $1, $2, $3
FROM validation v
WHERE v.existing_spend IS NULL
  AND v.utxo_hash = $4
  AND NOT v.output_frozen AND NOT v.tx_frozen
  AND ($6 OR NOT v.tx_locked)
  AND ($7 OR NOT v.tx_conflicting)
  AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
  AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
RETURNING 1
```

If INSERT returns 0 rows, re-query validation to determine the conflict reason. Map conflict types to the same error types the v3 `mapConflictsToSpends` produced:

- `existing_spend IS NOT NULL` and different data → `ErrSpent` with `ConflictingTxID`
- `output_frozen OR tx_frozen` → `ErrFrozen`
- `tx_locked` → `ErrLocked`
- `tx_conflicting` → `ErrConflicting`
- `utxo_hash mismatch` → `ErrStorage`
- `coinbase_immature` → `ErrStorage`

Keep the `IgnoreConflicting` / `IgnoreLocked` flag handling from v3's `mapConflictsToSpends`.

- [ ] **Step 3: Run spend tests**

Run: `go test -v -run "TestSpend|TestDoubleSpend" -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/spend.go stores/utxo/queue/store_test.go
git commit -m "feat(queue): v4 Spend — direct INSERT with validation CTE"
```

---

## Task 6: SetLocked + SetConflicting — Direct UPDATE tx_state

**Files:**

- Rewrite: `stores/utxo/queue/conflicting.go`

- [ ] **Step 1: Implement SetLocked, SetConflicting, MarkTransactionsOnLongestChain, GetConflictingChildren, GetCounterConflicting**

All UPDATE/query tx_state instead of transactions. Key SQL changes:

- `SetLocked`: `UPDATE tx_state SET locked = $2 WHERE tx_hash = ANY($1)`
- `SetConflicting`: `UPDATE tx_state SET conflicting = $2, delete_at_height = $3 WHERE tx_hash = ANY($1)` + INSERT conflicting_children
- `MarkTransactionsOnLongestChain`: onLongestChain=true: `UPDATE tx_state SET unmined_since = NULL WHERE tx_hash = ANY($1)`. onLongestChain=false: `UPDATE tx_state SET unmined_since = $2 WHERE tx_hash = ANY($1)`
- `GetConflictingChildren`: `SELECT child_tx_hash FROM conflicting_children WHERE tx_hash = $1`
- `GetCounterConflicting`: delegate to `utxo.GetCounterConflictingTxHashes()`

Follow v3 `conflicting.go` for the conflict children update logic (`updateParentConflictingChildren`).

- [ ] **Step 2: Run tests**

Run: `go test -v -timeout 30s ./stores/utxo/queue/`
Expected: PASS (all existing tests)

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/queue/conflicting.go
git commit -m "feat(queue): v4 SetLocked/SetConflicting — direct UPDATE tx_state"
```

---

## Task 7: SetMinedMulti — Direct INSERT + UPDATE

**Files:**

- Rewrite: `stores/utxo/queue/mined.go`

- [ ] **Step 1: Implement SetMinedMulti**

Key pattern: single transaction with bulk INSERT + bulk UPDATE:

```go
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
    if info.UnsetMined {
        return s.unsetMinedMulti(ctx, hashes, info)
    }

    // Bulk INSERT block_ids
    tx, _ := s.pool.Begin(ctx)
    for chunk := range chunks(hashes, 500) {
        hashBytes := hashesToBytes(chunk)
        tx.Exec(ctx, `INSERT INTO block_ids (tx_hash, block_id, block_height, subtree_idx)
            SELECT unnest($1::bytea[]), $2, $3, $4
            ON CONFLICT (tx_hash, block_id) DO NOTHING`,
            hashBytes, info.BlockID, info.BlockHeight, info.SubtreeIdx)
    }

    // Bulk UPDATE tx_state
    allHashBytes := hashesToBytes(hashes)
    if info.OnLongestChain {
        tx.Exec(ctx, `UPDATE tx_state SET locked = false, unmined_since = NULL WHERE tx_hash = ANY($1)`, allHashBytes)
    } else {
        tx.Exec(ctx, `UPDATE tx_state SET locked = false WHERE tx_hash = ANY($1)`, allHashBytes)
    }
    tx.Commit(ctx)

    return s.fetchBlockIDs(ctx, hashes)
}
```

Keep `unsetMinedMulti` (reorg path) — direct DELETE from block_ids, same as v3.

- [ ] **Step 2: Run tests**

Run: `go test -v -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/queue/mined.go
git commit -m "feat(queue): v4 SetMinedMulti — direct bulk INSERT + UPDATE"
```

---

## Task 8: Delete + Unspend — Updated for New Tables

**Files:**

- Rewrite: `stores/utxo/queue/delete.go`

- [ ] **Step 1: Implement Delete and Unspend**

`Delete`: add `DELETE FROM spends` and `DELETE FROM tx_state`:

```sql
DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM block_ids WHERE tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM inputs WHERE tx_hash = $1;
DELETE FROM tx_state WHERE tx_hash = $1;
DELETE FROM transactions WHERE tx_hash = $1;
```

`Unspend`: replace `UPDATE outputs SET spending_data = NULL` with `DELETE FROM spends`:

```sql
DELETE FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = $2
```

Keep the `setDAH` helper but update it to query tx_state for delete_at_height logic, and check spends table for fully-spent status:

```sql
SELECT NOT EXISTS(
    SELECT 1 FROM outputs o
    WHERE o.tx_hash = $1
    AND NOT EXISTS (SELECT 1 FROM spends sp WHERE sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx)
) AS all_spent
```

- [ ] **Step 2: Run tests**

Run: `go test -v -timeout 30s ./stores/utxo/queue/`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/queue/delete.go
git commit -m "feat(queue): v4 Delete/Unspend — spends + tx_state cleanup"
```

---

## Task 9: Iterators — JOIN tx_state

**Files:**

- Rewrite: `stores/utxo/queue/iterators.go`

- [ ] **Step 1: Implement iterators**

Key query change — JOIN tx_state for unmined_since and conflicting:

```sql
SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
       ts.locked, ts.unmined_since
FROM transactions t
JOIN tx_state ts ON ts.tx_hash = t.hash
WHERE ts.unmined_since IS NOT NULL AND ts.conflicting = false
ORDER BY t.hash
```

Prunable iterator adds: `AND ts.unmined_since <= $1`

Keep the same iterator struct pattern from v3. The `readOne` method queries inputs + block_ids per transaction (unchanged).

`QueryOldUnminedTransactions`: same query but SELECT from tx_state JOIN transactions.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/iterators.go
git commit -m "feat(queue): v4 iterators — JOIN tx_state"
```

---

## Task 10: Alert System — Freeze/Unfreeze on tx_state

**Files:**

- Rewrite: `stores/utxo/queue/alert_system.go`

- [ ] **Step 1: Implement FreezeUTXOs, UnFreezeUTXOs, ReAssignUTXO**

Key changes:

- `FreezeUTXOs`: check that output is not already spent (LEFT JOIN spends), check not already frozen. Update both `outputs SET frozen = true` (output-level) and `tx_state SET frozen = true` (tx-level).
- `UnFreezeUTXOs`: reverse — `SET frozen = false` on both tables.
- `ReAssignUTXO`: update `outputs SET utxo_hash = ..., frozen = false, spendable_in = ...`

Note: the v3 code updates `outputs.frozen`. In v4, output-level `frozen` is on the immutable outputs table. Since freezing is 1-in-a-billion, the UPDATE on outputs is acceptable here (it's not a hot path). The tx-level `frozen` on tx_state is also updated.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/alert_system.go
git commit -m "feat(queue): v4 alert system — freeze/unfreeze"
```

---

## Task 11: Preservation + Pruner — tx_state Columns

**Files:**

- Rewrite: `stores/utxo/queue/preservation.go`
- Rewrite: `stores/utxo/queue/pruner_provider.go`

- [ ] **Step 1: Implement preservation**

Key SQL changes — all operate on tx_state instead of transactions:

```sql
-- PreserveTransactions
UPDATE tx_state SET preserve_until = $1, delete_at_height = NULL WHERE tx_hash = ANY($2)

-- ProcessExpiredPreservations
UPDATE tx_state SET delete_at_height = $1, preserve_until = NULL
WHERE preserve_until IS NOT NULL AND preserve_until <= $2
```

- [ ] **Step 2: Implement pruner**

Key change: DELETE cascades through all tables including spends and tx_state:

```sql
-- deleteTombstoned: find tx_hashes to delete
SELECT tx_hash FROM tx_state WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1

-- For each: delete from all tables
DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM block_ids WHERE tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM inputs WHERE tx_hash = $1;
DELETE FROM tx_state WHERE tx_hash = $1;
DELETE FROM transactions WHERE tx_hash = $1;
```

Or use a CTE to batch the deletes.

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/queue/preservation.go stores/utxo/queue/pruner_provider.go
git commit -m "feat(queue): v4 preservation + pruner — tx_state columns"
```

---

## Task 12: Metrics — Direct Operation Metrics

**Files:**

- Rewrite: `stores/utxo/queue/metrics.go`

- [ ] **Step 1: Replace queue metrics with direct operation metrics**

Remove: `prometheusQueueBatchSize`, `prometheusQueueCopyDuration`, `prometheusQueueMaterializeDur`

Add:

```go
prometheusDirectCreateDuration  // Histogram: Create() duration
prometheusDirectSpendDuration   // Histogram: Spend() per-input duration
prometheusDirectMinedDuration   // Histogram: SetMinedMulti() duration
prometheusDirectConflicts       // Counter: spend conflicts detected
prometheusDirectCreate          // Counter: total Create calls
prometheusDirectSpend           // Counter: total Spend calls
```

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/metrics.go
git commit -m "feat(queue): v4 metrics — direct operation counters"
```

---

## Task 13: Throughput Benchmark — SQL vs Queue v4

**Files:**

- Update: `stores/utxo/throughput_test.go`

- [ ] **Step 1: Update throughput test for v4**

The `newQueueStoreForBench` function needs updating:

```go
func newQueueStoreForBench(t *testing.T) utxo.Store {
    t.Helper()
    cleanDB(t)
    ctx := context.Background()
    storeURL, _ := url.Parse(throughputDSN)
    storeURL.Scheme = "postgresqueue"
    tSettings := test.CreateBaseTestSettings(t)
    tSettings.UtxoStore.DBTimeout = 60 * time.Second
    logger := ulogger.TestLogger{}
    s, err := queue.New(ctx, logger, tSettings, storeURL)
    if err != nil {
        t.Fatalf("queue store: %v", err)
    }
    // No Start() needed — all direct writes
    t.Cleanup(func() { s.Stop() })
    return s
}
```

Remove the `terminateOtherConnections` call if no longer needed (no stale materializer connections).

- [ ] **Step 2: Run benchmark**

```bash
pkill -f "utxo.test"
command go test -v -run "TestThroughput_(SQLStore|QueueStore)" -timeout 300s -count=1 ./stores/utxo/
```

Expected: Queue v4 should show:

- Validator hot path: comparable or better than SQL store (direct writes, no materializer hop)
- SetMinedMulti: comparable to SQL store (direct bulk, no queue hop)

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/throughput_test.go
git commit -m "bench: v4 queue store throughput comparison"
```

---

## Task 14: Integration — Run Sequential Tests

- [ ] **Step 1: Run the full sequential test suite**

```bash
pkill -f "utxo.test"
make sequentialtest TEST_RETRY_COUNT=3
```

This runs the double-spend tests, longest-chain tests, and large-tx-reorg tests against the queue store. All should pass since the `utxo.Store` interface is unchanged.

- [ ] **Step 2: Fix any failures**

Each sequential test exercises specific conflict scenarios. If any fail, the issue is likely in the Spend validation logic or the SetConflicting/SetMined flows. Debug by running the failing test individually with `-v`.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "fix(queue): v4 sequential test fixes"
```
