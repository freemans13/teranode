# Turbo UTXO Store (v5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the v4 queue store to achieve 10x throughput via COPY protocol, 3-table schema, UNLOGGED tables, in-process cache, and connection pool tuning.

**Architecture:** Collapse 7 tables → 3 (txs, outputs, spends). All UNLOGGED. Creates use COPY to temp staging + INSERT...SELECT. Inputs stored as raw_tx blob. block_ids/conflicting_children stored as arrays on txs. LRU cache eliminates Get round-trips. Pool size 100 with synchronous_commit=off.

**Tech Stack:** Go, pgx/v5 (pgxpool, CopyFrom, SendBatch), PostgreSQL 17, hashicorp/golang-lru/v2

**Spec:** `docs/superpowers/specs/2026-04-10-turbo-utxo-store-v5-design.md`

**Starting point:** Existing v4 code in `stores/utxo/queue/` with 49 passing tests and working batchers.

---

## File Structure

| File | Responsibility | Change type |
|------|---------------|-------------|
| `stores/utxo/queue/schema.go` | DDL: 3 UNLOGGED tables, 16 partitions, staging tables | Rewrite |
| `stores/utxo/queue/store.go` | Store struct, pool config (100 conns, sync_commit=off), LRU cache, lifecycle | Modify |
| `stores/utxo/queue/create.go` | Create via COPY to staging + INSERT...SELECT batcher | Rewrite |
| `stores/utxo/queue/get.go` | Get from single txs table, raw_tx deserialization, cache | Rewrite |
| `stores/utxo/queue/spend.go` | Spend bulk validation — JOIN txs instead of tx_state | Modify (SQL only) |
| `stores/utxo/queue/mined.go` | SetMined via array append UPDATE on txs | Rewrite |
| `stores/utxo/queue/conflicting.go` | SetConflicting/SetLocked UPDATE txs, array children | Rewrite |
| `stores/utxo/queue/delete.go` | Delete from 3 tables, setDAH on txs | Rewrite |
| `stores/utxo/queue/iterators.go` | Unmined queries on txs directly (no JOIN) | Rewrite |
| `stores/utxo/queue/alert_system.go` | Freeze/unfreeze UPDATE txs.frozen | Modify |
| `stores/utxo/queue/preservation.go` | Preserve/expire UPDATE txs columns | Modify |
| `stores/utxo/queue/pruner_provider.go` | Prune from 3 tables | Rewrite |
| `stores/utxo/queue/store_test.go` | Update tests for new schema | Modify |
| `stores/utxo/throughput_test.go` | Benchmark SQL vs v5 | Modify |

---

## Task 1: Schema — 3 UNLOGGED Tables, 16 Partitions

The foundation. Everything depends on this.

**Files:**

- Rewrite: `stores/utxo/queue/schema.go`
- Modify: `stores/utxo/queue/store_test.go` (TestSchemaCreation)

- [ ] **Step 1: Rewrite schema.go**

Replace all DDL with 3 UNLOGGED tables + staging tables:

```go
const txsDDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS txs (
    hash                 BYTEA PRIMARY KEY,
    version              BIGINT NOT NULL,
    lock_time            BIGINT NOT NULL,
    fee                  BIGINT NOT NULL,
    size_in_bytes        BIGINT NOT NULL,
    coinbase             BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx               BYTEA,
    locked               BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting          BOOLEAN NOT NULL DEFAULT FALSE,
    frozen               BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since        BIGINT,
    delete_at_height     BIGINT,
    preserve_until       BIGINT,
    block_ids            INT[],
    block_heights        INT[],
    subtree_idxs         INT[],
    conflicting_children BYTEA[],
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (hash);`

const outputsDDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS outputs (
    tx_hash                  BYTEA   NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    coinbase_spending_height BIGINT  NOT NULL DEFAULT 0,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH (tx_hash);`

const spendsDDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`
```

16 partitions per table. fillfactor: txs=70 (HOT updates), outputs=100, spends=100.

Partial indexes on txs (not on partitions directly — PG propagates):

```sql
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;
```

Add staging table creation helper (called per-connection by the create batcher):

```go
const createStagingTablesSQL = `
CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs INCLUDING NOTHING) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs INCLUDING NOTHING) ON COMMIT DELETE ROWS;
`
```

Remove all v4 DDL constants: transactionsDDL, txStateDDL, txStateIndexesDDL, inputsDDL, blockIDsDDL, conflictingChildrenDDL.

Update `createSchemaWithPool` to create 16 partitions (not 64) for 3 tables (not 7).

- [ ] **Step 2: Update TestSchemaCreation**

Change table list from 7 to 3. Change partition count from 64 to 16. Check indexes on txs.

- [ ] **Step 3: Verify tests pass**

Run: `go test -v -run TestSchemaCreation -timeout 30s ./stores/utxo/queue/`

Note: Other tests will fail because they reference deleted tables. That's expected — we fix them in subsequent tasks.

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/schema.go stores/utxo/queue/store_test.go
git commit -m "feat(queue): v5 schema — 3 UNLOGGED tables, 16 partitions"
```

---

## Task 2: Store — Pool Size 100, sync_commit=off, LRU Cache

**Files:**

- Modify: `stores/utxo/queue/store.go`

- [ ] **Step 1: Add LRU cache dependency**

Check if `github.com/hashicorp/golang-lru/v2` is already in go.mod. If not, add it. Alternatively, use a simple sync.Map with size bounding, or a channel-based bounded cache. The simplest approach: use `sync.Map` with periodic size checks — avoids adding a dependency. Or even simpler: a map protected by RWMutex with a max size that evicts randomly when full.

For the initial implementation, use a simple bounded map:

```go
type txCache struct {
    mu      sync.RWMutex
    entries map[chainhash.Hash]*meta.Data
    maxSize int
}
```

- [ ] **Step 2: Update Store struct and New()**

Add to Store:

```go
type Store struct {
    // ... existing fields ...
    cache *txCache  // in-process LRU cache for recently created txs
}
```

Update New():

- `pgxConfig.MaxConns = 100` (was 20)
- Add `AfterConnect` to set `synchronous_commit = off`
- Initialize cache with 100,000 max entries

Update Start():

- Keep existing batcher initialization (create + spend batchers from v4)

- [ ] **Step 3: Verify build + TestHealth pass**

Run: `go build ./stores/utxo/queue/ && go test -v -run TestHealth -timeout 30s ./stores/utxo/queue/`

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/store.go
git commit -m "feat(queue): v5 store — pool 100, sync_commit=off, LRU cache"
```

---

## Task 3: Create — COPY to Staging + INSERT...SELECT

This is the biggest performance change. Replace the CTE-per-tx pipelining with COPY binary to staging tables + single INSERT...SELECT.

**Files:**

- Rewrite: `stores/utxo/queue/create.go`

- [ ] **Step 1: Rewrite create.go**

The Create() method signature is unchanged. The batcher callback (`sendCreateBatch`) changes from pgx.SendBatch with N CTEs to:

```go
func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
    conn, err := s.pool.Acquire(ctx)
    defer conn.Release()

    // 1. Ensure temp staging tables exist on this connection
    conn.Exec(ctx, createStagingTablesSQL)

    // 2. COPY tx rows into staging_txs (binary format)
    txRows := buildTxCopyRows(batch)
    conn.Conn().CopyFrom(ctx, pgx.Identifier{"staging_txs"}, txCols, txRows)

    // 3. COPY output rows into staging_outputs
    outRows := buildOutputCopyRows(batch)
    conn.Conn().CopyFrom(ctx, pgx.Identifier{"staging_outputs"}, outCols, outRows)

    // 4. Single transaction: INSERT...SELECT + detect which were new
    tx, _ := conn.Begin(ctx)
    // INSERT txs, detect new vs duplicate
    rows, _ := tx.Query(ctx, `
        WITH inserted AS (
            INSERT INTO txs SELECT * FROM staging_txs ON CONFLICT (hash) DO NOTHING RETURNING hash
        )
        SELECT hash FROM inserted
    `)
    newHashes := collectHashes(rows)

    // INSERT outputs (only for new txs to avoid wasted work on dupes)
    tx.Exec(ctx, `INSERT INTO outputs SELECT * FROM staging_outputs WHERE tx_hash = ANY($1) ON CONFLICT DO NOTHING`, newHashes)

    tx.Commit(ctx)

    // 5. Signal callers, add to cache
    for _, item := range batch {
        wasNew := newHashes.Contains(item.txHash)
        item.done <- result
        if wasNew {
            s.cache.Add(item.txHash, item.meta)
        }
    }
}
```

The `txCols` for COPY must match the txs table columns exactly:

```go
var txCols = []string{
    "hash", "version", "lock_time", "fee", "size_in_bytes", "coinbase", "raw_tx",
    "locked", "conflicting", "frozen", "unmined_since",
    "delete_at_height", "preserve_until",
    "block_ids", "block_heights", "subtree_idxs", "conflicting_children",
    "inserted_at",
}
```

Build rows from batch items. `raw_tx` = `tx.Bytes()`. `block_ids`/`block_heights`/`subtree_idxs` as `[]int32` (postgres INT[]). `conflicting_children` as `[][]byte` (BYTEA[]).

The `createDirect` path (unbatched, for tests) should use regular INSERT instead of COPY:

```sql
INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
    locked, conflicting, frozen, unmined_since, block_ids, block_heights, subtree_idxs, conflicting_children)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (hash) DO NOTHING
RETURNING hash
```

Then INSERT outputs via unnest (same as v4 but no inputs unnest — inputs are in raw_tx).

Remove all references to: inputs table, tx_state table, block_ids table, conflicting_children table.
Remove: `buildInputArrays()`, `insertConflictingChildrenDirect()`.

- [ ] **Step 2: Verify build compiles**

Run: `go build ./stores/utxo/queue/`

This will fail until get.go and other files are updated. Create minimal stubs if needed to get compilation, or do Tasks 3-5 as an atomic batch.

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/queue/create.go
git commit -m "feat(queue): v5 Create — COPY to staging + INSERT...SELECT"
```

---

## Task 4: Get — Single-Table Read with Cache + raw_tx Deserialization

**Files:**

- Rewrite: `stores/utxo/queue/get.go`

- [ ] **Step 1: Rewrite get.go**

**Get/getInternal** — Cache check first, then single SELECT from txs:

```go
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
    if cached := s.cache.Get(*hash); cached != nil {
        return cached, nil
    }
    return s.getInternal(ctx, hash, requestedFields)
}
```

**getInternal** — One query for tx metadata + state + raw_tx + arrays:

```sql
SELECT version, lock_time, fee, size_in_bytes, coinbase,
       locked, conflicting, frozen, unmined_since, raw_tx,
       block_ids, block_heights, subtree_idxs, conflicting_children
FROM txs WHERE hash = $1
```

- If Tx/Inputs field requested: deserialize raw_tx via `bt.NewTxFromBytes(rawTx)`. Build inputs/outputs from the deserialized tx.
- If Outputs/Utxos field requested: query outputs table (LEFT JOIN spends for Utxos).
- If BlockIDs field requested: read from block_ids/block_heights/subtree_idxs arrays (no separate query).
- If ConflictingChildren requested: read from conflicting_children BYTEA[] array.

**GetSpend** — JOIN txs instead of tx_state:

```sql
SELECT o.utxo_hash, o.coinbase_spending_height, sp.spending_data,
       o.frozen OR t.frozen, o.spendable_in, t.conflicting, t.locked
FROM outputs o
JOIN txs t ON t.hash = o.tx_hash
LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
WHERE o.tx_hash = $1 AND o.idx = $2
```

**BatchDecorate** — Bulk query txs (no JOIN needed for basic metadata). For inputs, deserialize raw_tx. For block_ids, read arrays.

**BatchPreviousOutputsDecorate** — Unchanged (queries outputs table only).

**PreviousOutputsDecorate** — Can optionally use raw_tx from txs to get parent output data. But the outputs table already has locking_script + satoshis, so keep querying outputs for simplicity.

Remove all references to: tx_state table, inputs table, block_ids table, conflicting_children table.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/get.go
git commit -m "feat(queue): v5 Get — single-table read, raw_tx, cache"
```

---

## Task 5: Spend — JOIN txs Instead of tx_state

**Files:**

- Modify: `stores/utxo/queue/spend.go`

- [ ] **Step 1: Update SQL queries**

Change all references from `tx_state` to `txs`:

In `trySendSpendBatch` bulk SELECT:

```sql
-- Before:
JOIN tx_state ts ON ts.tx_hash = v.hash
-- After:
JOIN txs t ON t.hash = v.hash
```

Update column aliases: `ts.locked` → `t.locked`, `ts.conflicting` → `t.conflicting`, `ts.frozen` → `t.frozen`.

In `spendDirect` validation CTE and diagnostic SQL: same rename.

In `Unspend`: if it references tx_state for SetLocked, update to txs.

No structural changes — just table/alias renames in SQL strings.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/spend.go
git commit -m "feat(queue): v5 Spend — JOIN txs instead of tx_state"
```

---

## Task 6: Mined — Array Append UPDATE on txs

**Files:**

- Rewrite: `stores/utxo/queue/mined.go`

- [ ] **Step 1: Rewrite SetMinedMulti**

Replace INSERT into block_ids + UPDATE tx_state with single UPDATE on txs using array concatenation:

```sql
UPDATE txs
SET block_ids = COALESCE(block_ids, '{}') || $2::int[],
    block_heights = COALESCE(block_heights, '{}') || $3::int[],
    subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
    locked = false,
    unmined_since = CASE WHEN $5 THEN NULL ELSE unmined_since END
WHERE hash = ANY($1)
```

One statement. One table. N hashes.

**UnsetMined** (reorg): Update txs to remove a block_id from the arrays. Use `array_remove` or rebuild arrays excluding the target block_id:

```sql
UPDATE txs
SET block_ids = array_remove(block_ids, $2::int),
    block_heights = -- rebuild without the entry at the removed index
    ...
```

Array removal by index is tricky in PostgreSQL. Simpler approach: read current arrays, modify in Go, write back:

```go
// Read current arrays
var blockIDs, blockHeights, subtreeIdxs []int32
pool.QueryRow(ctx, `SELECT block_ids, block_heights, subtree_idxs FROM txs WHERE hash = $1`, hash).Scan(...)

// Remove entry at matching index
for i, bid := range blockIDs {
    if bid == int32(blockIDToRemove) {
        blockIDs = append(blockIDs[:i], blockIDs[i+1:]...)
        blockHeights = append(blockHeights[:i], blockHeights[i+1:]...)
        subtreeIdxs = append(subtreeIdxs[:i], subtreeIdxs[i+1:]...)
        break
    }
}

// Write back
pool.Exec(ctx, `UPDATE txs SET block_ids = $2, block_heights = $3, subtree_idxs = $4, unmined_since = $5 WHERE hash = $1`, ...)
```

This is 2 round-trips for the rare reorg path. Acceptable.

**fetchBlockIDs**: Read arrays directly from txs row:

```sql
SELECT block_ids FROM txs WHERE hash = $1
```

Return value: convert []int32 to []uint32.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/mined.go
git commit -m "feat(queue): v5 SetMined — array append UPDATE on txs"
```

---

## Task 7: Conflicting + SetLocked — UPDATE txs, Array Children

**Files:**

- Rewrite: `stores/utxo/queue/conflicting.go`

- [ ] **Step 1: Rewrite conflicting.go**

All operations now UPDATE `txs` instead of `tx_state`:

**SetLocked:**

```sql
UPDATE txs SET locked = $2 WHERE hash = ANY($1)
```

**SetConflicting:**

```sql
UPDATE txs SET conflicting = $2, delete_at_height = $3 WHERE hash = ANY($1)
```

For conflicting children, UPDATE the parent's `conflicting_children` array:

```sql
UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
WHERE hash = $1
```

**GetConflictingChildren:** Read array from txs:

```sql
SELECT conflicting_children FROM txs WHERE hash = $1
```

Parse BYTEA[] into []chainhash.Hash.

**MarkTransactionsOnLongestChain:**

```sql
-- onLongestChain = true:
UPDATE txs SET unmined_since = NULL WHERE hash = ANY($1)
-- onLongestChain = false:
UPDATE txs SET unmined_since = $2 WHERE hash = ANY($1)
```

Remove all references to tx_state and conflicting_children tables.

- [ ] **Step 2: Commit**

```bash
git add stores/utxo/queue/conflicting.go
git commit -m "feat(queue): v5 conflicting — UPDATE txs, array children"
```

---

## Task 8: Delete + Iterators + Alert + Preservation + Pruner

These are smaller files with straightforward table renames.

**Files:**

- Rewrite: `stores/utxo/queue/delete.go`
- Rewrite: `stores/utxo/queue/iterators.go`
- Modify: `stores/utxo/queue/alert_system.go`
- Modify: `stores/utxo/queue/preservation.go`
- Rewrite: `stores/utxo/queue/pruner_provider.go`

- [ ] **Step 1: Rewrite delete.go**

Delete from 3 tables instead of 7:

```sql
DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM txs WHERE hash = $1;
```

**setDAH** — query txs directly:

```sql
SELECT NOT EXISTS(
    SELECT 1 FROM outputs o
    WHERE o.tx_hash = $1
    AND NOT EXISTS (SELECT 1 FROM spends sp WHERE sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx)
) AS all_spent,
(block_ids IS NOT NULL AND array_length(block_ids, 1) > 0) AS has_blocks,
(unmined_since IS NULL) AS on_longest_chain
FROM txs WHERE hash = $1
```

- [ ] **Step 2: Rewrite iterators.go**

Query txs directly (no JOIN):

```sql
SELECT hash, fee, size_in_bytes, inserted_at, coinbase, locked, unmined_since, raw_tx, block_ids
FROM txs
WHERE unmined_since IS NOT NULL AND conflicting = false
ORDER BY hash
```

The `readOne` method:

- Deserialize raw_tx for inputs via `bt.NewTxFromBytes()`.
- Read block_ids from the array column directly (no separate query).
- No separate inputs or block_ids queries.

- [ ] **Step 3: Modify alert_system.go**

Change `tx_state` references to `txs`:

```sql
-- Before:
UPDATE tx_state SET frozen = true WHERE tx_hash = $1
-- After:
UPDATE txs SET frozen = true WHERE hash = $1
```

- [ ] **Step 4: Modify preservation.go**

```sql
-- Before:
UPDATE tx_state SET preserve_until = $1, delete_at_height = NULL WHERE tx_hash = ANY($2)
-- After:
UPDATE txs SET preserve_until = $1, delete_at_height = NULL WHERE hash = ANY($2)
```

- [ ] **Step 5: Rewrite pruner_provider.go**

Query txs for tombstoned entries, delete from 3 tables:

```sql
SELECT hash FROM txs WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1

DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM txs WHERE hash = $1;
```

- [ ] **Step 6: Commit**

```bash
git add stores/utxo/queue/delete.go stores/utxo/queue/iterators.go stores/utxo/queue/alert_system.go stores/utxo/queue/preservation.go stores/utxo/queue/pruner_provider.go
git commit -m "feat(queue): v5 delete/iterators/alert/preservation/pruner — 3-table schema"
```

---

## Task 9: Tests — Update for v5 Schema

**Files:**

- Modify: `stores/utxo/queue/store_test.go`

- [ ] **Step 1: Update setupTestStore**

Drop all old tables (v3 + v4 + v5) in the cleanup:

```sql
DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
    tx_state, transactions, txs,
    create_queue, input_queue, output_queue, spend_queue, mined_queue,
    batch_notifications CASCADE;
```

- [ ] **Step 2: Fix all test assertions**

Update tests that reference old table names or column layouts. Key changes:

- Table count: 3 not 7
- Partition count: 16 not 64
- Direct SQL queries in tests: `txs` instead of `transactions`/`tx_state`
- Block IDs: read from `txs.block_ids` array instead of `block_ids` table
- Conflicting children: read from `txs.conflicting_children` array

- [ ] **Step 3: Run all tests**

Run: `go test -v -timeout 120s ./stores/utxo/queue/`
Expected: All 49 tests pass.

- [ ] **Step 4: Commit**

```bash
git add stores/utxo/queue/store_test.go
git commit -m "test(queue): v5 test updates for 3-table schema"
```

---

## Task 10: Benchmark — SQL vs v5

**Files:**

- Modify: `stores/utxo/throughput_test.go`

- [ ] **Step 1: Update cleanDB and newQueueStoreForBench**

Update cleanDB to drop v5 tables:

```sql
DROP TABLE IF EXISTS ... txs, ... CASCADE;
```

Update newQueueStoreForBench — no special changes needed (Start() initializes batchers, pool config comes from store.go).

- [ ] **Step 2: Run benchmark**

```bash
pkill -f "utxo.test"
command go test -v -run "TestThroughput_(SQLStore|QueueStore)" -timeout 300s -count=1 ./stores/utxo/
```

Expected targets (from spec):

| Workers | v4 batched | v5 target |
|---------|-----------|-----------|
| 1 | 30 TPS | 300 TPS |
| 10 | 227 TPS | 2,000 TPS |
| 100 | 1,625 TPS | 15,000 TPS |
| 500 | 4,905 TPS | 50,000 TPS |

Report actual numbers. If below target, identify the bottleneck (COPY overhead? pool contention? batch size?).

- [ ] **Step 3: Commit**

```bash
git add stores/utxo/throughput_test.go
git commit -m "bench: v5 turbo UTXO store throughput comparison"
```

---

## Task 11: Integration — Sequential Tests

- [ ] **Step 1: Run sequential double-spend tests**

```bash
pkill -f "utxo.test"
command go test -v -run TestDoubleSpendSqlQueue -timeout 120s -count=1 ./test/sequentialtest/double_spend/
```

Expected: All pass.

- [ ] **Step 2: Fix any failures**

Failures are likely in:

- SetConflicting (children stored as arrays now)
- SetMined (block_ids as arrays)
- Get with BlockIDs field (array parsing)

Debug individually with `-run TestName` and `-v`.

- [ ] **Step 3: Commit fixes**

```bash
git add -A
git commit -m "fix(queue): v5 sequential test fixes"
```
