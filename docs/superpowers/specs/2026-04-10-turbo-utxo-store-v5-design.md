# Turbo UTXO Store — 10x Performance Design

**Date:** 2026-04-10 (v5 — COPY protocol, 3-table schema, in-process cache)
**Supersedes:** v4 (append-only direct-write with batching)
**Target:** 50,000+ TPS on validator hot path (10x current ~5,000 TPS)
**Scope:** Performance rewrite of `stores/utxo/queue` package

## Problem

The v4 store with batching achieves ~5,000 TPS on the validator hot path and ~8,000 TPS on SetMined. This matches the SQL store but doesn't beat it meaningfully. The target is 10x.

## Root Cause Analysis

Profiling the v4 benchmark at 500 workers reveals five bottlenecks:

| Bottleneck | Impact | Evidence |
|------------|--------|----------|
| Per-statement overhead | ~100μs per SQL statement even with pipelining | 100 CTE creates via SendBatch = 10ms |
| WAL writes | Every INSERT/UPDATE writes WAL | UNLOGGED tables are 3x faster |
| Connection pool contention | 500 workers / 20 conns = 25x contention | Workers wait ~12ms per operation for a conn |
| Read round-trips for cached data | Get() reads data the same process just created | ~500μs wasted per Get |
| 7 tables × 64 partitions | JOINs and partition routing overhead | 448 partitions in the schema |

## Design: Five Techniques Stacked

### Technique 1: COPY Protocol for Creates

Replace pgx.SendBatch (100 individual CTE statements) with COPY to staging + single INSERT...SELECT.

**Current (v4):** 100 CTE statements pipelined via SendBatch.
Each CTE: parse → plan → execute = ~100μs. Total: ~10ms for 100 creates.

**v5:** COPY binary stream to unindexed staging tables, then single INSERT...SELECT.

```sql
-- Step 1: COPY 100 tx rows into staging (no indexes, no constraints)
COPY staging_txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
    locked, conflicting, frozen, unmined_since) FROM STDIN (FORMAT binary)
-- ~0.2ms for 100 rows (2μs per row, no SQL parsing)

-- Step 2: COPY outputs into staging
COPY staging_outputs (tx_hash, idx, locking_script, satoshis, utxo_hash,
    coinbase_spending_height, frozen, spendable_in) FROM STDIN (FORMAT binary)
-- ~0.3ms for ~200 output rows

-- Step 3: Single INSERT...SELECT with ON CONFLICT
BEGIN;
INSERT INTO txs SELECT * FROM staging_txs ON CONFLICT (hash) DO NOTHING;
INSERT INTO outputs SELECT * FROM staging_outputs ON CONFLICT (tx_hash, idx) DO NOTHING;
TRUNCATE staging_txs, staging_outputs;
COMMIT;
-- ~0.5ms total (one plan per INSERT, bulk execution)
```

Total: **~1ms for 100 creates** vs current ~10ms. **10x on creates.**

The staging tables are `TEMPORARY` (per-connection, in local memory, no shared buffer contention):

```sql
CREATE TEMPORARY TABLE IF NOT EXISTS staging_txs (LIKE txs INCLUDING NOTHING) ON COMMIT DELETE ROWS;
CREATE TEMPORARY TABLE IF NOT EXISTS staging_outputs (LIKE outputs INCLUDING NOTHING) ON COMMIT DELETE ROWS;
```

`ON COMMIT DELETE ROWS` auto-truncates on commit. No explicit TRUNCATE needed.

### Technique 2: UNLOGGED Tables

All three permanent tables are UNLOGGED. No WAL writes. No WAL flush. No WAL archiving.

Acceptable because the UTXO store is **derived state** — source of truth is the blockchain + Kafka. On postgres crash, rebuild from chain. The risk is bounded: teranode can re-process blocks to rebuild the UTXO set.

Expected gain: **3x on all writes.**

### Technique 3: In-Process LRU Cache

Cache recently-created transactions in Go memory. The validator hot path does Get→Spend→Create→Unlock. The Get is almost always for a transaction this process just created.

```go
type Store struct {
    ...
    txCache *lru.Cache[chainhash.Hash, *meta.Data]  // 100K entries, ~50MB
}

func (s *Store) Get(ctx, hash, fields) (*meta.Data, error) {
    if cached, ok := s.txCache.Get(*hash); ok {
        return cached, nil  // 0μs instead of 500μs
    }
    return s.getFromDB(ctx, hash, fields)
}

func (s *Store) Create(ctx, tx, blockHeight, opts) (*meta.Data, error) {
    result, err := s.createInDB(ctx, tx, blockHeight, opts)
    if err == nil {
        s.txCache.Add(*tx.TxIDChainHash(), result)
    }
    return result, err
}
```

Expected gain: **1.3x** (eliminates 1 of 4 round-trips per tx cycle).

### Technique 4: 3-Table Schema

Collapse 7 tables → 3 by embedding inputs, block_ids, and conflicting_children into the txs row:

```sql
CREATE UNLOGGED TABLE txs (
    hash                 BYTEA PRIMARY KEY,
    -- immutable
    version              BIGINT NOT NULL,
    lock_time            BIGINT NOT NULL,
    fee                  BIGINT NOT NULL,
    size_in_bytes        BIGINT NOT NULL,
    coinbase             BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx               BYTEA,
    -- mutable state (HOT-eligible: none in secondary indexes)
    locked               BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting          BOOLEAN NOT NULL DEFAULT FALSE,
    frozen               BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since        BIGINT,
    delete_at_height     BIGINT,
    preserve_until       BIGINT,
    -- block_ids as arrays (no separate table)
    block_ids            INT[],
    block_heights        INT[],
    subtree_idxs         INT[],
    -- conflicting children as array (no separate table)
    conflicting_children BYTEA[],
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH(hash);
-- Children partitions: fillfactor = 70 (reserves 30% for HOT updates)

CREATE UNLOGGED TABLE outputs (
    tx_hash                  BYTEA   NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    coinbase_spending_height BIGINT  NOT NULL DEFAULT 0,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH(tx_hash);
-- Children partitions: fillfactor = 100 (immutable, never updated)

CREATE UNLOGGED TABLE spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH(prev_tx_hash);
-- Children partitions: fillfactor = 100 (append-only)
```

**Indexes — absolute minimum:**

| Table | Index | Purpose |
|-------|-------|---------|
| txs | PK on hash | Create dedup, Get, spend validation |
| txs | partial on unmined_since WHERE NOT NULL | Unmined iterator (rare) |
| txs | partial on delete_at_height WHERE NOT NULL | Pruner (rare) |
| outputs | PK on (tx_hash, idx) | Spend validation, decoration |
| spends | UNIQUE on (prev_tx_hash, prev_output_idx) | Double-spend detection |

No other indexes. 5 total (down from 9).

**Partition count: 16 instead of 64.** Less routing overhead for current data volumes. Each partition still handles millions of rows efficiently. Increase to 64 for production scale if needed.

**What this eliminates:**

| Eliminated | Where it went |
|------------|---------------|
| tx_state table | Columns merged into txs (locked, conflicting, frozen, unmined_since, etc.) |
| inputs table | raw_tx BYTEA blob on txs (full serialized tx) |
| block_ids table | INT[] arrays on txs (block_ids, block_heights, subtree_idxs) |
| conflicting_children table | BYTEA[] array on txs |

**Impact on operations:**

| Operation | Before (v4) | After (v5) |
|-----------|------------|------------|
| Create | INSERT into 4 tables (txs + tx_state + inputs + outputs) | INSERT into 2 tables (txs + outputs) |
| Get | JOIN txs + tx_state, separate queries for inputs/outputs/block_ids | Single SELECT from txs (raw_tx has inputs, arrays have block_ids) |
| Spend validation | JOIN outputs + tx_state + spends | JOIN outputs + txs + spends (same count but tx_state eliminated) |
| SetMined | INSERT block_ids + UPDATE tx_state (2 tables, 2 ops) | UPDATE txs SET block_ids = block_ids \|\| $1 (1 table, 1 op) |
| SetLocked | UPDATE tx_state | UPDATE txs |
| Delete | DELETE from 7 tables | DELETE from 3 tables |

Expected gain: **1.5-2x** (fewer table writes, fewer JOINs).

### Technique 5: Pool Size + synchronous_commit

```go
pgxConfig.MaxConns = 100  // was 20
pgxConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
    _, err := conn.Exec(ctx, "SET synchronous_commit = off")
    return err
}
```

Expected gain: **2-3x at high concurrency** (pool contention was the dominant bottleneck at 500 workers).

## Operation Flows

### Create (COPY + INSERT...SELECT batched)

```go
func (s *Store) Create(ctx, tx, blockHeight, opts) (*meta.Data, error) {
    // 1. Pre-compute metadata, arrays
    // 2. Enqueue into create batcher
    // 3. Block on done channel
    // 4. On success, add to txCache
}

// Batcher flush callback:
func (s *Store) sendCreateBatch(items []batchCreateItem) {
    conn := pool.Acquire(ctx)

    // Ensure temp staging tables exist on this connection
    conn.Exec(ctx, `CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs INCLUDING NOTHING) ON COMMIT DELETE ROWS`)
    conn.Exec(ctx, `CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs INCLUDING NOTHING) ON COMMIT DELETE ROWS`)

    // COPY tx rows into staging (binary format)
    conn.Conn().CopyFrom(ctx, pgx.Identifier{"staging_txs"}, txCols, txSource)

    // COPY output rows into staging
    conn.Conn().CopyFrom(ctx, pgx.Identifier{"staging_outputs"}, outCols, outSource)

    // Single transaction: INSERT...SELECT into final tables
    tx := conn.Begin(ctx)
    tx.Exec(ctx, `INSERT INTO txs SELECT * FROM staging_txs ON CONFLICT (hash) DO NOTHING`)
    tx.Exec(ctx, `INSERT INTO outputs SELECT * FROM staging_outputs ON CONFLICT (tx_hash, idx) DO NOTHING`)
    tx.Commit(ctx)

    // Signal callers
}
```

### Spend (Bulk SELECT + Bulk INSERT, unchanged from v4 batched)

Same 3-phase bulk pattern. The validation SELECT changes slightly:

```sql
SELECT v.batch_idx, o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
       o.coinbase_spending_height, t.locked, t.conflicting, t.frozen AS tx_frozen,
       sp.spending_data AS existing_spend
FROM (VALUES ($1::bytea, $2::int, $3::int), ...) AS v(hash, idx, batch_idx)
JOIN outputs o ON o.tx_hash = v.hash AND o.idx = v.idx
JOIN txs t ON t.hash = v.hash                          -- was tx_state, now txs
LEFT JOIN spends sp ON sp.prev_tx_hash = v.hash AND sp.prev_output_idx = v.idx
```

Bulk INSERT INTO spends unchanged.

### Get (Cache first, single SELECT fallback)

```go
func (s *Store) Get(ctx, hash, fields) (*meta.Data, error) {
    // Check cache first
    if cached, ok := s.txCache.Get(*hash); ok {
        return cached, nil
    }

    // Single SELECT from txs (no JOINs for basic metadata)
    row := pool.QueryRow(ctx, `
        SELECT version, lock_time, fee, size_in_bytes, coinbase,
               locked, conflicting, frozen, unmined_since, raw_tx,
               block_ids, block_heights, subtree_idxs
        FROM txs WHERE hash = $1`, hash[:])

    // Deserialize raw_tx for inputs if requested
    // Extract block_ids from arrays
    // Only query outputs table if Utxos/Outputs field requested
}
```

Get with Tx/Inputs field: deserialize `raw_tx` in Go (`bt.NewTxFromBytes`).
Get with Outputs field: query outputs table.
Get with Utxos field: query outputs LEFT JOIN spends.
Get with BlockIDs field: read arrays from txs row (no separate query).

### SetMined (Single UPDATE with array append)

```sql
UPDATE txs
SET block_ids = block_ids || $2::int[],
    block_heights = block_heights || $3::int[],
    subtree_idxs = subtree_idxs || $4::int[],
    locked = false,
    unmined_since = CASE WHEN $5 THEN NULL ELSE unmined_since END
WHERE hash = ANY($1)
```

One statement. One table. Handles N hashes.

### SetLocked (Single UPDATE)

```sql
UPDATE txs SET locked = $2 WHERE hash = ANY($1)
```

### Unspend (DELETE from spends)

```sql
DELETE FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = $2
```

### Delete (3 tables instead of 7)

```sql
DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM txs WHERE hash = $1;
```

## The raw_tx Blob

Store the full serialized transaction as BYTEA on the txs row.

**Write path:** `raw_tx = tx.Bytes()` — called once during Create.

**Read path:** `bt.NewTxFromBytes(raw_tx)` — called during Get when Tx/Inputs field requested.

**PreviousOutputsDecorate:** Fetch parent's raw_tx, deserialize, extract output locking_script + satoshis. Slightly slower than a direct column read, but eliminates the inputs table entirely. For batch decoration, fetch N parent txs in one IN-clause query.

**Size:** Average BSV transaction is ~250 bytes. For 1M txs, that's ~250MB of raw_tx data. Acceptable.

## Expected Performance Stack

| Technique | Gain | Mechanism |
|-----------|------|-----------|
| COPY protocol for creates | 5-10x on creates | Binary stream, no SQL parse/plan per row |
| UNLOGGED tables | 3x on all writes | No WAL writes |
| In-process LRU cache | 1.3x overall | Eliminates Get round-trip |
| 3-table schema | 1.5x | Fewer JOINs, fewer table writes |
| Pool size 100 | 2-3x at high concurrency | Reduces connection wait |
| synchronous_commit=off | 1.2x | Skip fsync (on top of UNLOGGED) |

**Conservative combined estimate:** COPY(5x) × UNLOGGED(3x) on creates gives **15x on Create path**. For the full validator cycle (Get+Spend+Create+Unlock), the Create is ~40% of the time, Spend ~30%, Get ~15%, Unlock ~15%. Weighted: 0.4×15 + 0.3×3 + 0.15×∞(cache) + 0.15×3 = **~8-12x overall.**

## Trade-offs

| Trade-off | Impact | Mitigation |
|-----------|--------|------------|
| UNLOGGED data loss on crash | Full UTXO rebuild required | Rebuild from blockchain + Kafka (derived state) |
| raw_tx deserialization cost | Get with Tx field ~50μs slower | Only affects reads; in-process cache eliminates most reads |
| Wider txs rows | More bytes per UPDATE (HOT) | fillfactor=70; only 2-3 HOT updates per tx lifetime |
| Array append for block_ids | UPDATE instead of INSERT | Single UPDATE vs INSERT+separate table; net faster |
| 16 partitions (fewer) | Less parallelism for vacuum | Increase to 64 for production scale |
| In-process cache | Memory usage (~50MB for 100K entries) | Bounded LRU; configurable size |
| COPY staging tables | Temp table per connection | Auto-created, ON COMMIT DELETE ROWS |

## Files to Modify

- `stores/utxo/queue/schema.go` — 3-table DDL, 16 partitions, UNLOGGED, temp staging tables
- `stores/utxo/queue/store.go` — pool size 100, sync_commit=off, LRU cache, batcher lifecycle
- `stores/utxo/queue/create.go` — COPY to staging + INSERT...SELECT batcher
- `stores/utxo/queue/spend.go` — bulk SELECT/INSERT (minor: JOIN txs instead of tx_state)
- `stores/utxo/queue/get.go` — cache check, single-table Get from txs, raw_tx deserialization
- `stores/utxo/queue/mined.go` — array append UPDATE instead of separate table INSERT
- `stores/utxo/queue/conflicting.go` — UPDATE txs instead of tx_state, array for children
- `stores/utxo/queue/delete.go` — 3-table delete
- `stores/utxo/queue/iterators.go` — query txs directly (no JOIN)
- `stores/utxo/queue/alert_system.go` — UPDATE txs.frozen instead of tx_state.frozen
- `stores/utxo/queue/preservation.go` — UPDATE txs columns
- `stores/utxo/queue/pruner_provider.go` — 3-table cascade delete
- `stores/utxo/throughput_test.go` — updated benchmark

## Benchmark Targets

| Metric | v4 batched | v5 target | Improvement |
|--------|-----------|-----------|-------------|
| Validator 1 worker | 30 TPS | 300 TPS | 10x |
| Validator 10 workers | 227 TPS | 2,000 TPS | 9x |
| Validator 100 workers | 1,625 TPS | 15,000 TPS | 9x |
| Validator 500 workers | 4,905 TPS | 50,000 TPS | 10x |
| SetMined 10K txs | 7,764 TPS | 50,000 TPS | 6x |
