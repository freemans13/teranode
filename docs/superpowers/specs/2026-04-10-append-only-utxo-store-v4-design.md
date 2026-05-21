# Append-Only Direct-Write UTXO Store

**Date:** 2026-04-10 (v4 — all-direct, append-only, no materializer)
**Supersedes:** 2026-04-09 v3 (single materializer + pg_notify)
**Target:** Maximum write throughput on PostgreSQL for 40+ propagation pods
**Scope:** New `stores/utxo/queue` implementation (name retained for package continuity)

## Problem

The v3 queue store routes ALL operations through a single materializer (process_batch). Benchmarks show:

- **Validator hot path** (Get+Spend+Create+Unlock): ~4,500 TPS — comparable to SQL store
- **SetMinedMulti**: 761-2,500 TPS vs SQL store's 11,000-32,000 TPS (11-17x slower)

The materializer bottleneck hurts operations that don't need batching. Spends are single-row INSERTs that gain nothing from COPY batching. SetMined is a bulk operation that's faster as a direct write.

Additionally, the `UPDATE outputs SET spending_data` pattern causes MVCC bloat, dead tuples, and row-level lock contention on the widest table.

## Primary Approach: All-Direct (no materializer)

Every operation writes directly to snapshot tables. No queue tables, no stored procedure, no pg_cron, no pg_notify. Concurrent 40-pod writes are safe because:

- All hot-path tables are append-only (INSERT only, never UPDATE)
- `ON CONFLICT DO NOTHING` eliminates lock contention on duplicates
- 64-way hash partitioning spreads writes across partitions
- The only UPDATEd table (tx_state) is narrow with HOT updates

**Fallback:** If benchmarks show COPY batching is faster for creates (especially mega-transactions with thousands of inputs/outputs), the materializer approach from v3 can be restored for creates only. The spec documents both paths. Spends, SetMined, SetLocked, and SetConflicting remain direct regardless.

## Design Principles

1. **Append-only where possible** — INSERTs only, no UPDATEs on hot-path tables
2. **Direct operations for single-row writes** — bypass the materializer for spends, mined, locked
3. **Queue only for high-row-count batching** — creates (1 tx + N inputs + M outputs) benefit from COPY
4. **Narrow mutable table** — isolate all mutable state into a tiny tx_state table with HOT updates
5. **Optimize for writes at the expense of reads** — reads can JOIN, writes must be minimal

## Architecture

```text
40 Propagation Pods (k8s)
    │
    ├── Create ──► DIRECT INSERT (transactions + tx_state + inputs + outputs)
    │              unnest arrays for inputs/outputs, single transaction
    │              CopyFrom for mega-txs with thousands of inputs/outputs
    │
    ├── Spend ──► DIRECT INSERT into spends table
    │             (single row, immediate conflict feedback)
    │
    ├── SetMined ──► DIRECT INSERT block_ids + UPDATE tx_state
    │                (bulk operation, single transaction)
    │
    ├── SetLocked ──► DIRECT UPDATE tx_state
    │                 (single row, <1ms operation)
    │
    └── Get ──► DIRECT SELECT with JOINs
               (reads from immutable tables + spends + tx_state)
```

No queue tables. No stored procedures. No pg_cron. No pg_notify. No materializer.
All operations are synchronous direct writes with immediate return.

## Table Design

### Immutable Append-Only Tables (INSERT only, never UPDATE)

All use `fillfactor = 100` (pages packed full — no space reserved for updates that never happen).
All hash-partitioned into 64 partitions for parallel I/O and partition elimination.

```sql
-- Core transaction metadata. One row per unique transaction.
CREATE TABLE transactions (
    hash          BYTEA PRIMARY KEY,
    version       BIGINT NOT NULL,
    lock_time     BIGINT NOT NULL,
    fee           BIGINT NOT NULL,
    size_in_bytes BIGINT NOT NULL,
    coinbase      BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH(hash) WITH (fillfactor = 100);

-- Transaction inputs. Immutable after create.
CREATE TABLE inputs (
    tx_hash                   BYTEA  NOT NULL,
    idx                       BIGINT NOT NULL,
    previous_transaction_hash BYTEA  NOT NULL,
    previous_tx_idx           BIGINT NOT NULL,
    previous_tx_satoshis      BIGINT NOT NULL,
    previous_tx_script        BYTEA,
    unlocking_script          BYTEA  NOT NULL,
    sequence_number           BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);

-- Transaction outputs (UTXOs). Immutable after create.
-- spending_data is NO LONGER on this table — it moved to the spends table.
CREATE TABLE outputs (
    tx_hash                  BYTEA   NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    coinbase_spending_height BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);

-- Spend records. One row per spent output. Append-only.
-- UNIQUE constraint enables conflict detection via ON CONFLICT.
CREATE TABLE spends (
    prev_tx_hash    BYTEA NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH(prev_tx_hash) WITH (fillfactor = 100);

-- Block associations. Append-only (delete only on reorg).
CREATE TABLE block_ids (
    tx_hash      BYTEA  NOT NULL,
    block_id     BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    subtree_idx  BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, block_id)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);

-- Parent-to-conflicting-child tracking.
CREATE TABLE conflicting_children (
    tx_hash       BYTEA NOT NULL,
    child_tx_hash BYTEA NOT NULL,
    PRIMARY KEY (tx_hash, child_tx_hash)
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 100);
```

### Narrow Mutable Table (UPDATEs allowed, HOT-optimized)

```sql
-- All mutable transaction state in one narrow table.
-- fillfactor = 50 reserves 50% page space for HOT updates.
-- NO secondary indexes on mutable columns — all UPDATEs are HOT
-- (in-page, no index maintenance, no dead tuple in index).
CREATE TABLE tx_state (
    tx_hash        BYTEA   PRIMARY KEY,
    locked         BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting    BOOLEAN NOT NULL DEFAULT FALSE,
    frozen         BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since  BIGINT,
    delete_at_height BIGINT,
    preserve_until BIGINT
) PARTITION BY HASH(tx_hash) WITH (fillfactor = 50);

-- Only partial indexes for rare scan operations:
CREATE INDEX px_unmined_since ON tx_state (unmined_since)
    WHERE unmined_since IS NOT NULL;
CREATE INDEX px_delete_at_height ON tx_state (delete_at_height)
    WHERE delete_at_height IS NOT NULL;
```

### Queue Tables (FALLBACK ONLY — for materializer approach)

If benchmarks show COPY batching outperforms direct INSERTs for creates (likely only for mega-transactions with thousands of inputs/outputs), the v3 queue + materializer can be restored for creates only. Queue tables would be UNLOGGED (no WAL) with the same schema as v3 but excluding spend_queue and mined_queue. See the "Fallback: Materializer for Creates" section at the end of this document.

## Operation Flows

### Create (Direct — unnest arrays, single transaction)

**Why direct:** Average transaction has ~5 rows across 4 tables. The overhead of queue + materializer + pg_notify exceeds the cost of direct INSERTs. 40 pods writing concurrently is safe because all tables are append-only with ON CONFLICT DO NOTHING.

```go
func (s *Store) Create(ctx, tx, blockHeight, opts) (*meta.Data, error) {
    pgxTx := pool.Begin(ctx)

    // 1. Transaction metadata (1 row)
    pgxTx.Exec(ctx, `INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, coinbase)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (hash) DO NOTHING`, ...)

    // 2. Transaction state (1 row)
    pgxTx.Exec(ctx, `INSERT INTO tx_state (tx_hash, locked, frozen, unmined_since)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tx_hash) DO NOTHING`, ...)

    // 3. Inputs (N rows via unnest — single statement)
    pgxTx.Exec(ctx, `INSERT INTO inputs (tx_hash, idx, previous_transaction_hash, ...)
        SELECT unnest($1::bytea[]), unnest($2::bigint[]), unnest($3::bytea[]), ...
        ON CONFLICT (tx_hash, idx) DO NOTHING`, inputArrays...)

    // 4. Outputs (M rows via unnest — single statement)
    pgxTx.Exec(ctx, `INSERT INTO outputs (tx_hash, idx, locking_script, ...)
        SELECT unnest($1::bytea[]), unnest($2::bigint[]), unnest($3::bytea[]), ...
        ON CONFLICT (tx_hash, idx) DO NOTHING`, outputArrays...)

    pgxTx.Commit(ctx)
}
```

For mega-transactions with thousands of inputs/outputs, use `pgx.CopyFrom` instead of unnest:

```go
if len(inputs) > 100 {
    pgxTx.CopyFrom(ctx, pgx.Identifier{"inputs"}, inputCols, pgx.CopyFromRows(inputRows))
}
```

**No queue tables, no materializer, no pg_notify.** Immediate synchronous return.

### Spend (Direct — no queue, immediate feedback)

**Why direct:** Single-row INSERT into spends table. No batching benefit. Synchronous conflict feedback required.

```go
func (s *Store) Spend(ctx, tx, blockHeight) ([]*utxo.Spend, error) {
    for each input in tx {
        // Single SQL statement: validate + insert atomically
        // Returns 1 row if spend succeeded, 0 rows if blocked
        row := pool.QueryRow(ctx, spendSQL, prevTxHash, prevOutputIdx,
            spendingData, expectedUtxoHash, blockHeight, ignoreLocked, ignoreConflicting)
    }
}
```

**Spend SQL (validate + insert in one statement):**

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
WHERE v.existing_spend IS NULL                    -- not already spent
  AND v.utxo_hash = $4                            -- hash match
  AND NOT v.output_frozen AND NOT v.tx_frozen     -- not frozen (output or tx level)
  AND ($6 OR NOT v.tx_locked)                     -- not locked (unless ignored)
  AND ($7 OR NOT v.tx_conflicting)                -- not conflicting (unless ignored)
  AND NOT (v.coinbase_spending_height > 0
           AND v.coinbase_spending_height > $5)   -- coinbase mature
  AND NOT (COALESCE(v.spendable_in, 0) > 0
           AND $5 < COALESCE(v.spendable_in, 0))  -- spendable_in check
ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
RETURNING 1
```

If the INSERT returns 0 rows, re-query the validation CTE to determine the conflict reason (double_spend, frozen, locked, etc.) and return the appropriate error.

**Throughput advantage:** 40 pods spend concurrently against the spends table. Each INSERT hits a different partition (64-way hash). No single-materializer bottleneck. No queue hop. No pg_notify wait.

### SetMined (Direct — bulk INSERT + UPDATE)

**Why direct:** Bulk operation that benefits from a single transaction, not from COPY batching. SQL store achieves 11,000-32,000 TPS on this path.

```go
func (s *Store) SetMinedMulti(ctx, hashes, minedBlockInfo) {
    tx := pool.Begin(ctx)

    // 1. Bulk INSERT into block_ids (append-only)
    for chunk of hashes {
        tx.Exec(ctx, `INSERT INTO block_ids (tx_hash, block_id, block_height, subtree_idx)
            SELECT unnest($1::bytea[]), $2, $3, $4
            ON CONFLICT (tx_hash, block_id) DO NOTHING`, chunk, blockID, height, subtreeIdx)
    }

    // 2. Bulk UPDATE tx_state (narrow table, HOT updates)
    if onLongestChain {
        tx.Exec(ctx, `UPDATE tx_state SET locked = false, unmined_since = NULL
            WHERE tx_hash = ANY($1)`, hashes)
    } else {
        tx.Exec(ctx, `UPDATE tx_state SET locked = false
            WHERE tx_hash = ANY($1)`, hashes)
    }

    tx.Commit(ctx)

    // 3. Return block_ids for each hash
    ...
}
```

### SetLocked (Direct — single UPDATE)

```go
func (s *Store) SetLocked(ctx, txHashes, value) error {
    pool.Exec(ctx, `UPDATE tx_state SET locked = $2 WHERE tx_hash = ANY($1)`, hashes, value)
}
```

HOT update on narrow table. No index maintenance.

### SetConflicting (Direct — single UPDATE)

```go
func (s *Store) SetConflicting(ctx, txHashes, value) {
    pool.Exec(ctx, `UPDATE tx_state SET conflicting = $2 WHERE tx_hash = ANY($1)`, hashes, value)
}
```

### Get (Direct — JOINs across tables)

```sql
-- Transaction metadata + state
SELECT t.version, t.lock_time, t.fee, t.size_in_bytes, t.coinbase,
       ts.locked, ts.conflicting, ts.unmined_since
FROM transactions t
JOIN tx_state ts ON ts.tx_hash = t.hash
WHERE t.hash = $1

-- Outputs with spend status
SELECT o.idx, o.locking_script, o.satoshis, sp.spending_data, o.frozen
FROM outputs o
LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
WHERE o.tx_hash = $1
ORDER BY o.idx

-- Inputs, block_ids, conflicting_children: unchanged (single-table queries)
```

Reads add one extra JOIN each but hit partition-eliminated PK lookups. Cost is negligible for point queries.

### Unmined Iterator

```sql
SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
       ts.locked, ts.unmined_since
FROM transactions t
JOIN tx_state ts ON ts.tx_hash = t.hash
WHERE ts.unmined_since IS NOT NULL AND ts.conflicting = false
ORDER BY t.hash
```

Uses partial index `px_unmined_since` on tx_state.

### Unspend (Reorg — rare)

```sql
-- Remove spend record (append-only table, but deletes allowed for reorgs)
DELETE FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = $2
```

### Delete (Full tx removal — rare)

```sql
DELETE FROM spends WHERE prev_tx_hash = $1;
DELETE FROM block_ids WHERE tx_hash = $1;
DELETE FROM outputs WHERE tx_hash = $1;
DELETE FROM inputs WHERE tx_hash = $1;
DELETE FROM tx_state WHERE tx_hash = $1;
DELETE FROM transactions WHERE tx_hash = $1;
```

## PostgreSQL Tuning

| Setting | Value | Rationale |
|---------|-------|-----------|
| Queue tables | UNLOGGED | No WAL for ephemeral COPY data |
| Append-only fillfactor | 100 | Pack pages full, no wasted space |
| tx_state fillfactor | 50 | Reserve space for HOT updates |
| tx_state indexes | PK only + 2 partial | All column UPDATEs are HOT (not in any index) |
| Partitioning | 64-way HASH on all tables | Parallel I/O, partition elimination |
| spends constraint | UNIQUE not PK | Slightly lighter than PK for conflict detection |

## Index Summary

| Table | Index | Type | Purpose |
|-------|-------|------|---------|
| transactions | hash | PK | Create dedup, Get lookup |
| inputs | (tx_hash, idx) | PK | Get lookup |
| outputs | (tx_hash, idx) | PK | Spend validation, Get lookup |
| spends | (prev_tx_hash, prev_output_idx) | UNIQUE | Double-spend detection |
| tx_state | tx_hash | PK | Spend validation, SetLocked, SetMined |
| tx_state | unmined_since WHERE NOT NULL | Partial | Unmined iterator |
| tx_state | delete_at_height WHERE NOT NULL | Partial | DAH cleanup |
| block_ids | (tx_hash, block_id) | PK | SetMined dedup, Get lookup |
| conflicting_children | (tx_hash, child_tx_hash) | PK | Get lookup |

No other indexes. Every index slows INSERTs.

## What Changed from v3

| Aspect | v3 (current) | v4 (this design) |
|--------|-------------|-------------------|
| Create path | Queued → materializer → pg_notify | Direct INSERT (unnest/CopyFrom) |
| Spend path | Queued → materializer → UPDATE outputs → pg_notify | Direct INSERT into spends table |
| SetMined path | Queued → materializer → pg_notify, then per-hash UPDATE | Direct INSERT block_ids + bulk UPDATE tx_state |
| SetLocked path | Direct UPDATE transactions | Direct UPDATE tx_state (narrower) |
| outputs table | Mutable (spending_data updated) | Immutable after create |
| transactions table | Mutable (locked, conflicting, unmined_since) | Immutable after create |
| tx_state table | N/A | NEW — all mutable flags + frozen |
| spends table | N/A | NEW — append-only spend records |
| Queue tables | 5 (create, input, output, spend, mined) | None (fallback: 3 UNLOGGED for creates) |
| Stored procedure | process_batch + materialize_loop | None (fallback: creates-only proc) |
| pg_cron | Schedules materialize_loop every 1s | None (fallback: creates-only schedule) |
| pg_notify | Batch completions + conflicts JSON | None (fallback: creates-only max_batch) |
| Conflict detection | In materializer, async via pg_notify | In spend SQL, synchronous return |

## Expected Performance Impact

| Operation | v3 | v4 (expected) | Why |
|-----------|-----|---------------|-----|
| Create | ~4,500 TPS (materializer) | 5,000+ TPS (direct) | No queue hop, no pg_notify wait. Benchmark needed. |
| Spend | ~4,500 TPS (shared with create) | 10,000+ TPS | Direct INSERT, 40 pods concurrent, no materializer |
| SetMinedMulti | 761-2,500 TPS | 15,000-30,000 TPS | Direct bulk, matching SQL store performance |
| SetLocked | Through queue | <1ms direct | Single UPDATE on narrow table |
| Get | Direct reads | Direct reads + 1 JOIN | Negligible cost for point queries |

If create benchmarks show the direct path is slower than COPY batching, the materializer fallback is available for creates only.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| spends table grows without bound | Same DAH cleanup as transactions — delete spends when parent tx is deleted |
| Concurrent spends on same output (40 pods) | UNIQUE constraint + ON CONFLICT DO NOTHING — exactly one wins, rest get 0 rows |
| tx_state bloat from frequent UPDATEs | fillfactor=50 enables HOT updates; autovacuum tuned for this table |
| UNLOGGED queue tables lost on crash | Batchers re-send on reconnect; queue data is ephemeral |
| Read cost increases (extra JOINs) | All JOINs are PK lookups with partition elimination; benchmarks needed to confirm |
| Spend validation under high contention | 64 partitions spread load; UNIQUE constraint check is lightweight |

## Files to Modify

- `stores/utxo/queue/schema.go` — new table DDL (remove queue tables, stored procs, pg_cron)
- `stores/utxo/queue/create.go` — direct INSERT via unnest/CopyFrom (remove queue+wait pattern)
- `stores/utxo/queue/spend.go` — direct INSERT into spends table (remove queue+wait pattern)
- `stores/utxo/queue/mined.go` — direct INSERT block_ids + UPDATE tx_state (remove queue+wait)
- `stores/utxo/queue/get.go` — add JOINs for spends + tx_state
- `stores/utxo/queue/delete.go` — add DELETE FROM spends, DELETE FROM tx_state
- `stores/utxo/queue/iterators.go` — JOIN tx_state for unmined_since/conflicting
- `stores/utxo/queue/store.go` — remove batcher, listener, materializer goroutines; simplify Start/Stop
- `stores/utxo/queue/store_test.go` — remove StartTestMaterializer, update test setup
- `stores/utxo/throughput_test.go` — benchmark comparisons

**Files to delete:**

- `stores/utxo/queue/batcher.go` — no more COPY batching
- `stores/utxo/queue/listener.go` — no more pg_notify
- `stores/utxo/queue/buffer.go` — no more in-memory buffers
- `stores/utxo/queue/metrics.go` — replace with direct operation metrics

## Fallback: Materializer for Creates

If benchmarks show COPY batching outperforms direct INSERTs for creates, restore the v3 materializer for creates only:

- Add back 3 UNLOGGED queue tables: create_queue, input_queue, output_queue
- Add back batch_notifications table
- Add back a simplified process_batch that only handles creates (no spends, no mined, no conflict detection)
- Add back pg_cron schedule for materialize_loop
- Add back batcher.go (COPY to queue tables) and listener.go (pg_notify)
- pg_notify payload: `{"max_batch": N}` (no conflicts — spends are direct)

Spends, SetMined, SetLocked, SetConflicting, and Get remain direct regardless.

**When to use the fallback:**

- If direct Create TPS < v3 Create TPS at 40+ pods
- If mega-transactions (>100 inputs/outputs) show significant overhead from unnest vs COPY
- The threshold is measurable: run the throughput benchmark with both approaches and compare
