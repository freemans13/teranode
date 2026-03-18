# Design: createBatcher for SQL UTXO Store

## Problem

The SQL UTXO store executes each `Create()` call as a separate CTE+UNNEST statement with its own network round-trip to PostgreSQL. During block validation, 800 goroutines call `Create()` concurrently, each contending for 50 DB connections. This means:

- **100 network round-trips per 100 creates** (1 per tx)
- **Connection pool contention**: 800 goroutines fighting for 50 connections
- **17% CPU on network syscalls**, 12% on per-call array encoding, 17% on goroutine scheduling

The aerospike UTXO store already batches creates via `storeBatcher` (go-batcher), collecting individual `Create()` calls and executing them as a single `BatchOperate`. The SQL store is the only backend missing this optimization.

## Solution: SendBatch of CTEs

Add a `createBatcher` to the SQL store that collects N `Create()` calls via go-batcher, then sends all N CTE statements in a single `pgx.SendBatch` flush — one network round-trip instead of N.

### Architecture

```text
Create() goroutine 1 ──┐
Create() goroutine 2 ──┤
Create() goroutine 3 ──┼──► createBatcher ──► sendCreateBatch()
...                     │     (go-batcher)      │
Create() goroutine N ──┘     size=100           │
                              timeout=100ms      ▼
                                           pgx.SendBatch
                                           ┌─────────────┐
                                           │ CTE 1       │
                                           │ CTE 2       │ single
                                           │ CTE 3       │ network
                                           │ ...         │ flush
                                           │ CTE N       │
                                           └─────────────┘
                                                 │
                                                 ▼
                                           Read N results
                                           Route per-item
                                           errors to done
                                           channels
```

## Detailed Design

### Batch item types

```go
// batchCreateItem represents a single Create() request queued into the batcher.
type batchCreateItem struct {
    tx          *bt.Tx
    blockHeight uint32
    options     *utxo.CreateOptions
    done        chan batchCreateResult
}

// batchCreateResult holds the result routed back to the caller.
type batchCreateResult struct {
    Data *meta.Data
    Err  error
}
```

### Store changes

Add `createBatcher` field to Store struct:

```go
type Store struct {
    // ... existing fields ...
    createBatcher *batcher.Batcher[batchCreateItem]
}
```

Initialize in `New()` using existing settings (postgres-only):

```go
if storeURL.Scheme == "postgres" {
    storeBatchSize := tSettings.UtxoStore.StoreBatcherSize           // 100
    storeBatchDuration := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond  // 100ms
    s.createBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true)
    if tSettings.BatcherDrainMode {
        s.createBatcher.SetDrainMode(true)
    }
}
```

### Create() flow change

```go
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
    // ... existing option parsing, tracing ...

    // Postgres with batcher: enqueue and wait
    if s.createBatcher != nil {
        return s.createBatched(ctx, tx, blockHeight, options)
    }

    // Existing retry loop for SQLite / non-batched path
    // ... unchanged ...
}
```

### createBatched() — enqueue into batcher

```go
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
    done := make(chan batchCreateResult, 1)
    s.createBatcher.Put(&batchCreateItem{
        tx:          tx,
        blockHeight: blockHeight,
        options:     options,
        done:        done,
    })

    select {
    case result := <-done:
        return result.Data, result.Err
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

### sendCreateBatch() — batch callback

This is the core: receives N items, sends N CTEs in one SendBatch flush.

```go
func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
    // 1. Pre-compute per-item data (CPU-only, no DB)
    type preparedItem struct {
        txHash       *chainhash.Hash
        txMeta       *meta.Data
        isCoinbase   bool
        unminedSince interface{}
        inpArrs      inputArrayParams
        outArrs      outputArrayParams
        blkArrs      blockIDArrayParams
        err          error  // prep error
    }

    items := make([]preparedItem, len(batch))
    for i, item := range batch {
        // Compute txMeta, txHash, isCoinbase, unminedSince, arrays
        // Same logic as current createCTE preamble
        // If prep fails, store error in items[i].err
    }

    // 2. Send items with prep errors immediately
    // (don't include them in the DB batch)

    // 3. Get one pgx connection, queue all valid CTEs
    sqlConn, err := s.db.Conn(s.ctx)
    // ... handle error ...
    defer sqlConn.Close()

    sqlConn.Raw(func(driverConn interface{}) error {
        pgxConn := driverConn.(*stdlib.Conn).Conn()
        pgxBatch := &pgx.Batch{}

        for i, item := range items {
            if item.err != nil {
                continue  // skip prep failures
            }
            pgxBatch.Queue(createCTESQL,
                // $1-$10: transaction scalars
                item.txHash[:], batch[i].tx.Version, batch[i].tx.LockTime,
                item.txMeta.Fee, item.txMeta.SizeInBytes,
                item.isCoinbase, batch[i].options.Frozen,
                batch[i].options.Conflicting, batch[i].options.Locked,
                item.unminedSince,
                // $11-$17: input arrays
                item.inpArrs.idx, item.inpArrs.prevHash, item.inpArrs.prevIdx,
                item.inpArrs.prevSatoshis, item.inpArrs.prevScript,
                item.inpArrs.unlockScript, item.inpArrs.seqNum,
                // $18-$22: output arrays
                item.outArrs.idx, item.outArrs.lockingScript,
                item.outArrs.satoshis, item.outArrs.coinbaseSpendingHeight,
                item.outArrs.utxoHash,
                // $23-$25: block_id arrays
                item.blkArrs.blockID, item.blkArrs.blockHeight,
                item.blkArrs.subtreeIdx,
            )
        }

        br := pgxConn.SendBatch(s.ctx, pgxBatch)

        // 4. Read results and route to callers
        batchIdx := 0
        for i, item := range items {
            if item.err != nil {
                continue  // already handled
            }
            _, execErr := br.Exec()
            if execErr != nil {
                if pgErr := asPgUniqueViolation(execErr); pgErr != nil {
                    batch[i].done <- batchCreateResult{
                        Err: errors.NewTxExistsError("..."),
                    }
                } else {
                    batch[i].done <- batchCreateResult{
                        Err: errors.NewStorageError("..."),
                    }
                }
            } else {
                batch[i].done <- batchCreateResult{Data: item.txMeta}
            }
            batchIdx++
        }
        br.Close()
        return nil
    })

    // 5. Handle conflicting children (rare path, separate round-trips)
    for i, item := range items {
        if item.err != nil || !item.txMeta.Conflicting {
            continue
        }
        // insertConflictingChildrenPgx for this item
    }
}
```

## Design Decisions

### background=true for createBatcher

Unlike `spendBatcher` (background=false to prevent deadlocks from overlapping row locks), `createBatcher` uses background=true:

- Each CTE inserts a **unique transaction hash** — no row overlap between items
- Concurrent batches cannot deadlock because they touch disjoint rows
- Allows multiple batches to fly in parallel for higher throughput

### Reuse existing settings

`StoreBatcherSize` (100) and `StoreBatcherDurationMillis` (100ms) already exist in `UtxoStoreSettings` for the aerospike `storeBatcher`. Reusing them avoids new config keys and maintains parity.

### Lock retry stays in Create()

The batcher handles the happy path. If a batch item gets a lock error (SQLite-only, shouldn't happen with CTE on postgres), the error propagates back through the done channel. The existing retry loop in `Create()` is removed for the batched path since:

- CTE is auto-atomic (no explicit BEGIN/COMMIT)
- Unique violation is a definitive error (no retry needed)
- Lock errors don't apply to single-statement CTEs on PostgreSQL

### SQLite path unchanged

The batcher is only created when `engine == "postgres"`. SQLite continues using the existing `createWithRetry` path with per-row or batched INSERTs inside a `sql.Tx`.

### Error isolation

A unique violation on CTE #3 in a batch does NOT affect CTEs #1, #2, #4...N. Each CTE in a `pgx.Batch` is an independent statement — PostgreSQL processes them sequentially within the batch, and errors are per-statement.

## Files Changed

| File | Change |
|------|--------|
| `stores/utxo/sql/sql.go` | Add `createBatcher` field, `batchCreateItem`/`batchCreateResult` types, `createBatched()`, `sendCreateBatch()` |
| `stores/utxo/sql/sql.go` | Modify `Create()` to route to batcher when available |
| `stores/utxo/sql/sql.go` | Modify `New()` to initialize `createBatcher` for postgres |

No new files. No settings changes. No schema changes.

## Expected Impact

| Metric | Before (1 CTE/tx) | After (100 CTEs/flush) |
|--------|--------------------|-----------------------|
| Network round-trips per 100 creates | 100 | 1 |
| DB connections needed during create | ~50 (pool saturated) | ~1-2 per batch callback |
| Goroutine scheduling overhead | 800 goroutines contending | 800 goroutines waiting on channels (cheap) |
| Batch time (16 subtrees) | ~12s | Target: ~3-6s |

## Verification

- `go build ./...` compiles
- `go test -race ./stores/utxo/sql/...` passes (SQLite path unchanged)
- Deploy to teratestnet, observe batch times in logs
- Capture 30s CPU profile, compare against current CTE baseline
- Confirm per-tx error handling works (duplicate tx returns TxExistsError)
