# SQL UTXO Store Performance Backports — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the Create and Unlock batchers for the SQLite engine of the vanilla `stores/utxo/sql` store by adding a portable bulk-INSERT path, so both engines benefit from the same batching wins the Postgres engine already gets.

**Architecture:** The existing `createBatcher` / `unlockBatcher` constructors in `sql.go` are gated with `storeURL.Scheme == "postgres"`. The `sendCreateBatch` callback is pgx-specific (uses `driverConn.(*stdlib.Conn).Conn().SendBatch`). We rename that to `sendCreateBatchPostgres`, introduce a portable `sendCreateBatchSQL` that runs one `BEGIN…COMMIT` per batch with one bulk `INSERT` per table, and make `sendCreateBatch` a one-line engine dispatcher. The unlock batcher needs no callback changes — `setUnlockedBulk` already has a portable SQLite branch — so it's just a guard flip plus tests.

**Tech Stack:** Go `database/sql`, `modernc.org/sqlite` (SQLite 3.47+, supports `ON CONFLICT DO NOTHING RETURNING`), existing `go-batcher` package.

**Spec:** `docs/superpowers/specs/2026-04-20-sql-store-perf-backports-design.md`

**Starting point:** Branch `stu/utxo-sql-perf-backports` at `upstream/main`, spec committed.

**Out of scope (see spec):** txCache, spend-batcher `background=true`, pool sizing, schema changes.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `stores/utxo/sql/sql.go` | Modify | Batcher construction guards; rename + dispatch; add `sendCreateBatchSQL` |
| `stores/utxo/sql/unlock_batcher_sqlite_test.go` | Create | SQLite-side equivalent of `unlock_batcher_postgres_test.go` |
| `stores/utxo/sql/create_batcher_sqlite_test.go` | Create | Bulk create batcher tests (basic, duplicate, multi-tx) |

---

## Task 1 — Enable Unlock Batcher on SQLite

**Files:**

- Create: `stores/utxo/sql/unlock_batcher_sqlite_test.go`
- Modify: `stores/utxo/sql/sql.go:230-238` (remove `Scheme == "postgres"` guard)

- [ ] **Step 1.1: Write failing test for unlock batcher enabled on SQLite**

Create `stores/utxo/sql/unlock_batcher_sqlite_test.go`:

```go
package sql

import (
    "context"
    "testing"

    "github.com/bsv-blockchain/go-bt/v2/chainhash"
    "github.com/bsv-blockchain/teranode/stores/utxo"
    "github.com/bsv-blockchain/teranode/stores/utxo/tests"
    "github.com/stretchr/testify/require"
)

// TestUnlockBatcher_SQLite_Wired verifies the unlock batcher is constructed
// for the SQLite engine when LockedBatcherSize > 1.
func TestUnlockBatcher_SQLite_Wired(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    store, _ := setup(ctx, t)

    require.NotNil(t, store.unlockBatcher,
        "unlockBatcher must be initialised for the sqlite engine when LockedBatcherSize > 1")
}

// TestUnlockBatcher_SQLite_DAH verifies the batched unlock path correctly
// recalculates delete_at_height for a fully-spent, mined, on-longest-chain
// transaction on SQLite.
func TestUnlockBatcher_SQLite_DAH(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    store, _ := setup(ctx, t)
    require.NoError(t, store.SetBlockHeight(1000))

    _, err := store.Create(ctx, tests.ParentTx, 999)
    require.NoError(t, err)

    _, err = store.Create(ctx, tests.Tx, 1000,
        utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
            BlockID: 100, BlockHeight: 1000, SubtreeIdx: 0, OnLongestChain: true,
        }),
    )
    require.NoError(t, err)

    txHash := *tests.Tx.TxIDChainHash()

    // Spend all outputs (each Spend already recomputes DAH via PR #729).
    spendAllOutputsHelper(t, ctx, store, tests.Tx, 1001)

    // Lock, then unlock via the batcher single-hash path. SetLocked(true)
    // clears DAH; SetLocked(false) going through the batcher should
    // recalculate it back.
    require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{txHash}, true))
    require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{txHash}, false))

    var dah *int64
    err = store.db.QueryRowContext(ctx,
        "SELECT delete_at_height FROM transactions WHERE hash = $1",
        txHash[:]).Scan(&dah)
    require.NoError(t, err)
    require.NotNil(t, dah, "DAH must be set after batched unlock of fully-spent mined tx")
    retention := store.settings.GetUtxoStoreBlockHeightRetention()
    require.Equal(t, int64(store.blockHeight.Load()+1+retention), *dah)
}
```

Also add a shared helper used by both create and unlock tests — append to the existing `stores/utxo/sql/spend_dah_test.go`:

```go
// spendAllOutputsHelper spends every output of parentTx via the normal Spend path.
// Exposed for cross-test reuse; the non-exported spendAllOutputs has the same body
// but a different signature.
func spendAllOutputsHelper(t *testing.T, ctx context.Context, store *Store, parentTx *bt.Tx, blockHeight uint32) {
    spendAllOutputs(t, ctx, store, parentTx, blockHeight)
}
```

- [ ] **Step 1.2: Run the test and verify it fails**

```bash
go test -v -race -tags "testtxmetacache" -run TestUnlockBatcher_SQLite_Wired ./stores/utxo/sql/
```

Expected: FAIL — `unlockBatcher must be initialised for the sqlite engine when LockedBatcherSize > 1` (current code constructs the batcher only when `storeURL.Scheme == "postgres"`).

- [ ] **Step 1.3: Remove the postgres-only guard on the unlock batcher**

In `stores/utxo/sql/sql.go`, find the block that starts at ~line 230:

```go
    // Initialize unlock batcher for Postgres — batches single-hash SetLocked(false) calls.
    if storeURL.Scheme == "postgres" && tSettings.UtxoStore.LockedBatcherSize > 1 {
        unlockBatchSize := tSettings.UtxoStore.LockedBatcherSize
        unlockBatchDuration := time.Duration(tSettings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
        s.unlockBatcher = batcher.New(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true)
        if tSettings.BatcherDrainMode {
            s.unlockBatcher.SetDrainMode(true)
        }
    }
```

Replace with:

```go
    // Initialize unlock batcher — batches single-hash SetLocked(false) calls for both
    // engines. Postgres wins through `ANY($1)` + CTE; SQLite wins through fewer
    // BEGIN…COMMIT cycles on the single-writer file. `setUnlockedBulk` handles both.
    if tSettings.UtxoStore.LockedBatcherSize > 1 {
        unlockBatchSize := tSettings.UtxoStore.LockedBatcherSize
        unlockBatchDuration := time.Duration(tSettings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
        s.unlockBatcher = batcher.New(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true)
        if tSettings.BatcherDrainMode {
            s.unlockBatcher.SetDrainMode(true)
        }
    }
```

- [ ] **Step 1.4: Run the tests and verify they pass**

```bash
go test -v -race -tags "testtxmetacache" -run "TestUnlockBatcher_SQLite" ./stores/utxo/sql/
```

Expected: PASS for both `_Wired` and `_DAH`.

- [ ] **Step 1.5: Run full SQL short suite to confirm no regressions**

```bash
go test -race -tags "testtxmetacache" -short ./stores/utxo/sql/...
```

Expected: all previously-passing tests still pass.

- [ ] **Step 1.6: Commit**

```bash
git add stores/utxo/sql/sql.go stores/utxo/sql/unlock_batcher_sqlite_test.go stores/utxo/sql/spend_dah_test.go
git commit -m "$(cat <<'EOF'
feat(sql): enable unlock batcher on SQLite engine

The unlock batcher callback already delegates to setUnlockedBulk which
has a portable SQLite branch — the only thing keeping SQLite out was the
engine guard in the constructor. On disk-backed SQLite this collapses N
concurrent SetLocked(false) BEGIN…COMMIT cycles into one per batch,
reducing fsyncs and write-lock acquisitions.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2 — Rename existing pgx create-batch callback

No behaviour change. Pure rename so the dispatcher added in Task 3 has a clear target.

**Files:**

- Modify: `stores/utxo/sql/sql.go` — rename `sendCreateBatch` → `sendCreateBatchPostgres` at its definition and at the single call site in `batcher.New(..., s.sendCreateBatch, ...)`.

- [ ] **Step 2.1: Rename the function**

Find `func (s *Store) sendCreateBatch(batch []*batchCreateItem)` (around line 891) and change the receiver to:

```go
func (s *Store) sendCreateBatchPostgres(batch []*batchCreateItem) {
```

Find the single call site (the `batcher.New` for `createBatcher`, around line 224):

```go
        s.createBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendCreateBatchPostgres, true)
```

- [ ] **Step 2.2: Run the SQL suite (Postgres path)**

```bash
# With Docker / OrbStack running. Spin up postgres if not running:
docker ps --format '{{.Names}}' | grep -q pg-test || docker run -d --rm --name pg-test \
  -e POSTGRES_USER=teranode -e POSTGRES_PASSWORD=teranode -e POSTGRES_DB=teranode_test \
  -p 5432:5432 postgres:16

go test -race -tags "testtxmetacache" -run "Postgres" ./stores/utxo/sql/
```

Expected: all postgres-specific tests pass.

- [ ] **Step 2.3: Commit**

```bash
git add stores/utxo/sql/sql.go
git commit -m "$(cat <<'EOF'
refactor(sql): rename sendCreateBatch to sendCreateBatchPostgres

Preparatory rename — the existing callback is pgx-specific
(driverConn.(*stdlib.Conn).SendBatch). Naming it for what it is makes
room for a portable sendCreateBatchSQL alongside it in the next commit.

No behaviour change.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3 — Add the engine dispatcher (`sendCreateBatch`) and failing SQLite tests

**Files:**

- Modify: `stores/utxo/sql/sql.go` — add new dispatcher function; temporarily keep the `scheme == "postgres"` guard on batcher construction so Postgres behaviour is unchanged until Task 5.
- Create: `stores/utxo/sql/create_batcher_sqlite_test.go`

- [ ] **Step 3.1: Add the dispatcher**

Add anywhere in `stores/utxo/sql/sql.go` (suggested: immediately above `sendCreateBatchPostgres`):

```go
// sendCreateBatch dispatches a Create batch to the engine-appropriate
// implementation. Postgres uses pgx pipelined SendBatch; everything else
// uses the portable database/sql multi-row INSERT path.
func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
    if s.engine == "postgres" {
        s.sendCreateBatchPostgres(batch)
        return
    }
    s.sendCreateBatchSQL(batch)
}
```

Change the `batcher.New` call for `createBatcher` back to using `s.sendCreateBatch` (the dispatcher):

```go
        s.createBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true)
```

Add a stub for `sendCreateBatchSQL` so the package compiles — it panics for now, we're going to TDD the real implementation in Task 4:

```go
// sendCreateBatchSQL is the portable database/sql implementation. Stub — see Task 4.
func (s *Store) sendCreateBatchSQL(batch []*batchCreateItem) {
    panic("sendCreateBatchSQL: not yet implemented — see Task 4 of the perf backports plan")
}
```

- [ ] **Step 3.2: Write failing SQLite create-batcher tests**

Create `stores/utxo/sql/create_batcher_sqlite_test.go`:

```go
package sql

import (
    "context"
    "net/url"
    "testing"
    "time"

    "github.com/bsv-blockchain/go-bt/v2"
    "github.com/bsv-blockchain/teranode/errors"
    "github.com/bsv-blockchain/teranode/stores/utxo/tests"
    "github.com/bsv-blockchain/teranode/ulogger"
    "github.com/bsv-blockchain/teranode/util/test"
    "github.com/stretchr/testify/require"
)

// setupSQLiteBatched builds a sqlitememory store with the create batcher
// enabled (StoreBatcherSize > 1) so the SQLite create-batch path runs.
func setupSQLiteBatched(t *testing.T) (*Store, context.Context) {
    t.Helper()
    ctx := context.Background()

    tSettings := test.CreateBaseTestSettings(t)
    tSettings.UtxoStore.DBTimeout = 30 * time.Second
    tSettings.BatcherDrainMode = true
    tSettings.UtxoStore.StoreBatcherSize = 8
    tSettings.UtxoStore.StoreBatcherDurationMillis = 5

    storeURL, err := url.Parse("sqlitememory:///test")
    require.NoError(t, err)

    store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
    require.NoError(t, err)

    require.NotNil(t, store.createBatcher,
        "createBatcher must be initialised for sqlite when StoreBatcherSize > 1")

    return store, ctx
}

// TestCreateBatcher_SQLite_Basic creates three distinct txs through the
// batcher and verifies each is retrievable afterwards.
func TestCreateBatcher_SQLite_Basic(t *testing.T) {
    store, ctx := setupSQLiteBatched(t)

    txs := []*bt.Tx{tests.ParentTx, tests.Tx}

    for _, tx := range txs {
        _, err := store.Create(ctx, tx, 1000)
        require.NoError(t, err)
    }

    for _, tx := range txs {
        meta, err := store.Get(ctx, tx.TxIDChainHash())
        require.NoError(t, err)
        require.NotNil(t, meta)
    }
}

// TestCreateBatcher_SQLite_Duplicate verifies the batched path returns
// ErrTxExists when the same tx is created twice.
func TestCreateBatcher_SQLite_Duplicate(t *testing.T) {
    store, ctx := setupSQLiteBatched(t)

    _, err := store.Create(ctx, tests.Tx, 1000)
    require.NoError(t, err)

    _, err = store.Create(ctx, tests.Tx, 1000)
    require.Error(t, err)
    require.True(t, errors.Is(err, errors.ErrTxExists),
        "second Create of same tx should return ErrTxExists, got: %v", err)
}

// TestCreateBatcher_SQLite_Mined verifies that txs created with
// MinedBlockInfo through the batcher have their block_ids populated.
func TestCreateBatcher_SQLite_Mined(t *testing.T) {
    store, ctx := setupSQLiteBatched(t)

    _, err := store.Create(ctx, tests.Tx, 1000,
        // Use the existing helper from stores/utxo to build the option.
        withMinedBlockInfoForTest(100, 1000, 0, true),
    )
    require.NoError(t, err)

    meta, err := store.Get(ctx, tests.Tx.TxIDChainHash())
    require.NoError(t, err)
    require.Equal(t, []uint32{100}, meta.BlockIDs)
}
```

And above `setupSQLiteBatched`, add the tiny helper:

```go
// withMinedBlockInfoForTest wraps utxo.WithMinedBlockInfo with a convenient signature.
func withMinedBlockInfoForTest(blockID, blockHeight, subtreeIdx uint32, onLongest bool) utxo.CreateOption {
    return utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
        BlockID: blockID, BlockHeight: blockHeight, SubtreeIdx: int(subtreeIdx), OnLongestChain: onLongest,
    })
}
```

Fix the test file's imports to include `"github.com/bsv-blockchain/teranode/stores/utxo"`.

- [ ] **Step 3.3: Temporarily enable the create batcher for SQLite so the test can panic meaningfully**

In `stores/utxo/sql/sql.go`, find the create-batcher constructor (line ~221):

```go
    if storeURL.Scheme == "postgres" && tSettings.UtxoStore.StoreBatcherSize > 1 {
```

Change to:

```go
    if tSettings.UtxoStore.StoreBatcherSize > 1 {
```

- [ ] **Step 3.4: Run the tests and verify they fail with the stub panic**

```bash
go test -v -race -tags "testtxmetacache" -run "TestCreateBatcher_SQLite" ./stores/utxo/sql/
```

Expected: panics — `sendCreateBatchSQL: not yet implemented`. That's the signal the batcher is wired and the stub is being hit. We'll replace the stub in Task 4.

- [ ] **Step 3.5: Commit**

```bash
git add stores/utxo/sql/sql.go stores/utxo/sql/create_batcher_sqlite_test.go
git commit -m "$(cat <<'EOF'
wip(sql): dispatcher + failing tests for SQLite create batcher

Adds the engine dispatch (sendCreateBatch → Postgres|SQL) and the three
SQLite-side tests that will drive the sendCreateBatchSQL implementation
in the next commit. Tests currently panic on the stub — that's the RED
step of the TDD cycle; GREEN follows.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4 — Implement `sendCreateBatchSQL`

**Files:**

- Modify: `stores/utxo/sql/sql.go` — replace the `sendCreateBatchSQL` stub with the real bulk-INSERT implementation.

- [ ] **Step 4.1: Replace the stub**

Find the stub:

```go
func (s *Store) sendCreateBatchSQL(batch []*batchCreateItem) {
    panic("sendCreateBatchSQL: not yet implemented — see Task 4 of the perf backports plan")
}
```

Replace with the full implementation:

```go
// sendCreateBatchSQL runs a bulk Create batch against any database/sql backend
// (primarily SQLite). It issues one BEGIN…COMMIT plus up to four INSERTs —
// one for `transactions` using ON CONFLICT DO NOTHING RETURNING id,hash, then
// one each for inputs/outputs/block_ids populated from the returned ids.
// Items whose hash doesn't come back from the RETURNING clause are duplicates
// and receive ErrTxExists.
//
// Retries the whole batch on SQLITE_BUSY up to 3 times with exponential backoff,
// mirroring createWithRetry's behaviour.
func (s *Store) sendCreateBatchSQL(batch []*batchCreateItem) {
    const maxRetries = 3
    for attempt := 0; attempt <= maxRetries; attempt++ {
        retry, retryable := s.trySendCreateBatchSQL(batch)
        if !retry {
            return
        }
        if !retryable || attempt == maxRetries {
            for _, item := range batch {
                if item.done != nil {
                    item.done <- batchCreateResult{Err: errors.NewStorageError("[Create] SQLITE_BUSY persisted after %d retries", maxRetries)}
                    item.done = nil
                }
            }
            return
        }
        time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
    }
}

// trySendCreateBatchSQL attempts the batch once. Returns (false, _) on any
// terminal outcome (success, permanent error — callers have been notified via
// done channel, or per-item error recorded). Returns (true, true) when the
// whole batch should be retried because of a busy/locked condition.
func (s *Store) trySendCreateBatchSQL(batch []*batchCreateItem) (retry, retryable bool) {
    // Phase 1: pre-compute per-item data. Items whose pre-compute fails are
    // notified immediately and excluded from the batch.
    type prepared struct {
        txHash       *chainhash.Hash
        txMeta       *meta.Data
        isCoinbase   bool
        unminedSince interface{}
        tx           *bt.Tx
        options      *utxo.CreateOptions
        blockHeight  uint32
        done         chan batchCreateResult
    }
    preps := make([]*prepared, 0, len(batch))
    for _, item := range batch {
        if item.done == nil {
            // Already notified in a prior retry attempt.
            continue
        }
        txMeta, err := util.TxMetaDataFromTx(item.tx)
        if err != nil {
            item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
            item.done = nil
            continue
        }
        if item.options.Conflicting {
            txMeta.Conflicting = true
        }
        if item.options.Locked {
            txMeta.Locked = true
        }
        var unminedSince interface{}
        if len(item.options.MinedBlockInfos) == 0 {
            unminedSince = item.blockHeight
        }
        var txHash *chainhash.Hash
        if item.options.TxID != nil {
            txHash = item.options.TxID
        } else {
            txHash = item.tx.TxIDChainHash()
        }
        isCoinbase := item.tx.IsCoinbase()
        if item.options.IsCoinbase != nil {
            isCoinbase = *item.options.IsCoinbase
        }
        preps = append(preps, &prepared{
            txHash: txHash, txMeta: txMeta, isCoinbase: isCoinbase,
            unminedSince: unminedSince, tx: item.tx, options: item.options,
            blockHeight: item.blockHeight, done: item.done,
        })
    }
    if len(preps) == 0 {
        return false, false
    }

    // Phase 2: one BEGIN…COMMIT for the whole batch.
    txn, err := s.db.Begin()
    if err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, p := range preps {
            p.done <- batchCreateResult{Err: errors.NewStorageError("failed to begin tx", err)}
        }
        return false, false
    }
    defer func() {
        _ = txn.Rollback()
    }()

    // Phase 3: one multi-row INSERT INTO transactions with RETURNING id, hash.
    const txColsPerRow = 10
    txSQL := buildMultiValueInsert(
        `INSERT INTO transactions (hash,version,lock_time,fee,size_in_bytes,coinbase,frozen,conflicting,locked,unmined_since) VALUES `,
        txColsPerRow, len(preps), 1,
    ) + ` ON CONFLICT (hash) DO NOTHING RETURNING id, hash`

    txArgs := make([]interface{}, 0, len(preps)*txColsPerRow)
    for _, p := range preps {
        txArgs = append(txArgs,
            p.txHash[:], p.tx.Version, p.tx.LockTime,
            p.txMeta.Fee, p.txMeta.SizeInBytes,
            p.isCoinbase, p.options.Frozen, p.options.Conflicting,
            p.options.Locked, p.unminedSince,
        )
    }

    rows, err := txn.Query(txSQL, txArgs...)
    if err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, p := range preps {
            p.done <- batchCreateResult{Err: errors.NewStorageError("bulk INSERT transactions failed", err)}
        }
        return false, false
    }

    // Build hash → id map from RETURNING rows (only newly-inserted txs appear).
    hashToID := make(map[chainhash.Hash]int, len(preps))
    for rows.Next() {
        var id int
        var hashBytes []byte
        if err := rows.Scan(&id, &hashBytes); err != nil {
            rows.Close()
            for _, p := range preps {
                p.done <- batchCreateResult{Err: errors.NewStorageError("scan RETURNING failed", err)}
            }
            return false, false
        }
        var h chainhash.Hash
        copy(h[:], hashBytes)
        hashToID[h] = id
    }
    if err := rows.Err(); err != nil {
        rows.Close()
        for _, p := range preps {
            p.done <- batchCreateResult{Err: errors.NewStorageError("iterating RETURNING failed", err)}
        }
        return false, false
    }
    rows.Close()

    // Phase 4: for each successful tx, collect inputs/outputs/block_ids.
    type childRows struct {
        transactionID int
        prepared      *prepared
    }
    successes := make([]childRows, 0, len(preps))
    for _, p := range preps {
        id, ok := hashToID[*p.txHash]
        if !ok {
            // Not in RETURNING → already existed.
            p.done <- batchCreateResult{Err: errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v):", p.isCoinbase)}
            continue
        }
        successes = append(successes, childRows{transactionID: id, prepared: p})
    }
    if len(successes) == 0 {
        // Nothing to insert into children; commit is a no-op but safe.
        if err := txn.Commit(); err != nil {
            // Already notified everyone via ErrTxExists — just log.
            s.logger.Warnf("[Create] empty-batch commit failed: %v", err)
        }
        return false, false
    }

    // Phase 4a: inputs (cols: transaction_id, idx, previous_transaction_hash,
    // previous_tx_idx, previous_tx_satoshis, previous_tx_script,
    // unlocking_script, sequence_number)
    if err := s.bulkInsertInputs(txn, successes); err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, c := range successes {
            c.prepared.done <- batchCreateResult{Err: err}
        }
        return false, false
    }

    // Phase 4b: outputs (cols: transaction_id, idx, locking_script, satoshis,
    // coinbase_spending_height, utxo_hash, spending_data)
    if err := s.bulkInsertOutputs(txn, successes); err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, c := range successes {
            c.prepared.done <- batchCreateResult{Err: err}
        }
        return false, false
    }

    // Phase 4c: block_ids (only for items with MinedBlockInfos)
    if err := s.bulkInsertBlockIDs(txn, successes); err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, c := range successes {
            c.prepared.done <- batchCreateResult{Err: err}
        }
        return false, false
    }

    // Phase 5: commit.
    if err := txn.Commit(); err != nil {
        if isLockError(err) {
            return true, true
        }
        for _, c := range successes {
            c.prepared.done <- batchCreateResult{Err: errors.NewStorageError("batch commit failed", err)}
        }
        return false, false
    }

    // Notify successes.
    for _, c := range successes {
        c.prepared.done <- batchCreateResult{Data: c.prepared.txMeta}
    }
    return false, false
}

// bulkInsertInputs writes all inputs for every tx in `successes` via chunked
// multi-row INSERT. Preserves column ordering from createInputsBatched so
// the on-disk row shape matches the unbatched path exactly.
func (s *Store) bulkInsertInputs(txn *sql.Tx, successes []struct {
    transactionID int
    prepared      *prepared
}) error {
    const colsPerRow = 8
    const maxRowsPerChunk = maxPostgresParams / colsPerRow
    baseSQL := `INSERT INTO inputs (transaction_id,idx,previous_transaction_hash,previous_tx_idx,previous_tx_satoshis,previous_tx_script,unlocking_script,sequence_number) VALUES `

    // Flatten all inputs across all txs.
    type row struct{ args []interface{} }
    rows := make([]row, 0, len(successes)*2)
    for _, c := range successes {
        for i, input := range c.prepared.tx.Inputs {
            rows = append(rows, row{args: []interface{}{
                c.transactionID, i,
                input.PreviousTxIDChainHash()[:], input.PreviousTxOutIndex,
                input.PreviousTxSatoshis, input.PreviousTxScript,
                input.UnlockingScript, input.SequenceNumber,
            }})
        }
    }
    if len(rows) == 0 {
        return nil
    }

    for start := 0; start < len(rows); start += maxRowsPerChunk {
        end := start + maxRowsPerChunk
        if end > len(rows) {
            end = len(rows)
        }
        chunk := rows[start:end]
        q := buildMultiValueInsert(baseSQL, colsPerRow, len(chunk), 1)
        args := make([]interface{}, 0, len(chunk)*colsPerRow)
        for _, r := range chunk {
            args = append(args, r.args...)
        }
        if _, err := txn.Exec(q, args...); err != nil {
            return classifyInsertError(err, false, "input")
        }
    }
    return nil
}

// bulkInsertOutputs writes all outputs for every tx in `successes` via chunked
// multi-row INSERT.
func (s *Store) bulkInsertOutputs(txn *sql.Tx, successes []struct {
    transactionID int
    prepared      *prepared
}) error {
    const colsPerRow = 7
    const maxRowsPerChunk = maxPostgresParams / colsPerRow
    baseSQL := `INSERT INTO outputs (transaction_id,idx,locking_script,satoshis,coinbase_spending_height,utxo_hash,spending_data) VALUES `

    type row struct{ args []interface{} }
    rows := make([]row, 0, len(successes)*2)
    for _, c := range successes {
        var coinbaseSpendingHeight uint32
        if c.prepared.isCoinbase {
            coinbaseSpendingHeight = c.prepared.blockHeight + uint32(s.settings.ChainCfgParams.CoinbaseMaturity)
        }
        for i, output := range c.prepared.tx.Outputs {
            if output == nil {
                continue
            }
            iUint32, err := safeconversion.IntToUint32(i)
            if err != nil {
                return err
            }
            utxoHash, err := util.UTXOHashFromOutput(c.prepared.txHash, output, iUint32)
            if err != nil {
                return err
            }
            rows = append(rows, row{args: []interface{}{
                c.transactionID, i,
                output.LockingScript, output.Satoshis,
                coinbaseSpendingHeight, utxoHash[:], nil,
            }})
        }
    }
    if len(rows) == 0 {
        return nil
    }

    for start := 0; start < len(rows); start += maxRowsPerChunk {
        end := start + maxRowsPerChunk
        if end > len(rows) {
            end = len(rows)
        }
        chunk := rows[start:end]
        q := buildMultiValueInsert(baseSQL, colsPerRow, len(chunk), 1)
        args := make([]interface{}, 0, len(chunk)*colsPerRow)
        for _, r := range chunk {
            args = append(args, r.args...)
        }
        if _, err := txn.Exec(q, args...); err != nil {
            return classifyInsertError(err, false, "output")
        }
    }
    return nil
}

// bulkInsertBlockIDs writes block_ids entries for any tx whose options
// contained MinedBlockInfos.
func (s *Store) bulkInsertBlockIDs(txn *sql.Tx, successes []struct {
    transactionID int
    prepared      *prepared
}) error {
    const colsPerRow = 4
    const maxRowsPerChunk = maxPostgresParams / colsPerRow
    baseSQL := `INSERT INTO block_ids (transaction_id,block_id,block_height,subtree_idx) VALUES `

    type row struct{ args []interface{} }
    rows := make([]row, 0)
    for _, c := range successes {
        for _, bi := range c.prepared.options.MinedBlockInfos {
            rows = append(rows, row{args: []interface{}{c.transactionID, bi.BlockID, bi.BlockHeight, bi.SubtreeIdx}})
        }
    }
    if len(rows) == 0 {
        return nil
    }

    for start := 0; start < len(rows); start += maxRowsPerChunk {
        end := start + maxRowsPerChunk
        if end > len(rows) {
            end = len(rows)
        }
        chunk := rows[start:end]
        q := buildMultiValueInsert(baseSQL, colsPerRow, len(chunk), 1)
        args := make([]interface{}, 0, len(chunk)*colsPerRow)
        for _, r := range chunk {
            args = append(args, r.args...)
        }
        if _, err := txn.Exec(q, args...); err != nil {
            return classifyInsertError(err, false, "block_ids")
        }
    }
    return nil
}
```

Note on the shared `prepared` struct: the three `bulkInsertX` helpers use a locally-defined anonymous struct for `successes []struct{ transactionID int; prepared *prepared }`. Go does not collapse two anonymous struct types across function signatures — to avoid duplication, promote the `childRows` struct defined inside `trySendCreateBatchSQL` to a package-private type before the three helpers:

```go
// createBatchSuccess carries the transactionID returned by the INSERT RETURNING
// alongside the pre-computed prepared data for every tx that was successfully
// inserted into `transactions` in this batch.
type createBatchSuccess struct {
    transactionID int
    prepared      *preparedCreateSQL
}
```

And rename the local `prepared` struct type used inside `trySendCreateBatchSQL` to `preparedCreateSQL` at package scope (same package, private). Update the three helper signatures to `successes []createBatchSuccess`. This is a mechanical rename — do it in the same edit.

- [ ] **Step 4.2: Run the SQLite tests**

```bash
go test -v -race -tags "testtxmetacache" -run "TestCreateBatcher_SQLite" ./stores/utxo/sql/
```

Expected: all three tests pass (`_Basic`, `_Duplicate`, `_Mined`).

- [ ] **Step 4.3: Run the full SQL short suite**

```bash
go test -race -tags "testtxmetacache" -short ./stores/utxo/sql/...
```

Expected: all previously-passing tests still pass. If any break, fix before committing.

- [ ] **Step 4.4: Run the Postgres integration suite to confirm the dispatcher didn't regress pgx behaviour**

```bash
go test -race -tags "testtxmetacache" -run "Postgres" ./stores/utxo/sql/
```

Expected: PASS.

- [ ] **Step 4.5: Commit**

```bash
git add stores/utxo/sql/sql.go
git commit -m "$(cat <<'EOF'
feat(sql): portable bulk create batch for SQLite engine

sendCreateBatchSQL runs one BEGIN…COMMIT per batch with one bulk
INSERT per table (transactions / inputs / outputs / block_ids). Uses
ON CONFLICT (hash) DO NOTHING RETURNING id, hash to identify duplicates
per-item without poisoning the batch. Retries the whole batch on
SQLITE_BUSY up to 3 times, mirroring createWithRetry.

Column ordering on each INSERT matches the unbatched createX helpers
exactly so the on-disk row shape is unchanged.

Closes the SQLite side of the perf gap vs the dedicated postgres store
for the Create path without introducing pgx-specific code.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5 — Run the shared `stores/utxo/tests` suite against batched SQLite

Validates that the new batched path exhibits identical behaviour to the unbatched path across the full library of shared tests.

- [ ] **Step 5.1: Write the driver test**

Append to `stores/utxo/sql/create_batcher_sqlite_test.go`:

```go
// TestSharedSuite_SQLite_Batched runs the shared stores/utxo/tests suite
// against a SQLite store with the Create and Unlock batchers enabled.
// If any test in this suite fails, the batched path has behavioural drift
// from the unbatched path — that's a regression, not a perf concern.
func TestSharedSuite_SQLite_Batched(t *testing.T) {
    store, ctx := setupSQLiteBatched(t)

    t.Run("Store", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.Store(t, store)
    })
    t.Run("Spend", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.Spend(t, store)
    })
    t.Run("Restore", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.Restore(t, store)
    })
    t.Run("Freeze", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.Freeze(t, store)
    })
    t.Run("ReAssign", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.ReAssign(t, store)
    })
    t.Run("SetMined", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.SetMined(t, store)
    })
    t.Run("Conflicting", func(t *testing.T) {
        _ = store.Delete(ctx, tests.TXHash)
        tests.Conflicting(t, store)
    })
}
```

- [ ] **Step 5.2: Run**

```bash
go test -v -race -tags "testtxmetacache" -run "TestSharedSuite_SQLite_Batched" ./stores/utxo/sql/
```

Expected: every subtest passes.

- [ ] **Step 5.3: Commit**

```bash
git add stores/utxo/sql/create_batcher_sqlite_test.go
git commit -m "$(cat <<'EOF'
test(sql): shared utxo.Store suite runs against batched SQLite

Drives the whole stores/utxo/tests library through a sqlitememory store
with createBatcher + unlockBatcher active so any behavioural drift
between the batched and unbatched paths shows up as a test failure in
an existing test, not a silent bug.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6 — Benchmark sanity check

Optional as a release gate — still worth capturing so there's a before/after number on the PR.

- [ ] **Step 6.1: Write the benchmark**

Append to `stores/utxo/sql/create_batcher_sqlite_test.go`:

```go
// BenchmarkCreate_SQLite_Batched vs BenchmarkCreate_SQLite_Unbatched give a
// rough before/after for the portable Create batch path. Run against a
// disk-backed SQLite (not sqlitememory) for meaningful numbers.
//
//   go test -bench BenchmarkCreate_SQLite -run ^$ -benchtime=5s ./stores/utxo/sql/
func BenchmarkCreate_SQLite_Batched(b *testing.B) {
    benchmarkCreateSQLite(b, true)
}

func BenchmarkCreate_SQLite_Unbatched(b *testing.B) {
    benchmarkCreateSQLite(b, false)
}

func benchmarkCreateSQLite(b *testing.B, batched bool) {
    ctx := context.Background()
    tSettings := test.CreateBaseTestSettings(b)
    tSettings.UtxoStore.DBTimeout = 30 * time.Second
    tSettings.BatcherDrainMode = false
    if batched {
        tSettings.UtxoStore.StoreBatcherSize = 32
        tSettings.UtxoStore.StoreBatcherDurationMillis = 2
    } else {
        tSettings.UtxoStore.StoreBatcherSize = 1
    }

    dir := b.TempDir()
    u, err := url.Parse("sqlite:///" + dir + "/bench.db")
    require.NoError(b, err)
    store, err := New(ctx, ulogger.TestLogger{}, tSettings, u)
    require.NoError(b, err)
    b.Cleanup(func() { store.Stop() })

    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            tx := bt.NewTx()
            // Reuse tests.Tx shape but with a unique hash per iteration via a no-op
            // LockTime change. This keeps tx size / input / output counts realistic
            // without the benchmark stumbling on ErrTxExists.
            tx.LockTime = uint32(time.Now().UnixNano())
            // TODO in implementation: clone tests.Tx properly with a fresh hash. Left
            // as an exercise because the exact cloning strategy depends on bt helper
            // availability — benchmark is optional anyway.
            _, _ = store.Create(ctx, tx, 1000)
        }
    })
}
```

This benchmark is intentionally rough — if the cloning comment can't be resolved cleanly in 10 minutes, skip the benchmark entirely and rely on the shared suite + Task 5 tests for correctness. The real perf validation for this work is in production, not microbenchmarks.

- [ ] **Step 6.2: Run the benchmark (if the stub was finished)**

```bash
go test -bench BenchmarkCreate_SQLite -run ^$ -benchtime=5s ./stores/utxo/sql/
```

Expected: `Batched` variant has meaningfully higher ops/s than `Unbatched`. Exact multiplier is informational; regression is not a gate since the benchmark isn't on CI.

- [ ] **Step 6.3: Commit (only if benchmark works)**

```bash
git add stores/utxo/sql/create_batcher_sqlite_test.go
git commit -m "test(sql): add SQLite Create benchmark (batched vs unbatched)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7 — Push and open PR

- [ ] **Step 7.1: Push the branch**

```bash
git push -u origin stu/utxo-sql-perf-backports
```

- [ ] **Step 7.2: Open the PR**

```bash
gh pr create --repo bsv-blockchain/teranode \
  --base main --head freemans13:stu/utxo-sql-perf-backports \
  --title "perf(sql): enable create/unlock batchers on SQLite engine" \
  --body "$(cat <<'EOF'
## Summary

Ports batching wins from the dedicated postgres store (#684) to the vanilla `stores/utxo/sql` store on the SQLite engine — no schema changes, no pgx-specific code added to paths SQLite traverses.

**Unlock batcher:** removes the `scheme == "postgres"` guard; the callback already delegates to `setUnlockedBulk` which has a portable SQLite branch.

**Create batcher:** renames the existing pgx pipelined callback to `sendCreateBatchPostgres` and adds a portable `sendCreateBatchSQL` that issues one `BEGIN…COMMIT` per batch with one bulk `INSERT` per table (transactions / inputs / outputs / block_ids), using `ON CONFLICT (hash) DO NOTHING RETURNING id, hash` to identify duplicates per-item. Retries on `SQLITE_BUSY`. Shared `stores/utxo/tests` suite drives the batched path to catch behaviour drift.

Out of scope explicitly: pgx pipelining (postgres-only), COPY protocol (postgres-only), any in-process cache (#684's is dead code), switching spend batcher to `background=true`, pool sizing (already handled by `InitSQLDB`).

Spec: `docs/superpowers/specs/2026-04-20-sql-store-perf-backports-design.md`

## Test plan

- [x] `TestUnlockBatcher_SQLite_Wired` — batcher constructed when `LockedBatcherSize > 1`
- [x] `TestUnlockBatcher_SQLite_DAH` — DAH recomputes on batched unlock
- [x] `TestCreateBatcher_SQLite_Basic` — multi-tx batch via normal Create flow
- [x] `TestCreateBatcher_SQLite_Duplicate` — duplicate returns `ErrTxExists`
- [x] `TestCreateBatcher_SQLite_Mined` — `block_ids` populated through batched path
- [x] `TestSharedSuite_SQLite_Batched` — full shared suite passes against batched SQLite
- [x] Postgres integration suite (`-run Postgres`) still passes — dispatcher didn't regress pgx path

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.

---

## Self-review notes

- **Spec coverage:**
    - Component 1 (SQLite Create batcher) → Tasks 2, 3, 4
    - Component 2 (SQLite Unlock batcher) → Task 1
    - Component 3 → dropped in spec revision (already handled by `InitSQLDB`), no task
    - Testing section → Tasks 1.3, 4.2, 5, 6
- **Type consistency:** `preparedCreateSQL` is the package-private struct shared by `trySendCreateBatchSQL` and the three `bulkInsertX` helpers; `createBatchSuccess` is the package-private struct shared by the same three helpers.
- **Placeholder scan:** Task 6's benchmark has a "TODO in implementation" marker that's intentional (the benchmark is explicitly optional and the comment explains why the cloning strategy is left open). No other placeholders.
