# SQL UTXO Store Performance Backports

**Date:** 2026-04-20
**Target:** `stores/utxo/sql` (both SQLite and Postgres engines)
**Reference:** Techniques from `stores/utxo/postgres` (PR #684)
**Constraints:** No schema changes. No Postgres-specific code in paths SQLite traverses.

## Strategic Framing

There are two viable long-term shapes for the UTXO storage layer:

1. **Single portable `sql` store** that works with SQLite and Postgres. Accept that it will not match a dedicated pgx-tuned store at the extreme end of the throughput curve, but ship one code path that both engines are continuously tested against.
2. **Dedicated per-engine stores** (#684's `postgres` package) that can apply engine-specific wins like COPY, pgx pipelining, hash partitioning, and `synchronous_commit` tuning. Higher peak throughput at the cost of two parallel code paths to maintain and to integration-test.

This work advances option 1. It ports the subset of #684's techniques that do **not** require a schema change and do **not** lock out SQLite. If the post-backport gap against #684 is narrow enough in real deployments, #684 becomes an optional fast path rather than a coequal supported store — and the portable store remains the default, well-tested, one-to-keep-current.

## Problem

PR #684's dedicated Postgres store reached 106K TPS on the validator hot path versus ~38K TPS for the vanilla `sql` store. Cataloguing the gap, most of it comes from four techniques:

| # | Technique | Portable? | In scope? |
|---|---|---|---|
| 1 | pgx pipelining via `SendBatch` on the spend path | No — pgx-only | No |
| 2 | COPY protocol for creates | No — pgx-only | No |
| 3 | `synchronous_commit` + group commit + pool sizing | Partial — pool sizing is portable | Pool sizing only |
| 4 | Everything batched (create, spend, get, unlock) on all engines | Yes, but vanilla currently gates create + unlock batchers to Postgres | Yes |

Backportable pieces, in scope:

1. **Enable the Create batcher on SQLite**, using a new bulk `INSERT … VALUES` path (Option X below).
2. **Enable the Unlock batcher on SQLite**.

Originally a third item — setting `MaxOpenConns`/`MaxIdleConns` for the Postgres engine — was in scope. Investigation showed `util.InitSQLDB` already applies pool sizing via either `tSettings.UtxoStore.PostgresPool` (UTXO-specific override) or `tSettings.Postgres` (global defaults: 50 open, 10 idle). Operators who want to match #684's `MaxConns = 100` can already set `utxostore_postgres_maxopenconns=100` via environment — no code change required. Dropped.

Explicitly out of scope: pgx pipelining, COPY protocol, any in-process cache (#684's txCache is dead code — `cache.Add` is called but `cache.Get` is never invoked; its throughput numbers do not depend on it), switching the spend batcher to `background=true` (vanilla's bulk CTE has the same row-lock ordering concerns the existing comment flags), anything that changes the database schema.

## Design

### Component 1 — SQLite Create batcher

Currently the Create batcher construction at `sql.go:221` is guarded by `storeURL.Scheme == "postgres"`, and `sendCreateBatch` uses `driverConn.(*stdlib.Conn).Conn().SendBatch(...)` which is pgx-specific. Removing the guard without adding a SQLite implementation would panic on the type assertion.

**Change:**

- Drop the `scheme == "postgres"` guard; keep the `StoreBatcherSize > 1` guard so settings can disable batching.
- Rename the existing pgx pipelined implementation from `sendCreateBatch` to `sendCreateBatchPostgres`.
- Introduce `sendCreateBatchSQL`, a bulk `database/sql` implementation portable to SQLite (and, as a secondary benefit, also runnable on Postgres — see "Follow-up" below).
- Make `sendCreateBatch` a one-line dispatcher: `if s.engine == "postgres" { return s.sendCreateBatchPostgres(batch) } else { s.sendCreateBatchSQL(batch) }`.

**`sendCreateBatchSQL` (Option X) — shape:**

For each call:

1. Pre-compute per-item data (txHash, txMeta, unminedSince, isCoinbase, input/output/block_id arrays) as the current pgx path already does. Items whose pre-compute fails receive their error via `done` and are excluded from the batch.
2. `BEGIN`.
3. One `INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, coinbase, frozen, conflicting, locked, unmined_since) VALUES (…),(…),… ON CONFLICT (hash) DO NOTHING RETURNING id, hash` — emits rows only for newly-inserted transactions. Build a `hash → id` map from the result.
4. For every item whose hash is in the map, build multi-row VALUES lists for `inputs`, `outputs`, `block_ids` using the corresponding id. Fire at most three `INSERT` statements (skipping any whose list is empty).
5. `COMMIT`.

Per-item results:

- Items whose hash **did not** come back from step 3 → `ErrTxExists`.
- Items whose hash **did** come back and whose child inserts succeeded → `nil` + meta.
- Any step-3 / step-4 / commit error → every unnotified item in the batch gets a `StorageError`. This is the rare case: well-formed input shouldn't produce these.
- `SQLITE_BUSY` / `database is locked` errors at any step retry the whole batch at the outer retry level (mirrors the unbatched `createWithRetry` retry loop).

**Column list invariants:** the `INSERT INTO transactions` column list and the `buildInputArrays` / `buildOutputArrays` / `buildBlockIDArrays` helpers must stay identical to the unbatched path so there is one source of truth for what a tx row looks like on disk.

**Sub-options considered and rejected:**

- **SAVEPOINT per tx** (Option Y): adds complexity to handle a case (one malformed tx poisoning the whole batch) that doesn't routinely arise — validation happens upstream of the UTXO store, and `ON CONFLICT DO NOTHING` already handles the duplicate-hash case cleanly.
- **Fast path + per-tx fallback on error** (Option Z): ~2x code for the same reason. Skipped.

### Component 2 — SQLite Unlock batcher

No code changes beyond removing the `scheme == "postgres"` guard at `sql.go:231`. The existing `sendUnlockBatch` already delegates to `setUnlockedBulk`, which has a portable SQLite branch that runs sequential per-hash unlock + `setDAH` inside a single `BEGIN…COMMIT`. The win on SQLite comes from collapsing N separate `BEGIN…COMMIT` cycles (one per concurrent caller) into one per batch — fewer fsyncs, fewer write-lock acquisitions.

### Component 3 — Postgres pool sizing

Not needed as a code change. `util.InitSQLDB` (already called from the SQL store's `New`) applies `SetMaxOpenConns` / `SetMaxIdleConns` from either `tSettings.UtxoStore.PostgresPool` or the global `tSettings.Postgres`. Operators who want to match #684's `MaxConns=100` can set `utxostore_postgres_maxopenconns=100` at deployment time. No engine-specific default change bundled with this plan — changing the global default would affect every service.

## Testing

### New integration tests

- **`TestUnlockBatcher_SQLite_SingleHash`** — mirror of the existing `TestUnlockBatcher_Postgres_SingleHash`, pointed at `sqlitememory://`. Verifies the batcher path clears the `locked` flag.
- **`TestUnlockBatcher_SQLite_DAH`** — mirror of `TestUnlockBatcher_Postgres_DAH`. Verifies DAH is recalculated on unlock.
- **`TestCreateBatcher_SQLite_Basic`** — 10 distinct txs through the batcher concurrently, all succeed, all retrievable.
- **`TestCreateBatcher_SQLite_Duplicate`** — batch with a pre-existing tx mixed in: the pre-existing one returns `ErrTxExists`, the others succeed.
- **`TestCreateBatcher_SQLite_BusyRetry`** — drive the retry path by simulating `SQLITE_BUSY` (set a short busy timeout, have a second connection hold a write lock for a bounded time, confirm the batcher recovers once the lock releases).

### Existing test coverage

The shared `stores/utxo/tests` suite runs against SQLite with the batcher enabled via settings. Any behavioural divergence between the batched and unbatched path surfaces there without new tests.

### Benchmark sanity

`go test -bench BenchmarkCreate ./stores/utxo/sql -run ^$` before/after against a disk-backed SQLite. Target: measurable improvement at batch sizes ≥ 16. Exact multiplier not a gate; regression in either engine IS a gate.

## Settings additions

- No new settings. Reuse the existing `StoreBatcherSize` / `LockedBatcherSize` that gate the currently postgres-only paths; after this plan they'll gate on both engines.

## Follow-up (not in this design)

- Benchmark `sendCreateBatchSQL` on Postgres versus today's pgx pipelined `sendCreateBatchPostgres`. If the portable path is within a few percent of the pipelined path, retire the pgx-specific code and leave one Create batch implementation for both engines.
- Experimentally verify the `background=false` assumption on the spend batcher. The current comment asserts concurrent batches can deadlock via overlapping row-lock orderings, but two valid spend batches cannot target the same `(prev_tx, prev_output_idx)` (that would be a double-spend), so structurally the claim is suspect. Before flipping the flag, run a high-concurrency spend harness against `background=true` and count `40P01 deadlock_detected` errors — zero means the comment is stale and the flag can change; non-zero means there's a concrete scenario to reason about and the flag stays.

## Files affected

- `stores/utxo/sql/sql.go` — batcher construction guards; new `sendCreateBatchSQL`; existing `sendCreateBatch` renamed to `sendCreateBatchPostgres`; `sendCreateBatch` becomes dispatcher.
- `stores/utxo/sql/create_batcher_sqlite_test.go` (new) — create batcher SQLite tests.
- `stores/utxo/sql/unlock_batcher_sqlite_test.go` (new) — unlock batcher SQLite tests.
