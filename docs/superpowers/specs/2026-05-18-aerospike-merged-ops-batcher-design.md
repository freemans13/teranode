# Aerospike UTXO Store — Merged Ops Batcher (all 7 batchers)

**Date:** 2026-05-18
**Status:** Design — pending implementation plan
**Related:** PR #871 (end-to-end batch validation + propagation single-tx coalescer)

## Goal

Increase throughput and reduce per-op latency in the Aerospike UTXO store's per-tx
path by merging **all seven** currently-independent go-batchers (`getBatcher`,
`spendBatcher`, `storeBatcher`, `outpointBatcher`, `incrementBatcher`,
`setDAHBatcher`, `lockedBatcher`) into a single shared batcher. The merged
batcher fills proportionally faster than any individual batcher today, so the
duration-timeout fires much less often — flushes are driven by size, not delay.

An earlier draft of this design carved SetLocked (and the other "write-tail"
ops) out of scope on the grounds that "BA must not be mixed in". On
re-examination, that invariant is about *when* the caller invokes `SetLocked`
(only after `BA.Store` returns), not which Aerospike `BatchOperate` the op
rides in. Once queued, each op is records + a callback; the merged transport
preserves callback semantics, so all seven batchers can share one intake.

This is complementary to PR #871: that PR adds a separate *batch* API for
callers who already hold N txs; this design improves the *per-tx* path that
those same callers (and others, e.g. Kafka-fed flows) still hit.

## Non-Goals

- Do not alter validator phase ordering. Within one tx, GET still completes
  before SPEND, SPEND before CREATE, and SetLocked still happens only after
  the caller observes `BA.Store` success.
- Do not change the Aerospike record schema or Lua UDFs.
- Do not change the per-record policy semantics — each op-type keeps its own
  per-record policy in the merged call.

## Scope (which ops merge)

All seven existing per-op batchers in `aerospike.go:127-134` merge:

| Op | Current batcher | Item type | Notes |
|----|----|----|----|
| GET | `getBatcher` | `batchGetItem` | Read; no UDF |
| SPEND | `spendBatcher` | `batchSpend` | Lua UDF write; circuit breaker stays *outside* the merged path |
| CREATE (store) | `storeBatcher` | `BatchStoreItem` | Write with fee/conflicting expressions |
| Outpoint decorate | `outpointBatcher` | `batchOutpoint` | Read |
| Increment | `incrementBatcher` | `batchIncrement` | Write (counter add) |
| SetDAH | `setDAHBatcher` | `batchDAH` | Write (DAH bin) |
| SetLocked | `lockedBatcher` | `batchLocked` | Write (locked bin); only queued after `BA.Store` returns |

## Runtime Modes

A single setting selects behavior:

`aerospike_mergedOpsBatcherMode` — one of:

- `off` (default) — current behavior, three separate batchers; merged path
  is dead code.
- `single` — one batcher, one mixed `BatchOperate` per flush. Accepts the
  coupling that GET results in the call wait for the slowest SPEND UDF in the
  same call.
- `split` — one batcher fills the intake, but on flush the items are
  partitioned by op-type and dispatched as two concurrent `BatchOperate` calls
  (GET-only, SPEND+CREATE). Same intake win, decouples GET latency from UDF.

We will ship all three modes, bench them head-to-head, and remove the loser
once a winner is clear.

## Components

### `mergedOpsBatcher`

A new `batcherIfc[mixedOp]` field on `Store`, constructed alongside the existing
batchers in `aerospike.go`. Built via `batcher.NewWithPool` exactly like the
others; flush handler is `sendMergedOpsBatch`.

```go
type opKind uint8

const (
    opGet opKind = iota
    opSpend
    opCreate
    opOutpoint
    opIncrement
    opSetDAH
    opSetLocked
)

type mixedOp struct {
    kind      opKind
    get       *batchGetItem    // non-nil iff kind == opGet
    spend     *batchSpend      // non-nil iff kind == opSpend
    create    *BatchStoreItem  // non-nil iff kind == opCreate
    outpoint  *batchOutpoint
    increment *batchIncrement
    setDAH    *batchDAH
    setLocked *batchLocked
}
```

### Flush handler: `sendMergedOpsBatch(items []*mixedOp)`

Refactor each of the seven existing flush handlers (`sendGetBatch`,
`sendSpendBatchLua`, `sendStoreBatch`, `sendOutpointBatch`,
`sendIncrementBatch`, `sendSetDAHBatch`, `sendLockedBatch`) into a
`build<Op>Records` + dispatch-closure pair. The existing single-batcher
handlers become thin wrappers around their pair.

`sendMergedOpsBatch` then:

- **`single` mode:** concatenate records from all seven builders (preserving
  per-op slice offsets), one `client.BatchOperate`, dispatch by slice.
- **`split` mode:** partition into a **read slice** (GET + Outpoint) and a
  **write slice** (SPEND + CREATE + Increment + SetDAH + SetLocked); fire two
  `BatchOperate` calls under `errgroup.Group`; dispatch. Reads are pure-read
  policies and benefit from not waiting on UDF/write latency; writes share
  policy ergonomics.

### Submission sites

Each of `get.go`, `spend.go`, `create.go`, plus the four "tail" op sites
(outpoint, increment, setDAH, setLocked) currently calls
`s.<x>Batcher.Put(&item)`. Replace with a small helper:

```go
func (s *Store) submitOp(ctx context.Context, op *mixedOp) {
    if s.mergedOpsBatcher != nil {
        s.mergedOpsBatcher.PutCtx(ctx, op)
        return
    }
    switch op.kind {
    case opGet:        s.getBatcher.PutCtx(ctx, op.get)
    case opSpend:      s.spendBatcher.PutCtx(ctx, op.spend)
    case opCreate:     s.storeBatcher.PutCtx(ctx, op.create)
    case opOutpoint:   s.outpointBatcher.PutCtx(ctx, op.outpoint)
    case opIncrement:  s.incrementBatcher.PutCtx(ctx, op.increment)
    case opSetDAH:     s.setDAHBatcher.PutCtx(ctx, op.setDAH)
    case opSetLocked:  s.lockedBatcher.PutCtx(ctx, op.setLocked)
    }
}
```

`mergedOpsBatcher` is non-nil only when mode != `off`. When non-nil, the
per-op batchers are left constructed (cheap; preserves runtime-toggle option
for follow-up work).

## Settings

Added to `settings/utxostore_settings.go`:

- `aerospike_mergedOpsBatcherMode` (string, default `off`).
- `aerospike_mergedOpsBatcherSize` (int, default = max of current per-op
  batcher sizes).
- `aerospike_mergedOpsBatcherDurationMillis` (int, default = min of current
  per-op batcher durations).

Defaults ensure the merged batcher fills no slower than today's fastest filler
and flushes no later than today's tightest duration.

## Correctness Argument

- **No new cross-op dependency.** The validator still awaits GET before issuing
  SPEND for the same tx, and SPEND before CREATE. We only change how
  *independent* ops from different txs are coalesced into Aerospike calls.
- **BatchOperate mixed ops are supported.** PR #871's `batch_create.go`,
  `batch_spend.go`, and `batch_get_parents.go` already build mixed-op
  `BatchOperate` calls; we reuse the same client semantics.
- **Per-record result dispatch.** `BatchRecord` results return in submission
  order; our flush handler keeps per-op slice offsets and dispatches accordingly.
  Same pattern PR #871 uses.
- **Errors are per-record.** A failed Lua UDF on one SPEND record does not
  fail the whole batch; the per-record `ResultCode` is dispatched to that
  item's callback. Other items in the same call still receive their results.

## Testing

- **Unit:** mock `batcherIfc`; assert each submission site routes to the
  merged batcher when configured. Assert `sendMergedOpsBatch` dispatches to
  the correct per-item callback in both modes, including partial-failure cases.
- **Integration (testcontainers Aerospike):** parity tests that run a mixed
  workload (concurrent Get / Spend / Create / Outpoint-decorate / Increment /
  SetDAH / SetLocked) in all three modes and assert identical observable
  outcomes (utxo states, errors, returned values, ordering observed by
  callers).
- **Bench (real Aerospike, 128-core):** mirror PR #871's concurrency sweep
  (32 / 128 / 512 / 1024). Compare `off` / `single` / `split` head-to-head.
  Report ms/op per op-type plus end-to-end `Validate` ms/op.

## Verification (per `AGENTS.md`)

Before claiming success:

```bash
go test ./stores/utxo/aerospike/...
go test -race ./stores/utxo/aerospike/...
go vet ./...
golangci-lint run
```

Plus the Aerospike-tagged integration suite and the bench sweep above.

## Risks

- **Result-dispatch off-by-one.** Slice offsets must be exact. Mitigation:
  explicit offsets carried alongside builders; unit tests cover boundary cases
  (empty sub-slice, one of each op, all-of-one-op).
- **GET p99 regression in `single` mode.** Expected; that's exactly why `split`
  exists. The bench sweep makes the tradeoff visible before any rollout.
- **Circuit breaker scope.** `circuit_breaker.go` is per-batcher today. The
  merged batcher gets its own breaker instance; if it trips, all seven op
  flows are affected together. Acceptable for an experimental mode behind a
  default-off flag. Today's `spendCircuitBreaker` guard stays in
  `sendSpendBatchLua`'s pre-flight (runs before `submitOp`), so an open
  breaker still short-circuits SPEND items without affecting other op types
  queued in the merged batcher.
- **Backpressure interaction.** A slow downstream (Aerospike GC pause) that
  today only stalls one batcher will now stall the merged one — i.e. all
  seven op flows. Same as production reality (Aerospike is shared), but worth
  noting.
- **SetLocked ordering.** Earlier draft excluded SetLocked. We've concluded
  the BA-coordination invariant is enforced at the caller (don't queue
  SetLocked until BA.Store returns), not at the transport. Verify in parity
  tests that ordering observed by callers is unchanged.

## Rollout

- Land with `mode = off` default. Identical to today.
- Internal benches on terabuild / test cluster: sweep concurrency, record
  numbers, pick winner between `single` and `split`.
- Flip mode in `settings_local.conf` per-cluster (teratestnet → testnet →
  mainnet) once a winner emerges. Revert criterion: any p99 latency regression
  > 10% vs `off` on the same workload.
- Once a winner is committed, remove the loser branch and (optionally) the
  `off` branch + the three legacy batchers.
