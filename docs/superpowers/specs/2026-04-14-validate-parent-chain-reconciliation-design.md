# Validate Parent Chain Reconciliation

## Problem

When `loadUnminedTransactions` runs (on startup or reorg reset), it uses an Aerospike secondary index (SI) range query on `unmined_since` to scan all unmined transactions. Aerospike SI queries progress on a "moving target" — if a transaction's `unmined_since` is set while the scan has already passed the partition containing that record, the query misses it.

Meanwhile, `validateParentChain` uses `BatchDecorate` (direct primary key reads, always consistent) to check each transaction's parents. It finds parents with `unmined_since > 0` that the SI scan missed, producing the error:

```text
parent tx <hash> is unmined but not in processing list
```

This results in mining candidates that contain child transactions without their parents — structurally invalid blocks that SVNode correctly rejects.

The problem occurs in two scenarios:

1. **Reorg reset**: `reset()` calls `MarkTransactionsOnLongestChain` (sets `unmined_since`) then immediately calls `loadUnminedTransactions`. The SI scan races with the writes.
2. **Startup**: New transactions arrive via propagation/validation concurrently with the initial scan.

Restarting block assembly temporarily fixes it (the next scan picks up everything), but the problem recurs on subsequent resets or when new transactions arrive during scans.

## Solution: Two-Phase Load with Reconciliation

Evolve `validateParentChain` from a diagnostic/filter function into a reconciliation function. When it identifies a parent that is "unmined but not in processing list", instead of warning and skipping, it fetches the missing parent's full record from the UTXO store and adds it to the unmined list.

### Scope

All changes are in `services/blockassembly/BlockAssembler.go`, in the `validateParentChain` function. No changes to the Aerospike store, the iterator, the subtree processor, or any other service.

### Reconciliation Flow

```text
validateParentChain(unminedTxs, bestBlockHeaderIDsMap)
  │
  ├─ Pass 1: Build parentIndexMap, BatchDecorate all parents
  │   ├─ Parent mined on best chain → valid
  │   ├─ Parent unmined AND in list → valid (check ordering)
  │   ├─ Parent unmined NOT in list → collect for reconciliation
  │   ├─ Parent not found in store → invalid (genuinely missing)
  │   └─ Parent data inconsistency → invalid
  │
  ├─ If reconciliation candidates exist:
  │   ├─ BatchDecorate reconciliation candidates with full field set
  │   │   (fee, size, createdAt, blockIDs, locked, txInpoints)
  │   ├─ Filter out invalid candidates:
  │   │   ├─ Skip if block_ids on best chain (data inconsistency, not truly unmined)
  │   │   ├─ Skip if no createdAt (split record, not a real unmined tx)
  │   │   └─ Skip if BatchDecorate returns error (record doesn't exist)
  │   ├─ Build UnminedTransaction structs from valid candidates
  │   ├─ Append to validTxs
  │   ├─ Re-sort validTxs by createdAt
  │   ├─ Rebuild parentIndexMap
  │   └─ Pass 2: Validate ONLY reconciled txs and their direct children
  │       └─ Repeat up to 3 passes max
  │
  └─ Return validTxs (complete with reconciled parents)
```

### Key Details

**Fetching missing parents**: Use `BatchDecorate` with the full field set (same bins the iterator requests: fee, sizeInBytes, createdAt, blockIDs, locked, unminedSince, txInpoints). No new store method needed.

**Subsequent passes are scoped**: Only validate newly reconciled parents (their own parents might be missing) and children of reconciled parents that were previously marked invalid. This keeps cost proportional to reconciled count, not total list size.

**Max passes cap**: 3 passes maximum. If missing parents remain after 3 passes, fall back to current behavior — warn and skip/keep based on `OnRestartRemoveInvalidParentChainTxs` setting.

### Edge Cases

| Scenario | Handling |
|----------|----------|
| Reconciled parent has block_ids on best chain + unmined_since > 0 | Data inconsistency — do not add to unmined list. Already handled by `MarkTransactionsOnLongestChain`. |
| Reconciled parent has no createdAt | Split record — skip, not a real unmined tx. |
| Concurrent write during reconciliation | Acceptable — same class of problem. 3-pass cap + normal tx ingestion picks it up. |
| Deep ancestor chains (parent → grandparent → great-grandparent all missed) | Handled by multi-pass. Each pass discovers the next level. Capped at 3. |
| More than 1000 parents reconciled | Log WARN — indicates systemic issue, not normal scan race. |
| Max passes exceeded with remaining orphans | Fall back to current skip/keep behavior with WARN log. |

### Logging Changes

| Current | New |
|---------|-----|
| WARN: "parent tx X is unmined but not in processing list" | INFO: "reconciled N missing parent(s) from UTXO store (pass M)" |
| — | WARN: "reconciliation fetched N parents but M exceeded max passes — falling back to skip/keep" |
| — | WARN: "reconciliation found unusually high count (>1000) — possible systemic issue" |

### Performance Impact

The expensive work (SI scan + BatchDecorate for all parents) already happens today. The reconciliation adds:

- One BatchDecorate call for the missing parents (typically 10-100 txs, negligible vs the millions already fetched)
- A re-sort of the full list (already O(N log N), adding a handful of items doesn't change this)
- At most 2 additional scoped validation passes

Net impact: unmeasurable in practice.

### Testing

- Unit test: mock UTXO store returns incomplete iterator results but complete BatchDecorate results. Verify reconciliation fills the gap.
- Unit test: deep chain (3 levels of missing ancestors). Verify multi-pass resolves all.
- Unit test: max passes exceeded. Verify fallback to skip/keep behavior.
- Unit test: reconciled parent has block_ids on best chain. Verify it's excluded.
- Existing `validateParentChain` tests continue to pass (the happy path is unchanged).
