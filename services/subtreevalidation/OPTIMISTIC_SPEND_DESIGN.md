# Optimistic Spend-Validate-Rollback Architecture

## Executive Summary

This document proposes replacing the current prefetch-extend-validate architecture with an optimistic spend-validate-rollback approach for block validation. This change reduces memory usage, simplifies code, and improves performance for very large blocks (600M+ transactions).

## Current Architecture (Baseline)

### Current Flow

```text
1. Load all transactions from subtrees (chunked at 8M)
2. prefetchAndCacheParentUTXOs()
   - Identify external parents (two-pass)
   - BatchDecorate to fetch parent outputs
   - Pre-populate cache
3. batchExtendTransactions()
   - Fetch parent outputs again (fields.Outputs)
   - Extend transaction inputs with parent data
   - Mark transactions as extended
4. Validate transactions
   - Validator sees IsExtended() = true
   - Skips individual parent fetching
5. Spend UTXOs (happens during validation)
6. Create new UTXOs
```

### Current Memory Usage (per 8M tx chunk)

- Transaction data: 2GB
- External parent outputs (~5%): 400K × 100 bytes = 40MB (with fields.Outputs optimization)
- Maps and metadata: ~200MB
- **Total: ~2.2GB per chunk**

### Current Operations (per 8M tx chunk)

- Prefetch: ~80 batch operations (400K parents / 5K chunk)
- Extend fetch: ~80 batch operations
- Spend: 8M individual operations
- **Total: ~8M operations**

## Proposed Architecture

### New Flow

```text
1. Load all transactions from subtrees (chunked at 8M)
2. optimisticBatchSpend()
   - For each transaction input:
     - Call spend() which atomically:
       a. Checks UTXO is spendable
       b. Marks as spent
       c. Returns vout data (amount + script + block_ids)
   - Extend transactions inline with returned data
   - Track spends for potential rollback
3. Validate transactions
   - Use already-extended transactions
   - On validation failure:
     - Call unspend() to rollback
     - Mark transaction as invalid
4. Create new UTXOs (for valid transactions)
```

### New Memory Usage (per 8M tx chunk)

- Transaction data: 2GB
- External parent outputs: 0GB (returned from spend, used immediately, GC'd)
- Spend tracking map: ~100MB (for rollback)
- **Total: ~2.1GB per chunk** (5% reduction)

### New Operations (per 8M tx chunk)

- Spend with vout return: 8M operations (combined fetch + spend)
- Unspend rollback: ~800 operations (0.01% failure rate)
- **Total: ~8M operations** (same, but combined!)

## Benefits Analysis

### Memory Benefits

| Component | Current | Proposed | Savings |
|-----------|---------|----------|---------|
| Parent storage | 40MB | 0MB | 100% |
| Code paths | 2 (prefetch + extend) | 1 (optimistic spend) | 50% simpler |
| Per-chunk peak | 2.2GB | 2.1GB | 5% |

### Performance Benefits

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Aerospike ops | Fetch (160) + Spend (8M) | Spend (8M) | 160 fewer batch ops |
| Code complexity | 2 functions | 1 function | Simpler |
| Data transfer | Fetch outputs separately | Included in spend | Less network I/O |

### Operational Benefits

- **Simpler architecture**: Fewer moving parts
- **Atomic operations**: Spend + data return is atomic
- **Better error handling**: Explicit rollback path
- **Lower latency**: One round-trip instead of two

## Implementation Plan

### Phase 1: Interface Changes

#### 1.1 Define VoutData Structure

**File**: `stores/utxo/types.go` (new or existing)

```go
// VoutData contains the data from a transaction output needed for validation
type VoutData struct {
    // Amount in satoshis
    Amount uint64

    // LockingScript from the output
    LockingScript *bscript.Script

    // BlockIDs where this UTXO appears (for BIP68 validation)
    BlockIDs []uint32

    // Vout index
    Vout uint32
}
```

#### 1.2 Extend Store Interface

**File**: `stores/utxo/Interface.go`

Add methods:

```go
// SpendWithVoutData atomically spends a UTXO and returns its vout data
// This enables optimistic spending where validation happens after spending
// Returns error if UTXO is already spent or doesn't exist
SpendWithVoutData(ctx context.Context, spend *Spend) (*VoutData, error)

// Unspend rolls back a spend operation (for validation failures)
// Sets UTXO status back to OK and clears spent_by field
Unspend(ctx context.Context, txHash *chainhash.Hash, vout uint32) error

// BatchSpendWithVoutData performs batch optimistic spend operations
// Returns map of spend -> vout data for successful spends
// Fails atomically if any spend operation fails
BatchSpendWithVoutData(ctx context.Context, spends []*Spend) (map[string]*VoutData, error)

// BatchUnspend rolls back multiple spend operations
BatchUnspend(ctx context.Context, spends []*Spend) error
```

### Phase 2: SQL Implementation

**File**: `stores/utxo/sql/sql.go`

Implement the new methods for SQL backend (SQLite/PostgreSQL):

```go
func (s *Store) SpendWithVoutData(ctx context.Context, spend *Spend) (*VoutData, error) {
    // Begin transaction
    // SELECT amount, locking_script, block_ids WHERE tx_hash AND vout AND status='OK'
    // UPDATE status='SPENT', spent_by=spend.SpendingTxHash
    // Return VoutData
    // Commit
}

func (s *Store) Unspend(ctx context.Context, txHash *chainhash.Hash, vout uint32) error {
    // UPDATE status='OK', spent_by=NULL WHERE tx_hash AND vout
}
```

### Phase 3: Aerospike Lua Scripts

**File**: Document requirements (actual scripts deployed separately)

#### spend_with_data.lua

```lua
function spend_with_data(rec, spending_tx_hash, vout)
    if not rec then
        return map{success=false, error='utxo_not_found'}
    end

    if rec['status'] == 'SPENT' then
        return map{success=false, error='already_spent',spent_by=rec['spent_by']}
    end

    -- Get vout data
    local outputs = rec['outputs']  -- Assuming outputs stored as list
    if vout >= #outputs then
        return map{success=false, error='vout_out_of_range'}
    end

    local vout_data = outputs[vout]

    -- Mark as spent
    rec['status'] = 'SPENT'
    rec['spent_by'] = spending_tx_hash
    aerospike:update(rec)

    -- Return vout data
    return map{
        success = true,
        amount = vout_data['satoshis'],
        script = vout_data['locking_script'],
        block_ids = rec['block_ids']
    }
end

function unspend(rec, vout)
    if not rec then
        return map{success=false}
    end

    rec['status'] = 'OK'
    rec['spent_by'] = nil
    aerospike:update(rec)
    return map{success=true}
end
```

### Phase 4: Replace Prefetch+Extend in check_block_subtrees.go

**Remove** (lines 615-881):

- `prefetchAndCacheParentUTXOs` function
- `batchExtendTransactions` function

**Add** new function:

```go
func (u *Server) optimisticBatchSpendAndExtend(ctx context.Context, allTransactions []*bt.Tx) (map[*bt.Tx][]*Spend, error) {
    // Build spend list
    txToSpends := make(map[*bt.Tx][]*Spend)
    allSpends := make([]*Spend, 0)

    for _, tx := range allTransactions {
        if tx.IsCoinbase() || tx.IsExtended() {
            continue
        }

        spends := make([]*Spend, 0, len(tx.Inputs))
        for _, input := range tx.Inputs {
            spend := &Spend{
                TxID: input.PreviousTxID(),
                Vout: input.PreviousTxOutIndex,
                SpendingTxHash: tx.TxIDChainHash(),
            }
            spends = append(spends, spend)
            allSpends = append(allSpends, spend)
        }
        txToSpends[tx] = spends
    }

    // Batch spend with vout data
    voutDataMap, err := u.utxoStore.BatchSpendWithVoutData(ctx, allSpends)
    if err != nil {
        return nil, err
    }

    // Extend transactions with returned vout data
    for tx, spends := range txToSpends {
        for i, spend := range spends {
            key := fmt.Sprintf("%s:%d", spend.TxID, spend.Vout)
            voutData := voutDataMap[key]
            if voutData == nil {
                continue // Parent not found
            }

            tx.Inputs[i].PreviousTxSatoshis = voutData.Amount
            tx.Inputs[i].PreviousTxScript = voutData.LockingScript
        }
        tx.SetExtended(true)
    }

    return txToSpends, nil
}
```

**Update processTransactionsInLevels** (lines 912-933):

```go
// Optimistically spend UTXOs and extend transactions
txToSpends, err := u.optimisticBatchSpendAndExtend(ctx, allTransactions)
if err != nil {
    return errors.NewProcessingError("[processTransactionsInLevels] Optimistic spend failed", err)
}
```

### Phase 5: Add Rollback on Validation Failure

Update validation error handling to unspend on failure:

```go
if err := validator.Validate(tx); err != nil {
    // Rollback spends for this transaction
    if spends, ok := txToSpends[tx]; ok {
        _ = u.utxoStore.BatchUnspend(ctx, spends)
    }
    // Continue processing other transactions
}
```

### Testing Strategy

1. **Unit tests** for new UTXO store methods
2. **Integration tests** for optimistic spend flow
3. **Rollback tests** - force validation failures, verify unspend works
4. **Performance tests** - compare with current architecture
5. **Large block tests** - verify chunking still works

### Rollout Plan

1. Implement interfaces and SQL backend (works immediately)
2. Document Aerospike Lua requirements (deploy separately)
3. Add feature flag: `USE_OPTIMISTIC_SPEND`
4. Test with small blocks
5. Gradually increase block sizes
6. Monitor rollback rates and performance
7. Full cutover when validated

This is a significant architectural improvement that simplifies the code and improves performance!
