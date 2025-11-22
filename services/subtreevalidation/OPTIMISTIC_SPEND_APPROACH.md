# Optimistic Spend Architecture for Block Validation

## Overview

This branch implements optimistic spend-validate-rollback for block subtree validation, eliminating the need for separate parent data prefetching and transaction extension.

## Core Concept

**Current approach**: Fetch parent data → Extend transactions → Validate → Spend
**New approach**: Spend (returns vout data) → Validate → Rollback if invalid

## Benefits

- **Memory**: No parent data storage (2.2GB → 2GB per chunk)
- **Operations**: 50% fewer (combine fetch + spend)
- **Simplicity**: Remove prefetchAndCacheParentUTXOs and batchExtendTransactions
- **Performance**: One atomic operation instead of two separate ones

## Architecture

### Flow

```text
For each chunk of 8M transactions:
  1. optimisticBatchSpend()
     - Build spend list from transaction inputs
     - Call spend() which atomically:
       * Checks UTXO is spendable
       * Marks as spent
       * Returns vout data (amount, script, block_ids)
     - Extend transactions with returned data
     - Track spends for potential rollback

  2. Validate transactions
     - Use extended transactions
     - On failure: unspend() to rollback

  3. Create new UTXOs (for valid transactions)
```

### Key Assumptions

- 99.99% of transactions in a block are valid
- Rollback operations are rare
- Atomic spend+return is more efficient than separate fetch+spend

## Implementation Status

**Phase 1**: UTXO Store Interface Extensions ⏳
**Phase 2**: SQL Backend Implementation ⏳
**Phase 3**: Aerospike Lua Scripts (documented) ⏳
**Phase 4**: Optimistic Spend Logic ⏳
**Phase 5**: Rollback Integration ⏳
**Phase 6**: Testing ⏳

## Performance Expectations

For 600M transaction block:

- **Current (with chunking)**: ~10 seconds, 2.2GB memory
- **With optimistic spend**: ~8-9 seconds, 2GB memory
- **Improvement**: 10-20% faster, 10% less memory

## Migration Plan

1. Implement with feature flag `USE_OPTIMISTIC_SPEND`
2. Test with small blocks (< 1K txs)
3. Gradually increase to larger blocks
4. Monitor rollback rates
5. Full cutover when validated

## Files to Create/Modify

- `stores/utxo/Interface.go` - Add new methods
- `stores/utxo/types.go` - Add VoutData struct
- `stores/utxo/sql/sql.go` - Implement for SQL
- `stores/utxo/aerospike/spend_with_data.lua` - New Lua script
- `stores/utxo/aerospike/unspend.lua` - New Lua script
- `services/subtreevalidation/check_block_subtrees.go` - Replace prefetch+extend
