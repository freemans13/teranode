// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the CheckBlockIsInCurrentChain method, which determines whether
// specified blocks are part of the current main blockchain. Instead of walking the
// entire chain from tip via an expensive recursive CTE, this implementation uses an
// inverted approach: it maintains a small in-memory set of block IDs known to NOT be
// on the main chain (fork/orphan blocks). A block is on the main chain if it is not
// in this off-chain set. This provides O(1) lookups regardless of chain depth.
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if specified blocks are part of the current main chain.
// This implements a specialized blockchain validation method not directly defined in the Store interface.
//
// The implementation is fully in-memory with zero SQL queries, using a three-tier strategy:
//
//  1. chainMembershipCache (sync.Map): Fast positive cache — block IDs previously confirmed
//     on the main chain. Survives StoreBlock/SetBlock* calls, only cleared on reorgs.
//
//  2. offChainBlockIDs (map[uint32]struct{}): Small in-memory set of block IDs known to NOT
//     be on the main chain (fork/orphan blocks). Rebuilt on fork detection, invalidation, or
//     revalidation via rebuildOffChainSet(). Typically contains only a few hundred entries
//     across all of mainnet history.
//
//  3. maxBlockID (atomic.Uint64): Tracks the highest block ID ever stored. Any requested ID
//     above this cannot exist, so we return false without DB access.
//
// Parameters:
//   - ctx: Context for the operation (unused for DB, retained for tracing)
//   - blockIDs: Array of internal database IDs for the blocks to check
//
// Returns:
//   - bool: True if all specified blocks are part of the current main chain, false otherwise
//   - error: Always nil (retained for interface compatibility)
func (s *SQL) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	_, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithDebugLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	if len(blockIDs) == 0 {
		return false, nil
	}

	// Tier 1: Fast path — check if all block IDs are already confirmed on the main chain.
	// This cache survives StoreBlock/SetBlock* calls and is only cleared on reorgs.
	allCached := true
	for _, id := range blockIDs {
		if _, ok := s.chainMembershipCache.Load(id); !ok {
			allCached = false
			break
		}
	}

	if allCached {
		return true, nil
	}

	// Tier 2: Off-chain set — check if any block ID is a known fork/orphan block.
	// This set is typically tiny (a few hundred entries on all of mainnet).
	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	for _, id := range blockIDs {
		if _, isOffChain := offChain[id]; isOffChain {
			return false, nil
		}
	}

	// Tier 3: Existence check — reject block IDs beyond what's been stored.
	// maxBlockID is updated atomically on every StoreBlock.
	maxID := uint32(s.maxBlockID.Load()) //nolint:gosec // block IDs fit in uint32
	for _, id := range blockIDs {
		if id > maxID {
			return false, nil
		}
	}

	// All block IDs exist, none are off-chain — they're on the main chain.
	// Cache them for future Tier 1 hits.
	cacheGen := s.chainMembershipGen.Load()
	if cacheGen == s.chainMembershipGen.Load() {
		for _, id := range blockIDs {
			s.chainMembershipCache.Store(id, true)
		}
	}

	return true, nil
}
