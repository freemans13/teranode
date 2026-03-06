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
//
// The implementation is fully in-memory with zero SQL queries. It checks each block ID
// against offChainBlockIDs — a small set of block IDs known to NOT be on the main chain
// (fork/orphan blocks). This set is rebuilt via rebuildOffChainSet() on fork detection,
// invalidation, or revalidation, and typically contains only a few hundred entries across
// all of mainnet history.
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

	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	for _, id := range blockIDs {
		if _, isOffChain := offChain[id]; isOffChain {
			return false, nil
		}
	}

	return true, nil
}
