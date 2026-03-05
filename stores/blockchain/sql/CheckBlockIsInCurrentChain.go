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
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if specified blocks are part of the current main chain.
// This implements a specialized blockchain validation method not directly defined in the Store interface.
//
// The implementation uses a three-tier lookup strategy:
//
// 1. chainMembershipCache (sync.Map): Fast positive cache — block IDs previously confirmed
// on the main chain. Survives StoreBlock/SetBlock* calls, only cleared on reorgs.
//
// 2. offChainBlockIDs (map[uint32]struct{}): Small in-memory set of block IDs known to NOT
// be on the main chain (fork/orphan blocks). Rebuilt on fork detection, invalidation, or
// revalidation via rebuildOffChainSet(). Typically contains only a few hundred entries
// across all of mainnet history.
//
// 3. Recursive CTE fallback: Only used when the off-chain set is not yet initialized
// (offChainBlockIDs == nil). Once initialized, the CTE is never needed.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockIDs: Array of internal database IDs for the blocks to check
//
// Returns:
//   - bool: True if all specified blocks are part of the current main chain, false otherwise
//   - error: Any error encountered during the check
func (s *SQL) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckIfBlockIsInCurrentChain",
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

	// Tier 2: Off-chain set lookup — check if any block ID is in the set of blocks
	// known to NOT be on the main chain. This set is typically tiny (a few hundred
	// entries on all of mainnet) and provides O(1) lookups.
	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	if offChain != nil {
		// Check if any block ID is a known fork block
		for _, id := range blockIDs {
			if _, isOffChain := offChain[id]; isOffChain {
				return false, nil
			}
		}

		// Check if any block ID is beyond what's been stored (doesn't exist).
		// maxBlockID is updated atomically on every StoreBlock, so any ID above
		// it refers to a block that hasn't been stored yet.
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

	// Tier 3: Fallback to recursive CTE — only used when offChainBlockIDs has not been
	// initialized yet (nil). This happens on first startup before any fork is detected.
	// Once rebuildOffChainSet() runs (on first fork, invalidation, or revalidation),
	// this path is never taken again.
	return s.checkBlockIsInCurrentChainCTE(ctx, blockIDs)
}

// checkBlockIsInCurrentChainCTE is the original recursive CTE implementation, retained
// as a fallback for when the off-chain set has not yet been initialized.
func (s *SQL) checkBlockIsInCurrentChainCTE(ctx context.Context, blockIDs []uint32) (bool, error) {
	cacheGen := s.chainMembershipGen.Load()

	// Get current best block header
	_, bestBlockMeta, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewStorageError("failed to get best block header", err)
	}

	// Prepare the arguments and the CTE for block_ids
	args := make([]interface{}, 0, len(blockIDs)+2) // blockIDs + bestBlockID + recursionDepth

	// Generate placeholders for blockIDs
	blockIDPlaceholders := make([]string, len(blockIDs))

	for i, id := range blockIDs {
		placeholder := fmt.Sprintf("$%d", i+1)
		if s.engine == "sqlite" || s.engine == "sqlitememory" {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT CAST(%s as int) AS id", placeholder)
		} else {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT %s::INTEGER AS id", placeholder)
		}

		args = append(args, id)
	}

	blockIDsCTE := strings.Join(blockIDPlaceholders, " UNION ALL ")

	// Append the bestBlockID and recursionDepth to the arguments
	bestBlockID := bestBlockMeta.ID

	// get the lowest block id
	lowestBlockID := blockIDs[0] //nolint:gosec // length is checked above
	for _, id := range blockIDs {
		if id < lowestBlockID {
			lowestBlockID = id
		}
	}

	recursionDepthBlockID := bestBlockID - lowestBlockID
	if lowestBlockID > bestBlockID {
		recursionDepthBlockID = 0
	}

	args = append(args, bestBlockID, recursionDepthBlockID) // bestBlockID and recursionDepth

	// Calculate the positions for the placeholders
	bestBlockIDPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+1)
	recursionDepthPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+2)

	q := fmt.Sprintf(`
        WITH RECURSIVE
        block_ids(id) AS (
            %s
        ),
        ChainBlocks AS (
            SELECT id, parent_id, 1 AS depth, EXISTS (SELECT 1 FROM block_ids WHERE id = blocks.id) AS found_match
            FROM blocks
            WHERE id = %s
            UNION ALL
            SELECT
                bb.id,
                bb.parent_id,
                cb.depth + 1 AS depth,
                EXISTS (SELECT 1 FROM block_ids WHERE id = bb.id) AS found_match
            FROM blocks bb
            INNER JOIN ChainBlocks cb ON bb.id = cb.parent_id
            WHERE
                NOT cb.found_match -- Stop recursion if a match has been found
                AND cb.depth <= %s
        )
        SELECT CASE
            WHEN EXISTS (SELECT 1 FROM ChainBlocks WHERE found_match)
            THEN TRUE
            ELSE FALSE
        END AS is_in_current_chain;
    `, blockIDsCTE, bestBlockIDPlaceholder, recursionDepthPlaceholder)

	// Execute the query
	var result bool

	err = s.db.QueryRowContext(ctx, q, args...).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, errors.NewStorageError("failed to check if given blocks are part of the current chain", err)
	}

	// Cache positive results only if no reorg occurred during the query.
	if result && cacheGen == s.chainMembershipGen.Load() {
		for _, id := range blockIDs {
			s.chainMembershipCache.Store(id, true)
		}
	}

	return result, nil
}
