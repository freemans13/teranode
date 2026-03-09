package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if any of the specified blocks are on the current
// main chain. Pure in-memory O(1) lookup — no SQL queries.
//
// The check uses three tiers:
//  1. maxBlockID: any ID above the highest known block ID cannot exist → skip it
//  2. offChainBlockIDs: if the ID is in the off-chain set → it's on a fork → skip it
//  3. Otherwise the block is on the main chain → return true
//
// Returns true as soon as any block ID passes all checks (ANY-of semantics).
// This matches the old recursive CTE behavior where the query returned true if ANY
// input block was encountered during the chain walk from best block to genesis.
// The off-chain set is rebuilt by rebuildOffChainSet on fork detection, invalidation,
// or revalidation, and periodically by the background refresh loop.
func (s *SQL) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	_, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithDebugLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	if len(blockIDs) == 0 {
		return false, nil
	}

	maxID := uint32(s.maxBlockID.Load())

	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	// ANY-of semantics: return true if at least one block is on the main chain.
	// This matches the old CTE behavior and is required by callers like
	// BlockValidation.checkOldBlockIDs which passes candidate block IDs for a
	// transaction across forks and needs true if any candidate is on-chain.
	for _, id := range blockIDs {
		// IDs above the highest known block cannot exist in the database.
		if maxID > 0 && id > maxID {
			continue
		}
		if _, isOffChain := offChain[id]; !isOffChain {
			return true, nil
		}
	}

	return false, nil
}
