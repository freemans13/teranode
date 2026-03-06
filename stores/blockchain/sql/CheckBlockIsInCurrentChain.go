package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if any of the specified blocks are on the current
// main chain. Pure in-memory O(1) lookup against offChainBlockIDs — no SQL queries.
// Returns true as soon as any block ID is found that is NOT in the off-chain set.
// This matches the old recursive CTE semantics where the query returned true if ANY
// input block was encountered during the chain walk from best block to genesis.
// The off-chain set is rebuilt by rebuildOffChainSet on fork detection, invalidation, or revalidation.
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

	// ANY-of semantics: return true if at least one block is on the main chain.
	// This matches the old CTE behavior and is required by callers like
	// BlockValidation.checkOldBlockIDs which passes candidate block IDs for a
	// transaction across forks and needs true if any candidate is on-chain.
	for _, id := range blockIDs {
		if _, isOffChain := offChain[id]; !isOffChain {
			return true, nil
		}
	}

	return false, nil
}
