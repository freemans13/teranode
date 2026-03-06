package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if all specified blocks are on the current main chain.
// Pure in-memory O(1) lookup against offChainBlockIDs — no SQL queries.
// Returns false if any block ID is in the off-chain set (fork/orphan blocks).
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

	for _, id := range blockIDs {
		if _, isOffChain := offChain[id]; isOffChain {
			return false, nil
		}
	}

	return true, nil
}
