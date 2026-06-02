package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// OffChainBlockIDs returns the complete set of block IDs known NOT to be on the
// current main chain — the in-memory off-chain (forked) set that backs
// CheckBlockIsInCurrentChain's O(1) negative lookup.
//
// It is the batch, prefetch-friendly counterpart of CheckBlockIsInCurrentChain:
// instead of answering one candidate set per call, it hands the caller the whole
// negative set once so membership can be resolved locally (X is on the main chain
// iff X is not in this set), with no further round-trips.
//
// rebuilding is true when the set must not be trusted:
//   - the in-memory chain check is disabled (useInMemoryChainCheck == false), or
//   - a main-chain rebuild is in progress (startup or reorg), during which the
//     set may be empty or stale.
//
// In both cases callers must fall back to per-block CheckBlockIsInCurrentChain,
// which has its own authoritative SQL path. A returned (nil, false, nil) means
// "the off-chain set is genuinely empty" — i.e. every known block is on the main
// chain — which is the common case on a healthy chain.
func (s *SQL) OffChainBlockIDs(ctx context.Context) ([]uint32, bool, error) {
	_, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:OffChainBlockIDs")
	defer deferFn()

	if !s.useInMemoryChainCheck || s.mainChainRebuilding.Load() > 0 {
		return nil, true, nil
	}

	s.offChainBlockIDsMu.RLock()
	ids := make([]uint32, 0, len(s.offChainBlockIDs))
	for id := range s.offChainBlockIDs {
		ids = append(ids, id)
	}
	s.offChainBlockIDsMu.RUnlock()

	return ids, false, nil
}
