package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// CheckSubtreeSlicesForDuplicateTxs is the consensus integrity floor for the
// CVE-2012-2459 duplicate-transaction check on any code path that holds the
// block's subtree slices in memory but does not go through Block.Valid (which
// runs the full pooled/disk-backed checkDuplicateTransactions).
//
// It scans every node hash across all subtree slices into a plain Go map,
// skipping the coinbase placeholder at position [0][0], and returns a
// BlockInvalidError on the first duplicate. The function is intentionally
// lightweight — O(N) in the total transaction count with one map allocation —
// and is suitable for the below-checkpoint unified route where blocks are
// small enough that the slice-based in-memory scan is the right trade-off.
//
// Callers on the full validation path (Block.Valid) continue to use the
// existing pooled/disk-backed checkDuplicateTransactions; this helper is an
// independent, non-invasive floor for slice-only paths.
func CheckSubtreeSlicesForDuplicateTxs(slices []*subtreepkg.Subtree) error {
	// Estimate total node count for the initial map capacity to avoid rehashing.
	totalNodes := 0
	for _, st := range slices {
		if st != nil {
			totalNodes += len(st.Nodes)
		}
	}

	seen := make(map[chainhash.Hash]struct{}, totalNodes)

	for subIdx, subtree := range slices {
		if subtree == nil {
			continue
		}
		for txIdx, node := range subtree.Nodes {
			// Skip the coinbase placeholder (all-0xFF hash) that occupies the
			// first position of the first subtree. It is not a real transaction
			// hash and must not be deduped against itself.
			if subIdx == 0 && txIdx == 0 && node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				continue
			}

			if _, exists := seen[node.Hash]; exists {
				return errors.NewBlockInvalidError(
					"[CheckSubtreeSlicesForDuplicateTxs] block contains duplicate transaction %s (CVE-2012-2459)",
					node.Hash.String(),
				)
			}
			seen[node.Hash] = struct{}{}
		}
	}

	return nil
}
