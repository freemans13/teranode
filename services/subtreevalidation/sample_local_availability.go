package subtreevalidation

import (
	"context"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// sampleLocalAvailability checks a sample of transaction hashes from a subtree
// against the TxMetaCache to estimate what fraction of the subtree's transactions
// are available locally. Returns a hit rate between 0.0 and 1.0.
//
// This is used by CheckBlockSubtrees to decide whether to bulk-download subtree
// data from a peer or let ValidateSubtreeInternal handle the few missing txs.
func (u *Server) sampleLocalAvailability(ctx context.Context, subtree *subtreepkg.Subtree, sampleSize int) float64 {
	nodes := subtree.Nodes
	if len(nodes) == 0 {
		return 0.0
	}

	cache, ok := u.utxoStore.(*txmetacache.TxMetaCache)
	if !ok {
		return 0.0
	}

	if sampleSize <= 0 {
		sampleSize = 100
	}

	if sampleSize > len(nodes) {
		sampleSize = len(nodes)
	}

	// Evenly spaced sampling across the subtree for deterministic, representative coverage
	step := len(nodes) / sampleSize
	if step == 0 {
		step = 1
	}

	hits := 0
	checked := 0
	for i := 0; i < len(nodes) && checked < sampleSize; i += step {
		hash := nodes[i].Hash
		if hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}
		found, _ := cache.GetMetaCached(ctx, hash, &meta.Data{})
		if found {
			hits++
		}
		checked++
	}

	if checked == 0 {
		return 0.0
	}
	return float64(hits) / float64(checked)
}
