package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// ParsedSubtreeCache is an optional capability a SubtreeStore may implement to
// serve already-parsed subtree artifacts from memory. During in-process legacy
// below-checkpoint IBD the block handler parses and writes each subtree, then
// block finalization reads the same files straight back; when the store
// implements this interface, the parsed objects can be returned directly,
// avoiding the disk re-read and re-deserialize.
//
// It is purely an optimization. A SubtreeStore need not implement it; read
// paths type-assert for it and fall back to GetIoReader on a miss, so
// correctness never depends on the cache.
type ParsedSubtreeCache interface {
	// CachedSubtree returns the parsed subtree for the given root hash, if cached.
	CachedSubtree(hash chainhash.Hash) (*subtreepkg.Subtree, bool)
	// CachedSubtreeMeta returns the parsed subtree meta for the given root hash, if cached.
	CachedSubtreeMeta(hash chainhash.Hash) (*subtreepkg.Meta, bool)
	// EvictCachedSubtree releases the cached entry for the given root hash.
	EvictCachedSubtree(hash chainhash.Hash)
}
