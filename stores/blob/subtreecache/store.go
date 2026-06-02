package subtreecache

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/blob"
)

// nodeSizeBytes is a conservative per-node memory estimate used only when a
// subtree reports no SizeInBytes, so such entries still participate in cap
// eviction rather than appearing free. 32-byte hash + two uint64 fields.
const nodeSizeBytes = 48

// Store decorates a blob.Store with an in-process parsed-subtree cache. It
// behaves exactly like the wrapped store for all blob operations (methods are
// promoted from the embedded interface) and additionally lets a producer stash
// the parsed *Subtree/*Meta it already holds, so an in-process consumer can read
// them back without a disk round-trip or a second deserialize.
//
// The cache is bounded and best-effort: callers that miss must fall back to the
// embedded store, so correctness never depends on cache state.
type Store struct {
	blob.Store
	cache *Cache
}

// NewStore wraps inner with a parsed-subtree cache bounded to capBytes. A
// capBytes <= 0 leaves the cache disabled: the returned Store behaves as a
// transparent pass-through to inner.
func NewStore(inner blob.Store, capBytes int) *Store {
	return &Store{
		Store: inner,
		cache: New(capBytes),
	}
}

// PutParsedSubtree caches the parsed subtree and meta under its root hash,
// accounting a conservative memory size. The full subtree *data* (transaction
// bytes) is deliberately never cached — only the node list and meta.
func (s *Store) PutParsedSubtree(hash chainhash.Hash, subtree *subtreepkg.Subtree, meta *subtreepkg.Meta) {
	if subtree == nil {
		return
	}

	size := int(subtree.SizeInBytes) // nolint:gosec // bounded by block size
	if size <= 0 {
		size = len(subtree.Nodes) * nodeSizeBytes
	}

	s.cache.Put(hash, subtree, meta, size)
}

// CachedSubtree returns the cached parsed subtree for hash, if present.
func (s *Store) CachedSubtree(hash chainhash.Hash) (*subtreepkg.Subtree, bool) {
	return s.cache.Subtree(hash)
}

// CachedSubtreeMeta returns the cached parsed meta for hash, if present.
func (s *Store) CachedSubtreeMeta(hash chainhash.Hash) (*subtreepkg.Meta, bool) {
	return s.cache.Meta(hash)
}

// EvictCachedSubtree drops the cached entry for hash, releasing its memory.
// Called once finalization has consumed a block's subtrees.
func (s *Store) EvictCachedSubtree(hash chainhash.Hash) {
	s.cache.Evict(hash)
}
