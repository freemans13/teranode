package pruner

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// defaultPrunedTxSetCapacity is used when NewPrunedTxSet is called with maxEntries=0.
// At ~1 byte per entry, 2B entries ≈ 2 GiB — small relative to typical pruner pod
// memory budgets while comfortably covering production-scale sessions where
// parent/child gaps span many blocks.
const defaultPrunedTxSetCapacity = 2_000_000_000

// PrunedTxSet is a sharded cuckoo-filter-backed set tracking TXIDs of records
// pruned across sessions. It is used to skip wasteful parent updates for parents
// that have already been pruned.
//
// The backing filter (cuckooH32) is specialised for 32-byte chainhash inputs to
// avoid the per-op slice/interface allocations that a general-purpose cuckoo
// library imposes. Since chainhash is already a cryptographic digest, we read
// fingerprint and bucket index directly from the hash bytes — no re-hashing.
//
// Memory: ~1 byte per entry effective (4-slot buckets of 8-bit fingerprints).
// A capacity of 2B entries fits in ~2 GiB.
//
// False positives: ~3.1% (standard 8-bit cuckoo). In our context a false
// positive causes a child to incorrectly skip a parent update, suppressing the
// deletedChildren bin write for that parent. That bin is only consulted by the
// defensive-mode safety check (always off when prunedSet is non-nil), so FPs
// are behaviourally harmless.
//
// CheckAndRemove uses Delete which may, on rare fingerprint collisions, remove
// the wrong entry. That manifests as a lost future skip for the
// collision-evicted hash (one wasted Aerospike round-trip), not a correctness
// bug.
//
// Sharding picks a bucket from h[9] & mask (NOT h[0] — h[0] is consumed by the
// fingerprint, and we want shard distribution to be independent of fingerprint
// distribution). SHA-256-derived TXIDs have uniform byte values, so the
// distribution across shards is even.
type PrunedTxSet struct {
	shards         []prunedTxShard
	mask           uint8
	insertFailures atomic.Int64
	capacity       int64
}

type prunedTxShard struct {
	mu     sync.Mutex
	filter *cuckooH32
}

// NewPrunedTxSet creates a sharded cuckoo-filter-backed set sized to the given
// total maxEntries (across all shards). shardCount is rounded up to the next
// power of 2 (capped at 256). Pass maxEntries=0 to use the default capacity.
func NewPrunedTxSet(shardCount int, maxEntries int) *PrunedTxSet {
	n := 1
	for n < shardCount {
		n <<= 1
	}
	if n > 256 {
		n = 256
	}

	if maxEntries <= 0 {
		maxEntries = defaultPrunedTxSetCapacity
	}

	perShard := uint(maxEntries / n)
	if perShard < cuckooBucketSize {
		perShard = cuckooBucketSize
	}

	s := &PrunedTxSet{
		shards:   make([]prunedTxShard, n),
		mask:     uint8(n - 1),
		capacity: int64(maxEntries),
	}
	for i := range s.shards {
		s.shards[i].filter = newCuckooH32(perShard)
	}
	return s
}

// shard picks the per-shard filter using byte 9 of the hash, leaving bytes 0–8
// available to the cuckoo fingerprint+index derivation. With ≤256 shards
// (mask is uint8), a single byte gives uniform distribution.
func (s *PrunedTxSet) shard(h *chainhash.Hash) *prunedTxShard {
	return &s.shards[h[9]&s.mask]
}

// Add registers a TXID. The hash is passed by value through the API to keep
// the call site readable, but internally we take its address so we can pass
// *[32]byte to the cuckoo filter without allocating a slice header.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	sh := s.shard(&h)
	sh.mu.Lock()
	ok := sh.filter.Insert((*[32]byte)(&h))
	sh.mu.Unlock()
	if !ok {
		s.insertFailures.Add(1)
	}
}

// Contains returns true if the TXID is in the set. May report false positives
// at the cuckoo filter's standard rate (~3%).
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(&h)
	sh.mu.Lock()
	ok := sh.filter.Lookup((*[32]byte)(&h))
	sh.mu.Unlock()
	return ok
}

// CheckAndRemove returns true and deletes the TXID's fingerprint if it appears
// to be present. Subject to the same false-positive rate as Contains, and may
// on collisions remove the wrong fingerprint (causing a future
// Contains/CheckAndRemove for the collision-evicted hash to miss).
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(&h)
	sh.mu.Lock()
	ok := sh.filter.Delete((*[32]byte)(&h))
	sh.mu.Unlock()
	return ok
}

// Len returns the approximate number of fingerprints currently in the filter
// across all shards. Duplicate Adds inflate this count.
func (s *PrunedTxSet) Len() int {
	total := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		total += s.shards[i].filter.Count()
		s.shards[i].mu.Unlock()
	}
	return total
}

// Saturated reports whether the filter has rejected at least one Insert since
// construction (i.e. capacity was hit at some point). Sticky — once true,
// stays true even if entries are removed and capacity frees up.
func (s *PrunedTxSet) Saturated() bool {
	return s.insertFailures.Load() > 0
}

// InsertFailures returns the cumulative number of Insert calls that failed
// because the filter was full at the time. Useful for sizing.
func (s *PrunedTxSet) InsertFailures() int64 {
	return s.insertFailures.Load()
}
