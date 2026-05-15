package pruner

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

// defaultPrunedTxSetCapacity is used when NewPrunedTxSet is called with maxEntries=0.
// At ~1 byte per entry in the cuckoo filter, 2B entries ≈ 2 GiB — small relative to
// typical pruner pod memory budgets while comfortably covering production-scale
// sessions where parent/child gaps span many blocks.
const defaultPrunedTxSetCapacity = 2_000_000_000

// PrunedTxSet is a sharded cuckoo-filter-backed set tracking TXIDs of records pruned
// across sessions. It is used to skip wasteful parent updates for parents that have
// already been pruned.
//
// Memory: ~1 byte per entry effective (8-bit fingerprints × 4-slot buckets in the
// seiflotfy/cuckoofilter implementation). A capacity of 2B entries fits in ~2 GiB,
// roughly two orders of magnitude smaller than the exact-set implementation it
// replaces (~130 B/entry).
//
// False positives: ~3% (formula: 2 × bucketSize / 2^fp_bits = 8 / 256). In our
// context a false positive causes a child to incorrectly skip a parent update,
// suppressing the deletedChildren bin write for that parent. That bin is only
// consulted by the defensive-mode safety check (always off when prunedSet is
// non-nil), so FPs are behaviourally harmless.
//
// CheckAndRemove uses cuckoo Delete. On rare fingerprint collisions, Delete may
// remove the wrong entry. That manifests as a lost future skip for the
// collision-evicted hash (i.e. its child will incur one wasted Aerospike
// round-trip), not a correctness bug.
//
// Sharding picks a bucket from h[0] & mask. SHA-256-derived TXIDs have uniform
// first bytes, so the distribution across shards is even.
type PrunedTxSet struct {
	shards         []prunedTxShard
	mask           uint8
	insertFailures atomic.Int64
	capacity       int64
}

type prunedTxShard struct {
	mu     sync.Mutex
	filter *cuckoo.Filter
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
	if perShard < 1024 {
		perShard = 1024
	}

	s := &PrunedTxSet{
		shards:   make([]prunedTxShard, n),
		mask:     uint8(n - 1),
		capacity: int64(maxEntries),
	}
	for i := range s.shards {
		s.shards[i].filter = cuckoo.NewFilter(perShard)
	}
	return s
}

func (s *PrunedTxSet) shard(h chainhash.Hash) *prunedTxShard {
	return &s.shards[h[0]&s.mask]
}

// Add registers a TXID. Duplicate Adds are tolerated but each successful Insert
// increments the underlying cuckoo count — Len() may therefore overcount when
// the same TXID is Added multiple times (e.g. a partition rescanned after a
// timeout). When the cuckoo filter is full, Insert fails and the failure is
// counted in insertFailures.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	sh := s.shard(h)
	sh.mu.Lock()
	ok := sh.filter.Insert(h[:])
	sh.mu.Unlock()
	if !ok {
		s.insertFailures.Add(1)
	}
}

// Contains returns true if the TXID is in the set. May report false positives
// at the cuckoo filter's standard rate (~3%).
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	ok := sh.filter.Lookup(h[:])
	sh.mu.Unlock()
	return ok
}

// CheckAndRemove returns true and deletes the TXID's fingerprint if it appears
// to be present. Subject to the same false-positive rate as Contains, and may
// on collisions remove the wrong fingerprint (causing a future Contains/CheckAndRemove
// for the collision-evicted hash to miss — see type doc).
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	ok := sh.filter.Delete(h[:])
	sh.mu.Unlock()
	return ok
}

// Len returns the approximate number of fingerprints currently in the filter
// across all shards. Duplicate Adds inflate this count.
func (s *PrunedTxSet) Len() int {
	total := uint(0)
	for i := range s.shards {
		s.shards[i].mu.Lock()
		total += s.shards[i].filter.Count()
		s.shards[i].mu.Unlock()
	}
	return int(total)
}

// Saturated reports whether the cuckoo filter has rejected at least one Insert
// since construction (i.e. capacity was hit). This is sticky — once true,
// it stays true even if entries are removed and capacity frees up.
func (s *PrunedTxSet) Saturated() bool {
	return s.insertFailures.Load() > 0
}

// InsertFailures returns the cumulative number of Insert calls that failed
// because the filter was full at the time. Useful for sizing.
func (s *PrunedTxSet) InsertFailures() int64 {
	return s.insertFailures.Load()
}
