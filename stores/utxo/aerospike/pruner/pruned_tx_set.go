package pruner

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// defaultPrunedTxSetCapacity is used when NewPrunedTxSet is called with maxEntries=0.
// At ~1 byte per entry split across two generations, 2B total entries ≈ 2 GiB.
const defaultPrunedTxSetCapacity = 2_000_000_000

// PrunedTxSet is a sharded, two-generation cuckoo-filter-backed set tracking
// TXIDs of records pruned across sessions. It is used to skip wasteful parent
// updates for parents that have already been pruned.
//
// Why two generations:
//
//	A single cuckoo filter saturates eventually (entries only leave via
//	CheckAndRemove, and most TXIDs added are never asked about by a future
//	child). Once saturated, the filter freezes — new entries cannot be
//	added, so children of currently-being-pruned txs can never find their
//	parents. On dev-scale-1 at 1.7M TPS the single-filter design saturated
//	in ~50 min and the steady-state catch rate of would-be-wasted parent
//	updates collapsed from ~98% to ~14%.
//
//	The two-generation design rotates: each shard keeps a `current` filter
//	(receives new Adds) and a `previous` filter (holds the prior epoch's
//	entries, read-only). When `current` saturates, it slides into the
//	`previous` slot (dropping whatever was there) and a fresh `current` is
//	allocated. Lookup/CheckAndRemove check `current` first, then fall back
//	to `previous` on miss. This guarantees that recently-Added entries
//	are always reachable, and the set never freezes.
//
// Throughput-preserving properties:
//
//   - Before the first rotation, `previous` is nil and the hot path is
//     byte-identical to a single-filter implementation. Fresh pods see
//     zero overhead vs the previous design.
//   - After rotation, `Add` still costs one atomic.Pointer.Load + one
//     cuckoo CAS (~1 ns extra). Lookup/CheckAndRemove that HIT in current
//     return immediately — also no extra cost. Only the miss path pays
//     the ~5-10 ns extra to check `previous`.
//   - In tight-chain workloads (parent of tx_N is tx_{N-1}, produced
//     seconds apart by the same blaster worker), parent is almost always
//     in `current`, so the miss path is rare.
//
// Memory budget: total memory ≈ 2 × maxEntries × ~1 byte (both
// generations live simultaneously). NewPrunedTxSet sizes each generation
// at maxEntries / (2 × shardCount) so the SUM of capacity across all
// shards and both generations equals the configured maxEntries.
//
// Sharding picks a bucket from h[9] & mask (NOT h[0] — h[0] is consumed by
// the cuckoo fingerprint, and we want shard distribution to be independent
// of fingerprint distribution). SHA-256-derived TXIDs have uniform byte
// values, so the distribution across shards is even.
type PrunedTxSet struct {
	shards         []prunedTxShard
	mask           uint8
	perShardCap    uint // capacity of each generation in each shard
	insertFailures atomic.Int64
	rotations      atomic.Int64 // number of generation rotations across all shards
	capacity       int64
}

type prunedTxShard struct {
	current  atomic.Pointer[cuckooH32]
	previous atomic.Pointer[cuckooH32]
}

// NewPrunedTxSet creates a sharded two-generation cuckoo-filter-backed set
// sized to the given total maxEntries (counting BOTH generations across
// all shards). shardCount is rounded up to the next power of 2 (capped at
// 256). Pass maxEntries=0 to use the default capacity.
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

	// Each shard has 2 generations; total live capacity = 2 × shardCount ×
	// perShardCap. Divide accordingly so memory budget matches the
	// configured maxEntries.
	perShard := uint(maxEntries / n / 2)
	if perShard < cuckooBucketSize {
		perShard = cuckooBucketSize
	}

	s := &PrunedTxSet{
		shards:      make([]prunedTxShard, n),
		mask:        uint8(n - 1),
		perShardCap: perShard,
		capacity:    int64(maxEntries),
	}
	for i := range s.shards {
		s.shards[i].current.Store(newCuckooH32(perShard))
		// previous stays nil until first rotation in this shard
	}
	return s
}

// shard picks the per-shard pair using byte 9 of the hash, leaving bytes
// 0–8 available to the cuckoo fingerprint+index derivation.
func (s *PrunedTxSet) shard(h *chainhash.Hash) *prunedTxShard {
	return &s.shards[h[9]&s.mask]
}

// Add registers a TXID. If the current generation refuses (saturated), the
// shard rotates — current slides into previous (replacing it), a fresh
// current is allocated, and the Add is retried.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	sh := s.shard(&h)
	cur := sh.current.Load()
	if cur.Insert((*[32]byte)(&h)) {
		return
	}
	// Saturated — rotate this shard. Only one goroutine wins the CAS;
	// the others observe the new current on their retry below.
	s.rotateShard(sh, cur)
	if sh.current.Load().Insert((*[32]byte)(&h)) {
		return
	}
	// Even the fresh current refused — should be impossible (filter just
	// allocated) but bookkeep for visibility.
	s.insertFailures.Add(1)
}

// rotateShard atomically swaps the shard's `current` for a fresh filter,
// preserving the old current as `previous` (replacing whatever was there).
// If another goroutine already rotated, this is a no-op.
func (s *PrunedTxSet) rotateShard(sh *prunedTxShard, oldCur *cuckooH32) {
	newCur := newCuckooH32(s.perShardCap)
	if sh.current.CompareAndSwap(oldCur, newCur) {
		sh.previous.Store(oldCur)
		s.rotations.Add(1)
	}
}

// Contains returns true if the TXID is in either generation. Checks
// current first (cheap hit on recent entries) and falls back to previous
// only on miss.
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(&h)
	if sh.current.Load().Lookup((*[32]byte)(&h)) {
		return true
	}
	if prev := sh.previous.Load(); prev != nil {
		return prev.Lookup((*[32]byte)(&h))
	}
	return false
}

// CheckAndRemove returns true and deletes the TXID's fingerprint from
// whichever generation holds it. Tries current first.
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(&h)
	if sh.current.Load().Delete((*[32]byte)(&h)) {
		return true
	}
	if prev := sh.previous.Load(); prev != nil {
		return prev.Delete((*[32]byte)(&h))
	}
	return false
}

// Len returns the approximate number of fingerprints currently stored
// across both generations and all shards. Eventually consistent under
// concurrent ops.
func (s *PrunedTxSet) Len() int {
	total := 0
	for i := range s.shards {
		total += s.shards[i].current.Load().Count()
		if prev := s.shards[i].previous.Load(); prev != nil {
			total += prev.Count()
		}
	}
	return total
}

// Saturated reports whether the set has experienced any insert failures
// since construction. With the two-generation design, the only way to
// see InsertFailures > 0 is if a freshly-allocated generation refused
// an Insert (effectively never in normal operation), so Saturated() is
// now best read as "something went badly wrong" rather than "we're full".
// Use Rotations() to see how often the set is recycling.
func (s *PrunedTxSet) Saturated() bool {
	return s.insertFailures.Load() > 0
}

// InsertFailures returns the cumulative count of Insert calls that
// failed even on a freshly-rotated generation. Should be ~0 in normal
// operation.
func (s *PrunedTxSet) InsertFailures() int64 {
	return s.insertFailures.Load()
}

// Rotations returns the cumulative number of times any shard has rotated
// its current generation into previous. Useful for sizing: rotations
// imply per-shard saturation, and a high rate suggests perShardCap is
// too small for the workload.
func (s *PrunedTxSet) Rotations() int64 {
	return s.rotations.Load()
}
