package pruner

import (
	"sync/atomic"
	"unsafe"
)

// cuckooH32 is a lock-free cuckoo filter specialised for 32-byte
// cryptographic hashes (chainhash.Hash).
//
// Why this exists (a previous-developer FAQ):
//
//   - Why not use github.com/seiflotfy/cuckoofilter? We did initially. Its
//     API takes []byte, which caused the 32-byte hash backing array to
//     escape to the heap on every Insert/Lookup/Delete via the hasher
//     interface dispatch. At ~1.5M ops/sec that was ~50 MB/sec of
//     allocation churn, triggering 2 GC/sec with p99 pauses of 5 ms —
//     enough to cause visible throughput regression on dev-scale-1.
//
//   - Why specialised for *[32]byte? Two reasons. (a) chainhash is already
//     a SHA-256 digest, so the bytes are uniformly distributed and we can
//     derive fingerprint + bucket index by reading bytes directly — no
//     extra hash compute. (b) Taking a pointer instead of a slice avoids
//     the heap escape that plagued the library version.
//
//   - Why lock-free? Each bucket is 4 fingerprint bytes packed into a
//     uint32, naturally aligned in the bucket slice. That lets Lookup be
//     a single atomic.LoadUint32 + SWAR byte-equality test, and
//     Insert/Delete be CompareAndSwapUint32. Removing the per-shard
//     sync.Mutex eliminated ~28% of total CPU spent in lock2/futex/
//     procyield on the previous (locked) version.
//
// Concurrency caveats (acceptable for use as a probabilistic skip hint):
//   - Concurrent Inserts on the same bucket can race so that two newly
//     inserted fingerprints displace each other during eviction. The
//     filter remains valid (no torn writes); some hashes may end up at
//     unexpected positions, causing future Lookups to miss. That is a
//     lost-skip, not a correctness bug.
//   - Count is approximate under contention (atomic Int64).
//
// Standard cuckoo parameters: 4-slot buckets × 8-bit fingerprints → ~3.1%
// false-positive rate. False positives in our context cause a parent
// update to be incorrectly skipped, which only suppresses a write to the
// `deletedChildren` bin — never read when defensive mode is off (see the
// invariants block in PruneWithPartitions).

const (
	cuckooBucketSize  = 4
	cuckooMaxKicks    = 500
	cuckooNullFinger  = 0
	cuckooMaskMixer64 = 0xc4ceb9fe1a85ec53 // SplitMix64 mix constant
)

// cuckooBucket holds 4 fingerprint bytes. Naturally 4-byte aligned in the
// `buckets` slice, so atomic uint32 ops on the underlying memory are safe.
type cuckooBucket [cuckooBucketSize]uint8

type cuckooH32 struct {
	buckets []cuckooBucket
	mask    uint64
	count   atomic.Int64

	// saturated latches true the first time the eviction loop fails to
	// place a fingerprint. Subsequent Insert calls that find both
	// candidate buckets full will skip the eviction loop entirely and
	// return false immediately. Without this, a saturated filter burns
	// ~15 us of CPU per Insert (500 kicks × ~30 ns) chasing slots that
	// can't exist — at ~1.5M Adds/sec that consumes ~22 cores' worth of
	// CPU on a single pod (observed on dev-scale-1: pod pegged at 15
	// cores with throughput collapsed from 2.4M to ~200K rec/sec).
	//
	// Sticky-once-set: even if CheckAndRemove later frees slots, we
	// don't try eviction again. The fast path (tryInsertCAS in either
	// bucket) still succeeds when slots open up, so the optimisation
	// recovers automatically; we just refuse to do the expensive
	// rebalancing work. To force a reset, restart the process.
	saturated atomic.Bool
}

func newCuckooH32(capacity uint) *cuckooH32 {
	n := uint64(1)
	target := uint64(capacity)
	if target < cuckooBucketSize {
		target = cuckooBucketSize
	}
	for n*cuckooBucketSize < target {
		n <<= 1
	}
	return &cuckooH32{
		buckets: make([]cuckooBucket, n),
		mask:    n - 1,
	}
}

func (cf *cuckooH32) bucketAddr(i uint64) *uint32 {
	return (*uint32)(unsafe.Pointer(&cf.buckets[i][0]))
}

// extract derives fingerprint and primary bucket index from the hash.
// chainhash is already a SHA-256 digest, so byte slices of it are
// uniformly distributed — no extra hashing required.
func (cf *cuckooH32) extract(h *[32]byte) (fp uint8, i uint64) {
	fp = h[0]
	if fp == cuckooNullFinger {
		fp = 1
	}
	i = (uint64(h[1]) |
		uint64(h[2])<<8 |
		uint64(h[3])<<16 |
		uint64(h[4])<<24 |
		uint64(h[5])<<32 |
		uint64(h[6])<<40 |
		uint64(h[7])<<48 |
		uint64(h[8])<<56) & cf.mask
	return fp, i
}

func (cf *cuckooH32) altIndex(fp uint8, i uint64) uint64 {
	return (i ^ (uint64(fp) * cuckooMaskMixer64)) & cf.mask
}

// bucketHas tests whether any slot in bucket i matches fp, using a single
// atomic load + a SWAR (SIMD-within-a-register) byte-equality test.
func (cf *cuckooH32) bucketHas(i uint64, fp uint8) bool {
	v := atomic.LoadUint32(cf.bucketAddr(i))
	broadcast := uint32(fp) | uint32(fp)<<8 | uint32(fp)<<16 | uint32(fp)<<24
	t := v ^ broadcast
	// Standard zero-byte detection: ((t - 0x01010101) & ~t & 0x80808080) != 0
	return (t-0x01010101)&^t&0x80808080 != 0
}

// tryInsertCAS attempts to claim any empty slot in bucket i for fp.
// Returns true on success.
func (cf *cuckooH32) tryInsertCAS(fp uint8, i uint64) bool {
	addr := cf.bucketAddr(i)
	for slot := 0; slot < cuckooBucketSize; slot++ {
		shift := uint(slot * 8)
		for retry := 0; retry < 4; retry++ {
			cur := atomic.LoadUint32(addr)
			b := uint8(cur >> shift)
			if b != cuckooNullFinger {
				break // slot occupied; advance to next slot
			}
			newWord := cur | (uint32(fp) << shift)
			if atomic.CompareAndSwapUint32(addr, cur, newWord) {
				cf.count.Add(1)
				return true
			}
		}
	}
	return false
}

// bucketCASDelete clears the first slot in bucket i whose byte equals fp.
func (cf *cuckooH32) bucketCASDelete(i uint64, fp uint8) bool {
	addr := cf.bucketAddr(i)
	for slot := 0; slot < cuckooBucketSize; slot++ {
		shift := uint(slot * 8)
		for retry := 0; retry < 4; retry++ {
			cur := atomic.LoadUint32(addr)
			b := uint8(cur >> shift)
			if b != fp {
				break
			}
			newWord := cur &^ (uint32(0xff) << shift)
			if atomic.CompareAndSwapUint32(addr, cur, newWord) {
				cf.count.Add(-1)
				return true
			}
		}
	}
	return false
}

// Insert places h's fingerprint in one of its two candidate buckets,
// performing cuckoo eviction if both are full.
//
// Once the filter has previously hit saturation (eviction loop exhausted
// MaxKicks without placing the fingerprint), the eviction path is
// permanently disabled for this filter instance: any Insert that finds
// both candidate buckets full returns false immediately. Without this
// short-circuit, a saturated filter at high write rates burns the host's
// CPU on hopeless eviction churn (~15 us per failed Insert).
func (cf *cuckooH32) Insert(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.tryInsertCAS(fp, i1) {
		return true
	}
	i2 := cf.altIndex(fp, i1)
	if cf.tryInsertCAS(fp, i2) {
		return true
	}

	// Both candidate buckets are full. Short-circuit if we've previously
	// proven the filter cannot accommodate evictions — the loop would just
	// waste CPU. The fast path above still recovers automatically when
	// CheckAndRemove frees up slots.
	if cf.saturated.Load() {
		return false
	}

	// Eviction loop — race-tolerant. Concurrent inserts may displace each
	// other; the worst outcome is a lost future skip for some fingerprint.
	i := i1
	if (uint64(fp)+i1)&1 == 1 {
		i = i2
	}
	for k := 0; k < cuckooMaxKicks; k++ {
		slot := int((uint64(fp) ^ uint64(k)) & (cuckooBucketSize - 1))
		shift := uint(slot * 8)
		addr := cf.bucketAddr(i)
		var displaced uint8
		swapped := false
		for retry := 0; retry < 4; retry++ {
			cur := atomic.LoadUint32(addr)
			displaced = uint8(cur >> shift)
			newWord := (cur &^ (uint32(0xff) << shift)) | (uint32(fp) << shift)
			if atomic.CompareAndSwapUint32(addr, cur, newWord) {
				swapped = true
				break
			}
		}
		if !swapped {
			cf.saturated.Store(true)
			return false
		}
		fp = displaced
		i = cf.altIndex(fp, i)
		if cf.tryInsertCAS(fp, i) {
			return true
		}
	}
	// Eviction loop exhausted — filter is saturated. Latch the flag so
	// future Inserts that find both buckets full skip this loop and
	// return immediately rather than re-burning ~15 us per call.
	cf.saturated.Store(true)
	return false
}

// Lookup returns true if h appears to be in the filter.
func (cf *cuckooH32) Lookup(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.bucketHas(i1, fp) {
		return true
	}
	return cf.bucketHas(cf.altIndex(fp, i1), fp)
}

// Delete removes one occurrence of h's fingerprint from the filter.
func (cf *cuckooH32) Delete(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.bucketCASDelete(i1, fp) {
		return true
	}
	return cf.bucketCASDelete(cf.altIndex(fp, i1), fp)
}

// Count returns the approximate number of fingerprints stored.
func (cf *cuckooH32) Count() int { return int(cf.count.Load()) }
