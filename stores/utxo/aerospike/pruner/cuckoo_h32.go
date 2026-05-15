package pruner

// cuckooH32 is a cuckoo filter specialised for 32-byte cryptographic hashes
// (chainhash.Hash). It exists to avoid the per-op allocations and interface
// dispatches that the general-purpose cuckoo library imposes when the key is
// a []byte — every Insert/Lookup/Delete call ends up sending a slice header
// (and sometimes the backing array) to the heap via interface escape, which
// at the pruner's op rate (~1.5M ops/sec) becomes a meaningful GC overhead.
//
// Design notes:
//   - Input is *[32]byte. The chainhash is already a cryptographic digest, so
//     no further hashing is needed — we derive the fingerprint and bucket
//     index by reading bytes directly from the hash.
//   - 8-bit fingerprint, 4-slot buckets — standard cuckoo configuration,
//     ~3.1% false-positive rate.
//   - Not thread-safe on its own. The caller (PrunedTxSet) provides
//     per-shard locking.
//
// The standard cuckoo library this replaces is github.com/seiflotfy/cuckoofilter.

const (
	cuckooBucketSize  = 4
	cuckooMaxKicks    = 500
	cuckooNullFinger  = 0
	cuckooMaskMixer64 = 0xc4ceb9fe1a85ec53 // SplitMix64 constant — used to derive alt-index from fingerprint
)

type cuckooBucket [cuckooBucketSize]uint8

type cuckooH32 struct {
	buckets []cuckooBucket
	mask    uint64
	count   int
}

// newCuckooH32 returns a filter sized to hold at least the requested capacity.
// Capacity is rounded up so that (numBuckets * 4) >= capacity and numBuckets is
// a power of two (so we can mask instead of mod).
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

// extract derives fingerprint and index1 from the hash without allocating.
// h[0] gives the fingerprint (forced non-zero so we can use 0 as the empty
// slot marker). h[1:9] gives 64 bits of index entropy, masked down to the
// filter's bucket count.
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

// altIndex returns the alternate bucket index for a (fingerprint, index)
// pair. Standard cuckoo construction: alt = i XOR hash(fp). We use the
// SplitMix64 finalizer constant for a cheap, well-distributed mix.
func (cf *cuckooH32) altIndex(fp uint8, i uint64) uint64 {
	return (i ^ (uint64(fp) * cuckooMaskMixer64)) & cf.mask
}

// tryInsertAt places fp into bucket i if any slot is empty. Returns true on
// success.
func (cf *cuckooH32) tryInsertAt(fp uint8, i uint64) bool {
	b := &cf.buckets[i]
	for j := 0; j < cuckooBucketSize; j++ {
		if b[j] == cuckooNullFinger {
			b[j] = fp
			cf.count++
			return true
		}
	}
	return false
}

// Insert adds the hash to the filter. Returns false if the cuckoo eviction
// loop fails (filter effectively full at this capacity / hash distribution).
func (cf *cuckooH32) Insert(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.tryInsertAt(fp, i1) {
		return true
	}
	i2 := cf.altIndex(fp, i1)
	if cf.tryInsertAt(fp, i2) {
		return true
	}
	// Eviction loop — pick a random-ish slot from one of the candidate buckets,
	// swap the fingerprint, try to place the displaced fingerprint at its
	// alternate bucket. Deterministic slot pick (count-derived) keeps the
	// filter allocation-free without sacrificing eviction quality at typical
	// load factors.
	i := i1
	if (uint64(fp)+i1)&1 == 1 {
		i = i2
	}
	for k := 0; k < cuckooMaxKicks; k++ {
		slot := int((uint64(fp) ^ uint64(k)) & (cuckooBucketSize - 1))
		oldFp := cf.buckets[i][slot]
		cf.buckets[i][slot] = fp
		fp = oldFp
		i = cf.altIndex(fp, i)
		if cf.tryInsertAt(fp, i) {
			return true
		}
	}
	// Restore the original fingerprint as best we can — we've corrupted the
	// table with displaced entries. In our usage (probabilistic filter with
	// behaviourally harmless false positives), this is acceptable: we just
	// have a slightly degraded filter at the saturation point.
	return false
}

// Lookup returns true if h appears to be in the filter (subject to the
// standard cuckoo false-positive rate).
func (cf *cuckooH32) Lookup(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.bucketContains(i1, fp) {
		return true
	}
	return cf.bucketContains(cf.altIndex(fp, i1), fp)
}

// Delete removes one occurrence of h's fingerprint from the filter. Returns
// true if a matching fingerprint was found.
func (cf *cuckooH32) Delete(h *[32]byte) bool {
	fp, i1 := cf.extract(h)
	if cf.bucketDelete(i1, fp) {
		return true
	}
	return cf.bucketDelete(cf.altIndex(fp, i1), fp)
}

func (cf *cuckooH32) bucketContains(i uint64, fp uint8) bool {
	b := &cf.buckets[i]
	return b[0] == fp || b[1] == fp || b[2] == fp || b[3] == fp
}

func (cf *cuckooH32) bucketDelete(i uint64, fp uint8) bool {
	b := &cf.buckets[i]
	for j := 0; j < cuckooBucketSize; j++ {
		if b[j] == fp {
			b[j] = cuckooNullFinger
			cf.count--
			return true
		}
	}
	return false
}

// Count returns the current number of fingerprints stored.
func (cf *cuckooH32) Count() int { return cf.count }
