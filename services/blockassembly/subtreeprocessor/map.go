package subtreeprocessor

import (
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

type SplitSwissMap struct {
	m           map[uint16]*swiss.Map[chainhash.Hash, struct{}]
	mu          map[uint16]*sync.RWMutex
	nrOfBuckets uint16
}

// NewSplitSwissMap creates a new SplitSwissMap with the specified number of buckets and total length.
// The length is divided equally among the buckets.
// This map is safe for concurrent writes, but does not lock when reading.
//
// Parameters:
//   - nrOfBuckets: The number of buckets to split the map into.
//   - length: The total expected length of the map.
//
// Returns:
//   - A pointer to the newly created SplitSwissMap.
func NewSplitSwissMap(nrOfBuckets uint16, length int) *SplitSwissMap {
	m := make(map[uint16]*swiss.Map[chainhash.Hash, struct{}], nrOfBuckets)
	mu := make(map[uint16]*sync.RWMutex, nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = swiss.NewMap[chainhash.Hash, struct{}](uint32(length / int(nrOfBuckets)))
		mu[i] = &sync.RWMutex{}
	}

	return &SplitSwissMap{
		m:           m,
		mu:          mu,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	_, ok := s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Get(hash)
	return ok
}

func (s *SplitSwissMap) Length() int {
	var length int

	for bucket := uint16(0); bucket < s.nrOfBuckets; bucket++ {
		s.mu[bucket].Lock()
		length += s.m[bucket].Count()
		s.mu[bucket].Unlock()
	}

	return length
}

func (s *SplitSwissMap) Buckets() uint16 {
	return s.nrOfBuckets
}

func (s *SplitSwissMap) Put(hash chainhash.Hash) error {
	bucket := txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)

	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	s.m[bucket].Put(hash, struct{}{})

	return nil
}

func (s *SplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash) error {
	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	for _, hash := range hashes {
		s.m[bucket].Put(hash, struct{}{})
	}

	return nil
}

func (s *SplitSwissMap) Iter(f func(hash chainhash.Hash, v struct{}) bool) {
	for _, swissMap := range s.m {
		swissMap.Iter(f)
	}
}

// Clear empties every bucket in place without reallocating bucket arrays or
// the per-bucket swiss.Map. The underlying swiss.Map.Clear walks its existing
// control + group arrays and zeroes them, so capacity is retained for reuse.
// Used to recycle a pooled SplitSwissMap across blocks.
//
// Each per-bucket Clear is independent (the swiss.Map and its RWMutex are
// per-bucket), so we run them in parallel: clearing a ~600 K-entry swiss.Map
// is memory-bandwidth-bound on its key/value/ctrl arrays, and with 1024
// buckets the work is naturally embarrassingly parallel.
func (s *SplitSwissMap) Clear() {
	ncpu := runtime.NumCPU()
	if ncpu > int(s.nrOfBuckets) {
		ncpu = int(s.nrOfBuckets)
	}

	if ncpu < 1 {
		ncpu = 1
	}

	// numWorkers fits in uint16 because it is clamped to nrOfBuckets.
	numWorkers := uint16(ncpu)

	if numWorkers == 1 {
		for bucket := uint16(0); bucket < s.nrOfBuckets; bucket++ {
			s.mu[bucket].Lock()
			s.m[bucket].Clear()
			s.mu[bucket].Unlock()
		}

		return
	}

	var wg sync.WaitGroup

	wg.Add(int(numWorkers))

	for w := uint16(0); w < numWorkers; w++ {
		start := w

		go func() {
			defer wg.Done()

			for bucket := start; bucket < s.nrOfBuckets; bucket += numWorkers {
				s.mu[bucket].Lock()
				s.m[bucket].Clear()
				s.mu[bucket].Unlock()
			}
		}()
	}

	wg.Wait()
}

type txInpointsBucket struct {
	mu sync.Mutex
	m  *swiss.Map[chainhash.Hash, *subtreepkg.TxInpoints]
}

type SplitTxInpointsMap struct {
	buckets     []txInpointsBucket
	nrOfBuckets uint16
}

func NewSplitTxInpointsMap(nrOfBuckets uint16) *SplitTxInpointsMap {
	buckets := make([]txInpointsBucket, nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		buckets[i].m = swiss.NewMap[chainhash.Hash, *subtreepkg.TxInpoints](64)
	}

	return &SplitTxInpointsMap{
		buckets:     buckets,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitTxInpointsMap) Delete(hash chainhash.Hash) bool {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	ok := b.m.Has(hash)
	if ok {
		b.m.Delete(hash)
	}
	b.mu.Unlock()
	return ok
}

func (s *SplitTxInpointsMap) Exists(hash chainhash.Hash) bool {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	ok := b.m.Has(hash)
	b.mu.Unlock()
	return ok
}

func (s *SplitTxInpointsMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	v, ok := b.m.Get(hash)
	b.mu.Unlock()
	return v, ok
}

func (s *SplitTxInpointsMap) Length() int {
	length := 0
	for i := uint16(0); i < s.nrOfBuckets; i++ {
		b := &s.buckets[i]
		b.mu.Lock()
		length += b.m.Count()
		b.mu.Unlock()
	}
	return length
}

func (s *SplitTxInpointsMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	b.m.Put(hash, inpoints)
	b.mu.Unlock()
}

func (s *SplitTxInpointsMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	if existing, ok := b.m.Get(hash); ok {
		b.mu.Unlock()
		return existing, false
	}
	b.m.Put(hash, inpoints)
	b.mu.Unlock()
	return inpoints, true
}

func (s *SplitTxInpointsMap) Clear() {
	for i := uint16(0); i < s.nrOfBuckets; i++ {
		b := &s.buckets[i]
		b.mu.Lock()
		b.m = swiss.NewMap[chainhash.Hash, *subtreepkg.TxInpoints](64)
		b.mu.Unlock()
	}
}

// ParallelBulkSetIfNotExists inserts multiple entries in parallel, grouped by bucket.
// Each bucket is processed by a separate goroutine with a single lock acquisition.
// wasSet[i] is set to true if hashes[i] was newly inserted (not already present).
func (s *SplitTxInpointsMap) ParallelBulkSetIfNotExists(
	hashes []chainhash.Hash,
	inpoints []*subtreepkg.TxInpoints,
	wasSet []bool,
) {
	n := len(hashes)
	if n == 0 {
		return
	}

	if len(inpoints) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(inpoints) must equal len(hashes)")
	}
	if wasSet == nil || len(wasSet) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(wasSet) must equal len(hashes)")
	}

	// Phase 1: Group indices by bucket (O(N), no locks)
	bucketIndices := make([][]int, s.nrOfBuckets)
	for i := 0; i < n; i++ {
		bucket := txmap.Bytes2Uint16Buckets(hashes[i], s.nrOfBuckets)
		bucketIndices[bucket] = append(bucketIndices[bucket], i)
	}

	// Phase 2: Process each non-empty bucket in parallel (one lock per bucket)
	var wg sync.WaitGroup
	for bIdx := uint16(0); bIdx < s.nrOfBuckets; bIdx++ {
		indices := bucketIndices[bIdx]
		if len(indices) == 0 {
			continue
		}
		wg.Add(1)
		go func(b *txInpointsBucket, indices []int) {
			defer wg.Done()
			b.mu.Lock()
			for _, idx := range indices {
				if !b.m.Has(hashes[idx]) {
					b.m.Put(hashes[idx], inpoints[idx])
					wasSet[idx] = true
				}
			}
			b.mu.Unlock()
		}(&s.buckets[bIdx], indices)
	}
	wg.Wait()
}

// Buckets returns the configured bucket count.
func (s *SplitTxInpointsMap) Buckets() uint16 {
	return s.nrOfBuckets
}

// BucketFor returns the destination bucket index for a given hash, matching the
// scheme used by Get/Set/SetIfNotExists.
func (s *SplitTxInpointsMap) BucketFor(hash chainhash.Hash) uint16 {
	return txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)
}

// PutMultiBucketTxInpoints inserts a batch of (hash, inpoints) pairs that all
// belong to the same bucket. Takes the per-bucket lock exactly once for the
// whole batch instead of once per entry.
//
// All entries MUST belong to the named bucket; callers are expected to have
// partitioned by BucketFor beforehand. The returned slice has length
// min(len(keys), len(values)); wasInserted[i] is true if keys[i] was newly
// added, false if it already existed.
func (s *SplitTxInpointsMap) PutMultiBucketTxInpoints(bucket uint16, keys []chainhash.Hash, values []*subtreepkg.TxInpoints) []bool {
	n := len(keys)
	if len(values) < n {
		n = len(values)
	}
	wasInserted := make([]bool, n)
	if n == 0 {
		return wasInserted
	}
	b := &s.buckets[bucket]
	b.mu.Lock()
	for i := 0; i < n; i++ {
		if !b.m.Has(keys[i]) {
			b.m.Put(keys[i], values[i])
			wasInserted[i] = true
		}
	}
	b.mu.Unlock()
	return wasInserted
}
