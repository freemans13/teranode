package pruner

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// PrunedTxSet is a concurrent sharded set tracking TXIDs of records pruned during a session.
// It is used to skip wasteful parent updates for parents that have already been pruned.
type PrunedTxSet struct {
	shards []prunedTxShard
	mask   uint8 // shardCount - 1, for fast modulo via bitwise AND
	count  atomic.Int64
}

type prunedTxShard struct {
	mu sync.Mutex
	m  map[chainhash.Hash]struct{}
}

// NewPrunedTxSet creates a new PrunedTxSet with the given number of shards.
// shardCount must be a power of 2 (will be rounded up if not).
func NewPrunedTxSet(shardCount int) *PrunedTxSet {
	// Round up to next power of 2
	n := 1
	for n < shardCount {
		n <<= 1
	}
	if n > 256 {
		n = 256 // cap at 256 shards — more than enough for 400K entries
	}

	s := &PrunedTxSet{
		shards: make([]prunedTxShard, n),
		mask:   uint8(n - 1),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[chainhash.Hash]struct{}, 64)
	}
	return s
}

func (s *PrunedTxSet) shard(h chainhash.Hash) *prunedTxShard {
	return &s.shards[h[0]&s.mask]
}

// Add registers a TXID as pruned. Duplicate adds are idempotent and do not affect the count.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	sh := s.shard(h)
	sh.mu.Lock()
	_, exists := sh.m[h]
	if !exists {
		sh.m[h] = struct{}{}
	}
	sh.mu.Unlock()
	if !exists {
		s.count.Add(1)
	}
}

// Contains checks if a TXID is in the set without removing it.
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	_, ok := sh.m[h]
	sh.mu.Unlock()
	return ok
}

// CheckAndRemove checks if a TXID is in the set. If found, removes it and returns true.
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	_, ok := sh.m[h]
	if ok {
		delete(sh.m, h)
	}
	sh.mu.Unlock()
	if ok {
		s.count.Add(-1)
	}
	return ok
}

// Len returns the approximate number of entries in the set.
func (s *PrunedTxSet) Len() int {
	return int(s.count.Load())
}
