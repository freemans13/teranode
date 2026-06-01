package netsync

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
)

// parentOutputCache is an off-heap, byte-bounded recency cache of confirmed
// transaction outputs (locking script + satoshis) keyed by outpoint. It exists
// only during legacy catch-up, where block processing is sequential and spends
// exhibit strong temporal locality — on mainnet ~73% of spent outputs were
// created within the previous ~1000 blocks. It lets extendTransactions resolve
// previous outputs from memory instead of the UTXO store, attacking the
// dominant per-block read cost (the decorate) during IBD.
//
// It is deliberately NOT a store wrapper: the populate (putTx) and consult (get)
// calls are explicit in the legacy-sync code paths (createUtxos / extendTransactions).
// It is backed by txmetacache's mmap-based ImprovedCache, so a multi-hundred-MB
// cache lives off the Go heap and adds no GC pressure under the catch-up memory
// ceiling (GOGC=off + GOMEMLIMIT).
//
// Correctness: a created output's (script, satoshis) is immutable, so a cache
// hit is always correct — there is no staleness/coherence hazard. The cache
// feeds transaction *extension* only; spend validation remains authoritative in
// the UTXO store, so a cached entry for an output that was later spent is
// harmless (it is never consulted for spentness).
type parentOutputCache struct {
	cache  *txmetacache.ImprovedCache
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newParentOutputCache creates an off-heap output cache bounded to maxBytes.
func newParentOutputCache(maxBytes int) (*parentOutputCache, error) {
	c, err := txmetacache.New(maxBytes, txmetacache.Native)
	if err != nil {
		return nil, err
	}

	return &parentOutputCache{cache: c}, nil
}

// outpointKey encodes an outpoint as 36 bytes: 32-byte tx hash ‖ 4-byte LE index.
func outpointKey(hash *chainhash.Hash, idx uint32) []byte {
	k := make([]byte, 36)
	copy(k, hash[:])
	binary.LittleEndian.PutUint32(k[32:], idx)

	return k
}

// putTx caches every (spendable) output of tx — value = 8-byte LE satoshis ‖
// locking script — keyed by its outpoint, so later blocks' inputs that spend
// these outputs can be extended from memory.
func (p *parentOutputCache) putTx(tx *bt.Tx) {
	if p == nil || tx == nil {
		return
	}

	h := tx.TxIDChainHash()

	for i, out := range tx.Outputs {
		if out == nil || out.LockingScript == nil {
			continue
		}

		script := *out.LockingScript

		v := make([]byte, 8+len(script))
		binary.LittleEndian.PutUint64(v[:8], out.Satoshis)
		copy(v[8:], script)

		// Set errors are non-fatal: a cache miss later just falls through to the
		// store. Never fail block processing on a cache write.
		idx, err := safeconversion.Int64ToUint32(int64(i))
		if err != nil {
			continue
		}

		_ = p.cache.Set(outpointKey(h, idx), v)
	}
}

// fillInput populates input.PreviousTxScript/Satoshis from the cache for the
// given parent outpoint, returning true on a cache hit. The script is copied out
// of the cache's scratch buffer so a subsequent Get (which reuses dst) cannot
// corrupt it. Increments hit/miss counters.
func (p *parentOutputCache) fillInput(input *bt.Input, dst *[]byte) bool {
	if p == nil || input == nil {
		return false
	}

	// ImprovedCache.Get appends to dst; reset to zero-length so a reused buffer
	// yields exactly this entry's value rather than accumulating prior lookups.
	*dst = (*dst)[:0]

	if err := p.cache.Get(dst, outpointKey(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)); err != nil {
		p.misses.Add(1)
		return false
	}

	b := *dst
	if len(b) < 8 {
		p.misses.Add(1)
		return false
	}

	satoshis := binary.LittleEndian.Uint64(b[:8])

	script := make([]byte, len(b)-8)
	copy(script, b[8:])

	input.PreviousTxScript = bscript.NewFromBytes(script)
	input.PreviousTxSatoshis = satoshis
	p.hits.Add(1)

	return true
}

// stats returns and resets the hit/miss counters (called per block for logging).
func (p *parentOutputCache) stats() (hits, misses uint64) {
	if p == nil {
		return 0, 0
	}

	return p.hits.Swap(0), p.misses.Swap(0)
}
