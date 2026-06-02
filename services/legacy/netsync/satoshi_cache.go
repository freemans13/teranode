package netsync

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
)

// satoshiCache is an off-heap, byte-bounded recency cache of confirmed output
// *satoshi values* keyed by outpoint. It exists only during legacy catch-up in
// quickValidationMode (syncing to a known checkpoint), where the chain is
// trusted and scripts are never re-validated — so the only thing a fee
// calculation needs from a parent output is its satoshis.
//
// It deliberately stores satoshis ONLY (8 bytes/output), not the locking script:
// below the checkpoint the script is never consulted, so caching it would waste
// ~4x the memory and shrink the recency window. A smaller per-entry footprint
// means more outputs fit in the same budget, raising the cross-block hit rate
// (on mainnet ~73% of spent outputs were created within the previous ~1000
// blocks; same-block parents are resolved from the in-memory txMap, not here).
//
// It is NOT a store wrapper: putTx (populate) and satoshis (consult) are called
// explicitly from the legacy-sync paths (createUtxos / createSubtrees). Backed
// by txmetacache's mmap-based ImprovedCache so a multi-hundred-MB cache lives
// off the Go heap and adds no GC pressure under the catch-up memory ceiling.
//
// Correctness: a created output's satoshis are immutable, so a cache hit is
// always correct — there is no staleness/coherence hazard. The cache feeds fee
// computation only; spend validation is authoritative in the UTXO store, so a
// cached entry for an output later spent is harmless (never consulted for it).
//
// All methods are nil-safe: a nil *satoshiCache (feature off) is a no-op that
// always misses, so call sites need no nil guard.
type satoshiCache struct {
	cache  *txmetacache.ImprovedCache
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newSatoshiCache creates an off-heap satoshi cache bounded to maxBytes.
func newSatoshiCache(maxBytes int) (*satoshiCache, error) {
	c, err := txmetacache.New(maxBytes, txmetacache.Native)
	if err != nil {
		return nil, err
	}

	return &satoshiCache{cache: c}, nil
}

// satoshiOutpointKey encodes an outpoint as 36 bytes: 32-byte tx hash ‖ 4-byte
// LE index.
func satoshiOutpointKey(hash *chainhash.Hash, idx uint32) []byte {
	k := make([]byte, 36)
	copy(k, hash[:])
	binary.LittleEndian.PutUint32(k[32:], idx)

	return k
}

// put caches a single output's satoshis (8-byte LE) keyed by its outpoint. Set
// errors are non-fatal: a later miss just falls through to the store, so block
// processing is never failed on a cache write.
func (p *satoshiCache) put(hash *chainhash.Hash, idx uint32, satoshis uint64) {
	if p == nil {
		return
	}

	var v [8]byte
	binary.LittleEndian.PutUint64(v[:], satoshis)

	_ = p.cache.Set(satoshiOutpointKey(hash, idx), v[:])
}

// putTx caches every output's satoshis keyed by its outpoint, so later blocks'
// inputs spending these outputs can have their fee contribution resolved from
// memory.
func (p *satoshiCache) putTx(tx *bt.Tx) {
	if p == nil || tx == nil {
		return
	}

	h := tx.TxIDChainHash()

	for i, out := range tx.Outputs {
		if out == nil {
			continue
		}

		idx, err := safeconversion.Int64ToUint32(int64(i))
		if err != nil {
			continue
		}

		p.put(h, idx, out.Satoshis)
	}
}

// satoshis returns the cached satoshis for the given parent outpoint and true on
// a cache hit, or (0, false) on a miss. dst is a caller-owned scratch buffer
// reused across calls (per goroutine) to avoid per-lookup allocations; it is
// reset to zero-length on entry so a reused buffer yields exactly this entry.
func (p *satoshiCache) satoshis(hash *chainhash.Hash, idx uint32, dst *[]byte) (uint64, bool) {
	if p == nil {
		return 0, false
	}

	*dst = (*dst)[:0]

	if err := p.cache.Get(dst, satoshiOutpointKey(hash, idx)); err != nil {
		p.misses.Add(1)
		return 0, false
	}

	b := *dst
	if len(b) < 8 {
		p.misses.Add(1)
		return 0, false
	}

	p.hits.Add(1)

	return binary.LittleEndian.Uint64(b[:8]), true
}

// stats returns and resets the hit/miss counters (called per block for logging).
func (p *satoshiCache) stats() (hits, misses uint64) {
	if p == nil {
		return 0, 0
	}

	return p.hits.Swap(0), p.misses.Swap(0)
}
