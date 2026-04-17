package blockchain

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

// receivedAtStore records the wall-clock time a block header was first seen by
// this node. It is consulted by the subtree-only liveness gate to decide whether
// validation can skip the subtreeData download path.
//
// Entries expire after a TTL (set large relative to SubtreeValidation.LivenessWindow
// so a live-for-the-window header is never evicted prematurely). Expired or absent
// entries are reported as "not found" — the gate treats that as "not live" and
// falls back to subtreeData, which is safe.
//
// Within a single TTL window the first stamp for a given hash wins and repeated
// inserts are no-ops — this matches the semantic "when did we first learn about
// this header?" After the TTL elapses the entry disappears; if the same hash is
// stamped again later (unusual in practice for a growing chain) the store
// records the new observation rather than resurrecting a stale one.
type receivedAtStore struct {
	mu sync.Mutex
	m  *expiringmap.ExpiringMap[chainhash.Hash, time.Time]
}

func newReceivedAtStore(ttl time.Duration) *receivedAtStore {
	return &receivedAtStore{
		m: expiringmap.New[chainhash.Hash, time.Time](ttl),
	}
}

// stamp records the first-seen time for hash. Subsequent calls for the same
// hash within the TTL do not overwrite the initial stamp. After the TTL
// elapses the prior entry has been evicted; a new stamp then records the new
// observation rather than reviving the expired one.
func (s *receivedAtStore) stamp(hash *chainhash.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m.Get(*hash); ok {
		return
	}
	s.m.Set(*hash, time.Now())
}

// lookup returns the stamp and true if the hash was seen within the TTL,
// or a zero time and false otherwise.
func (s *receivedAtStore) lookup(hash *chainhash.Hash) (time.Time, bool) {
	return s.m.Get(*hash)
}

// Stop releases resources owned by the underlying expiring map, including any
// background cleanup goroutine started for non-zero TTLs. Safe to call on a
// nil receiver or double-call.
func (s *receivedAtStore) Stop() {
	if s == nil || s.m == nil {
		return
	}
	s.m.Stop()
}

// receivedAtTTL returns the TTL for the receivedAt store, ensuring the store
// never evicts a header while it would still be considered "live" by the
// subtree-only gate. The floor of 30 minutes prevents misconfigured tiny
// LivenessWindows from shrinking the TTL so far that headers time out before
// any gate check happens; the multiplier provides headroom for clock drift
// and operator changes.
func receivedAtTTL(livenessWindow time.Duration) time.Duration {
	const floor = 30 * time.Minute
	if scaled := 2 * livenessWindow; scaled > floor {
		return scaled
	}
	return floor
}
