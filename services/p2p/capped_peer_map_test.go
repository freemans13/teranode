package p2p

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCappedPeerMapFloodBounded pins issue 1409: a distinct-hash flood must
// not grow the attribution map past its cap between sweeps. Before the inline
// cap, every announcement inserted unconditionally and only the timer-driven
// sweep clawed the map back, so peak memory tracked the flood size and the
// sweep's full-map sort scaled with it.
func TestCappedPeerMapFloodBounded(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(100)

	now := time.Now()
	for i := 0; i < 250; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	require.Equal(t, 100, m.Len(), "map must not grow past its cap under a distinct-hash flood")
	require.Equal(t, int64(150), m.EvictedSinceLastRead())
	require.Equal(t, int64(0), m.EvictedSinceLastRead(), "counter must reset on read")
}

// TestCappedPeerMapKeepsNewestUnderFlood pins the security-critical direction
// of the eviction policy: a flooder that fills every slot with junk must NOT
// be able to suppress attribution for the announcement that arrives next.
// Refusing new keys at capacity would let an attacker switch off the node's
// only automatic ban path for invalid blocks; evicting the oldest keeps the
// newest announcement attributable.
func TestCappedPeerMapKeepsNewestUnderFlood(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(50)

	now := time.Now()
	for i := 0; i < 500; i++ {
		m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	// The honest announcement arriving after the flood must be recorded.
	m.Store("real-block", peerMapEntry{peerID: "honest-peer", timestamp: now})

	entry, ok := m.Load("real-block")
	require.True(t, ok, "announcement after a full-map flood must still be attributable")
	require.Equal(t, "honest-peer", entry.peerID)
	require.Equal(t, 50, m.Len())

	// The oldest junk is what got dropped.
	_, ok = m.Load("junk-0")
	require.False(t, ok, "oldest entries are the ones evicted")
}

// TestStorePeerMapEntryKeepsAttributionUnderFlood drives the production insert
// path: after a flood through storePeerMapEntry fills the map, the next
// announcement must still be attributable via the same lookup the
// invalid-block ban path uses.
func TestStorePeerMapEntryKeepsAttributionUnderFlood(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.blockPeerMap.setMaxSize(20)

	now := time.Now()
	for i := 0; i < 200; i++ {
		s.storePeerMapEntry(&s.blockPeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	require.Equal(t, 20, s.blockPeerMap.Len(), "gossip inserts must not grow the map past the cap")

	realHash := fmt.Sprintf("%064d", 999999)
	s.storePeerMapEntry(&s.blockPeerMap, realHash, "peer-to-ban", now)

	peerID, err := s.getPeerFromMap(&s.blockPeerMap, realHash, "block")
	require.NoError(t, err, "the ban path must still find the announcing peer after a flood")
	require.Equal(t, "peer-to-ban", peerID)
}

// TestCappedPeerMapUpdateAndDelete pins the non-growth operations: updating an
// existing key refreshes it (and its recency) without evicting, and a delete
// frees a slot.
func TestCappedPeerMapUpdateAndDelete(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(2)

	now := time.Now()
	m.Store("a", peerMapEntry{peerID: "p1", timestamp: now})
	m.Store("b", peerMapEntry{peerID: "p1", timestamp: now})

	// Update 'a': no eviction, and 'a' becomes the most recent.
	m.Store("a", peerMapEntry{peerID: "p2", timestamp: now})
	require.Equal(t, 2, m.Len())
	require.Equal(t, int64(0), m.EvictedSinceLastRead(), "updating an existing key must not evict")

	entry, ok := m.Load("a")
	require.True(t, ok)
	require.Equal(t, "p2", entry.peerID)

	// A new key now evicts 'b', which is the oldest after 'a' was refreshed.
	m.Store("c", peerMapEntry{peerID: "p3", timestamp: now})
	_, ok = m.Load("b")
	require.False(t, ok, "refreshed key must outlive the un-refreshed one")
	_, ok = m.Load("a")
	require.True(t, ok)

	m.Delete("a")
	require.Equal(t, 1, m.Len())
}

// TestCappedPeerMapZeroValue pins the zero-value affordance the test fixtures
// rely on: no cap configured means unbounded storage, and all methods are
// usable without construction.
func TestCappedPeerMapZeroValue(t *testing.T) {
	var m cappedPeerMap

	now := time.Now()
	for i := 0; i < 300; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 300, m.Len())
	require.Equal(t, int64(0), m.EvictedSinceLastRead())

	m.Clear()
	require.Equal(t, 0, m.Len())

	// Usable again after Clear.
	m.Store("after-clear", peerMapEntry{peerID: "p", timestamp: now})
	require.Equal(t, 1, m.Len())
}

// TestCappedPeerMapRangeDeletes pins that the sweep's usage — deleting while
// ranging — is safe, and that iteration runs oldest-first.
func TestCappedPeerMapRangeDeletes(t *testing.T) {
	var m cappedPeerMap

	base := time.Now()
	for i := 0; i < 10; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: base.Add(time.Duration(i) * time.Second)})
	}

	var seen []string

	m.Range(func(hash string, _ peerMapEntry) bool {
		seen = append(seen, hash)
		m.Delete(hash)

		return true
	})

	require.Len(t, seen, 10)
	require.Equal(t, "hash-0", seen[0], "iteration must run oldest-first")
	require.Equal(t, "hash-9", seen[9])
	require.Equal(t, 0, m.Len())
}
