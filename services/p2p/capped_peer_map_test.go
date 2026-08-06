package p2p

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
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

// requireMapConsistent asserts the map and its insertion-order list describe
// the same set of entries. They are two structures kept in step by hand, so a
// path that updates one and not the other would leak: entries orphaned in the
// list are invisible to Len() yet still hold memory, which is the very failure
// issue 1409 is about.
func requireMapConsistent(t *testing.T, m *cappedPeerMap) {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.order == nil {
		require.Empty(t, m.entries, "map must be empty when the order list is unset")
		return
	}

	require.Equal(t, len(m.entries), m.order.Len(), "order list and map must hold the same number of entries")

	for element := m.order.Front(); element != nil; element = element.Next() {
		node := element.Value.(*peerMapNode)

		stored, ok := m.entries[node.hash]
		require.True(t, ok, "order list holds %q but the map does not", node.hash)
		require.Same(t, element, stored, "map and order list disagree about %q", node.hash)
	}
}

// TestCappedPeerMapConcurrent exercises the mutex this type introduced in place
// of a sync.Map. Run under -race it catches unsynchronised access; the
// consistency check afterwards catches the order-list leak that a race detector
// cannot see. Gossip drives every one of these operations concurrently in
// production: handlers store, the ban path loads, and the sweep ranges and
// deletes.
func TestCappedPeerMapConcurrent(t *testing.T) {
	const (
		maxSize    = 64
		iterations = 400
		storers    = 8
		deleters   = 4
		rangers    = 2
	)

	var m cappedPeerMap

	m.setMaxSize(maxSize)

	now := time.Now()

	var wg sync.WaitGroup

	for g := 0; g < storers; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				m.Store(fmt.Sprintf("hash-%d-%d", g, i), peerMapEntry{peerID: fmt.Sprintf("peer-%d", g), timestamp: now})
			}
		}(g)
	}

	for g := 0; g < deleters; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				m.Delete(fmt.Sprintf("hash-%d-%d", g, i))
				m.Load(fmt.Sprintf("hash-%d-%d", g, i))
				m.Len()
			}
		}(g)
	}

	// The sweep's shape: range the map and delete from inside the callback.
	for g := 0; g < rangers; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < iterations/10; i++ {
				m.Range(func(hash string, entry peerMapEntry) bool {
					if entry.timestamp.Before(now) {
						m.Delete(hash)
					}

					return true
				})

				m.DeleteExpired(now.Add(-time.Hour))
			}
		}()
	}

	wg.Wait()

	require.LessOrEqual(t, m.Len(), maxSize, "the cap must hold under concurrent inserts")
	requireMapConsistent(t, &m)
}

// TestCappedPeerMapDeleteExpired pins the TTL sweep's single-pass expiry:
// entries older than the cutoff go, newer ones stay, and the structures stay
// in step.
func TestCappedPeerMapDeleteExpired(t *testing.T) {
	var m cappedPeerMap

	base := time.Now()
	for i := 0; i < 10; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: base.Add(time.Duration(i) * time.Minute)})
	}

	// Cutoff at +5m expires hash-0..hash-4, whose timestamps precede it.
	require.Equal(t, 5, m.DeleteExpired(base.Add(5*time.Minute)))
	require.Equal(t, 5, m.Len())

	_, ok := m.Load("hash-4")
	require.False(t, ok, "an entry older than the cutoff must be expired")
	_, ok = m.Load("hash-5")
	require.True(t, ok, "an entry at the cutoff must survive")

	requireMapConsistent(t, &m)

	// Nothing left to expire is not an error, and a zero value is safe.
	require.Equal(t, 0, m.DeleteExpired(base))

	var zero cappedPeerMap

	require.Equal(t, 0, zero.DeleteExpired(base))
}

// TestCappedPeerMapClearRetainsMaxSize pins that Clear frees the entries but
// keeps the configured cap. Stop calls Clear, so a cap dropped here would leave
// the maps unbounded for the rest of the process's life.
func TestCappedPeerMapClearRetainsMaxSize(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(3)

	now := time.Now()
	for i := 0; i < 3; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	m.Clear()
	require.Equal(t, 0, m.Len())

	for i := 0; i < 50; i++ {
		m.Store(fmt.Sprintf("after-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 3, m.Len(), "the cap must survive Clear")
	requireMapConsistent(t, &m)
}

// TestApplyPeerMapLimits pins that the configured size actually reaches both
// attribution maps. Nothing else asserts it, so a refactor could drop the
// wiring and leave the maps unbounded with every other test still green.
func TestApplyPeerMapLimits(t *testing.T) {
	now := time.Now()

	fill := func(t *testing.T, s *Server, n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			s.blockPeerMap.Store(fmt.Sprintf("b-%d", i), peerMapEntry{peerID: "p", timestamp: now})
			s.subtreePeerMap.Store(fmt.Sprintf("s-%d", i), peerMapEntry{peerID: "p", timestamp: now})
		}
	}

	t.Run("configured size binds both maps", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = 7
		tSettings.P2P.PeerMapTTL = 3 * time.Minute

		s := &Server{}
		s.applyPeerMapLimits(tSettings)

		require.Equal(t, 7, s.peerMapMaxSize)
		require.Equal(t, 3*time.Minute, s.peerMapTTL)

		fill(t, s, 20)
		require.Equal(t, 7, s.blockPeerMap.Len())
		require.Equal(t, 7, s.subtreePeerMap.Len())
	})

	t.Run("unset settings fall back to the service defaults", func(t *testing.T) {
		s := &Server{}
		s.applyPeerMapLimits(&settings.Settings{})

		require.Equal(t, defaultPeerMapMaxSize, s.peerMapMaxSize)
		require.Equal(t, defaultPeerMapTTL, s.peerMapTTL)

		// Bounded, not unbounded: the default cap must reach the maps too.
		fill(t, s, 5)
		require.Equal(t, 5, s.blockPeerMap.Len())
		require.Equal(t, 5, s.subtreePeerMap.Len())
	})
}
