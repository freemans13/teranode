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

	// Eviction count is exactly the overflow — cost tracks the flood only in
	// the sense that each surplus insert drops exactly one entry, never more.
	evictions := m.EvictionsSinceLastRead()
	require.Equal(t, int64(150), evictions.total)
	require.Equal(t, "attacker", evictions.topPeer, "a single flooder must be named")
	require.Equal(t, int64(150), evictions.topCount)
	require.False(t, evictions.spread)
	require.Equal(t, "all from peer attacker", evictions.String())

	require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "counter must reset on read")
}

// TestCappedPeerMapEvictionAttribution pins the signal the at-capacity warning
// relies on: an operator has to be able to tell sustained legitimate
// throughput (pressure spread across peers, where raising the cap helps) from
// one peer spraying hashes (where raising the cap only buys the attacker more
// memory — issue 1503). The tracker is itself bounded, so past
// evictorTrackLimit contributors it reports the spread instead of a name.
func TestCappedPeerMapEvictionAttribution(t *testing.T) {
	now := time.Now()

	t.Run("one dominant contributor is named", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		for i := 0; i < 100; i++ {
			peerID := "busy-peer"
			if i%10 == 0 {
				peerID = "quiet-peer"
			}

			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: peerID, timestamp: now})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(90), evictions.total)
		require.Equal(t, "busy-peer", evictions.topPeer)
		require.False(t, evictions.spread)
		require.Contains(t, evictions.String(), "top contributor peer busy-peer")
	})

	t.Run("pressure spread past the tracking limit reports the spread", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		for i := 0; i < 200; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("peer-%d", i%(evictorTrackLimit*4)),
				timestamp: now,
			})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(190), evictions.total)
		require.True(t, evictions.spread, "more contributors than the tracker can name must report as spread")
		require.Contains(t, evictions.String(), "spread across more than")

		// The tracker itself must stay bounded: that is the failure mode of
		// issue 1409, and a diagnostic is no exception.
		m.mu.Lock()
		require.LessOrEqual(t, len(m.evictors), evictorTrackLimit)
		m.mu.Unlock()
	})

	t.Run("the flooder is named, not the honest peers it displaced", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		// Honest peers fill the map first, so every entry the flood evicts
		// belongs to one of them.
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("honest-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("honest-peer-%d", i),
				timestamp: now,
			})
		}

		require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "filling to the cap must not evict")

		// Flood less than the cap, so every entry dropped belongs to an honest
		// peer and none of the attacker's own entries are reached.
		for i := 0; i < 8; i++ {
			m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		// Attribution must follow the insert that forced the eviction, not the
		// entry that was dropped: blaming the dropped entries would name eight
		// honest peers, one eviction each, and point the operator at the
		// victims of the flood rather than its source.
		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(8), evictions.total)
		require.Equal(t, "attacker", evictions.topPeer, "the peer applying the pressure must be named")
		require.Equal(t, int64(8), evictions.topCount)
		require.False(t, evictions.spread)
	})

	t.Run("attribution resets with the counter", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(2)

		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		require.Equal(t, int64(8), m.EvictionsSinceLastRead().total)

		next := m.EvictionsSinceLastRead()
		require.Equal(t, int64(0), next.total)
		require.Empty(t, next.topPeer)
		require.Equal(t, "none", next.String())
	})
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

// TestStorePeerMapEntryKeepsSubtreeAttributionUnderFlood mirrors the block-side
// flood test on the subtree path, through the same two helpers the handlers
// use. The subtree map is the one that realistically sits at capacity — by the
// sizing analysis roughly 16 subtrees a second fills the default cap for a
// whole TTL window — and storePeerMapEntry(&s.subtreePeerMap, …) has exactly
// one call site, all of it production code, so nothing else would catch the
// subtree handler being pointed at the wrong map.
func TestStorePeerMapEntryKeepsSubtreeAttributionUnderFlood(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.subtreePeerMap.setMaxSize(20)

	now := time.Now()
	for i := 0; i < 200; i++ {
		s.storePeerMapEntry(&s.subtreePeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	require.Equal(t, 20, s.subtreePeerMap.Len(), "gossip inserts must not grow the subtree map past the cap")
	require.Equal(t, 0, s.blockPeerMap.Len(), "the subtree path must not touch the block map")

	realHash := fmt.Sprintf("%064d", 999999)
	s.storePeerMapEntry(&s.subtreePeerMap, realHash, "peer-to-ban", now)

	peerID, err := s.getPeerFromMap(&s.subtreePeerMap, realHash, "subtree")
	require.NoError(t, err, "the ban path must still find the announcing peer after a flood")
	require.Equal(t, "peer-to-ban", peerID)
}

// TestCappedPeerMapCostBoundedByCapNotFlood pins the other half of what issue
// 1409 asks for: not just that the map stays small, but that the work done to
// keep it small does not scale with the flood.
//
// Two properties, both deterministic. Inserting at capacity does a fixed
// number of allocations regardless of how many hashes have already been thrown
// at the map — the old scheme let the surplus accumulate and then paid for a
// full-map sort. And the sweep, the only whole-map walk left, allocates
// nothing and visits at most cap entries however large the flood was; the
// removed sort.Slice snapshotted and sorted every one of them.
func TestCappedPeerMapCostBoundedByCapNotFlood(t *testing.T) {
	const (
		maxSize    = 64
		smallFlood = 1_000
		largeFlood = 100_000
	)

	now := time.Now()

	// Pre-generate keys so the measured closure allocates nothing of its own.
	keys := make([]string, largeFlood+2_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("hash-%d", i)
	}

	newFloodedMap := func(flood int) *cappedPeerMap {
		m := &cappedPeerMap{}
		m.setMaxSize(maxSize)

		for i := 0; i < flood; i++ {
			m.Store(keys[i], peerMapEntry{peerID: "attacker", timestamp: now})
		}

		require.Equal(t, maxSize, m.Len(), "a flood of %d must leave exactly the cap behind", flood)

		return m
	}

	allocsPerStoreAfter := func(flood int) float64 {
		m := newFloodedMap(flood)
		next := flood

		return testing.AllocsPerRun(1_000, func() {
			m.Store(keys[next], peerMapEntry{peerID: "attacker", timestamp: now})
			next++
		})
	}

	small := allocsPerStoreAfter(smallFlood)
	large := allocsPerStoreAfter(largeFlood)
	require.InDelta(t, small, large, 0.5,
		"insert cost at capacity must not depend on how many hashes were flooded before it")

	// The sweep is the only whole-map walk left. However large the flood was,
	// it visits at most cap entries and allocates nothing — where the removed
	// sort.Slice snapshotted and sorted every entry the flood had produced.
	m := newFloodedMap(largeFlood)

	walkAllocs := testing.AllocsPerRun(100, func() {
		// Cutoff in the past: walks all maxSize entries, removes none, so
		// every run measures exactly the same work.
		m.DeleteExpired(now.Add(-time.Hour))
	})
	require.Zero(t, walkAllocs, "the sweep's whole-map walk must allocate nothing")

	require.Equal(t, maxSize, m.DeleteExpired(now.Add(time.Hour)),
		"the sweep touches at most cap entries after a %d-hash flood", largeFlood)
	require.Equal(t, 0, m.Len())
}

// BenchmarkCappedPeerMapStoreUnderFlood is the wall-clock companion to
// TestCappedPeerMapCostBoundedByCapNotFlood: ns/op must stay flat as the flood
// grows by two orders of magnitude. Against the removed sweep-time sort it
// would not have.
func BenchmarkCappedPeerMapStoreUnderFlood(b *testing.B) {
	now := time.Now()

	for _, flood := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("flood=%d", flood), func(b *testing.B) {
			var m cappedPeerMap

			m.setMaxSize(defaultPeerMapMaxSize)

			keys := make([]string, flood)
			for i := range keys {
				keys[i] = fmt.Sprintf("hash-%d", i)
			}

			for i := 0; i < flood; i++ {
				m.Store(keys[i], peerMapEntry{peerID: "attacker", timestamp: now})
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				m.Store(keys[i%flood], peerMapEntry{peerID: "attacker", timestamp: now})
			}
		})
	}
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
	require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "updating an existing key must not evict")

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

// TestCappedPeerMapZeroValueIsBounded pins that the control fails CLOSED. The
// zero value is reachable from any bare Server literal, and from any future
// construction path that forgets applyPeerMapLimits; if that meant "unbounded"
// then the one guarantee this type exists to provide would be off by default
// and every test would still be green. An unconfigured map takes the default
// cap instead — there is no unbounded mode.
func TestCappedPeerMapZeroValueIsBounded(t *testing.T) {
	var m cappedPeerMap

	now := time.Now()
	for i := 0; i < defaultPeerMapMaxSize+250; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, defaultPeerMapMaxSize, m.Len(), "an unconfigured map must still be bounded")
	require.Equal(t, int64(250), m.EvictionsSinceLastRead().total)

	// A non-positive cap is not an opt-out either.
	m.setMaxSize(0)
	m.Store("still-bounded", peerMapEntry{peerID: "p", timestamp: now})
	require.Equal(t, defaultPeerMapMaxSize, m.Len())

	m.Clear()
	require.Equal(t, 0, m.Len())

	// Usable again after Clear.
	m.Store("after-clear", peerMapEntry{peerID: "p", timestamp: now})
	require.Equal(t, 1, m.Len())
	requireMapConsistent(t, &m)
}

// TestCappedPeerMapConvergesWhenOverCap pins that a map holding more than its
// cap drains back down instead of sitting over-cap forever. Evicting exactly
// one entry per insert would hold it at the same size indefinitely, and the
// sweep no longer has a size pass to correct that.
func TestCappedPeerMapConvergesWhenOverCap(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(50)

	now := time.Now()
	for i := 0; i < 50; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 50, m.Len())

	// Lower the cap on a populated map: the next insert must bring it all the
	// way down, not shave off a single entry.
	m.setMaxSize(5)
	m.Store("after-shrink", peerMapEntry{peerID: "p", timestamp: now})

	require.Equal(t, 5, m.Len(), "an over-cap map must converge to the new cap")
	requireMapConsistent(t, &m)
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
		sweepers   = 2
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

	// The sweep's shape: a whole-map expiry pass, plus the eviction-counter
	// read that follows it, running against live inserts.
	for g := 0; g < sweepers; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < iterations/10; i++ {
				m.DeleteExpired(now.Add(-time.Hour))
				m.DeleteExpired(now.Add(time.Hour))
				m.EvictionsSinceLastRead()
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

	// Evict a few so there is pressure recorded to carry across the Clear.
	for i := 0; i < 5; i++ {
		m.Store(fmt.Sprintf("pressure-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	m.Clear()
	require.Equal(t, 0, m.Len())

	// The counters described entries that no longer exist, so they go with
	// them: otherwise the sweep after a Clear reports pressure from before it.
	cleared := m.EvictionsSinceLastRead()
	require.Equal(t, int64(0), cleared.total, "Clear must drop the eviction counter")
	require.Empty(t, cleared.topPeer, "Clear must drop the eviction attribution")

	for i := 0; i < 50; i++ {
		m.Store(fmt.Sprintf("after-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 3, m.Len(), "the cap must survive Clear")
	requireMapConsistent(t, &m)
}

// requireConfiguredCap asserts the cap a map was actually configured with,
// read under its own lock. Behavioural fills cannot substitute here: since the
// zero value now resolves to defaultPeerMapMaxSize, a map that lost its wiring
// still enforces the default, so only the configured value distinguishes
// "wired" from "fell back".
func requireConfiguredCap(t *testing.T, m *cappedPeerMap, want int) {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	require.Equal(t, want, m.maxSize, "configured cap did not reach the map")
}

// TestApplyPeerMapLimits pins that the configured size actually reaches both
// attribution maps. Nothing else asserts it, so a refactor could drop the
// wiring and leave both maps silently on the fallback with every other test
// still green.
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

		require.Equal(t, 3*time.Minute, s.peerMapTTL)
		requireConfiguredCap(t, &s.blockPeerMap, 7)
		requireConfiguredCap(t, &s.subtreePeerMap, 7)

		fill(t, s, 20)
		require.Equal(t, 7, s.blockPeerMap.Len())
		require.Equal(t, 7, s.subtreePeerMap.Len())
	})

	t.Run("unset settings fall back to the service defaults", func(t *testing.T) {
		s := &Server{}
		s.applyPeerMapLimits(&settings.Settings{})

		require.Equal(t, defaultPeerMapTTL, s.peerMapTTL)

		// The default must be applied to the maps, not merely relied on as
		// their fallback: this is what would fail if the wiring were dropped.
		requireConfiguredCap(t, &s.blockPeerMap, defaultPeerMapMaxSize)
		requireConfiguredCap(t, &s.subtreePeerMap, defaultPeerMapMaxSize)

		fill(t, s, 5)
		require.Equal(t, 5, s.blockPeerMap.Len())
		require.Equal(t, 5, s.subtreePeerMap.Len())
	})

	t.Run("an unconfigured TTL does not expire everything on the next sweep", func(t *testing.T) {
		// The zero TTL is the mirror hazard of the zero cap. It reads like
		// "expire nothing" and behaves like "expire everything": the sweep
		// cutoff lands on now, so every announcement is gone before the block
		// it names finishes validating and the ban path finds nobody to blame.
		s := &Server{logger: ulogger.TestLogger{}}
		require.Zero(t, s.peerMapTTL, "the hazard only exists while the field is unset")
		require.Equal(t, defaultPeerMapTTL, s.peerMapTTLOrDefault())

		s.storePeerMapEntry(&s.blockPeerMap, "block-hash", "peer-to-ban", now)
		s.storePeerMapEntry(&s.subtreePeerMap, "subtree-hash", "peer-to-ban", now)

		s.cleanupPeerMaps()

		peerID, err := s.getPeerFromMap(&s.blockPeerMap, "block-hash", "block")
		require.NoError(t, err, "a sweep on an unconfigured Server must not wipe attribution")
		require.Equal(t, "peer-to-ban", peerID)

		peerID, err = s.getPeerFromMap(&s.subtreePeerMap, "subtree-hash", "subtree")
		require.NoError(t, err)
		require.Equal(t, "peer-to-ban", peerID)
	})

	t.Run("a non-positive configured size cannot unbound the maps", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = -1

		s := &Server{}
		s.applyPeerMapLimits(tSettings)

		requireConfiguredCap(t, &s.blockPeerMap, defaultPeerMapMaxSize)
		requireConfiguredCap(t, &s.subtreePeerMap, defaultPeerMapMaxSize)
	})
}
