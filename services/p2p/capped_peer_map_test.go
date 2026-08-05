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
	accepted := 0

	for i := 0; i < 250; i++ {
		if m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "attacker", timestamp: now}) {
			accepted++
		}
	}

	require.Equal(t, 100, m.Len(), "map must not grow past its cap under a distinct-hash flood")
	require.Equal(t, 100, accepted)
	require.Equal(t, int64(150), m.RejectedSinceLastRead())
	require.Equal(t, int64(0), m.RejectedSinceLastRead(), "counter must reset on read")
}

// TestCappedPeerMapUpdateAndDeleteAtCap pins the cap semantics: updating an
// existing key succeeds at capacity, and a delete frees space for a new key.
func TestCappedPeerMapUpdateAndDeleteAtCap(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(2)

	now := time.Now()
	require.True(t, m.Store("a", peerMapEntry{peerID: "p1", timestamp: now}))
	require.True(t, m.Store("b", peerMapEntry{peerID: "p1", timestamp: now}))
	require.False(t, m.Store("c", peerMapEntry{peerID: "p2", timestamp: now}), "new key at cap must be rejected")

	// Updating an existing key is not growth and must succeed.
	require.True(t, m.Store("a", peerMapEntry{peerID: "p2", timestamp: now}))

	entry, ok := m.Load("a")
	require.True(t, ok)
	require.Equal(t, "p2", entry.peerID)

	// Deleting frees a slot: the previously rejected key now fits.
	m.Delete("b")
	require.True(t, m.Store("c", peerMapEntry{peerID: "p2", timestamp: now}))
	require.Equal(t, 2, m.Len())
}

// TestCappedPeerMapZeroValue pins the zero-value affordance the test fixtures
// rely on: no cap configured means unbounded storage, and all methods are
// usable without construction.
func TestCappedPeerMapZeroValue(t *testing.T) {
	var m cappedPeerMap

	now := time.Now()
	for i := 0; i < 300; i++ {
		require.True(t, m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now}))
	}

	require.Equal(t, 300, m.Len())

	m.Clear()
	require.Equal(t, 0, m.Len())
}

// TestStorePeerMapEntryEnforcesCap pins the production insert path end to end:
// the gossip handlers' storePeerMapEntry must respect the configured cap.
func TestStorePeerMapEntryEnforcesCap(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.blockPeerMap.setMaxSize(3)

	now := time.Now()
	for i := 0; i < 10; i++ {
		s.storePeerMapEntry(&s.blockPeerMap, fmt.Sprintf("%064d", i), "peer-1", now, "block")
	}

	require.Equal(t, 3, s.blockPeerMap.Len(), "gossip inserts must not grow the map past the cap")
}
