package legacy

// Tests for reconcileConnAccounting — the strike-based backstop beneath the
// phantom-connection source fixes. The invariants under test: a phantom
// (tracked as open, backed by no live outbound peer) is evicted only after
// connPhantomStrikes consecutive audits; a live-backed id is never evicted
// and has any accumulated strikes cleared; strikes for ids the manager no
// longer tracks are purged (no unbounded growth).

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newReconcileTestServer(spy *connMgrSpy) *server {
	return &server{
		logger:         ulogger.TestLogger{},
		settings:       &settings.Settings{},
		connManager:    spy,
		targetOutbound: 8,
	}
}

// TestReconcileConnAccounting_EvictsPhantomAfterStrikes: an id in the book
// with no backing peer survives the first two audits (could be mid-handshake)
// and is evicted exactly on the third.
func TestReconcileConnAccounting_EvictsPhantomAfterStrikes(t *testing.T) {
	spy := &connMgrSpy{openIDs: []uint64{7}}
	s := newReconcileTestServer(spy)
	state := newTestPeerState()
	strikes := make(map[uint64]int)

	for pass := 1; pass < connPhantomStrikes; pass++ {
		s.reconcileConnAccounting(state, strikes)
		require.Empty(t, spy.disconnectIDs, "pass %d: a phantom must survive until it reaches %d strikes (handshake grace)", pass, connPhantomStrikes)
	}

	s.reconcileConnAccounting(state, strikes)

	require.Equal(t, []uint64{7}, spy.disconnectIDs, "the phantom must be evicted on strike %d", connPhantomStrikes)
	require.Empty(t, strikes, "an evicted id's strike entry must be cleared")
}

// TestReconcileConnAccounting_SparesLiveAndClearsStrikes: an id that becomes
// backed by a live outbound peer is never evicted, and any strikes it
// accumulated while handshaking are cleared — a later unbacked stretch must
// start its count from zero.
func TestReconcileConnAccounting_SparesLiveAndClearsStrikes(t *testing.T) {
	spy := &connMgrSpy{}
	s := newReconcileTestServer(spy)
	state := newTestPeerState()
	strikes := make(map[uint64]int)

	// A real outbound peer whose connReq id (zero-value id 0) is in the book.
	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, s.settings, &peer.Config{}, "127.0.0.1:8333")
	require.NoError(t, err)

	sp := newServerPeer(s, false)
	sp.Peer = p
	sp.connReq = &connmgr.ConnReq{}
	spy.openIDs = []uint64{sp.connReq.ID()}

	// Two strikes accumulate while the peer is not yet registered...
	s.reconcileConnAccounting(state, strikes)
	s.reconcileConnAccounting(state, strikes)
	require.Equal(t, connPhantomStrikes-1, strikes[sp.connReq.ID()], "unbacked id accumulates strikes")

	// ...then the peer registers (handshake completed) before the third audit.
	state.outboundPeers.Set(sp.ID(), sp)

	s.reconcileConnAccounting(state, strikes)
	s.reconcileConnAccounting(state, strikes)

	require.Empty(t, spy.disconnectIDs, "a live-backed id must never be evicted")
	require.Empty(t, strikes, "a live-backed id's strikes must be cleared")
}

// TestReconcileConnAccounting_PurgesStaleStrikes: an id that leaves the book
// through the normal disconnect path between audits must have its strike
// entry purged, so the strikes map cannot grow without bound.
func TestReconcileConnAccounting_PurgesStaleStrikes(t *testing.T) {
	spy := &connMgrSpy{openIDs: []uint64{7}}
	s := newReconcileTestServer(spy)
	state := newTestPeerState()
	strikes := make(map[uint64]int)

	s.reconcileConnAccounting(state, strikes)
	require.Len(t, strikes, 1)

	// The id disconnects normally; the book no longer tracks it.
	spy.openIDs = nil

	s.reconcileConnAccounting(state, strikes)
	require.Empty(t, strikes, "strikes for ids no longer in the book must be purged")
	require.Empty(t, spy.disconnectIDs, "an id that already left the book must not be re-disconnected")
}
