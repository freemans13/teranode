package legacy

import (
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// connMgrSpy implements connManagerI, wrapping no real manager but recording
// the calls the disconnect path makes against it. This follows the
// spy-around-a-real-contract pattern used elsewhere in the codebase (e.g.
// commitOrderSpyClient in services/blockvalidation): the interface is exactly
// the subset the server uses, so the spy is behaviourally faithful.
type connMgrSpy struct {
	disconnectIDs []uint64
	openIDs       []uint64
}

func (s *connMgrSpy) Start()                     {}
func (s *connMgrSpy) Stop()                      {}
func (s *connMgrSpy) Connect(_ *connmgr.ConnReq) {}
func (s *connMgrSpy) NewConnReq()                {}
func (s *connMgrSpy) Disconnect(id uint64)       { s.disconnectIDs = append(s.disconnectIDs, id) }
func (s *connMgrSpy) OpenConnIDs() []uint64      { return s.openIDs }

// TestHandleDonePeerMsg_NotifiesConnMgr is the Part-2 regression test: a
// dropped outbound peer that carries a connReq must notify the connection
// manager so its internal count decrements and it re-dials. Without the
// notification the count goes stale and the manager stops replenishing (the
// peer-starvation root cause). It uses a real outbound *peer.Peer and a spied
// connManagerI, driving the real handleDonePeerMsg.
func TestHandleDonePeerMsg_NotifiesConnMgr(t *testing.T) {
	spy := &connMgrSpy{}

	tSettings := &settings.Settings{}
	s := &server{
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		connManager: spy,
	}

	// Build a real outbound peer (no live connection needed). AllowBlockPriority
	// is false via the empty settings, so no association is created and the peer
	// reports Inbound()=false, IsStreamPeer()=false, VersionKnown()=false.
	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{}, "127.0.0.1:8333")
	require.NoError(t, err)

	c := &connmgr.ConnReq{}

	sp := newServerPeer(s, false)
	sp.Peer = p
	sp.connReq = c

	state := newTestPeerState()
	state.outboundPeers.Set(sp.ID(), sp)

	s.handleDonePeerMsg(state, sp)

	require.Equal(t, []uint64{c.ID()}, spy.disconnectIDs, "outbound peer with connReq must notify the connmgr on drop")
	_, present := state.outboundPeers.Get(sp.ID())
	require.False(t, present, "dropped peer must be removed from outbound list")
}

// TestHandleDonePeerMsg_InboundDoesNotNotify verifies the notification is
// scoped to outbound peers: an inbound peer drop must not call Disconnect
// (inbound peers have no outbound connReq to reconcile).
func TestHandleDonePeerMsg_InboundDoesNotNotify(t *testing.T) {
	spy := &connMgrSpy{}

	tSettings := &settings.Settings{}
	s := &server{
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		connManager: spy,
	}

	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	sp := newServerPeer(s, false)
	sp.Peer = p

	state := newTestPeerState()
	state.inboundPeers.Set(sp.ID(), sp)

	s.handleDonePeerMsg(state, sp)

	require.Empty(t, spy.disconnectIDs, "inbound peer drop must not notify the connmgr")
}

func newTestPeerState() *peerState {
	return &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
		banned:          txmap.NewSyncedMap[string, time.Time](),
		outboundGroups:  txmap.NewSyncedMap[string, int](),
		connectionCount: txmap.NewSyncedMap[string, int](),
	}
}
