package legacy

import (
	"sync/atomic"
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
// the calls the watchdog and disconnect path make against it. This follows the
// spy-around-a-real-contract pattern used elsewhere in the codebase (e.g.
// commitOrderSpyClient in services/blockvalidation): the interface is exactly
// the subset the server uses, so the spy is behaviourally faithful.
type connMgrSpy struct {
	newConnReqCalls atomic.Int64
	disconnectIDs   []uint64
}

func (s *connMgrSpy) Start()                     {}
func (s *connMgrSpy) Stop()                      {}
func (s *connMgrSpy) Connect(_ *connmgr.ConnReq) {}
func (s *connMgrSpy) NewConnReq()                { s.newConnReqCalls.Add(1) }
func (s *connMgrSpy) Disconnect(id uint64)       { s.disconnectIDs = append(s.disconnectIDs, id) }

// connReqSpy counts the number of forced outbound dials requested by the
// replenishment watchdog via the bare NewConnReq callback.
type connReqSpy struct {
	calls atomic.Int64
}

func (s *connReqSpy) NewConnReq() {
	s.calls.Add(1)
}

// TestReplenishOutboundDials_FiresDeficit verifies the watchdog forces exactly
// (target - real) dials when the real outbound peer count is below target.
func TestReplenishOutboundDials_FiresDeficit(t *testing.T) {
	spy := &connReqSpy{}

	dialed := replenishOutboundDials(3, 8, spy.NewConnReq)

	require.Equal(t, 5, dialed, "should dial the deficit")
	require.EqualValues(t, 5, spy.calls.Load(), "should call NewConnReq exactly deficit times")
}

// TestReplenishOutboundDials_AtTarget verifies the watchdog does nothing when
// the real outbound count already meets the target.
func TestReplenishOutboundDials_AtTarget(t *testing.T) {
	spy := &connReqSpy{}

	dialed := replenishOutboundDials(8, 8, spy.NewConnReq)

	require.Equal(t, 0, dialed, "should not dial when at target")
	require.EqualValues(t, 0, spy.calls.Load(), "should not call NewConnReq when at target")
}

// TestReplenishOutboundDials_AboveTarget verifies the watchdog does nothing
// when the real outbound count exceeds the target (never disconnects).
func TestReplenishOutboundDials_AboveTarget(t *testing.T) {
	spy := &connReqSpy{}

	dialed := replenishOutboundDials(10, 8, spy.NewConnReq)

	require.Equal(t, 0, dialed, "should not dial when above target")
	require.EqualValues(t, 0, spy.calls.Load())
}

// TestReplenishOutboundDials_TargetZero verifies a zero or negative target
// never dials (defensive; target is clamped by MaxPeers upstream).
func TestReplenishOutboundDials_TargetZero(t *testing.T) {
	spy := &connReqSpy{}

	dialed := replenishOutboundDials(0, 0, spy.NewConnReq)

	require.Equal(t, 0, dialed)
	require.EqualValues(t, 0, spy.calls.Load())
}

// TestReplenishOutboundDials_ViaConnMgr wires the watchdog helper to the
// connManagerI seam exactly as peerHandler does, and asserts a below-target
// count forces the deficit as NewConnReq calls on the (spied) manager.
func TestReplenishOutboundDials_ViaConnMgr(t *testing.T) {
	spy := &connMgrSpy{}

	dialed := replenishOutboundDials(2, 8, spy.NewConnReq)

	require.Equal(t, 6, dialed)
	require.EqualValues(t, 6, spy.newConnReqCalls.Load(), "watchdog must dial the deficit through the connmgr")
}

// TestWatchdogEnabled verifies the enable predicate the peer handler uses to
// decide whether to arm the replenishment ticker: off when the interval is
// non-positive (disabled) and off in connect-only mode (pinned peers).
func TestWatchdogEnabled(t *testing.T) {
	tests := []struct {
		name         string
		interval     time.Duration
		connectPeers int
		want         bool
	}{
		{name: "enabled with interval and no connect peers", interval: 30 * time.Second, connectPeers: 0, want: true},
		{name: "disabled by zero interval", interval: 0, connectPeers: 0, want: false},
		{name: "disabled by negative interval", interval: -1, connectPeers: 0, want: false},
		{name: "disabled in connect-only mode", interval: 30 * time.Second, connectPeers: 3, want: false},
		{name: "disabled by both", interval: 0, connectPeers: 3, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, watchdogEnabled(tt.interval, tt.connectPeers))
		})
	}
}

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
