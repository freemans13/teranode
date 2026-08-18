package legacy

import (
	"net"
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestFeelerBudget pins the three ways the reservation is refused.
//
// Each of them matters for a different reason. A configured zero is the single
// rollback lever, and it has to switch off the reservation as well as the
// probing, or an operator who disabled feelers would still be paying an inbound
// slot for them. Connect-only mode cannot run a probe at all — the address
// source is not installed and MaxPeers has been resized to the configured list
// — so reserving there would strand a peer the operator explicitly asked for.
// And a budget that consumes the node's whole capacity is never what anyone
// meant.
func TestFeelerBudget(t *testing.T) {
	tests := []struct {
		name        string
		configured  int
		connectOnly bool
		maxPeers    int
		want        int
	}{
		{name: "default budget of one", configured: 1, maxPeers: 125, want: 1},
		{name: "operator raises the budget", configured: 3, maxPeers: 125, want: 3},
		{name: "zero is the disable lever", configured: 0, maxPeers: 125, want: 0},
		{name: "negative is treated as disabled", configured: -1, maxPeers: 125, want: 0},
		{name: "connect-only reserves nothing", configured: 1, connectOnly: true, maxPeers: 4, want: 0},
		{name: "never reserve the whole capacity", configured: 1, maxPeers: 1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, feelerBudget(tt.configured, tt.connectOnly, tt.maxPeers))
		})
	}
}

// TestPeerAdmissionCeilingReservesFeelerSlots is the test that proves a probe is
// paid for rather than borrowed.
//
// The arithmetic half is straightforward. The behavioural half drives the real
// door, handleAddPeerMsg, with a node whose cap is three and one slot reserved:
// the third ordinary peer must be turned away, and the same node with no
// reservation must let it in. If the comparand there ever drifts back to the
// raw MaxPeers, the feeler becomes a slot the node quietly overspends.
func TestPeerAdmissionCeilingReservesFeelerSlots(t *testing.T) {
	require.Equal(t, 124, peerAdmissionCeiling(125, 1))
	require.Equal(t, 125, peerAdmissionCeiling(125, 0))
	require.Equal(t, 0, peerAdmissionCeiling(1, 1))
	require.Equal(t, 0, peerAdmissionCeiling(1, 4), "the ceiling never goes negative")

	// cfg is a package-level variable read by handleAddPeerMsg; save and restore
	// it so this test does not leak into the rest of the package.
	origCfg := cfg
	defer func() { cfg = origCfg }()

	cfg = &config{MaxPeers: 3, MaxPeersPerIP: 8}

	for _, tc := range []struct {
		name        string
		feelerSlots int
		admitted    bool
	}{
		{name: "one slot reserved: the third peer is refused", feelerSlots: 1, admitted: false},
		{name: "no reservation: the third peer is admitted", feelerSlots: 0, admitted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &server{
				logger:      ulogger.TestLogger{},
				settings:    settings.NewSettings(),
				banList:     banlist.New(nil, "", ulogger.TestLogger{}),
				feelerSlots: tc.feelerSlots,
			}

			state := newTestPeerState()

			// Seeded directly rather than through handleAddPeerMsg, because a
			// peer that has not handshaked has no peer ID yet and both would
			// land on the same map key. The door itself is what this test is
			// driving, and it is driven once, with the third peer.
			for i, addr := range []string{"8.8.8.8:8333", "1.1.1.1:8333"} {
				sp := newTestOutboundPeer(t, srv, addr)
				state.outboundPeers.Set(int32(i+1), sp)
			}

			require.Equal(t, 2, state.CountExcludingPermanent())

			third := newTestOutboundPeer(t, srv, "9.9.9.9:8333")
			require.Equal(t, tc.admitted, srv.handleAddPeerMsg(state, third))

			if tc.admitted {
				require.Equal(t, 3, state.CountExcludingPermanent())
			} else {
				require.Equal(t, 2, state.CountExcludingPermanent(),
					"the reserved slot must not be handed to an ordinary peer")

				_, tracked := state.outboundPeers.Get(third.ID())
				require.False(t, tracked)
			}
		})
	}
}

// TestFeelerGateRequiresTierAtTarget pins svnode's precondition: probe only once
// the automatic outbound tier is already full (net.cpp:1865). Below target the
// node still needs real peers, and a probe would be competing for exactly the
// dials it is short of.
//
// The zero-target case is the reason feelerAllowed reads the target off the
// connection manager rather than recomputing it. New substitutes its own default
// when the caller leaves the target unset, so a node that recomputed would see a
// target of zero, decide it was at target with no peers at all, and probe from a
// cold start — the precise opposite of the rule.
func TestFeelerGateRequiresTierAtTarget(t *testing.T) {
	t.Run("no connection manager", func(t *testing.T) {
		srv := &server{logger: ulogger.TestLogger{}}
		require.False(t, srv.feelerAllowed())
	})

	t.Run("below target then at target", func(t *testing.T) {
		cmgr, conns := startTestConnManager(t, 2)

		srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

		require.False(t, srv.feelerAllowed(), "no automatic outbound peers yet")

		conns(t, 1)
		require.False(t, srv.feelerAllowed(), "one peer short of the target of two")

		conns(t, 1)
		require.True(t, srv.feelerAllowed(), "the tier is at target")
	})

	t.Run("unset target must not read as zero", func(t *testing.T) {
		cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
			Dial: func(net.Addr) (net.Conn, error) { return nil, errNoTestDial },
		})
		require.NoError(t, err)

		srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

		require.Equal(t, uint32(8), cmgr.TargetOutbound(), "New substitutes its own default")
		require.False(t, srv.feelerAllowed(),
			"a node with no outbound peers must never be judged to be at target")
	})
}

var errNoTestDial = net.UnknownNetworkError("test dialer is never expected to run")

// newTestPeerState builds the peer bookkeeping handleAddPeerMsg expects.
func newTestPeerState() *peerState {
	return &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
		banned:          txmap.NewSyncedMap[string, time.Time](),
	}
}

// newTestOutboundPeer builds an automatic outbound serverPeer at the given
// address, with no live connection behind it.
func newTestOutboundPeer(t *testing.T, srv *server, addr string) *serverPeer {
	t.Helper()

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, settings.NewSettings(), &peer.Config{}, addr)
	require.NoError(t, err)

	return &serverPeer{Peer: p, server: srv}
}

// startTestConnManager returns a running connection manager with the given
// target, plus a helper that establishes n automatic outbound connections
// through the real Connect path and waits for them to register.
func startTestConnManager(t *testing.T, target uint32) (*connmgr.ConnManager, func(*testing.T, int)) {
	t.Helper()

	var (
		closers []net.Conn
		nextIP  int
	)

	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		TargetOutbound: target,
		Dial: func(net.Addr) (net.Conn, error) {
			ours, theirs := net.Pipe()
			closers = append(closers, ours, theirs)

			return ours, nil
		},
	})
	require.NoError(t, err)

	cmgr.Start()

	t.Cleanup(func() {
		cmgr.Stop()
		cmgr.Wait()

		for _, c := range closers {
			_ = c.Close()
		}
	})

	establish := func(t *testing.T, n int) {
		t.Helper()

		before := cmgr.AutomaticOutboundCount()

		for i := 0; i < n; i++ {
			nextIP++

			addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(
				net.IPv4(10, 0, 0, byte(nextIP)).String(), "8333"))
			require.NoError(t, err)

			req := &connmgr.ConnReq{}
			req.SetAddr(addr)

			go cmgr.Connect(req)
		}

		require.Eventually(t, func() bool {
			return cmgr.AutomaticOutboundCount() >= before+n
		}, 5*time.Second, 5*time.Millisecond, "connections did not register")
	}

	return cmgr, establish
}
