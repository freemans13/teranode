package legacy

import (
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestFeelerBudget pins the three ways the reservation is refused.
//
// Each of them matters for a different reason. A configured zero is the single
// rollback lever, and it has to switch off the reservation as well as the
// probing, or an operator who disabled feelers would still be paying an inbound
// slot for them. Connect-only mode has MaxPeers resized to the configured list
// and never dials anything else, so a verified address has nothing to feed and
// reserving there would strand a peer the operator explicitly asked for. And a
// budget that consumes the node's whole capacity is never what anyone meant.
func TestFeelerBudget(t *testing.T) {
	tests := []struct {
		name           string
		configured     int
		connectOnly    bool
		maxPeers       int
		targetOutbound int
		want           int
	}{
		// The shipped shape: legacy_config_MaxPeers = 20 in settings.conf against
		// the manager's default target of 8, so the reserved slot comes out of the
		// inbound share and the outbound tier is untouched.
		{name: "shipped defaults", configured: 1, maxPeers: 20, targetOutbound: 8, want: 1},
		{name: "operator raises the budget", configured: 3, maxPeers: 125, targetOutbound: 8, want: 3},
		{name: "zero is the disable lever", configured: 0, maxPeers: 125, targetOutbound: 8, want: 0},
		{name: "negative is treated as disabled", configured: -1, maxPeers: 125, targetOutbound: 8, want: 0},
		{name: "connect-only reserves nothing", configured: 1, connectOnly: true, maxPeers: 4, targetOutbound: 4, want: 0},
		{name: "never reserve the whole capacity", configured: 1, maxPeers: 1, targetOutbound: 1, want: 0},

		// The reservation must never push the admission ceiling below the
		// automatic outbound target. A node in that state sits permanently below
		// target, dialling and being refused in a loop — connection churn with no
		// obvious cause, and a reserved slot the probe can never use either.
		{name: "a tight cap gives up the probe rather than the tier", configured: 1, maxPeers: 8, targetOutbound: 8, want: 0},
		{name: "one spare slot above the tier is enough", configured: 1, maxPeers: 9, targetOutbound: 8, want: 1},
		{name: "a raised target squeezes the probe out", configured: 2, maxPeers: 10, targetOutbound: 9, want: 0},
		{name: "a raised target with room still probes", configured: 2, maxPeers: 12, targetOutbound: 9, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, feelerBudget(ulogger.TestLogger{}, tt.configured, tt.connectOnly, tt.maxPeers, tt.targetOutbound))
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
		mtx     sync.Mutex
		closers []net.Conn
		nextIP  int
	)

	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		TargetOutbound: target,
		Dial: func(net.Addr) (net.Conn, error) {
			ours, theirs := net.Pipe()

			// Dials run on their own goroutines, so the bookkeeping needs a lock.
			mtx.Lock()
			closers = append(closers, ours, theirs)
			mtx.Unlock()

			return ours, nil
		},
	})
	require.NoError(t, err)

	cmgr.Start()

	t.Cleanup(func() {
		cmgr.Stop()
		cmgr.Wait()

		mtx.Lock()
		defer mtx.Unlock()

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

// TestPoissonNextIsExponential pins the shape of the pacing, not just its
// average.
//
// A fixed two-minute period would be a fingerprint: an observer who sees probes
// exactly two minutes apart can recognise the node across address changes and
// predict the next one, and a fleet started together would probe in lockstep.
// svnode randomises for the same reason. A single-sample test would pass
// happily against a constant, so this checks the mean AND the spread.
func TestPoissonNextIsExponential(t *testing.T) {
	const (
		draws = 20000
		mean  = time.Millisecond
	)

	var (
		total   time.Duration
		sawLong bool
		sawTiny bool
	)

	for i := 0; i < draws; i++ {
		d := poissonNext(mean)
		total += d

		require.GreaterOrEqual(t, d, time.Duration(0), "a delay must never be negative")

		if d > 3*mean {
			sawLong = true
		}

		if d < mean/10 {
			sawTiny = true
		}
	}

	observed := float64(total) / draws

	require.InEpsilon(t, float64(mean), observed, 0.05,
		"the sample mean must sit within five percent of the configured mean")
	require.True(t, sawLong, "an exponential draw must sometimes run well over its mean")
	require.True(t, sawTiny, "an exponential draw must sometimes come in well under its mean")
}

// TestFeelerSkipsCandidatesAlreadyHeldOrOccupied covers the two exclusions that
// keep a probe from taking anything away from a real peer.
//
// The earlier sketch on stu/legacy-svnode-align claimed the netgroup filter
// alone guaranteed the node would never open a second connection to a peer it
// already held. That is not true: the netgroup set is derived from the automatic
// outbound list only, so inbound and named peers are invisible to it. Both
// filters are checked here separately, against the same address.
func TestFeelerSkipsCandidatesAlreadyHeldOrOccupied(t *testing.T) {
	swapTestConfig(t, "")

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)

	tests := []struct {
		name string
		snap feelerSnapshot
		want bool
	}{
		{
			name: "host already connected",
			snap: feelerSnapshot{hosts: map[string]struct{}{"8.8.8.8": {}}},
			want: false,
		},
		{
			name: "netgroup already occupied",
			snap: feelerSnapshot{outboundGroups: map[string]struct{}{addrmgr.GroupKey(na): {}}},
			want: false,
		},
		{
			name: "nothing in the way",
			snap: feelerSnapshot{},
			want: true,
		},
	}

	t.Run("a banned address is never a candidate", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.banList = bannedTestBanList(t, "8.8.8.8")
		srv.addrManager.AddAddress(na, testSourceAddr())

		require.Nil(t, srv.feelerCandidate(),
			"a banned address would be dropped the moment it answered, so it is not worth a probe")
	})

	t.Run("an already verified address is never a candidate", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.addrManager.AddAddress(na, testSourceAddr())
		srv.addrManager.Good(na)

		require.Equal(t, 1, srv.addrManager.NumAddresses(),
			"the address is still known, it has just moved into tried")
		require.Nil(t, srv.feelerCandidate(),
			"probing an address that is already verified achieves nothing")
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newFeelerTestServer(t)
			serveFeelerSnapshot(srv, tt.snap)

			srv.addrManager.AddAddress(na, testSourceAddr())

			got := srv.feelerCandidate()

			if !tt.want {
				require.Nil(t, got, "the candidate should have been skipped")
				return
			}

			require.NotNil(t, got)
			require.Equal(t, "8.8.8.8:8333", addrmgr.NetAddressKey(got))
		})
	}
}

// TestStartFeelerHonoursTheDisableLever pins the second half of the rollback
// lever. Setting the budget to zero has to stop the goroutine from starting as
// well as stop the slot being reserved; a version that reserved nothing but
// still ran the loop would be a disabled feature that still probes.
//
// Observed through the server's wait group, which is what startFeeler adds to.
// A count of zero cannot be observed through the probe rate, because a token
// channel of capacity zero would hand out no probes either way.
func TestStartFeelerHonoursTheDisableLever(t *testing.T) {
	t.Run("disabled: nothing is started", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		srv.feelerSlots = 0

		srv.startFeeler()

		require.True(t, waitGroupSettles(&srv.wg, 5*time.Second),
			"a disabled feeler must not leave a goroutine running")
	})

	t.Run("enabled: the loop is running", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.startFeeler()

		require.False(t, waitGroupSettles(&srv.wg, time.Second),
			"an enabled feeler must leave its loop running until shutdown")
	})
}

// waitGroupSettles reports whether the wait group drained within the timeout.
func waitGroupSettles(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestFeelerWaitsForTheOutboundTier pins the gate where it is actually applied,
// in the probe loop, rather than only in the predicate it calls.
//
// svnode probes only once its outbound connections are all up (net.cpp:1865),
// and the reason is supply: below target the node is short of real peers and
// the replenishment loop is trying to close that gap, so a probe launched then
// is competing for exactly the dials the node is missing.
//
// The second half matters as much as the first. svnode does not restart its
// wait when it finds itself below target, so a node that has been held back
// fires as soon as the tier fills. Asserting only that nothing happens below
// target would be satisfied by a feeler that never ran at all.
func TestFeelerWaitsForTheOutboundTier(t *testing.T) {
	ln, served := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})

	cmgr, establish := startTestConnManager(t, 2)
	srv.connManager = cmgr

	establish(t, 1)
	require.False(t, srv.feelerAllowed(), "one peer short of the target of two")

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	startFeelerLoop(t, srv)

	require.Never(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 2*time.Second, 25*time.Millisecond,
		"a node below its outbound target must not spend dials on probing")

	establish(t, 1)
	require.True(t, srv.feelerAllowed())

	select {
	case <-served:
	case <-time.After(20 * time.Second):
		t.Fatal("the probe did not start once the outbound tier reached target")
	}
}

// TestFeelerRecordsAFailedDial covers the half of the story that is easy to
// forget: the probe has to teach the book bad news as well as good.
//
// recordFailedDial is wired into the connection manager's own dial closure, and
// a probe dials directly, so it bypasses that wiring entirely. Without the
// explicit call the feeler would only ever mark addresses good, and a host that
// had stopped answering would keep its full selection weight for ever -- which
// is the exact bug PR 1601 fixed for the main dial path.
func TestFeelerRecordsAFailedDial(t *testing.T) {
	// A dial that always fails, standing in for a host that has gone away.
	swapTestConfig(t, "")

	cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errDeadHost
	}

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	require.True(t, srv.addrManager.UnverifiedAddress().LastAttempt().IsZero(),
		"the address starts out with no attempt against it")

	// One probe, run inline. The KnownAddress accessors are documented as
	// unsafe to read while the address manager is being written to, so the
	// assertions below have to happen with no probe in flight.
	runOneProbe(srv)

	ka := srv.addrManager.UnverifiedAddress()
	require.NotNil(t, ka, "a failed dial must not move the address anywhere")
	require.False(t, ka.LastAttempt().IsZero(),
		"a dial that produced nothing must be recorded against the address")
	require.Positive(t, ka.Attempts(),
		"with the node connected elsewhere, the failure counts against the address")
	require.Equal(t, uint64(1), srv.feelerAttempted.Load())
	require.Equal(t, uint64(0), srv.feelerVerified.Load())
}

var errDeadHost = net.UnknownNetworkError("the host under test is not answering")

// TestFeelerSnapshotQueryReportsPeers drives the peer handler's answer to the
// probe's one question, which is the half of the exchange the end-to-end tests
// stub out.
//
// It also pins the difference between the two halves of the snapshot. The
// netgroup set covers automatic outbound peers only, because that is the tier
// whose diversity the node is protecting. The host set covers every tier,
// because a second connection to a host is a problem whichever tier the first
// one is in — and the earlier sketch got exactly this wrong, claiming the
// netgroup check made the host check unnecessary.
func TestFeelerSnapshotQueryReportsPeers(t *testing.T) {
	srv := &server{logger: ulogger.TestLogger{}, settings: settings.NewSettings()}
	state := newTestPeerState()

	outbound := newTestOutboundPeer(t, srv, "8.8.8.8:8333")
	inbound := newTestOutboundPeer(t, srv, "1.1.1.1:8333")
	named := newTestOutboundPeer(t, srv, "9.9.9.9:8333")

	state.outboundPeers.Set(1, outbound)
	state.inboundPeers.Set(2, inbound)
	state.persistentPeers.Set(3, named)

	reply := make(chan feelerSnapshot, 1)
	srv.handleQuery(state, getFeelerSnapshotMsg{reply: reply})

	snap := <-reply

	require.Contains(t, snap.hosts, "8.8.8.8")
	require.Contains(t, snap.hosts, "1.1.1.1", "an inbound peer still occupies its host")
	require.Contains(t, snap.hosts, "9.9.9.9", "a named peer still occupies its host")
	require.Len(t, snap.hosts, 3)

	require.Contains(t, snap.outboundGroups, addrmgr.GroupKey(outbound.NA()))
	require.NotContains(t, snap.outboundGroups, addrmgr.GroupKey(inbound.NA()),
		"only the automatic outbound tier claims a netgroup")
	require.NotContains(t, snap.outboundGroups, addrmgr.GroupKey(named.NA()))
}

// TestFeelerPromotesAnAddressEndToEnd drives the whole probe: the real dial
// path, a real handshake against a real listener, and the address book.
//
// Promotion is observed through exported API alone. Moving an address from new
// to tried leaves the total unchanged while emptying the new table, so
// "UnverifiedAddress returns nothing while NumAddresses is still one" is exactly
// the promotion, with no test-only accessor needed.
//
// The book entry has to be a routable address, because the address manager
// refuses to store loopback, while the socket has to be loopback because that is
// where the test listener is. cfg.dial bridges the two: it is a real production
// field, set by loadConfig, so redirecting it exercises the production dial path
// without adding a seam to production code.
func TestFeelerPromotesAnAddressEndToEnd(t *testing.T) {
	ln, served := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atFeelerTarget(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	require.Equal(t, 1, srv.addrManager.NumAddresses())
	require.NotNil(t, srv.addrManager.UnverifiedAddress(), "the address starts out unverified")

	startFeelerLoop(t, srv)

	select {
	case <-served:
	case <-time.After(20 * time.Second):
		t.Fatal("the probe never reached the listener")
	}

	require.Eventually(t, func() bool {
		return srv.addrManager.UnverifiedAddress() == nil
	}, 20*time.Second, 10*time.Millisecond,
		"a verified address must leave the new table")

	require.Equal(t, 1, srv.addrManager.NumAddresses(),
		"promotion moves the address between tables, it does not add or drop one")
	require.Equal(t, uint64(1), srv.feelerVerified.Load())
}

// TestFeelerDoesNotPromoteNonBSVPeer is the counterpart, and it guards against
// the sketch's worst defect: it installed no version listener at all and marked
// every address that completed a handshake as good. A BTC or BCH node that
// answered would have been promoted, and because promotion can evict an existing
// tried entry, that does not merely waste a probe -- it pushes out a real BSV
// peer.
func TestFeelerDoesNotPromoteNonBSVPeer(t *testing.T) {
	for _, tt := range []struct {
		name           string
		disableBanning bool
		wantBanned     bool
	}{
		{name: "bans by default so it is not probed again", wantBanned: true},
		{name: "disable banning still rejects without a ban", disableBanning: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ln, served := startFeelerTestListener(t, "/Satoshi:0.21.0/")

			swapTestConfig(t, ln.Addr().String())
			cfg.DisableBanning = tt.disableBanning

			srv := newFeelerTestServer(t)
			serveFeelerSnapshot(srv, feelerSnapshot{})

			// Two established automatic peers, so countFailedDial has the evidence it
			// needs to hold an address responsible. Below that threshold an attempt is
			// recorded but never counted, and the attempt tally would stay at zero for
			// reasons that have nothing to do with this probe.
			atLeastTwoAutomaticPeers(t, srv)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			// One probe, run inline, so the assertions below see a settled state rather
			// than racing the loop. runOneProbe returns when the probe has finished
			// deciding, which is what makes the promotion check meaningful: polling for
			// the attempt instead would look at the address before the user agent had
			// even been read, and would pass whatever the code did.
			runOneProbe(srv)

			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("the probe never reached the listener")
			}

			require.Equal(t, uint64(1), srv.feelerAttempted.Load())
			require.Equal(t, uint64(0), srv.feelerVerified.Load(),
				"a node that is not a BSV node must never be promoted")

			ka := srv.addrManager.UnverifiedAddress()
			require.NotNil(t, ka, "the address must stay in the new table")
			require.Positive(t, ka.Attempts(), "the attempt is still recorded against it")
			require.Equal(t, tt.wantBanned, srv.banList.IsBanned("8.8.8.8"))

			if tt.wantBanned {
				require.Nil(t, srv.feelerCandidate(),
					"a banned non-BSV address must not be drawn again")
			}
		})
	}
}

// testSourceAddr is the "who told us about this address" address. It only has
// to be routable and distinct from the address under test.
func testSourceAddr() *wire.NetAddress {
	return wire.NewNetAddressIPPort(net.ParseIP("173.194.115.1"), 8333, wire.SFNodeNetwork)
}

// emptyWritableBanList returns a ban list backed by an in-memory database,
// which Add needs because it writes through to storage.
func emptyWritableBanList(t *testing.T) *p2p.BanList {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, settings.NewSettings())
	require.NoError(t, err)

	bl := banlist.New(store.GetDB(), util.SqliteMemory, ulogger.TestLogger{})
	require.NoError(t, bl.Init(t.Context()))

	t.Cleanup(bl.Stop)

	return bl
}

// bannedTestBanList returns a ban list already holding the given address.
func bannedTestBanList(t *testing.T, ip string) *p2p.BanList {
	t.Helper()

	bl := emptyWritableBanList(t)
	require.NoError(t, bl.Add(t.Context(), ip, time.Now().Add(time.Hour)))

	return bl
}

// swapTestConfig replaces the package-level legacy config for the duration of a
// test. When redirectTo is set, every dial goes there instead of the address
// asked for, which is what lets a probe of a routable book entry land on a
// loopback listener.
//
// cfg.dial is a real production field, set by loadConfig, so this exercises the
// production dial path rather than a test-only injection point.
func swapTestConfig(t *testing.T, redirectTo string) {
	t.Helper()

	orig := cfg

	// MaxPeers matches what ships rather than bsvd's compiled-in 125:
	// settings.conf sets legacy_config_MaxPeers = 20 and the reflection loader in
	// config.go applies it to this field on every real run.
	c := &config{
		MaxPeers:        20,
		MaxPeersPerIP:   5,
		TrickleInterval: 10 * time.Second,
		BanDuration:     24 * time.Hour,
	}

	c.dial = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		if redirectTo != "" {
			addr = redirectTo
		}

		return net.DialTimeout(network, addr, timeout)
	}

	cfg = c

	// Registered as a cleanup rather than returned, so it runs LAST. Cleanups
	// run in reverse order of registration and after every deferred call, and
	// the test server is built after this, so its shutdown -- which waits for
	// probes still reading cfg -- is guaranteed to happen first.
	t.Cleanup(func() { cfg = orig })
}

// newFeelerTestServer builds the smallest server the probe path needs.
func newFeelerTestServer(t *testing.T) *server {
	t.Helper()

	tSettings := settings.NewSettings()
	tSettings.Legacy.FeelerInterval = time.Millisecond

	srv := &server{
		ctx:         t.Context(),
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		addrManager: addrmgr.New(ulogger.TestLogger{}, t.TempDir(), nil),
		// A writable ban list rather than banlist.New(nil, ...). A nil database
		// makes Add dereference nil and panic rather than error, so the cheaper
		// construction turned any test whose probe met a non-BSV user agent into
		// a panicking binary instead of a failing assertion — one edited string
		// literal away, for whoever extends these tests next.
		banList:     emptyWritableBanList(t),
		quit:        make(chan struct{}),
		query:       make(chan interface{}),
		feelerSlots: 1,
		services:    wire.SFNodeNetwork,
	}

	t.Cleanup(func() {
		beginFeelerShutdown(srv)
		srv.wg.Wait()
		drainFeelerProbes(t, srv)
	})

	return srv
}

// beginFeelerShutdown puts the node into shutting-down state, and is safe to
// call twice because the cleanup calls it too.
//
// The check-then-close is not safe against concurrent closers in general. It is
// safe here because the only two callers are a test body and its own cleanup,
// which run in sequence on the same goroutine.
func beginFeelerShutdown(srv *server) {
	select {
	case <-srv.quit:
		return
	default:
	}

	close(srv.quit)
}

// drainFeelerProbes waits until no probe is in flight, by taking every slot
// token. A probe holds its token for its whole life, so holding them all means
// nothing is still running.
//
// The probe loop must already have stopped, or it will keep taking tokens back.
// This matters because probe goroutines are deliberately not tracked by the
// server's wait group, so waiting on that alone can return while a probe is
// still reading package state the test is about to put back.
func drainFeelerProbes(t *testing.T, srv *server) {
	t.Helper()

	for i := 0; i < cap(srv.feelerTokens); i++ {
		select {
		case <-srv.feelerTokens:
		case <-time.After(45 * time.Second):
			t.Error("a feeler probe never finished")
			return
		}
	}
}

// serveFeelerSnapshot answers the probe's peer-set query with a fixed snapshot,
// standing in for the peer handler.
func serveFeelerSnapshot(srv *server, snap feelerSnapshot) {
	go func() {
		for {
			select {
			case <-srv.quit:
				return
			case q := <-srv.query:
				if msg, ok := q.(getFeelerSnapshotMsg); ok {
					msg.reply <- snap
				}
			}
		}
	}()
}

// atFeelerTarget gives the server a connection manager that is at its outbound
// target, which is the condition feelerAllowed requires.
func atFeelerTarget(t *testing.T, srv *server) {
	t.Helper()

	cmgr, establish := startTestConnManager(t, 1)
	establish(t, 1)

	srv.connManager = cmgr

	require.True(t, srv.feelerAllowed())
}

// atLeastTwoAutomaticPeers puts the node at target with two established
// automatic outbound peers, which is also the threshold countFailedDial needs
// before it will hold an address responsible for a failure.
func atLeastTwoAutomaticPeers(t *testing.T, srv *server) {
	t.Helper()

	cmgr, establish := startTestConnManager(t, 2)
	establish(t, 2)

	srv.connManager = cmgr

	require.True(t, srv.feelerAllowed())
	require.True(t, srv.countFailedDial())
}

// runOneProbe runs exactly one probe on the calling goroutine and returns when
// it has finished.
//
// Used where a test needs to read the address book afterwards: the
// KnownAddress accessors are documented as unsafe to read while the address
// manager is being written to, so those assertions cannot race a probe. The
// pacing loop around this is covered separately.
func runOneProbe(srv *server) {
	srv.feelerTokens = make(chan struct{}, 1)

	srv.feelerProbe()
}

// startFeelerLoop starts the feeler exactly as peerHandler does.
func startFeelerLoop(t *testing.T, srv *server) {
	t.Helper()

	srv.startFeeler()

	require.NotNil(t, srv.feelerTokens, "the feeler must have started")
}

// startFeelerTestListener accepts one connection and answers it the way a
// remote node would: it reads the probe's version message and replies with its
// own, carrying the given user agent, then a verack.
//
// Deliberately a raw wire responder rather than a peer.Peer. Two peers built in
// this process share the sent-nonce cache the protocol uses to spot a node that
// has dialled itself, so an in-process peer would recognise the probe's own
// nonce and hang up on it as a self-connection.
func startFeelerTestListener(t *testing.T, userAgent string) (net.Listener, <-chan struct{}) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	net2 := settings.NewSettings().ChainCfgParams.Net
	served := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		msg, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2)
		if err != nil {
			return
		}

		if _, ok := msg.(*wire.MsgVersion); !ok {
			return
		}

		me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)
		you := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)

		reply := wire.NewMsgVersion(me, you, rand.Uint64(), 0)
		reply.UserAgent = userAgent
		reply.Services = wire.SFNodeNetwork

		if err := wire.WriteMessage(conn, reply, wire.ProtocolVersion, net2); err != nil {
			return
		}

		if err := wire.WriteMessage(conn, wire.NewMsgVerAck(), wire.ProtocolVersion, net2); err != nil {
			return
		}

		close(served)

		// Hold the connection open so the probe is the side that hangs up.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln, served
}

// TestFeelerTokensCapProbesInFlight is the test for the whole point of the
// reservation.
//
// The issue asks for slots "reserved from the peer cap rather than taken out of
// the automatic tier". Reserving them is only half of it — the probes actually
// have to respect the reservation. If more probes can be in flight than slots
// were held back, the node is quietly using peer capacity it never reserved, and
// the accounting PR 1601 fixed is wrong again by a different route.
//
// The token channel is that cap. This drains it and shows a probe cannot start,
// then returns a token and shows one can.
//
// Note what this does NOT cover: it exercises the channel directly, not
// feelerHandler, so deleting feelerHandler's acquisition and letting every tick
// start a probe would leave this test green. That is covered separately, by
// TestFeelerHandlerWaitsForASlotToken, which drives the real loop.
func TestFeelerTokensCapProbesInFlight(t *testing.T) {
	srv := &server{
		logger:       ulogger.TestLogger{},
		feelerSlots:  2,
		feelerTokens: make(chan struct{}, 2),
	}
	for i := 0; i < srv.feelerSlots; i++ {
		srv.feelerTokens <- struct{}{}
	}

	// Two probes may run at once, because two slots were reserved.
	acquired := 0

	for i := 0; i < srv.feelerSlots; i++ {
		select {
		case <-srv.feelerTokens:
			acquired++
		default:
		}
	}

	require.Equal(t, 2, acquired, "both reserved slots must be available to probes")

	// A third must not, however long the loop has been waiting to fire. This is
	// the non-blocking default branch in feelerHandler: skip, do not queue.
	select {
	case <-srv.feelerTokens:
		require.Fail(t, "a third probe started with only two slots reserved")
	default:
	}

	// A finished probe returns its token and the next one may start.
	srv.feelerTokens <- struct{}{}

	select {
	case <-srv.feelerTokens:
	default:
		require.Fail(t, "a returned token must let the next probe start")
	}
}

// TestSetFeelerBudgetUsesTheManagersEffectiveTarget pins a guard that used to
// judge itself against the wrong number.
//
// feelerBudget refuses a reservation that would leave the automatic outbound
// tier unable to reach its target. It used to be handed the target computed
// from configuration, before connmgr.New had a say -- and New substitutes its
// own default of eight for a configured zero. The guard therefore compared
// against zero in exactly the case it was written for: no reservation can look
// like it starves a tier that is aiming for nothing. With MaxPeers at eight the
// node reserved a slot anyway, dropped its admission ceiling to seven, and was
// left dialling for an eighth peer its own door would refuse, indefinitely.
//
// Reading the target off the manager is what its own accessor documentation
// asks callers to do, and what feelerAllowed already did.
func TestSetFeelerBudgetUsesTheManagersEffectiveTarget(t *testing.T) {
	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		Dial: func(net.Addr) (net.Conn, error) { return nil, errNoTestDial },
	})
	require.NoError(t, err)
	require.Equal(t, uint32(8), cmgr.TargetOutbound(),
		"New substitutes its default for an unset target, which is the whole trap")

	srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 8)
	require.Equal(t, 0, srv.feelerSlots,
		"reserving one of eight leaves seven, below the eight the manager will chase")

	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 1)
	require.Equal(t, 0, srv.feelerSlots,
		"a reservation that consumes the node's whole capacity is refused")

	// 20 is what actually ships: settings.conf sets legacy_config_MaxPeers = 20,
	// which the reflection loader in config.go puts on this very field. The 125 in
	// config.go is only bsvd's compiled-in fallback, and is overridden on every
	// real run. So the shipped shape is a cap of 20 against a target of 8: the
	// reserved slot comes wholly out of the inbound share, taking it from 12 to
	// 11, and the outbound tier is untouched.
	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 20)
	require.Equal(t, 1, srv.feelerSlots,
		"the shipped defaults leave the outbound tier untouched, so the slot is granted")
}

// TestFeelerHandlerWaitsForASlotToken pins the enforcement site itself.
//
// The reservation is only worth anything if the loop actually respects it. The
// earlier test for this drove a channel it built by hand rather than the loop,
// so deleting the acquisition in feelerHandler and letting every tick start a
// probe left the whole package green -- the cap was the one part of "paid for
// rather than borrowed" that nothing checked.
//
// This starts the real loop with its only slot already spoken for and shows no
// probe runs, then returns the token and shows one does. The second half is
// what makes the first half mean anything: without it, a loop that had simply
// died would pass.
func TestFeelerHandlerWaitsForASlotToken(t *testing.T) {
	swapTestConfig(t, "")

	cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errDeadHost
	}

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	// Built here rather than by startFeeler, and left empty. Filling it and
	// then draining it would leave a window in which the loop could take the
	// token before the test removed it.
	srv.feelerTokens = make(chan struct{}, 1)

	srv.wg.Add(1)

	go srv.feelerHandler()

	require.Never(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 3*time.Second, 25*time.Millisecond,
		"with its only slot taken the loop must skip its turn rather than probe anyway")

	srv.feelerTokens <- struct{}{}

	require.Eventually(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 20*time.Second, 25*time.Millisecond,
		"a returned token must let the loop probe, or the silence above proved nothing")
}

// TestBoundedDurationRefusesAConversionThatDoesNotFit pins the guard under
// poissonNext, deterministically and on either architecture.
//
// It is tested here rather than through poissonNext because poissonNext cannot
// show the bug on every platform: at a mean of MaxInt64 the guard returns the
// mean and an unguarded arm64 saturates to the same MaxInt64, so the two are
// indistinguishable. Feeding the conversion directly makes the fallback visible
// wherever the suite runs -- deleting the guard yields MaxInt64 on arm64 and
// MinInt64 on amd64, and neither is the fallback.
func TestBoundedDurationRefusesAConversionThatDoesNotFit(t *testing.T) {
	fallback := 120 * time.Second

	require.Equal(t, 5*time.Second, boundedDuration(float64(5*time.Second), fallback),
		"a value that fits is converted, not replaced")
	require.Equal(t, fallback, boundedDuration(1e30, fallback),
		"past MaxInt64: amd64 would report a negative gap, arm64 a 292-year one")
	require.Equal(t, fallback, boundedDuration(-1e30, fallback),
		"past MinInt64")
	require.Equal(t, fallback, boundedDuration(math.NaN(), fallback),
		"NaN loses every ordinary comparison, so the test has to be written to catch it")
}

func TestDefaultFeelerHandshakeTimeoutBeatsPeerNegotiateTimeout(t *testing.T) {
	require.Less(t, defaultFeelerHandshakeTimeout, peer.NegotiateTimeout)
}

// TestFeelerHandshakeTimeoutGuardsBothConfiguredEdges pins the two promises the
// setting's own documentation makes about values it cannot use.
//
// Both are worth a test rather than a comment. A non-positive deadline fires
// before the far side can answer, so every probe would report a timeout. And a
// deadline at or beyond peer.NegotiateTimeout loses the race to the peer
// package, whose hang-up is logged at warning level on the line the
// disconnect-rate measurements count -- exactly the noise the deadline exists to
// keep out.
func TestFeelerHandshakeTimeoutGuardsBothConfiguredEdges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{name: "zero falls back to the default", configured: 0, want: defaultFeelerHandshakeTimeout},
		{name: "negative falls back to the default", configured: -time.Second, want: defaultFeelerHandshakeTimeout},
		{name: "equal to the peer timeout is brought inside it", configured: peer.NegotiateTimeout, want: peer.NegotiateTimeout - time.Second},
		{name: "beyond the peer timeout is brought inside it", configured: peer.NegotiateTimeout + time.Hour, want: peer.NegotiateTimeout - time.Second},
		{name: "the shipped default is left alone", configured: defaultFeelerHandshakeTimeout, want: defaultFeelerHandshakeTimeout},
		{name: "a short testing value is left alone", configured: 250 * time.Millisecond, want: 250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, feelerHandshakeTimeout(ulogger.TestLogger{}, tc.configured))
		})
	}
}

// TestFeelerProbeUsesTheConfiguredHandshakeTimeout pins the second hop, which
// the settings loader test cannot see: that the loaded value actually reaches
// the probe's timer.
//
// The listener accepts the connection and then says nothing, which is the
// failure the deadline is for. With the setting honoured the probe gives up
// after the configured moment; with the setting ignored in favour of any of the
// constants around it, the probe sits there for tens of seconds. The generous
// budget is deliberate -- it is not measuring the timeout, only proving the
// configured one is the one being used.
func TestFeelerProbeUsesTheConfiguredHandshakeTimeout(t *testing.T) {
	ln := startMuteFeelerTestListener(t)

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	srv.settings.Legacy.FeelerHandshakeTimeout = 250 * time.Millisecond
	serveFeelerSnapshot(srv, feelerSnapshot{})

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	done := make(chan struct{})

	go func() {
		defer close(done)
		runOneProbe(srv)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the probe ignored the configured handshake timeout")
	}

	require.Equal(t, uint64(1), srv.feelerAttempted.Load())
	require.Equal(t, uint64(0), srv.feelerVerified.Load(),
		"a host that never identified itself must never be promoted")
}

// startMuteFeelerTestListener accepts one connection, reads the probe's version
// message so the probe is genuinely waiting on a reply, and then holds the
// connection open without answering.
//
// Holding it open is the whole point: a listener that closed instead would end
// the probe through its disconnect arm, which would satisfy a timing assertion
// whatever the deadline was set to.
func startMuteFeelerTestListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	net2 := settings.NewSettings().ChainCfgParams.Net

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		if _, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2); err != nil {
			return
		}

		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln
}

// TestFeelerProbeWritesNothingWhileShuttingDown covers the guard immediately
// after the dial, on both of its arms.
//
// peerHandler stops the address manager the moment its loop exits, so a write
// that loses that race is silently lost. The guard gives up instead. Before this
// test all three of the probe's shutdown checks could be deleted with the whole
// package staying green, which is why each of them now has one.
//
// Both arms matter and they are separate code paths: the failure arm records an
// attempt through recordFailedDial, the success arm goes on to build a peer and
// can promote. Each subtest is paired with its running counterpart, because
// "nothing was written" is only evidence if something is written when the node
// is up.
func TestFeelerProbeWritesNothingWhileShuttingDown(t *testing.T) {
	t.Run("failed dial", func(t *testing.T) {
		for _, tt := range []struct {
			name         string
			shuttingDown bool
			wantRecorded bool
		}{
			{name: "running records the failure", wantRecorded: true},
			{name: "shutting down records nothing", shuttingDown: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				swapTestConfig(t, "")

				cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
					return nil, errDeadHost
				}

				srv := newFeelerTestServer(t)
				serveFeelerSnapshot(srv, feelerSnapshot{})
				atLeastTwoAutomaticPeers(t, srv)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				if tt.shuttingDown {
					beginFeelerShutdown(srv)
				}

				runOneProbe(srv)

				ka := srv.addrManager.UnverifiedAddress()
				require.NotNil(t, ka, "a failed dial must not move the address anywhere")
				require.Equal(t, tt.wantRecorded, !ka.LastAttempt().IsZero(),
					"a dial failure is recorded against the address only while the node is running")
			})
		}
	})

	t.Run("successful dial", func(t *testing.T) {
		for _, tt := range []struct {
			name         string
			shuttingDown bool
			wantRecorded bool
			wantPromoted bool
		}{
			{name: "running promotes", wantRecorded: true, wantPromoted: true},
			{name: "shutting down writes nothing", shuttingDown: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				ln, _ := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

				swapTestConfig(t, ln.Addr().String())

				srv := newFeelerTestServer(t)
				serveFeelerSnapshot(srv, feelerSnapshot{})
				atLeastTwoAutomaticPeers(t, srv)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				if tt.shuttingDown {
					beginFeelerShutdown(srv)
				}

				runOneProbe(srv)

				if tt.wantPromoted {
					require.Nil(t, srv.addrManager.UnverifiedAddress(),
						"a verified address must leave the new table")
					require.Equal(t, uint64(1), srv.feelerVerified.Load())

					return
				}

				ka := srv.addrManager.UnverifiedAddress()
				require.NotNil(t, ka, "an abandoned probe must leave the address where it was")
				require.Equal(t, tt.wantRecorded, !ka.LastAttempt().IsZero(),
					"an abandoned probe must not record an attempt either")
				require.Equal(t, uint64(0), srv.feelerVerified.Load())
			})
		}
	})
}

// TestAttemptIfRunningRefusesToWriteWhileShuttingDown pins the second of the
// three shutdown checks, the one covering the attempt recorded after the TCP
// connect.
//
// It is its own function rather than a step in a probe because the check it
// guards is only reachable when shutdown begins *between* the dial and this
// write. Driving a whole probe cannot land in that window on purpose, so the
// window is tested where it lives.
func TestAttemptIfRunningRefusesToWriteWhileShuttingDown(t *testing.T) {
	for _, tt := range []struct {
		name         string
		shuttingDown bool
		wantWritten  bool
	}{
		{name: "running writes", wantWritten: true},
		{name: "shutting down does not", shuttingDown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			swapTestConfig(t, "")

			srv := newFeelerTestServer(t)
			atLeastTwoAutomaticPeers(t, srv)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			require.True(t, srv.addrManager.UnverifiedAddress().LastAttempt().IsZero(),
				"the address starts out with no attempt against it")

			if tt.shuttingDown {
				beginFeelerShutdown(srv)
			}

			require.Equal(t, tt.wantWritten, srv.attemptIfRunning(na),
				"attemptIfRunning must report whether it wrote")

			require.Equal(t, tt.wantWritten, !srv.addrManager.UnverifiedAddress().LastAttempt().IsZero(),
				"and the report must match what reached the address book")
		})
	}
}

// TestJudgeVersionHonoursAVersionThatRacedTheTeardown is the test for the fix to
// the probe's select.
//
// A version can land in the same instant the far side hangs up, or the handshake
// deadline expires. A select picks uniformly among cases that are already ready,
// so before this the hang-up and timeout arms could throw away a version the
// probe already had. The promotion that goes missing is a nuisance; the ban that
// goes missing is the problem, because the address stays in the new table and the
// feeler spends its whole allowance rediscovering the same BTC and BCH nodes.
//
// Measured before the fix, with a delay widening the window the probe reaches
// the select through: 23 of 40 probes of a BSV host failed to promote it and 20
// of 40 probes of a BTC host failed to ban it. A clean coin toss, as advertised.
//
// So each arm is driven here with a version already in hand, and has to reach the
// same verdict the version arm would.
func TestJudgeVersionHonoursAVersionThatRacedTheTeardown(t *testing.T) {
	for _, fallback := range []string{
		"hung up before its version",
		"timed out",
	} {
		t.Run(fallback, func(t *testing.T) {
			t.Run("a BSV version is still a promotion", func(t *testing.T) {
				swapTestConfig(t, "")

				srv := newFeelerTestServer(t)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				res := settledFeelerResult("/Bitcoin SV:1.1.0/")

				require.Equal(t, "verified", srv.judgeVersion(na, "8.8.8.8:8333", res, fallback))
				require.Nil(t, srv.addrManager.UnverifiedAddress(),
					"a verified address must leave the new table however its wait ended")
				require.Equal(t, uint64(1), srv.feelerVerified.Load())
			})

			t.Run("a non-BSV version is still a ban", func(t *testing.T) {
				swapTestConfig(t, "")

				srv := newFeelerTestServer(t)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				res := settledFeelerResult("/Satoshi:0.21.0/")

				require.Equal(t, "answered but is not a BSV node",
					srv.judgeVersion(na, "8.8.8.8:8333", res, fallback))
				require.True(t, srv.banList.IsBanned("8.8.8.8"),
					"a host that identifies itself as non-BSV must be banned whichever arm woke")
				require.Equal(t, uint64(0), srv.feelerVerified.Load())
			})
		})
	}

	// The counterpart, and the half that stops the re-check swallowing the honest
	// case: with no version in hand the arm's own reason must survive untouched.
	t.Run("no version means the arm keeps its own reason", func(t *testing.T) {
		swapTestConfig(t, "")

		srv := newFeelerTestServer(t)

		na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
		srv.addrManager.AddAddress(na, testSourceAddr())

		res := &feelerResult{done: make(chan struct{})}

		require.Equal(t, "hung up before its version",
			srv.judgeVersion(na, "8.8.8.8:8333", res, "hung up before its version"))
		require.NotNil(t, srv.addrManager.UnverifiedAddress(),
			"a host that never answered must stay where it was")
		require.Equal(t, uint64(0), srv.feelerVerified.Load())
	})
}

// TestJudgeVersionRefusesToPromoteWhileShuttingDown pins the last of the three
// shutdown checks, the one inside the verdict.
//
// It is reachable only when shutdown begins after a version has arrived, and it
// is the belt to the select's own quit arm: with both ready that select is a
// coin toss too, so the promotion has to be refused on either outcome.
func TestJudgeVersionRefusesToPromoteWhileShuttingDown(t *testing.T) {
	for _, tt := range []struct {
		name         string
		shuttingDown bool
		wantOutcome  string
	}{
		{name: "running promotes", wantOutcome: "verified"},
		{name: "shutting down abandons", shuttingDown: true, wantOutcome: "abandoned, shutting down"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			swapTestConfig(t, "")

			srv := newFeelerTestServer(t)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			if tt.shuttingDown {
				beginFeelerShutdown(srv)
			}

			res := settledFeelerResult("/Bitcoin SV:1.1.0/")

			require.Equal(t, tt.wantOutcome,
				srv.judgeVersion(na, "8.8.8.8:8333", res, "no version received"))

			require.Equal(t, !tt.shuttingDown, srv.addrManager.UnverifiedAddress() == nil,
				"the address moves out of the new table only while the node is running")
		})
	}
}

// settledFeelerResult is a feelerResult that has already taken a version, as one
// woken by res.done would be.
func settledFeelerResult(userAgent string) *feelerResult {
	res := &feelerResult{done: make(chan struct{})}
	res.ua = userAgent
	res.once.Do(func() { close(res.done) })

	return res
}
