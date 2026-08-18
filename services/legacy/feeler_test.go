package legacy

import (
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
	restoreCfg := swapTestConfig(t, "")
	defer restoreCfg()

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

	restoreCfg := swapTestConfig(t, ln.Addr().String())
	defer restoreCfg()

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
	restoreCfg := swapTestConfig(t, "")
	defer restoreCfg()

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

	startFeelerLoop(t, srv)

	require.Eventually(t, func() bool {
		ka := srv.addrManager.UnverifiedAddress()
		return ka != nil && !ka.LastAttempt().IsZero() && ka.Attempts() > 0
	}, 20*time.Second, 10*time.Millisecond,
		"a dial that produced nothing must be recorded against the address")

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

	restoreCfg := swapTestConfig(t, ln.Addr().String())
	defer restoreCfg()

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
	ln, served := startFeelerTestListener(t, "/Satoshi:0.21.0/")

	restoreCfg := swapTestConfig(t, ln.Addr().String())
	defer restoreCfg()

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})

	// Two established automatic peers, so countFailedDial has the evidence it
	// needs to hold an address responsible. Below that threshold an attempt is
	// recorded but never counted, and the attempt tally would stay at zero for
	// reasons that have nothing to do with this probe.
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	startFeelerLoop(t, srv)

	select {
	case <-served:
	case <-time.After(20 * time.Second):
		t.Fatal("the probe never reached the listener")
	}

	// Wait for the probe to finish deciding, which the counter records.
	require.Eventually(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 20*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		ka := srv.addrManager.UnverifiedAddress()
		return ka != nil && ka.Attempts() > 0
	}, 20*time.Second, 10*time.Millisecond,
		"the attempt must be recorded against the address")

	// Never, not Eventually. The probe records its attempt before it has seen
	// the user agent, so an Eventually that stopped at the attempt could be
	// checking the promotion a moment too early and would pass whatever the
	// code did. This watches for a window many probe intervals long.
	require.Never(t, func() bool {
		return srv.feelerVerified.Load() > 0 || srv.addrManager.UnverifiedAddress() == nil
	}, 3*time.Second, 25*time.Millisecond,
		"a node that is not a BSV node must never be promoted out of the new table")
}

// testSourceAddr is the "who told us about this address" address. It only has
// to be routable and distinct from the address under test.
func testSourceAddr() *wire.NetAddress {
	return wire.NewNetAddressIPPort(net.ParseIP("173.194.115.1"), 8333, wire.SFNodeNetwork)
}

// bannedTestBanList returns a ban list already holding the given address. It is
// backed by an in-memory database because Add writes through to storage.
func bannedTestBanList(t *testing.T, ip string) *p2p.BanList {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, settings.NewSettings())
	require.NoError(t, err)

	bl := banlist.New(store.GetDB(), util.SqliteMemory, ulogger.TestLogger{})
	require.NoError(t, bl.Init(t.Context()))

	t.Cleanup(bl.Stop)
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
func swapTestConfig(t *testing.T, redirectTo string) func() {
	t.Helper()

	orig := cfg

	c := &config{
		MaxPeers:        125,
		MaxPeersPerIP:   5,
		TrickleInterval: 10 * time.Second,
	}

	c.dial = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		if redirectTo != "" {
			addr = redirectTo
		}

		return net.DialTimeout(network, addr, timeout)
	}

	cfg = c

	return func() { cfg = orig }
}

// newFeelerTestServer builds the smallest server the probe path needs.
func newFeelerTestServer(t *testing.T) *server {
	t.Helper()

	tSettings := settings.NewSettings()
	tSettings.Legacy.FeelerInterval = time.Millisecond

	srv := &server{
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		addrManager: addrmgr.New(ulogger.TestLogger{}, t.TempDir(), nil),
		banList:     banlist.New(nil, "", ulogger.TestLogger{}),
		quit:        make(chan struct{}),
		query:       make(chan interface{}),
		feelerSlots: 1,
		services:    wire.SFNodeNetwork,
	}

	t.Cleanup(func() {
		close(srv.quit)
		srv.wg.Wait()
	})

	return srv
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

// startFeelerLoop runs the real feeler goroutine against the test server.
func startFeelerLoop(t *testing.T, srv *server) {
	t.Helper()

	srv.wg.Add(1)

	go srv.feelerHandler()
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
