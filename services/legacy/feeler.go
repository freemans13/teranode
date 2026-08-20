package legacy

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/version"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// defaultFeelerInterval is the fallback mean gap between probes, used when
// legacy_feelerInterval is not positive. Matches svnode's FEELER_INTERVAL
// (net.h:88).
const defaultFeelerInterval = 120 * time.Second

// feelerBudget returns how many peer slots to reserve for feeler probes.
//
// The reservation is what makes a probe paid for rather than borrowed. svnode
// expresses the same idea as arithmetic: its inbound ceiling is
// nMaxConnections - (nMaxOutbound + nMaxFeeler) (net.cpp:1261), so the feeler's
// permit comes out of the inbound share and never out of the outbound target.
// Teranode has one joint ceiling over inbound and automatic outbound rather
// than two separate ones, so the faithful translation is to lower that joint
// ceiling and leave the automatic outbound target completely alone.
//
// Three cases return zero, and each disables the probe loop and the reservation
// together, so the node can never end up paying for a feature that is off:
//
//   - A configured budget of zero or less. This is the single rollback lever.
//   - Connect-only mode. There the node's entire connectivity is the configured
//     list, and MaxPeers has already been resized to the length of that list, so
//     every slot is spoken for. The node also stops discovering peers for
//     itself — newAddressFunc is not installed — so there is nothing for a
//     verified address to feed. Reserving a slot would strand a configured peer
//     for nothing.
//   - A budget that would leave no room for an ordinary peer. Reserving the
//     node's whole capacity for probing is never what an operator meant.
func feelerBudget(logger ulogger.Logger, configured int, connectOnly bool, maxPeers, targetOutbound int) int {
	if configured <= 0 || connectOnly {
		return 0
	}

	// The reservation must leave room for the WHOLE automatic outbound tier, not
	// merely for one peer. svnode takes its feeler allowance out of the inbound
	// share and never touches nMaxOutbound, so probing can never cost it a peer
	// it chose to dial. Teranode has one combined ceiling instead of two, so the
	// same guarantee has to be asserted here: if reserving would push the
	// admission ceiling below the outbound target, the node would sit
	// permanently below target, dialling and being refused in a loop, and the
	// operator would see connection churn with no obvious cause.
	//
	// Giving up the probe is the right way to lose that argument. Real peers are
	// what the node is for; the feeler only exists to make finding them easier.
	if maxPeers-configured < targetOutbound {
		logger.Warnf("[Feeler] Disabled: reserving %d of %d peer slots would leave less than the automatic outbound target of %d", configured, maxPeers, targetOutbound)
		return 0
	}

	return configured
}

// setFeelerBudget fixes the slot reservation against the outbound target the
// connection manager will actually chase. connmgr.New substitutes its default
// for a configured zero, so the number judged here has to be the manager's,
// not the caller's.
//
// Must be called after connmgr.New has returned, and before peerHandler starts:
// handleAddPeerMsg and handleQuery are the only readers of feelerSlots, and
// neither runs until then.
func (s *server) setFeelerBudget(logger ulogger.Logger, configured int, connectOnly bool, maxPeers int) {
	if s.connManager == nil {
		s.feelerSlots = 0
		return
	}

	s.feelerSlots = feelerBudget(logger, configured, connectOnly, maxPeers, int(s.connManager.TargetOutbound()))
}

// peerAdmissionCeiling is how many inbound and automatic outbound peers the
// node will admit: MaxPeers less the slots held back for feeler probes.
//
// Named (addnode) peers are not counted against this, and are not meant to be:
// they have their own budget and are additive, which is what
// CountExcludingPermanent exists to express. Not counted is not the same as not
// gated, though — handleAddPeerMsg applies the comparison to every peer it
// admits, named ones included — so a node whose inbound and automatic tiers are
// already full still turns a named peer away, and the reservation makes that
// bite one peer sooner. connectNodeAdmitted, the runtime addnode door,
// deliberately does not apply the ceiling to a permanent request, so the two
// doors disagree on this point. That predates the feeler; the TODO at the check
// itself is where it is tracked.
func peerAdmissionCeiling(maxPeers, feelerSlots int) int {
	ceiling := maxPeers - feelerSlots
	if ceiling < 0 {
		return 0
	}

	return ceiling
}

// feelerAllowed reports whether the automatic outbound tier is at its target,
// which is the only condition under which svnode probes (net.cpp:1865).
//
// The reason for the gate is supply, not politeness. Below target the node is
// short of real peers and the replenishment loop is trying to close that gap; a
// probe launched then competes for exactly the dials the node is missing, and
// on a busy address book it can lose the race for a good address to itself.
//
// The target is read off the connection manager rather than recomputed from
// configuration on purpose. connmgr.New substitutes its own default when the
// caller leaves the target unset, so a recomputed target would be zero, every
// count would clear it, and the node would probe from a cold start with no
// outbound peers at all.
func (s *server) feelerAllowed() bool {
	if s.connManager == nil {
		return false
	}

	return s.connManager.AutomaticOutboundCount() >= int(s.connManager.TargetOutbound())
}

// feelerPollInterval is how often the feeler loop wakes to ask whether it is
// time to probe. svnode's connection thread reaches the same decision on a
// 500ms sleep (net.cpp:1802); a second is the same idea, one wakeup cheaper.
//
// It is also the floor under legacy_feelerInterval: the deadline is only ever
// examined on a tick, so any mean below a second is served at a second, and the
// realised mean of a sub-second setting is higher than the one configured. That
// only matters to a test winding the interval down, and it is written into the
// setting's own documentation.
const feelerPollInterval = time.Second

// feelerCandidateTries bounds one selection pass. It matches newAddressFunc, so
// the probe and the real dial path give up after the same effort, and their
// escalation thresholds below line up with each other.
const feelerCandidateTries = 100

// defaultFeelerHandshakeTimeout is the fallback when
// legacy_feelerHandshakeTimeout is not positive. It must sit inside
// peer.NegotiateTimeout so a mute host is hung up by the feeler, not logged
// as a lost peer.
const defaultFeelerHandshakeTimeout = 25 * time.Second

// poissonNext returns an exponentially distributed delay with the given mean.
//
// A fixed period would be a fingerprint. An observer who sees probes at t,
// t+120s, t+240s can recognise the node across address changes and predict the
// next one; it would also synchronise a fleet of nodes started together. A
// memoryless gap leaks neither. svnode randomises its feeler pacing the same
// way, in PoissonNextSend (net.cpp:3326), and for the same reason.
//
// Unbounded above, as svnode's is, but not unguarded: see boundedDuration.
func poissonNext(mean time.Duration) time.Duration {
	return boundedDuration(rand.ExpFloat64()*float64(mean), mean)
}

// boundedDuration turns a nanosecond count into a Duration, falling back when
// the value will not fit an int64. The mean is operator-settable and
// ExpFloat64 reaches about 745, so a large mean overflows; converting a float
// that does not fit is undefined in Go. The comparison is a negated in-range
// test so that NaN, which loses every ordinary comparison, takes the fallback
// too.
func boundedDuration(ns float64, fallback time.Duration) time.Duration {
	if !(ns > math.MinInt64 && ns < math.MaxInt64) {
		return fallback
	}

	return time.Duration(ns)
}

// startFeeler launches the probe loop, unless feelers are switched off.
//
// Separated from peerHandler so that everything except the single call line is
// reachable from a test. peerHandler itself cannot be constructed in a unit
// test: it starts the sync manager, which needs a blockchain client, a
// validator, a UTXO store, a subtree store and three validation clients.
func (s *server) startFeeler() {
	if s.feelerSlots <= 0 {
		s.logger.Infof("[Feeler] Disabled")
		return
	}

	// One token per reserved slot. A probe holds a token for its whole life, so
	// the number in flight can never exceed the number of peer slots held back
	// for them, and at the default budget of one this is svnode's single feeler
	// exactly. Created here rather than inside the loop so that the loop and the
	// probes it starts share one channel.
	s.feelerTokens = make(chan struct{}, s.feelerSlots)
	for i := 0; i < s.feelerSlots; i++ {
		s.feelerTokens <- struct{}{}
	}

	s.wg.Add(1)

	go s.feelerHandler()
}

// feelerHandler probes one unverified address at a time, so that the addresses
// the node will later dial for real are addresses something has checked.
//
// The problem it solves: the address book only learns that an address works as
// a side effect of a connection the node wanted anyway. Nothing ever checks an
// address the node is not already using, so the pool of known-reachable
// addresses only ever decays. When a peer is lost — constantly, during a long
// initial block download — the replacement is drawn from that decaying pool and
// the node can spend a long time dialling hosts that stopped answering months
// ago. svnode states the goal in one line at net.cpp:1855: "Increase the number
// of connectable addresses in the tried table."
//
// Three properties of the control flow below are svnode's, not incidental:
//
//   - Below target, the deadline is NOT re-rolled. A node that has been waiting
//     while short of peers fires as soon as the tier refills, rather than
//     starting its wait over. svnode gets the same effect by skipping the
//     whole feeler block while below target, so nNextFeeler is left alone
//     (net.cpp:1865).
//   - The deadline is re-rolled at the decision, not after the probe. A slow
//     probe does not shorten the following gap, and a long stretch below target
//     does not bank up a burst of probes (net.cpp:1869).
//   - There is no pre-dial sleep. svnode adds a random 0-1s before a feeler
//     dial (net.cpp:1934) purely to break up the half-second granularity of its
//     own connect loop. Our deadline is an absolute time drawn at nanosecond
//     granularity, so the jitter is already there.
//
// It must be run in a goroutine.
func (s *server) feelerHandler() {
	defer s.wg.Done()

	interval := s.settings.Legacy.FeelerInterval
	if interval <= 0 {
		interval = defaultFeelerInterval
		s.logger.Warnf("[Feeler] legacy_feelerInterval must be positive, using %s (set legacy_maxFeelerPeers to 0 to disable feelers)", interval)
	}

	s.logger.Infof("[Feeler] Starting with %d slot(s), mean interval %s", s.feelerSlots, interval)

	deadline := time.Now().Add(poissonNext(interval))

	ticker := time.NewTicker(feelerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
		}

		if !s.feelerAllowed() || time.Now().Before(deadline) {
			continue
		}

		deadline = time.Now().Add(poissonNext(interval))

		select {
		case <-s.feelerTokens:
			go s.feelerProbe()
		default:
			// Every slot is already probing. Skip this one; the deadline has
			// already moved on, so the pace is unchanged.
		}
	}
}

// feelerProbe dials one unverified address, waits for it to identify itself,
// records what it learned, and hangs up.
//
// It never builds a serverPeer and never goes near the connection manager.
// Membership of state.outboundPeers is the netgroup claim and the peer count,
// so a probe registered as an ordinary peer would take both from a real one.
// The connection manager's job is to keep connections alive: handed a probe,
// it would count it against the outbound target and dial a replacement when
// it hung up.
func (s *server) feelerProbe() {
	defer func() { s.feelerTokens <- struct{}{} }()

	na := s.feelerCandidate()
	if na == nil {
		return
	}

	addrString := addrmgr.NetAddressKey(na)

	netAddr, err := addrStringToNetAddr(addrString)
	if err != nil {
		s.logger.Debugf("[Feeler] Cannot resolve %s: %v", addrString, err)
		return
	}

	s.feelerAttempted.Add(1)

	conn, err := bsvdDial(netAddr)

	// Everything past the dial wants to write to the address book, and by now
	// the node may be shutting down: peerHandler stops the address manager
	// immediately after its loop exits, so a write that loses that race is
	// silently lost. Give up rather than write — on both arms, because the
	// failure arm records against the book too.
	if s.shuttingDown() {
		if conn != nil {
			_ = conn.Close()
		}

		return
	}

	if err != nil {
		// A dial that produced nothing is the only evidence the book ever gets
		// that an address is dead. recordFailedDial is wired into the connection
		// manager's own Dial closure, which a direct dial bypasses, so without
		// this call the probe would only ever teach the book good news.
		s.recordFailedDial(netAddr)
		s.logger.Debugf("[Feeler] Dial %s failed: %v", addrString, err)

		return
	}

	if s.banList.IsBanned(conn.RemoteAddr().String()) {
		s.logger.Debugf("[Feeler] %s resolved to a banned address, dropping", addrString)
		_ = conn.Close()

		return
	}

	res := &feelerResult{done: make(chan struct{})}

	p, err := peer.NewOutboundPeer(s.logger, s.settings, s.feelerPeerConfig(res), addrString)
	if err != nil {
		s.logger.Debugf("[Feeler] Cannot create peer for %s: %v", addrString, err)
		_ = conn.Close()

		return
	}

	p.AssociateConnection(conn)

	// After the TCP connect, matching outboundPeerConnected and svnode, which
	// records the attempt on both arms of ConnectNode. countFailedDial is what
	// stops a spell of broken local networking blaming the whole address book.
	//
	// Shutdown is re-checked rather than leaning on the check after the dial:
	// the steps between are short but not free, and this is the same rule Good()
	// follows below. Skipping the write is not a loss — the book it would land
	// in has already been saved.
	if !s.shuttingDown() {
		s.addrManager.Attempt(na, s.countFailedDial())
	}

	gone := make(chan struct{})

	go func() {
		p.WaitForDisconnect()
		close(gone)
	}()

	timeout := s.settings.Legacy.FeelerHandshakeTimeout
	if timeout <= 0 {
		timeout = defaultFeelerHandshakeTimeout
		s.logger.Warnf("[Feeler] legacy_feelerHandshakeTimeout must be positive, using %s", timeout)
	}

	if timeout >= peer.NegotiateTimeout {
		timeout = peer.NegotiateTimeout - time.Second
		s.logger.Warnf("[Feeler] legacy_feelerHandshakeTimeout must be less than the %s peer negotiate timeout, using %s", peer.NegotiateTimeout, timeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	outcome := "no version received"

	select {
	case <-res.done:
		switch {
		case s.shuttingDown():
			outcome = "abandoned, shutting down"
		case !isBSVUserAgent(res.userAgent()):
			outcome = "answered but is not a BSV node"
			s.banNonBSVHost(addrString)
		default:
			// Promotes the address from new to tried, which is the entire point
			// of the exercise. svnode's feeler clears the same bar at the same
			// moment, in ProcessVersionMessage rather than on verack, and so
			// does teranode's own outbound path in OnVersion.
			s.addrManager.Good(na)
			s.feelerVerified.Add(1)

			outcome = "verified"
		}

	case <-gone:
		outcome = "hung up before its version"

	case <-timer.C:
		outcome = "timed out"

	case <-s.quit:
		outcome = "abandoned, shutting down"
	}

	// Debug rather than Info, because "Disconnecting (%s) reason:" is the line
	// the disconnect-rate measurements key on, and a probe hanging up on purpose
	// is not a peer the node lost.
	p.DisconnectWithLogFunc("feeler probe complete", s.logger.Debugf)

	s.logger.Infof("[Feeler] Probe %s: %s (user agent %q, attempted %d, verified %d)",
		addrString, outcome, res.userAgent(), s.feelerAttempted.Load(), s.feelerVerified.Load())
}

// shuttingDown reports whether the server has begun shutting down. Used by the
// probe to stop before it writes to an address book that is about to be saved.
func (s *server) shuttingDown() bool {
	select {
	case <-s.quit:
		return true
	default:
		return false
	}
}

func (s *server) banNonBSVHost(addrString string) {
	if cfg.DisableBanning {
		return
	}

	host, _, err := net.SplitHostPort(addrString)
	if err != nil {
		host = addrString
	}

	if err := s.banList.Add(s.ctx, host, time.Now().Add(cfg.BanDuration)); err != nil {
		s.logger.Debugf("[Feeler] Cannot ban %s: %v", host, err)
	}
}

// feelerResult carries what the probe learned out of the peer callback.
type feelerResult struct {
	mtx  sync.Mutex
	ua   string
	once sync.Once
	done chan struct{}
}

func (r *feelerResult) userAgent() string {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.ua
}

// feelerPeerConfig is a throwaway peer configuration with exactly one listener.
//
// Nothing here can register the connection anywhere: no server callbacks, no
// sync manager, no association. Multistream is off because a probe has no use
// for a second TCP stream and asking for one would make the remote set one up
// for a peer that is about to vanish.
func (s *server) feelerPeerConfig(res *feelerResult) *peer.Config {
	return &peer.Config{
		Listeners: peer.MessageListeners{
			OnVersion: func(_ *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
				res.mtx.Lock()
				res.ua = msg.UserAgent
				res.mtx.Unlock()

				res.once.Do(func() { close(res.done) })

				// Never a reject, even for a node we will not promote. A reject
				// is written to the wire and then fails negotiation, which the
				// peer package reports by disconnecting at warning level — and
				// that warning is the same line the disconnect-rate measurements
				// count. We hang up ourselves instead, quietly.
				return nil
			},
		},
		AddrMe:            addrMe,
		HostToNetAddress:  s.addrManager.HostToNetAddress,
		Proxy:             cfg.Proxy,
		UserAgentName:     userAgentName,
		UserAgentVersion:  version.String(),
		UserAgentComments: cfg.UserAgentComments,
		ChainParams:       s.settings.ChainCfgParams,
		Services:          s.services,
		DisableRelayTx:    true,
		ProtocolVersion:   peer.MaxProtocolVersion,
		TrickleInterval:   cfg.TrickleInterval,
	}
}

// feelerCandidate picks an address worth probing, or nil if this pass found
// nothing suitable.
//
// The escalation thresholds mirror newAddressFunc exactly, so the probe and the
// dial path judge an address the same way. Two deliberate differences from
// svnode:
//
//   - An occupied netgroup skips the candidate rather than abandoning the whole
//     pass. svnode breaks out on the first unlucky draw (net.cpp:1882), which
//     throws away a two-minute slot; newAddressFunc continues, and agreeing with
//     the sibling function in this file matters more than copying the quirk.
//   - No service-flag filter. svnode has one (net.cpp:1902); teranode's dial
//     path does not, and a probe that is stricter than the thing it feeds would
//     verify addresses the node then declines to use.
func (s *server) feelerCandidate() *wire.NetAddress {
	snap := s.feelerPeerSnapshot()

	for tries := 0; tries < feelerCandidateTries; tries++ {
		// Drawn from the new table only. This is the point of the whole
		// exercise: a probe exists to move an address into tried, so drawing
		// one that is already there achieves nothing. svnode restricts the same
		// way, with Select(newOnly) at addrman.cpp:337.
		ka := s.addrManager.UnverifiedAddress()
		if ka == nil {
			return nil
		}

		na := ka.NetAddress()

		// Filtered at selection rather than at dial time, unlike svnode, which
		// only notices a ban inside OpenNetworkConnection (net.cpp:2113) and so
		// burns the whole slot on it.
		if s.banList.IsBanned(addrmgr.NetAddressKey(na)) {
			continue
		}

		// Never a host the node is already talking to. A second connection to a
		// peer we are mid-download from is a good way to lose the first one.
		// The netgroup set below cannot stand in for this: it is derived from
		// the automatic outbound list alone, so inbound and named peers are
		// invisible to it.
		if _, held := snap.hosts[na.IP.String()]; held {
			continue
		}

		// Never a netgroup an automatic outbound peer occupies, so a probe can
		// never be mistaken for the node claiming a second address in a segment
		// it already reaches.
		if _, occupied := snap.outboundGroups[addrmgr.GroupKey(na)]; occupied {
			continue
		}

		// Only allow recently attempted nodes after 30 failed tries.
		if tries < 30 && time.Since(ka.LastAttempt()) < 10*time.Minute {
			continue
		}

		// Allow nondefault ports after 50 failed tries.
		if tries < 50 && fmt.Sprintf("%d", na.Port) != activeNetParams.DefaultPort {
			continue
		}

		return na
	}

	return nil
}
