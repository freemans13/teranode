package legacy

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/version"
)

// feelerCandidateTries bounds how many addresses a single feeler pass will
// consider before giving up for this interval. The address manager can hand
// back candidates we must skip (banned, same net group as a peer we already
// hold), and without a bound a pathological table would spin here.
const feelerCandidateTries = 32

// feelerHandler periodically opens a short-lived, throwaway connection to one
// address from the address manager, completes the version/verack handshake and
// hangs up.
//
// Why this exists: during the checkpoint desert the node is effectively
// single-sourced, so the address table is full of entries nobody has ever
// actually spoken to. When a peer is lost, the connection manager picks from
// that unverified pool and can burn minutes dialling dead hosts before it finds
// a live one — every second of which is download bandwidth we do not get back.
// A successful feeler handshake calls addrManager.Good, which promotes the
// address from the "new" table to the "tried" table, so the pool the
// replenishment loop draws from stays known-good and a disconnect becomes cheap
// to recover from. A failed one records the attempt, which ages the address out.
//
// The hard constraint: a feeler must not consume one of the TargetOutbound
// automatic outbound slots. That accounting lives in two places, and this
// function stays clear of both — it never touches the connection manager
// (cm.conns is populated by handleConnected before OnConnection fires, so any
// route through Connect/NewConnReq would occupy a slot), and it never creates a
// serverPeer or pushes to s.newPeers (which is what populates
// state.outboundPeers, outboundGroups and connectionCount in handleAddPeerMsg).
// It dials with bsvdDial directly and drives a bare peer.Peer.
//
// It must be run in a goroutine.
func (s *server) feelerHandler() {
	defer s.wg.Done()

	interval := s.settings.Legacy.FeelerInterval

	// Zero disables, which is the rollback lever: with no feeler the node
	// behaves exactly as it does today.
	if interval <= 0 {
		return
	}

	// Connect-only mode deliberately avoids discovering and contacting anything
	// beyond its configured peers, so probing the address table would violate
	// that contract.
	if len(cfg.ConnectPeers) > 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return

		case <-ticker.C:
			s.feelerAttempt()
		}
	}
}

// feelerAttempt performs at most one probe. It returns as soon as the handshake
// resolves one way or the other, and is hard-bounded by feelerHandshakeTimeout
// so a black-hole address cannot wedge the single feeler goroutine.
func (s *server) feelerAttempt() {
	ka := s.feelerCandidate()
	if ka == nil {
		return
	}

	na := ka.NetAddress()
	addrString := addrmgr.NetAddressKey(na)

	netAddr, err := addrStringToNetAddr(addrString)
	if err != nil {
		s.logger.Debugf("[feeler] cannot resolve %s: %v", addrString, err)
		return
	}

	// Record the attempt before dialling, exactly as the connection manager
	// path does: an address that never answers must still age, otherwise a dead
	// entry stays a candidate forever.
	s.addrManager.Attempt(na)

	conn, err := bsvdDial(netAddr)
	if err != nil {
		s.logger.Debugf("[feeler] dial %s failed: %v", addrString, err)
		return
	}

	verack := make(chan struct{})

	var once sync.Once

	// A throwaway config: no server callbacks, no sync manager, nothing that
	// could register this connection anywhere. The only listener records that
	// the far side completed the handshake.
	feelerCfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnVerAck: func(_ *peer.Peer, _ *wire.MsgVerAck) {
				once.Do(func() { close(verack) })
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

	p, err := peer.NewOutboundPeer(s.logger, s.settings, feelerCfg, addrString)
	if err != nil {
		conn.Close()
		s.logger.Debugf("[feeler] cannot create peer for %s: %v", addrString, err)

		return
	}

	p.AssociateConnection(conn)

	done := make(chan struct{})

	go func() {
		p.WaitForDisconnect()
		close(done)
	}()

	timer := time.NewTimer(feelerHandshakeTimeout)
	defer timer.Stop()

	good := false

	select {
	case <-verack:
		good = true

	case <-done:
		// Peer tore the connection down before completing the handshake.

	case <-timer.C:
		// Black hole: connected at TCP level but never negotiated.

	case <-s.quit:
	}

	// Debugf rather than Infof deliberately: 'Disconnecting (%s) reason: %s' is
	// the single anchor the disconnect accounting keys on, and a feeler hanging
	// up on purpose is not a peer we lost. Logging it at info would inflate the
	// disconnect rate by one per feeler interval and make the soak numbers lie.
	p.DisconnectWithLogFunc("feeler probe complete", s.logger.Debugf)

	select {
	case <-done:
	case <-time.After(feelerHandshakeTimeout):
		// Teardown itself wedged. Nothing more we can safely do; the goroutine
		// waiting on WaitForDisconnect is abandoned rather than held onto here.
		s.logger.Warnf("[feeler] teardown of %s did not complete", addrString)
	}

	if good {
		// Promotes new -> tried, which is the entire point of the exercise.
		s.addrManager.Good(na)
		s.logger.Debugf("[feeler] %s verified", addrString)
	}
}

// feelerCandidate picks an address worth probing, or nil if there is nothing
// suitable this interval.
func (s *server) feelerCandidate() *addrmgr.KnownAddress {
	for tries := 0; tries < feelerCandidateTries; tries++ {
		select {
		case <-s.quit:
			return nil
		default:
		}

		ka := s.addrManager.GetAddress()
		if ka == nil {
			return nil
		}

		na := ka.NetAddress()
		addrString := addrmgr.NetAddressKey(na)

		// Dialling a banned address succeeds at TCP level and then gets thrown
		// away — the same pointless churn newAddressFunc avoids.
		if s.banList.IsBanned(addrString) {
			continue
		}

		// Never probe a net group we already hold an outbound peer in. This is
		// coarser than "not already connected", and deliberately so: it
		// guarantees the feeler can never open a second connection to a peer we
		// are mid-download from, which the remote side would likely answer by
		// dropping us.
		groupCount, ok := s.outboundGroupCountForFeeler(addrmgr.GroupKey(na))
		if !ok {
			return nil
		}

		if groupCount != 0 {
			continue
		}

		return ka
	}

	return nil
}

// outboundGroupCountForFeeler is a shutdown-aware OutboundGroupCount. The
// server's own accessor blocks forever on the unbuffered query channel once
// peerHandler's loop has exited, which during shutdown would pin this goroutine
// and hang WaitForShutdown. The bool reports whether the answer is usable.
func (s *server) outboundGroupCountForFeeler(key string) (int, bool) {
	reply := make(chan int, 1)

	select {
	case s.query <- getOutboundGroup{key: key, reply: reply}:
	case <-s.quit:
		return 0, false
	}

	select {
	case n := <-reply:
		return n, true
	case <-s.quit:
		return 0, false
	}
}
