package legacy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// peerDisconnectsWithin reports whether p's WaitForDisconnect returns inside d.
func peerDisconnectsWithin(p *peer.Peer, d time.Duration) bool {
	done := make(chan struct{})

	go func() {
		p.WaitForDisconnect()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// TestServerPeer_ARejectedBodyBansTheHostSoItCannotReconnect pins the ban, and
// what it rests on. The peer package has already decided the connection's fate
// when it calls OnBlockBodyRejected; this decides whether the host may come
// back. It may not, for cfg.BanDuration, when and only when the rejection is
// ProvenBad: the body arrived in full and the sink raised the ban marker at one
// of its three SV Node DoS(100) parity sites. Every other rejection (the marker
// on a body cut short, an invalid verdict without the marker, a corrupt
// delivery) costs the connection and nothing more; so does a proven-bad body
// when banning is disabled or the host is whitelisted, the two exemptions
// addBanScore has.
//
// The rejection is raised on a DATA1 sub-peer of an association, which is
// where a body arrives under BlockPriority, and the ban is checked against a
// fresh connection from the same host: handleBanPeerMsg bans by host, which
// both streams share, so a ban raised on the sub-peer refuses the primary's
// reconnect too. The ban list is a real one over a sqlitememory store.
//
// No sync manager is wired, so peerDoneHandler's DonePeer never runs here; the
// ledger release that follows a primary's disconnect is netsync's and is pinned
// by TestPeer_ARejectedBodyDropsTheAssociationPrimary there.
func TestServerPeer_ARejectedBodyBansTheHostSoItCannotReconnect(t *testing.T) {
	origCfg := cfg
	t.Cleanup(func() { cfg = origCfg })

	hash := chainhash.HashH([]byte("block"))

	for _, tc := range []struct {
		name           string
		rejected       *peer.BlockBodyRejectedError
		disableBanning bool
		whitelisted    bool
		wantBan        bool
	}{
		{
			name:     "a body proven bad bans the host",
			rejected: &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockInvalidError("merkle root does not match", errors.ErrBlockBodyMismatch)},
			wantBan:  true,
		},
		{
			name:     "the marker on a body cut short is not banned: it was never judged in full",
			rejected: &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockInvalidError("duplicate transaction", errors.ErrBlockBodyMismatch), Truncated: true},
		},
		{
			name:     "an invalid verdict without the marker is not banned",
			rejected: &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockInvalidError("declares more transactions than its body can hold")},
		},
		{
			name:     "a corrupt delivery is not banned",
			rejected: &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockCorruptError("the declared transactions used fewer bytes than declared")},
		},
		{
			name:           "banning disabled",
			rejected:       &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockInvalidError("merkle root does not match", errors.ErrBlockBodyMismatch)},
			disableBanning: true,
		},
		{
			name:        "a whitelisted host",
			rejected:    &peer.BlockBodyRejectedError{Hash: hash, Err: errors.NewBlockInvalidError("merkle root does not match", errors.ErrBlockBodyMismatch)},
			whitelisted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg = &config{
				DisableBanning: tc.disableBanning,
				BanThreshold:   100,
				BanDuration:    24 * time.Hour,
				MaxPeers:       8,
				MaxPeersPerIP:  8,
			}

			srv := &server{
				ctx:      context.Background(),
				logger:   ulogger.TestLogger{},
				banList:  emptyWritableBanList(t),
				banPeers: make(chan *serverPeer, 1),
				banChan:  make(chan p2p.BanEvent, 1),
			}
			state := newTestPeerState()

			// The rejection arrives on the DATA1 sub-peer of an association
			// whose primary is a separate peer from the same host.
			primary := newTestOutboundPeer(t, srv, "10.0.0.1:8333")
			assoc := peer.NewAssociation([]byte{0x01, 0x02, 0x03}, primary.Peer)
			primary.Peer.SetAssociation(assoc)

			sp := newTestOutboundPeer(t, srv, "10.0.0.1:8333")
			sp.ctx = context.Background()
			sp.isWhitelisted = tc.whitelisted
			require.True(t, assoc.AddStream(wire.StreamTypeData1, sp.Peer))
			sp.Peer.SetAssociation(assoc)
			sp.Peer.SetStreamType(wire.StreamTypeData1)

			sp.OnBlockBodyRejected(sp.Peer, tc.rejected)

			var banned *serverPeer

			select {
			case banned = <-srv.banPeers:
			case <-time.After(100 * time.Millisecond):
			}

			reconnecting := newTestOutboundPeer(t, srv, "10.0.0.1:8333")

			if !tc.wantBan {
				require.Nil(t, banned, "nothing may be queued for the ban handler")
				require.True(t, srv.handleAddPeerMsg(state, reconnecting), "the host must be free to reconnect")

				return
			}

			require.NotNil(t, banned, "the ban must be queued for the peerHandler")
			require.Same(t, sp, banned)

			// The peerHandler's own step.
			srv.handleBanPeerMsg(state, banned)

			select {
			case event := <-srv.banChan:
				require.Equal(t, "add", event.Action)
				require.Equal(t, "10.0.0.1", event.IP)
			case <-time.After(time.Second):
				t.Fatal("the ban must be published as a ban event")
			}

			banEnd, ok := state.banned.Get("10.0.0.1")
			require.True(t, ok, "the host, not the connection, is what is banned")
			require.False(t, banEnd.Before(time.Now().Add(cfg.BanDuration-time.Second)), "banned for cfg.BanDuration")

			require.False(t, srv.handleAddPeerMsg(state, reconnecting), "a fresh connection from the banned host must be refused")
			require.True(t, peerDisconnectsWithin(reconnecting.Peer, time.Second), "and disconnected")
			require.True(t, srv.banList.IsBanned("10.0.0.1:8333"), "the ban list, which the outbound dial sites consult, holds the host")
		})
	}
}

// TestHandleDonePeerMsg_AStreamSubPeerLeavingDropsThePrimary pins the stream
// branch of the peer server's done handling: an association is one logical
// peer, so a DATA1 sub-peer leaving for any reason takes the primary with it.
// Before this the branch only removed the stream, and netsync's
// handleDonePeerMsg returns at once for a sub-peer, so the primary stayed sync
// peer with every block it owed still owed and no DATA1 left to deliver any.
//
// The rest of the chain is pinned elsewhere and cited rather than re-proved:
// the primary's own done path tearing down its streams is
// TestTearDownAssociationStreams; netsync releasing a departed primary's owed
// blocks is TestPeer_ARejectedBodyDropsTheAssociationPrimary.
func TestHandleDonePeerMsg_AStreamSubPeerLeavingDropsThePrimary(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	newPeer := func() *peer.Peer {
		return peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})
	}

	buildAssoc := func() (*peer.Association, *peer.Peer, *peer.Peer) {
		primary := newPeer()
		assoc := peer.NewAssociation([]byte{0x01, 0x02, 0x03}, primary)
		primary.SetAssociation(assoc)

		data1 := newPeer()
		require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
		data1.SetAssociation(assoc)
		data1.SetStreamType(wire.StreamTypeData1)

		return assoc, primary, data1
	}

	s := &server{logger: ulogger.TestLogger{}}

	t.Run("the DATA1 stream leaving drops the primary", func(t *testing.T) {
		assoc, primary, data1 := buildAssoc()

		s.handleDonePeerMsg(newTestPeerState(), &serverPeer{Peer: data1, server: s})

		require.Nil(t, assoc.Stream(wire.StreamTypeData1), "the stream is removed from the association")
		require.True(t, peerDisconnectsWithin(primary, time.Second), "the primary must be disconnected: nothing re-opens a lost DATA1, and only the primary's departure reaches netsync")
	})

	t.Run("a primary already gone costs nothing more", func(t *testing.T) {
		assoc, primary, data1 := buildAssoc()

		primary.DisconnectWithInfo("went first")

		require.NotPanics(t, func() {
			s.handleDonePeerMsg(newTestPeerState(), &serverPeer{Peer: data1, server: s})
		})

		require.Nil(t, assoc.Stream(wire.StreamTypeData1))
		require.True(t, peerDisconnectsWithin(primary, time.Second))
	})
}

// TestNewStreamServerPeer_ReadsTheWhitelist pins the outbound DATA1 stream
// peer's whitelist. newServerPeer leaves isWhitelisted false and
// openRequiredStreams used to leave it there, so a whitelisted host's DATA1
// sub-peer, the peer whose read loop sees every block body under
// BlockPriority, could be banned for a body while its whitelisted primary
// could not.
func TestNewStreamServerPeer_ReadsTheWhitelist(t *testing.T) {
	origCfg := cfg
	t.Cleanup(func() { cfg = origCfg })

	_, whitelisted, err := net.ParseCIDR("10.0.0.1/32")
	require.NoError(t, err)

	cfg = &config{whitelists: []*net.IPNet{whitelisted}}

	s := &server{
		ctx:         context.Background(),
		logger:      ulogger.TestLogger{},
		settings:    test.CreateBaseTestSettings(t),
		addrManager: addrmgr.New(ulogger.TestLogger{}, t.TempDir(), nil),
	}

	for _, tc := range []struct {
		name   string
		remote string
		want   bool
	}{
		{name: "a whitelisted host", remote: "10.0.0.1", want: true},
		{name: "any other host", remote: "10.0.0.2", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := peer.NewInboundPeer(ulogger.TestLogger{}, s.settings, &peer.Config{})
			assoc := peer.NewAssociation([]byte{0x01, 0x02, 0x03}, primary)

			ours, theirs := net.Pipe()
			t.Cleanup(func() { _ = ours.Close(); _ = theirs.Close() })

			conn := &tcpAddrConn{
				Conn:   ours,
				local:  &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 8333},
				remote: &net.TCPAddr{IP: net.ParseIP(tc.remote), Port: 8333},
			}

			sp := s.newStreamServerPeer(assoc, conn)

			require.Equal(t, tc.want, sp.isWhitelisted)
			require.Equal(t, wire.StreamTypeData1, sp.Peer.StreamType())
			require.Same(t, assoc, sp.Peer.AssociationRef())
			require.NotNil(t, assoc.Stream(wire.StreamTypeData1), "the stream is registered with the association")
		})
	}
}
