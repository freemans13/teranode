package legacy

// Regression test for the mainnet peer-starvation phantom-connection leak
// (2026-07-16, conns=8/target=8 with only ONE real peer).
//
// handleConnected stores the ConnReq in the connection manager's conns map
// BEFORE OnConnection fires. outboundPeerConnected's banned-peer branch then
// closed the socket and returned WITHOUT connManager.Disconnect(c.ID()), so
// the ConnReq stayed in the books forever: one permanent phantom per banned
// address dialed. Seven such rejections plus one live peer made the replenish
// check see conns=8 >= target=8 and stop dialing — the node starved on a
// single peer while claiming a full complement.

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newTestBanList mirrors internal/banlist/ban_list_test.go: a real BanList
// over an in-memory sqlite blockchain store (no mocks, per testing rules).
func newTestBanList(t *testing.T) *banlist.BanList {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	bl := banlist.New(store.GetDB(), util.SqliteMemory, ulogger.TestLogger{})
	require.NoError(t, bl.Init(context.Background()))

	return bl
}

// newLoopbackConn returns a real established TCP connection to a local
// listener, so conn.RemoteAddr() is a genuine 127.0.0.1 address the ban list
// can match.
func newLoopbackConn(t *testing.T) net.Conn {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	type acceptResult struct {
		conn net.Conn
		err  error
	}

	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, aerr := ln.Accept()
		acceptCh <- acceptResult{c, aerr}
	}()

	dialed, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dialed.Close() })

	res := <-acceptCh
	require.NoError(t, res.err)
	t.Cleanup(func() { _ = res.conn.Close() })

	// The server side of the pair: its RemoteAddr is the dialer's 127.0.0.1
	// ephemeral address, matching how outboundPeerConnected sees a peer.
	return res.conn
}

// TestOutboundPeerConnected_BannedReleasesConnMgrSlot: rejecting a banned
// outbound peer MUST release the connection manager's slot. Without the
// Disconnect, the ConnReq stored by handleConnected before OnConnection fired
// stays in cm.conns forever and permanently counts toward TargetOutbound.
func TestOutboundPeerConnected_BannedReleasesConnMgrSlot(t *testing.T) {
	spy := &connMgrSpy{}
	bl := newTestBanList(t)

	// Ban the loopback address the test connection will come from.
	require.NoError(t, bl.Add(context.Background(), "127.0.0.1", time.Now().Add(time.Hour)))

	tSettings := test.CreateBaseTestSettings(t)
	s := &server{
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		banList:     bl,
		connManager: spy,
	}

	conn := newLoopbackConn(t)
	require.True(t, bl.IsBanned(conn.RemoteAddr().String()), "test precondition: the conn's remote address must be banned")

	c := &connmgr.ConnReq{}

	s.outboundPeerConnected(c, conn)

	require.Equal(t, []uint64{c.ID()}, spy.disconnectIDs,
		"rejecting a banned outbound peer must release the connmgr slot (phantom-connection leak)")
}
