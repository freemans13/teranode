package connmgr

// Regression tests for the stuck-pending leak (mainnet pending=22, 2026-07-16).
//
// NewConnReq registers the ConnReq in cm.pending BEFORE it has an address.
// The duplicate-address early returns (already-connected / already-pending)
// then abandon that address-less entry: the only pending removals are the
// dial-lifecycle handlers (handleConnected, handleFailedConn, the cancel arm
// of handleDisconnected), and an entry that never dialed can trigger none of
// them. Each abandoned entry also silently kills its replacement-dial chain.

import (
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newPendingReleaseTestManager(t *testing.T, addr net.Addr) *ConnManager {
	t.Helper()

	cm, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: 1,
		GetNewAddress:  func() (net.Addr, error) { return addr, nil },
		Dial: func(_ net.Addr) (net.Conn, error) {
			t.Fatal("dial must not run in the duplicate-address dedup tests")
			return nil, nil
		},
	})
	require.NoError(t, err)

	// Run the request handler only (not Start, which would launch
	// TargetOutbound unsolicited NewConnReq calls of its own). connHandler
	// calls wg.Done() on exit, so balance it here.
	cm.wg.Add(1)
	go cm.connHandler()
	t.Cleanup(func() { close(cm.quit) })

	return cm
}

// TestNewConnReqDuplicateConnectedAddrReleasesPending: when the fresh request
// is abandoned because the address is already CONNECTED, the just-registered
// pending entry must be released.
func TestNewConnReqDuplicateConnectedAddrReleasesPending(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 8333}
	cm := newPendingReleaseTestManager(t, addr)

	existing := &ConnReq{}
	existing.SetAddr(addr)
	cm.conns.Set(99, existing)

	cm.NewConnReq()

	require.Eventuallyf(t, func() bool { return cm.pending.Length() == 0 },
		2*time.Second, 10*time.Millisecond,
		"a request abandoned by the already-connected dedup must release its pending slot (got %d pending)", cm.pending.Length())
}

// TestNewConnReqDuplicatePendingAddrReleasesPending: same invariant for the
// already-PENDING dedup return. Exactly one pending entry (the pre-seeded
// in-flight dial) may remain; the abandoned duplicate must not accumulate.
func TestNewConnReqDuplicatePendingAddrReleasesPending(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.11"), Port: 8333}
	cm := newPendingReleaseTestManager(t, addr)

	inflight := &ConnReq{}
	inflight.SetAddr(addr)
	cm.pending.Set(98, inflight)

	cm.NewConnReq()

	require.Eventuallyf(t, func() bool { return cm.pending.Length() == 1 },
		2*time.Second, 10*time.Millisecond,
		"a request abandoned by the already-pending dedup must release its own pending slot (got %d pending)", cm.pending.Length())
}
