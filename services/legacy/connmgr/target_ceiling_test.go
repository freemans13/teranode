package connmgr

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingDialer wraps mockDialer so a test can assert how many dials a single
// event actually cost, rather than only that the node recovered.
func countingDialer(dials *atomic.Int32) func(net.Addr) (net.Conn, error) {
	return func(addr net.Addr) (net.Conn, error) {
		dials.Add(1)

		return mockDialer(addr)
	}
}

// TestOneLostPeerCostsOneDial is the ceiling test for the replenishment work.
//
// Every other replenishment test asserts a floor — that the node climbs back TO
// target. None asserted the ceiling, that it stops AT target, and that gap hid a
// bug in which every single disconnect cost two replacement dials and left the
// node sitting one connection above TargetOutbound.
//
// The mechanism was a window in which a slot belonged to nobody. handleFailedConn
// deleted the dead request from cm.pending and launched the replacement, but the
// replacement registered itself by sending on cm.requests — a channel serviced by
// connHandler, which was at that moment still inside handleFailedConn and could
// not service anything. The replenishment loop runs on its own goroutine, so it
// read the books during that window, saw an empty slot that was already being
// filled, and dialed a second address for it. Reserving the slot in cm.pending at
// the moment the replacement is decided closes the window.
//
// The interval is set far beyond the test's lifetime so no periodic pass can run:
// anything this test observes is the event-driven wake path, which is where the
// bug lived. A unique address per dial matters for the same reason it does in
// replenish_test.go — a repeated address would be suppressed by the dedup checks
// and hold the dial count down for reasons that have nothing to do with the
// accounting under test.
func TestOneLostPeerCostsOneDial(t *testing.T) {
	const target = 4

	connected := make(chan *ConnReq, 64)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: 10 * time.Minute,
		Dial:              countingDialer(&dials),
		GetNewAddress:     freshAddrFn(&addrCount),
		OnConnection: func(c *ConnReq, conn net.Conn) {
			connected <- c
		},
	})
	require.NoError(t, err)

	cmgr.Start()
	defer cmgr.Stop()

	var first *ConnReq

	for i := 0; i < target; i++ {
		select {
		case c := <-connected:
			if first == nil {
				first = c
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d initial connections established", i, target)
		}
	}

	require.Eventually(t, func() bool {
		return cmgr.AutomaticOutboundCount() == target
	}, 5*time.Second, 10*time.Millisecond, "connection manager never reached target")

	baseline := dials.Load()

	// Disconnect, not Remove: this is the re-pend plus handleFailedConn path that
	// a real peer loss takes. Remove deliberately does not dial again.
	cmgr.Disconnect(first.ID())

	// Wait for the refill, then keep watching. The second dial arrived within
	// milliseconds of the first, so a test that stopped at "back to target" would
	// pass against the bug.
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("freed outbound slot was never refilled")
	}

	require.Never(t, func() bool {
		return cmgr.AutomaticOutboundCount() > target
	}, time.Second, 10*time.Millisecond,
		"connection manager exceeded TargetOutbound: a freed slot was dialed more than once")

	require.Equal(t, int32(1), dials.Load()-baseline,
		"one lost peer must cost exactly one replacement dial")
}
