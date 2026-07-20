package peer

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are source-inspection tests, matching TestIdleTimerUsesTheBudgetSelectorAtEverySite
// in ibd_deadlines_test.go: the two behaviours below live inside the inHandler
// goroutine's timer callbacks and loop-exit, which are impractical to drive
// deterministically end-to-end (a 10-minute AfterFunc firing under induced
// backpressure, a read-loop exit racing an external disconnect). Asserting the
// required structure in the source is the non-fragile way this package pins that
// kind of goroutine-internal logic.

// TestProcessingTimerHasBackpressureCarveOut: the per-message processing watchdog
// must NOT execute a peer for slowness that is the node's own (block processing
// behind → handlers park), exactly as the idle timer already does. Without this a
// fresh inbound peer was disconnected for "Timeout processing message 'verack' ...
// waited 10m0s" purely because our OnVerAck was parked behind a congested queue
// during IBD (measured live 2026-07-20).
func TestProcessingTimerHasBackpressureCarveOut(t *testing.T) {
	src, err := os.ReadFile("peer.go")
	require.NoError(t, err)
	s := string(src)

	// The timer must be declared in the var-then-assign form so its callback can
	// re-arm itself (a := form cannot self-reference).
	require.Contains(t, s, "var processingTimer *time.Timer",
		"processingTimer must be declared so its callback can Reset itself on backpressure")

	// The callback must consult ReadBackpressured and re-arm before the disconnect,
	// mirroring the idle timer's carve-out.
	carveOut := regexp.MustCompile(
		`(?s)processingTimer = time\.AfterFunc\(.*?ReadBackpressured != nil && p\.cfg\.ReadBackpressured\(\).*?processingTimer\.Reset\(.*?\).*?return.*?Timeout processing message`)
	require.Regexp(t, carveOut, s,
		"the processing watchdog must re-arm (not disconnect) while the node is read-backpressured, "+
			"before it blames the peer for our own processing delay")
}

// TestInHandlerBreakOutIsNotLoggedWhenAlreadyDisconnecting: the read loop exits via
// its `p.disconnect == 0` condition whenever another path (stall handler, idle
// timer, netsync) tears the peer down, so an unguarded disconnect at the loop tail
// logs a spurious SECOND "inHandler break out" reason for every such peer. The
// teardown is deduped but the log line is not, which inflated disconnect-reason
// counts ~2-3x. The tail disconnect must be guarded on p.disconnect == 0 so only a
// genuine loop-internal exit (duplicate version/verack) logs it.
func TestInHandlerBreakOutIsNotLoggedWhenAlreadyDisconnecting(t *testing.T) {
	src, err := os.ReadFile("peer.go")
	require.NoError(t, err)
	s := string(src)

	guarded := regexp.MustCompile(
		`(?s)if atomic\.LoadInt32\(&p\.disconnect\) == 0 \{\s*p\.DisconnectWithInfo\("Peer appears to be stalled or misbehaving, inHandler break out"\)`)
	require.Regexp(t, guarded, s,
		"the inHandler-break-out disconnect must be guarded on p.disconnect == 0 so it does not emit a "+
			"redundant reason line for a peer already being torn down elsewhere")
}
