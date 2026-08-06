package blockassembly

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLivenessDoesNotRestartAnIdleNode pins the property that makes this probe
// safe to enable: liveness measures whether the main loop can still be
// SERVICED, not whether work arrived. A node with no blocks is healthy — on
// mainnet the gap between blocks is routinely tens of minutes — so an idle
// node must never be restarted (issue 1447).
func TestLivenessDoesNotRestartAnIdleNode(t *testing.T) {
	server, _ := setupServer(t)

	// Shrink the tick so this runs in milliseconds instead of tens of seconds.
	// Set before Start, on this assembler only: a package-level variable would
	// be read by other tests' running loops and race with them.
	const tick = 50 * time.Millisecond
	server.blockAssembler.heartbeatInterval = tick

	// The timeout must sit BETWEEN one tick and the wait below. Above one tick
	// (plus scheduler jitter) so a beating loop stays healthy; below the wait so
	// a loop that had stopped beating would be caught. Review caught the first
	// version of this test passing with the tick disabled — it proved nothing,
	// which is the worst kind of test for a safety property.
	server.settings.BlockAssembly.LivenessStallTimeout = 4 * tick

	require.NoError(t, server.blockAssembler.Start(t.Context()))

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "loop must take ownership of the heartbeat")

	// No blocks, no transactions — only the idle tick can keep this healthy.
	time.Sleep(10 * tick)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "an idle but responsive node must stay healthy: %s", msg)
}

// TestLivenessDoesNotRestartDuringStartup pins the window review found to be
// the most dangerous: the service is constructed during Init, but its loop
// only starts well into Start, behind work that is legitimately unbounded
// (waiting on pending block validation, reloading a large unmined set). Ageing
// the heartbeat through that preamble would report a healthy, still-starting
// node as wedged — and because a restart re-enters the same preamble, the node
// could never finish starting.
func TestLivenessDoesNotRestartDuringStartup(t *testing.T) {
	server, _ := setupServer(t) // Init only — the loop has NOT started

	server.settings.BlockAssembly.LivenessStallTimeout = time.Nanosecond

	time.Sleep(10 * time.Millisecond)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a service still starting up must never be restarted: %s", msg)
}

// TestLivenessStartupWorkCannotStartTheClock pins the same window one level
// down, at the seam that actually broke it. validateParentChain beats on each
// completed batch so a long-but-progressing reset is not mistaken for a wedge —
// but it is also reached from Start, via loadUnminedTransactions, BEFORE the
// loop owns the heartbeat. A plain Beat there starts the clock mid-startup and
// the rest of the preamble (bulk-loading the unmined set) then ages it, which
// is precisely the crash loop the never-beaten state exists to prevent. Hence
// BeatIfStarted: silent until the loop has claimed the heartbeat.
func TestLivenessStartupWorkCannotStartTheClock(t *testing.T) {
	server, _ := setupServer(t)
	ba := server.blockAssembler

	// Stand in for startup work that beats before the loop is running.
	ba.heartbeat.BeatIfStarted()

	require.Zero(t, ba.heartbeat.Age(), "startup work must not take ownership of the heartbeat")

	server.settings.BlockAssembly.LivenessStallTimeout = time.Nanosecond

	time.Sleep(10 * time.Millisecond)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "startup work must not arm the probe: %s", msg)

	// Once the loop owns it, the same call does refresh — otherwise a long reset
	// running inside a select case would look identical to a wedge.
	ba.heartbeat.Beat()
	ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))
	ba.heartbeat.BeatIfStarted()

	require.Less(t, ba.heartbeat.Age(), time.Minute, "once started, progress must refresh the heartbeat")
}

// TestLivenessReportsAWedgedLoop pins the other half: once the loop stops being
// serviced for longer than the configured timeout, liveness must report
// unhealthy so the orchestrator can restart the pod.
//
// The loop is deliberately NOT started. Health only needs the assembler to
// exist, and a running loop would race this test — it beats on every pass, so a
// beat landing between the backdated heartbeat and the probe would flip the
// result.
func TestLivenessReportsAWedgedLoop(t *testing.T) {
	server, _ := setupServer(t)

	server.settings.BlockAssembly.LivenessStallTimeout = time.Millisecond

	// Stand in for a loop that can no longer service its select.
	server.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, msg, "has not made progress")
}

// TestLivenessDisabledByDefault pins the opt-in: with the default timeout the
// probe cannot restart anything, so merging this change alters no deployment's
// behaviour until an operator chooses a value.
func TestLivenessDisabledByDefault(t *testing.T) {
	server, _ := setupServer(t)

	require.Zero(t, server.settings.BlockAssembly.LivenessStallTimeout)

	server.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-24 * time.Hour))

	status, _, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "default must not restart anything")
}
