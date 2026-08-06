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
	// Shrink the tick so this runs in milliseconds instead of tens of seconds.
	restore := blockAssemblerHeartbeatInterval
	blockAssemblerHeartbeatInterval = 10 * time.Millisecond

	defer func() { blockAssemblerHeartbeatInterval = restore }()

	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	// Choose a timeout SHORTER than the wait below, so the assertion can only
	// pass if the idle tick is genuinely beating. Review caught the first
	// version of this test passing with the tick disabled — it proved nothing,
	// which is the worst kind of test for a safety property.
	server.settings.BlockAssembly.LivenessStallTimeout = 2 * blockAssemblerHeartbeatInterval

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "loop must take ownership of the heartbeat")

	// No blocks, no transactions — only the idle tick can keep this healthy.
	time.Sleep(5 * blockAssemblerHeartbeatInterval)

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

// TestLivenessReportsAWedgedLoop pins the other half: once the loop stops being
// serviced for longer than the configured timeout, liveness must report
// unhealthy so the orchestrator can restart the pod.
func TestLivenessReportsAWedgedLoop(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

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
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	require.Zero(t, server.settings.BlockAssembly.LivenessStallTimeout)

	server.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-24 * time.Hour))

	status, _, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "default must not restart anything")
}
