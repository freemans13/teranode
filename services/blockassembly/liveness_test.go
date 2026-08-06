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
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	server.settings.BlockAssembly.LivenessStallTimeout = time.Minute

	// No blocks, no transactions — just an assembler whose loop is running.
	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "an idle but responsive node must stay healthy: %s", msg)
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
