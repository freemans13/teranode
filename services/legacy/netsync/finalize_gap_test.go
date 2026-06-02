package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestGapStallDetector_NoStallWhenDraining: with nothing buffered (pending 0) the
// finalizer is making progress, so no stall is ever reported.
func TestGapStallDetector_NoStallWhenDraining(t *testing.T) {
	d := &gapStallDetector{timeout: 5 * time.Second}
	t0 := time.Unix(1000, 0)

	require.False(t, d.observe(100, true, 0, t0))
	require.False(t, d.observe(101, true, 0, t0.Add(time.Minute)))
}

// TestGapStallDetector_NotStartedNeverStalls: before the finalizer has a start
// height there is nothing to wait for.
func TestGapStallDetector_NotStartedNeverStalls(t *testing.T) {
	d := &gapStallDetector{timeout: 5 * time.Second}
	require.False(t, d.observe(0, false, 0, time.Unix(1000, 0)))
}

// TestGapStallDetector_FiresAfterTimeout: waiting for the same height with blocks
// piled up behind it for >= timeout reports a stall.
func TestGapStallDetector_FiresAfterTimeout(t *testing.T) {
	d := &gapStallDetector{timeout: 5 * time.Second}
	t0 := time.Unix(1000, 0)

	// First observation arms the timer (waiting for 100, 3 blocks buffered).
	require.False(t, d.observe(100, true, 3, t0))
	// Still within the timeout window.
	require.False(t, d.observe(100, true, 3, t0.Add(4*time.Second)))
	// Past the timeout → stall.
	require.True(t, d.observe(100, true, 4, t0.Add(5*time.Second)))
}

// TestGapStallDetector_ResetsOnProgress: when the awaited height advances the
// timer restarts, so normal forward progress never trips the detector.
func TestGapStallDetector_ResetsOnProgress(t *testing.T) {
	d := &gapStallDetector{timeout: 5 * time.Second}
	t0 := time.Unix(1000, 0)

	require.False(t, d.observe(100, true, 2, t0))
	// Height advanced to 101 → timer reset even though wall time passed.
	require.False(t, d.observe(101, true, 2, t0.Add(10*time.Second)))
	// Now stalled at 101; must wait the full timeout again from t0+10s.
	require.False(t, d.observe(101, true, 2, t0.Add(13*time.Second)))
	require.True(t, d.observe(101, true, 2, t0.Add(15*time.Second)))
}

// TestGapStallDetector_ReArmsAfterFiring: after firing, the detector waits another
// full timeout before firing again (so it nudges periodically, not every tick).
func TestGapStallDetector_ReArmsAfterFiring(t *testing.T) {
	d := &gapStallDetector{timeout: 5 * time.Second}
	t0 := time.Unix(1000, 0)

	require.False(t, d.observe(100, true, 1, t0))
	require.True(t, d.observe(100, true, 1, t0.Add(5*time.Second)))
	// Immediately after firing it does not fire again.
	require.False(t, d.observe(100, true, 1, t0.Add(6*time.Second)))
	// Fires again only after another full timeout.
	require.True(t, d.observe(100, true, 1, t0.Add(10*time.Second)))
}

// TestMaybeResyncFinalizeGap_NoSyncPeerNoOp: with no sync peer there is nobody to
// re-request from, so it must be a safe no-op that does not consume the shared
// rate-limit slot (so a later real attempt is not throttled).
func TestMaybeResyncFinalizeGap_NoSyncPeerNoOp(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.NotPanics(t, func() { sm.maybeResyncFinalizeGap(500) })
	require.Zero(t, sm.lastMissingParentResyncNano.Load(), "rate-limit slot not consumed when no peer")
}
