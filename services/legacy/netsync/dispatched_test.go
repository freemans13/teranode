package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestDispatched_Lifecycle: a height marked dispatched reads back as in-flight
// until it is marked finalized.
func TestDispatched_Lifecycle(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.False(t, sm.isDispatched(100), "nil/empty set: not dispatched")

	sm.markDispatched(100)
	sm.markDispatched(101)
	require.True(t, sm.isDispatched(100))
	require.True(t, sm.isDispatched(101))
	require.False(t, sm.isDispatched(102))

	sm.markFinalized(100)
	require.False(t, sm.isDispatched(100), "finalized → no longer in-flight")
	require.True(t, sm.isDispatched(101))
}

// TestPruneDispatchedBelow: stale/finalized heights below the finalize floor are
// dropped so the set stays bounded even if a height is re-dispatched after the
// finalizer passed it.
func TestPruneDispatchedBelow(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}
	sm.markDispatched(100)
	sm.markDispatched(101)
	sm.markDispatched(102)

	sm.pruneDispatchedBelow(102)

	require.False(t, sm.isDispatched(100))
	require.False(t, sm.isDispatched(101))
	require.True(t, sm.isDispatched(102), "the floor itself is retained")
}

// TestGapShouldResync: the watchdog only re-requests when the stall detector has
// fired AND the awaited height was never dispatched (a real lost body). A height
// that is dispatched-but-not-yet-finalized (the betfair false-positive) is
// suppressed.
func TestGapShouldResync(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	// Detector not fired → never resync, regardless of dispatch state.
	require.False(t, sm.gapShouldResync(false, 500))

	// Fired but the awaited height is in-flight (dispatched, finalization just
	// slow) → suppress (this was the 42x false-positive on betfair).
	sm.markDispatched(500)
	require.False(t, sm.gapShouldResync(true, 500), "in-flight height must not be re-requested")

	// Fired and the awaited height was never dispatched → real gap → resync.
	require.True(t, sm.gapShouldResync(true, 700))
}
