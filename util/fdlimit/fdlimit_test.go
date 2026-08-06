package fdlimit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnsureReportsABudgetRatherThanRefusing pins the choice that keeps this
// safe: Ensure never fails because the limit is small. The semaphores it feeds
// are a ceiling on concurrent operations, not a reservation, so a node whose
// ceiling exceeds the OS limit still runs fine — refusing to start it would
// turn a bounded, handled condition into total unavailability (issue 1431).
func TestEnsureReportsABudgetRatherThanRefusing(t *testing.T) {
	restoreLimit(t)

	_, hard, err := get()
	require.NoError(t, err, "this platform should expose RLIMIT_NOFILE")

	// Far beyond anything the OS will grant.
	budget, _, err := Ensure(hard + 1_000_000)
	require.NoError(t, err, "an unreachable budget must not be an error")
	require.Greater(t, budget, uint64(0), "a usable budget must still be reported")
	require.Less(t, budget, hard+1_000_000, "the budget must reflect reality, not the request")
}

// TestEnsureReservesHeadroom pins that the reported budget always leaves
// descriptors for everything the semaphores do NOT bound — sockets, gRPC
// connections, database pools, log files.
func TestEnsureReservesHeadroom(t *testing.T) {
	restoreLimit(t)

	soft, _, err := get()
	require.NoError(t, err)
	require.Greater(t, soft, Headroom, "test host has an unusably small limit")

	budget, _, err := Ensure(1)
	require.NoError(t, err)
	require.LessOrEqual(t, budget+Headroom, maxU64(soft, budget+Headroom),
		"budget plus headroom must never exceed the effective limit")

	// With a tiny request the limit is untouched, so the budget is exactly the
	// current limit minus the reserve.
	require.Equal(t, soft-Headroom, budget)
}

// TestEnsureRaisesASoftLimitThatIsTooLow pins the useful half of the feature:
// where the hard limit allows it, the soft limit is raised rather than the
// budget being reported as small.
func TestEnsureRaisesASoftLimitThatIsTooLow(t *testing.T) {
	restoreLimit(t)

	_, hard, err := get()
	require.NoError(t, err)

	// Drive the raise from a deliberately lowered soft limit rather than the
	// ambient one. Depending on the ambient limit made this test silently skip
	// (Go's runtime raises soft to hard-1 at startup, and an earlier test here
	// used to leave it pinned at hard), so the only coverage of the raise —
	// the useful half of this feature — never ran.
	const lowered = 1024
	if hard < lowered*2 {
		t.Skip("hard limit too low to exercise a raise")
	}

	require.NoError(t, set(lowered, hard))

	want := uint64(lowered) // needs lowered+Headroom, above the lowered soft limit

	budget, raised, err := Ensure(want)
	require.NoError(t, err)
	require.True(t, raised, "a raisable soft limit must be raised")
	require.GreaterOrEqual(t, budget, want, "the budget must cover what was asked for")

	soft, _, err := get()
	require.NoError(t, err)
	require.Greater(t, soft, uint64(lowered), "the soft limit must actually have moved")
}

// restoreLimit snapshots RLIMIT_NOFILE and puts it back when the test ends.
// Ensure mutates a process-global resource, so without this one test silently
// changes what every later test in the binary observes.
func restoreLimit(t *testing.T) {
	t.Helper()

	soft, hard, err := get()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = set(soft, hard)
	})
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}
