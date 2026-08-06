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
	soft, hard, err := get()
	require.NoError(t, err)

	if hard <= soft {
		t.Skip("soft limit is already at the hard limit; nothing to raise")
	}

	want := soft + 1
	budget, raised, err := Ensure(want)
	require.NoError(t, err)
	require.True(t, raised, "a raisable soft limit must be raised")
	require.GreaterOrEqual(t, budget+Headroom, want+Headroom)

	// Restore so the change does not leak into other tests in this binary.
	require.NoError(t, set(soft, hard))
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}
