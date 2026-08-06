package fdlimit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnsureAcceptsAFittingBudget pins the normal case: a budget that already
// fits under the OS limit must not error and must not need a raise.
func TestEnsureAcceptsAFittingBudget(t *testing.T) {
	soft, _, err := get()
	require.NoError(t, err, "this platform should expose RLIMIT_NOFILE")
	require.Greater(t, soft, Headroom, "test host has an unusably small limit")

	// Ask for a budget comfortably inside the current limit.
	effective, raised, err := Ensure(soft - Headroom - 1)
	require.NoError(t, err)
	require.False(t, raised, "a budget that already fits needs no raise")
	require.GreaterOrEqual(t, effective, soft)
}

// TestEnsureRejectsAnImpossibleBudget pins the point of the change: a budget
// that cannot fit even after raising to the hard limit must fail startup with
// an actionable message, rather than letting the node run into EMFILE later
// (issue 1431).
func TestEnsureRejectsAnImpossibleBudget(t *testing.T) {
	_, hard, err := get()
	require.NoError(t, err)

	// Beyond the hard limit, which an unprivileged process cannot exceed.
	_, _, err = Ensure(hard + 1_000_000)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open-file limit too low")
	require.Contains(t, err.Error(), "headroom")
}

// TestEnsureAccountsForHeadroom pins that the budget is not allowed to consume
// the entire limit: sockets, log files and database pools also need
// descriptors, and none of them are bounded by the file store's semaphores.
func TestEnsureAccountsForHeadroom(t *testing.T) {
	soft, hard, err := get()
	require.NoError(t, err)

	if hard > soft {
		t.Skip("cannot test the ceiling when the soft limit can still be raised")
	}

	// Exactly the limit, leaving nothing for anything else, must be refused.
	_, _, err = Ensure(soft)
	require.Error(t, err)
}
