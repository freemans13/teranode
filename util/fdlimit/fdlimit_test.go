package fdlimit

import (
	"math"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
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

	// Far beyond anything the OS will grant. Where the hard limit is already
	// RLIM_INFINITY (root in a container, LimitNOFILE=infinity), adding to it
	// wraps to a small number that the OS would happily grant, which would both
	// stop testing the unreachable case and fail the comparison below — so ask
	// for the hard limit itself, which is still more than can be handed out once
	// the headroom is reserved.
	request := hard
	if hard <= math.MaxUint64-1_000_000 {
		request = hard + 1_000_000
	}

	budget, _, err := Ensure(request)
	require.NoError(t, err, "an unreachable budget must not be an error")
	require.Greater(t, budget, uint64(0), "a usable budget must still be reported")
	require.Less(t, budget, request, "the budget must reflect reality, not the request")
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

	// With a tiny request the limit is untouched, so the budget is exactly the
	// current limit minus the reserve.
	require.Equal(t, soft-Headroom, budget)
	require.LessOrEqual(t, budget+Headroom, soft, "the reserve must come out of the effective limit")
}

// TestEnsureSaturatesInsteadOfWrapping pins that a required value within
// Headroom of the largest representable one still raises toward the hard limit.
// Adding the headroom to it overflows, and a wrapped total would look tiny and
// silently skip the raise instead of taking everything the OS allows.
func TestEnsureSaturatesInsteadOfWrapping(t *testing.T) {
	restoreLimit(t)

	_, hard, err := get()
	require.NoError(t, err)

	const lowered = 1024
	if hard < lowered*2 {
		t.Skip("hard limit too low to exercise a raise")
	}

	require.NoError(t, set(lowered, hard))

	// Adding Headroom to this overflows uint64. A wrapped total would look tiny,
	// sit below even the lowered soft limit, and skip the raise altogether.
	_, raised, err := Ensure(math.MaxUint64 - 1)
	require.NoError(t, err, "an unreachable budget must not be an error")
	require.True(t, raised, "the raise must still be attempted, not skipped by a wrapped total")

	soft, _, err := get()
	require.NoError(t, err)
	require.Greater(t, soft, uint64(lowered), "the soft limit must actually have moved")
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

// withSyntheticLimit swaps the rlimit syscalls for an in-memory pair so a test
// can pose limits the process cannot really adopt — notably a LOWERED hard
// limit, which is irreversible for a real process and would otherwise leak into
// every later test in this binary. Returns a pointer to the simulated soft limit
// so a test can assert whether a raise happened.
func withSyntheticLimit(t *testing.T, soft, hard uint64) *uint64 {
	t.Helper()

	realGet, realSet := get, set
	t.Cleanup(func() { get, set = realGet, realSet })

	curSoft := soft

	get = func() (uint64, uint64, error) { return curSoft, hard, nil }
	set = func(s, _ uint64) error {
		if s > hard {
			return errors.NewProcessingError("cannot exceed the hard limit")
		}

		curSoft = s

		return nil
	}

	return &curSoft
}

// TestEnsureKeepsATinyLimitClampable pins the case that had no protection at
// all: where the entire limit is no larger than Headroom, subtracting a fixed
// reserve leaves a budget of zero, the caller reads zero as "no limit worth
// clamping to", and the file store keeps its full configured concurrency —
// 1024 concurrent operations against 512 descriptors.
func TestEnsureKeepsATinyLimitClampable(t *testing.T) {
	withSyntheticLimit(t, 512, 512)

	budget, raised, err := Ensure(1024)
	require.NoError(t, err)
	require.False(t, raised, "soft is already at the hard limit; nothing to raise")
	require.Greater(t, budget, uint64(0), "a limit of 512 must still yield a clampable budget")
	require.Less(t, budget, uint64(512), "the budget must leave descriptors for sockets")
	require.Equal(t, uint64(256), budget, "half the limit is reserved when 512 cannot be")
}

// TestEnsureReserveIsFixedOnceTheLimitIsAmpleEnough pins that the proportional
// reserve is only a floor for tiny limits and does not change the normal case.
func TestEnsureReserveIsFixedOnceTheLimitIsAmpleEnough(t *testing.T) {
	withSyntheticLimit(t, 100_000, 100_000)

	budget, _, err := Ensure(1024)
	require.NoError(t, err)
	require.Equal(t, uint64(100_000)-Headroom, budget, "an ample limit reserves exactly Headroom")
}

// TestEnsureRaisesTowardTheHardLimit pins the raise using synthetic limits, so
// the assertion does not depend on what the test host happens to allow.
func TestEnsureRaisesTowardTheHardLimit(t *testing.T) {
	soft := withSyntheticLimit(t, 1024, 8192)

	budget, raised, err := Ensure(4096)
	require.NoError(t, err)
	require.True(t, raised)
	require.Equal(t, uint64(4096)+Headroom, *soft, "raised to exactly what was needed")
	require.Equal(t, uint64(4096), budget)
}

// TestEnsureStopsAtTheHardLimit pins that an unreachable request raises as far
// as the hard limit permits and reports the budget that leaves, rather than
// failing.
func TestEnsureStopsAtTheHardLimit(t *testing.T) {
	soft := withSyntheticLimit(t, 1024, 4096)

	budget, raised, err := Ensure(1_000_000)
	require.NoError(t, err)
	require.True(t, raised)
	require.Equal(t, uint64(4096), *soft, "raised to the hard limit and no further")
	require.Equal(t, uint64(4096)-Headroom, budget)
}

// TestEnsureReportsUnreadableLimit pins the platform-without-RLIMIT_NOFILE path:
// a zero budget and an error, which the caller reads as "carry on unclamped".
func TestEnsureReportsUnreadableLimit(t *testing.T) {
	realGet := get
	t.Cleanup(func() { get = realGet })

	get = func() (uint64, uint64, error) {
		return 0, 0, errors.NewProcessingError("no limit on this platform")
	}

	budget, raised, err := Ensure(1024)
	require.Error(t, err)
	require.Equal(t, uint64(0), budget)
	require.False(t, raised)
}

// TestEnsureSurvivesAFailedRaise pins that a refused setrlimit is not fatal: the
// budget falls back to what the unraised soft limit allows.
func TestEnsureSurvivesAFailedRaise(t *testing.T) {
	realGet, realSet := get, set
	t.Cleanup(func() { get, set = realGet, realSet })

	get = func() (uint64, uint64, error) { return 2048, 1 << 20, nil }
	set = func(uint64, uint64) error { return errors.NewProcessingError("refused") }

	budget, raised, err := Ensure(8192)
	require.NoError(t, err, "a refused raise must not fail startup")
	require.False(t, raised)
	require.Equal(t, uint64(2048)-Headroom, budget, "budget reflects the unraised limit")
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
