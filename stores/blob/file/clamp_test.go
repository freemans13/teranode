package file

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestClampSemaphoreLimits pins the behaviour that replaced refusing to start:
// when the configured concurrency exceeds what the OS allows, it is scaled
// down to fit rather than aborting the node (issue 1431).
func TestClampSemaphoreLimits(t *testing.T) {
	t.Run("scales proportionally and fits the budget", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 512)

		// Exact values, not just "read exceeds write": half the budget for a 3:1
		// split is 384/128, and it consumes the budget exactly. A weaker check
		// here passes even if the scaling is dropped altogether.
		require.Equal(t, 384, read)
		require.Equal(t, 128, write)
		require.Equal(t, 512, read+write, "the clamped total must use the budget exactly")
	})

	t.Run("keeps the store functional on a tiny budget", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 2)

		// Two descriptors is the smallest budget that still fits one of each.
		require.Equal(t, MinSemaphoreLimit, read)
		require.Equal(t, MinSemaphoreLimit, write)
	})

	t.Run("a single-descriptor budget costs one descriptor of headroom", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 1)

		// The floor of one read and one write wins over the budget, so this is
		// the one case where the total overshoots. It borrows a single descriptor
		// from fdlimit.Headroom's 512-descriptor reserve, which is the price of
		// keeping the store able to do anything at all.
		require.Equal(t, MinSemaphoreLimit, read)
		require.Equal(t, MinSemaphoreLimit, write)
		require.Equal(t, 2, read+write, "the floor deliberately exceeds a budget of one")
	})

	t.Run("a zero budget still yields a usable store", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 0)

		require.Equal(t, MinSemaphoreLimit, read)
		require.Equal(t, MinSemaphoreLimit, write)
	})
}

// TestResolveConcurrency pins the decision InitSemaphores makes. It lives in its
// own function precisely so it can be tested: InitSemaphores is guarded by a
// sync.Once and only runs once per process, so driving these cases through it
// would need one process each.
func TestResolveConcurrency(t *testing.T) {
	const total = uint64(1024) // 768 + 256

	t.Run("clamps when the budget is short", func(t *testing.T) {
		read, write, clamped := resolveConcurrency(768, 256, total, true, 512, nil)

		require.True(t, clamped)
		require.Equal(t, 384, read)
		require.Equal(t, 128, write)
	})

	t.Run("leaves an ample budget alone", func(t *testing.T) {
		read, write, clamped := resolveConcurrency(768, 256, total, true, 100_000, nil)

		require.False(t, clamped, "nothing to clamp when the budget covers the total")
		require.Equal(t, 768, read)
		require.Equal(t, 256, write)
	})

	t.Run("a budget exactly equal to the total is not clamped", func(t *testing.T) {
		_, _, clamped := resolveConcurrency(768, 256, total, true, total, nil)

		require.False(t, clamped, "the budget fits exactly; reducing it would be wrong")
	})

	t.Run("the operator can opt out", func(t *testing.T) {
		read, write, clamped := resolveConcurrency(768, 256, total, false, 512, nil)

		// useSystemLimits=false means "use the explicit values regardless of
		// system limits", accepting the risk. The configured numbers must stand
		// even though the budget cannot cover them.
		require.False(t, clamped)
		require.Equal(t, 768, read)
		require.Equal(t, 256, write)
	})

	t.Run("an unreadable limit leaves concurrency alone", func(t *testing.T) {
		read, write, clamped := resolveConcurrency(768, 256, total, true, 0,
			errors.NewProcessingError("no limit on this platform"))

		// Windows and friends: there is no limit to clamp against, and a zero
		// budget here means "unknown", not "no descriptors".
		require.False(t, clamped)
		require.Equal(t, 768, read)
		require.Equal(t, 256, write)
	})

	t.Run("a genuinely tiny budget clamps rather than being ignored", func(t *testing.T) {
		read, write, clamped := resolveConcurrency(768, 256, total, true, 256, nil)

		// The case that previously escaped: a host whose whole open-file limit is
		// at or below the headroom used to yield budget 0 and skip the clamp,
		// running 1024 concurrent operations against 512 descriptors.
		require.True(t, clamped)
		require.LessOrEqual(t, read+write, 256)
		require.Greater(t, read, write)
	})
}
