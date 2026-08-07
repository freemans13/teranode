package file

import (
	"testing"

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
