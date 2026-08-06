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

		require.LessOrEqual(t, read+write, 512, "clamped total must fit the budget")
		require.Greater(t, read, write, "the 3:1 read/write split must be preserved")
	})

	t.Run("keeps the store functional on a tiny budget", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 1)

		require.GreaterOrEqual(t, read, MinSemaphoreLimit, "must still allow a read")
		require.GreaterOrEqual(t, write, MinSemaphoreLimit, "must still allow a write")
	})

	t.Run("a zero budget still yields a usable store", func(t *testing.T) {
		read, write := clampSemaphoreLimits(768, 256, 0)

		require.Equal(t, MinSemaphoreLimit, read)
		require.Equal(t, MinSemaphoreLimit, write)
	})
}
