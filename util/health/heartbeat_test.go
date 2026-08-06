package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeartbeatStalled(t *testing.T) {
	t.Run("a fresh heartbeat is not stalled", func(t *testing.T) {
		h := New()
		require.False(t, h.Stalled(time.Minute))
	})

	t.Run("a stale heartbeat is stalled", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(2 * time.Minute)
		require.True(t, h.Stalled(time.Minute))
		require.Equal(t, 2*time.Minute, h.Age())

		// A beat clears it.
		h.Beat()
		require.False(t, h.Stalled(time.Minute))
	})

	t.Run("a zero deadline disables the check", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(24 * time.Hour)
		require.False(t, h.Stalled(0), "an operator who has not opted in must never be restarted")
		require.False(t, h.Stalled(-time.Second))
	})

	t.Run("a heartbeat that never beat is not a stall", func(t *testing.T) {
		// A service constructed but not yet started must not be killed.
		var h Heartbeat

		require.Zero(t, h.Age())
		require.False(t, h.Stalled(time.Nanosecond))
	})

	t.Run("a backwards clock step is not a stall", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(-time.Hour)
		require.Zero(t, h.Age())
		require.False(t, h.Stalled(time.Second))
	})
}
