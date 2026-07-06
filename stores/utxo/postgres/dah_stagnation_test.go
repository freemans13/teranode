package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClassifyStall(t *testing.T) {
	threshold := 900 * time.Second

	cases := []struct {
		name          string
		sinceProgress time.Duration
		backlog       int64
		want          stallLevel
	}{
		{"no backlog never alarms regardless of age", 24 * time.Hour, 0, stallNone},
		{"fresh progress with backlog", 10 * time.Second, 5000, stallNone},
		{"just under warn threshold", 449 * time.Second, 5000, stallNone},
		{"warn at threshold/2", 450 * time.Second, 5000, stallWarn},
		{"still warn just under threshold", 899 * time.Second, 5000, stallWarn},
		{"error at threshold", 900 * time.Second, 5000, stallError},
		{"error far past threshold", 36 * time.Hour, 1, stallError},
		{"tiny backlog still alarms", 900 * time.Second, 1, stallError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyStall(tc.sinceProgress, tc.backlog, threshold))
		})
	}
}

func TestClassifyStallDisabledThreshold(t *testing.T) {
	// threshold <= 0 disables the alarm entirely (explicit ops opt-out).
	require.Equal(t, stallNone, classifyStall(24*time.Hour, 5000, 0))
}
