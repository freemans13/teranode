package connmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplenishDeficit locks in the fix for the dead periodic peer-replenishment
// backstop. The original ticker used a monotonic id counter as the loop bound and
// subtracted two uint32 values, which both (a) never dialed after startup and
// (b) underflowed to ~4 billion when open > target. replenishDeficit must return
// the real, non-negative deficit and never a huge underflowed number.
func TestReplenishDeficit(t *testing.T) {
	tests := []struct {
		name   string
		open   int
		target int
		want   int
	}{
		{name: "cold start dials full target", open: 0, target: 8, want: 8},
		{name: "below target dials the gap", open: 3, target: 8, want: 5},
		{name: "at target dials nothing", open: 8, target: 8, want: 0},
		{name: "above target dials nothing (no underflow)", open: 12, target: 8, want: 0},
		{name: "single below target", open: 7, target: 8, want: 1},
		{name: "zero target", open: 0, target: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replenishDeficit(tt.open, tt.target)
			require.Equal(t, tt.want, got, "replenishDeficit(open=%d, target=%d)", tt.open, tt.target)
			require.GreaterOrEqual(t, got, 0, "deficit must never be negative")
			require.LessOrEqual(t, got, tt.target, "deficit must be bounded to at most target dials per tick")
		})
	}
}
