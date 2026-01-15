package model

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetOptimalSubtreeWorkerCount tests worker count calculation
func TestGetOptimalSubtreeWorkerCount(t *testing.T) {
	tests := []struct {
		name           string
		numSubtrees    int
		configuredSize int
		expected       int
	}{
		{
			name:           "default calculation with many subtrees",
			numSubtrees:    2000,
			configuredSize: 0,
			expected:       runtime.GOMAXPROCS(0) * 64,
		},
		{
			name:           "default calculation with few subtrees",
			numSubtrees:    5,
			configuredSize: 0,
			expected:       5,
		},
		{
			name:           "configured size overrides",
			numSubtrees:    1000,
			configuredSize: 256,
			expected:       256,
		},
		{
			name:           "minimum of 1 worker",
			numSubtrees:    0,
			configuredSize: 0,
			expected:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getOptimalSubtreeWorkerCount(tt.numSubtrees, tt.configuredSize)
			require.Equal(t, tt.expected, result)
		})
	}
}
