package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestIsLocalError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: true,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "wrapped context canceled",
			err:      errors.NewContextCanceledError("test", context.Canceled),
			expected: true,
		},
		{
			name:     "storage error",
			err:      errors.NewStorageError("test"),
			expected: true,
		},
		{
			name:     "network error - should retry with other peers",
			err:      errors.NewNetworkTimeoutError("test"),
			expected: false,
		},
		{
			name:     "service error - should retry with other peers",
			err:      errors.NewServiceError("test"),
			expected: false,
		},
		{
			name:     "processing error - should retry with other peers",
			err:      errors.NewProcessingError("test"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLocalError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}
