package file

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSemaphoreDefaults verifies that the default semaphores are created in init()
func TestSemaphoreDefaults(t *testing.T) {
	// Verify the semaphores exist and were initialized
	require.NotNil(t, readSemaphore, "readSemaphore should be initialized")
	require.NotNil(t, writeSemaphore, "writeSemaphore should be initialized")

	// Verify they have non-zero capacity
	require.Greater(t, cap(readSemaphore), 0, "readSemaphore should have positive capacity")
	require.Greater(t, cap(writeSemaphore), 0, "writeSemaphore should have positive capacity")
}
