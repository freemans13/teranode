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

	// Note: golang.org/x/sync/semaphore.Weighted doesn't expose capacity,
	// so we can't verify the capacity directly. Checking non-nil is sufficient.
}

// TestAppliedSemaphoreLimitsBeforeInit pins that the accessor reports the
// concurrency genuinely in force before InitSemaphores runs — the defaults set
// up in init(). Reporting zeros here would name a concurrency that applies
// nowhere, and startup prints these numbers.
//
// Nothing else in this package calls InitSemaphores, so this holds whatever
// order the tests run in.
func TestAppliedSemaphoreLimitsBeforeInit(t *testing.T) {
	read, write, clamped := AppliedSemaphoreLimits()

	require.Equal(t, defaultReadLimit, read)
	require.Equal(t, defaultWriteLimit, write)
	require.False(t, clamped, "no clamp can have happened before InitSemaphores")
}
