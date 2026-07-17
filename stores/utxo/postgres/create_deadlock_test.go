// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package postgres

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestIsPgDeadlock: only SQLSTATE 40P01 (deadlock) is treated as retryable, raw or
// wrapped; other pg errors and plain errors are not.
func TestIsPgDeadlock(t *testing.T) {
	require.True(t, isPgDeadlock(&pgconn.PgError{Code: "40P01"}), "raw 40P01 is a deadlock")
	// runChunk calls isPgDeadlock on the RAW error returned by attemptChunk (before
	// it is wrapped in a teranode StorageError), because a teranode-wrapped error
	// does not expose its cause to errors.As — so detection must happen on the raw
	// pgconn error, which is exactly the production path.
	require.False(t, isPgDeadlock(errors.NewStorageError("create batch rows error", &pgconn.PgError{Code: "40P01"})),
		"a teranode-wrapped error is NOT traversable by errors.As — production checks the raw error")
	require.False(t, isPgDeadlock(&pgconn.PgError{Code: "23505"}), "unique-violation is NOT a deadlock")
	require.False(t, isPgDeadlock(&pgconn.PgError{Code: "40001"}), "serialization-failure is NOT 40P01")
	require.False(t, isPgDeadlock(errors.NewStorageError("plain")), "plain error is not a deadlock")
	require.False(t, isPgDeadlock(nil), "nil is not a deadlock")
}

// TestDeadlockRetryBackoff: bounded, positive, jittered, and capped at 500ms across
// all attempts (including well past the retry cap, so the shift cannot overflow).
func TestDeadlockRetryBackoff(t *testing.T) {
	for attempt := 0; attempt < 64; attempt++ {
		b := deadlockRetryBackoff(attempt)
		require.Greater(t, b, time.Duration(0), "backoff must be positive (attempt %d)", attempt)
		require.LessOrEqual(t, b, 500*time.Millisecond, "backoff must be capped at 500ms (attempt %d)", attempt)
	}

	// Full-jitter should produce a spread (not a constant) at a given attempt, which
	// is what desynchronises concurrently-deadlocked batchers.
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 50; i++ {
		seen[deadlockRetryBackoff(5)] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "jitter must vary the backoff, not return a constant")
}
