package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestIsSweepBudgetExceeded pins the classifier that decides whether a per-partition
// sweep failure is a time-budget overrun (→ shrink the height window and retry) or a
// hard error (→ abort the sweep). Misclassifying a statement_timeout as a hard error
// is exactly the bug that froze the DAH watermark: the sweep retried the same
// oversized window forever and never advanced.
//
// The helper is always called on the RAW driver error (the value pgx returns from
// Query/Exec), before any teranode errors.NewStorageError wrapping — so these
// direct-value cases match real usage. (teranode wrapping does not preserve the
// driver error for errors.As, which is why the check is done pre-wrap.)
func TestIsSweepBudgetExceeded(t *testing.T) {
	t.Run("nil is not a budget overrun", func(t *testing.T) {
		require.False(t, isSweepBudgetExceeded(nil))
	})

	t.Run("arbitrary errors are hard errors, not budget overruns", func(t *testing.T) {
		require.False(t, isSweepBudgetExceeded(errors.NewStorageError("disk on fire")))
		require.False(t, isSweepBudgetExceeded(errors.NewProcessingError("some failure")))
	})

	t.Run("non-timeout postgres errors are hard errors", func(t *testing.T) {
		// 23505 = unique_violation — a real error, not a budget overrun.
		require.False(t, isSweepBudgetExceeded(&pgconn.PgError{Code: usql.PgErrUniqueViolation}))
	})

	t.Run("statement_timeout (query_canceled 57014) is a budget overrun", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: usql.PgErrQueryCanceled, Message: "canceling statement due to statement timeout"}
		require.True(t, isSweepBudgetExceeded(pgErr))
	})

	t.Run("context deadline / cancellation is a budget overrun", func(t *testing.T) {
		// A sibling partition's failure cancels the errgroup context; the raw
		// context error must classify as a budget overrun, not a hard error.
		require.True(t, isSweepBudgetExceeded(context.DeadlineExceeded))
		require.True(t, isSweepBudgetExceeded(context.Canceled))
	})
}
