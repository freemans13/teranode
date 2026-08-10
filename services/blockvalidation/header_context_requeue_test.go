package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestShouldRequeueForHeaderContext pins the one decision both re-queue sites make, so they
// cannot answer it differently.
//
// They used to. The optimistic site re-queued ANY error out of CheckHeaderContextual that was not
// ErrBlockInvalid, while the non-optimistic site narrowed to ErrBlockHeaderContext with a
// documented reason: the revalidation worker is a single goroutine off a two-slot channel that
// retries three times, so re-queuing a permanent failure buys four full validation passes and
// nothing else. Both sites were right about the narrow case and only one enforced it.
//
// Nothing separates them today — every other error CheckHeaderContextual can return is
// unreachable (CalculateMedianTimestamp errors only on an empty window, which MedianTimeWindow
// already refuses, and the safeconversion calls cannot overflow a value derived from a uint32
// timestamp). That is exactly why this is worth pinning rather than leaving to two copies of a
// condition to keep in step: the day one becomes reachable is not the day anyone rechecks both.
func TestShouldRequeueForHeaderContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		opts *ValidateBlockOptions
		want bool
	}{
		{
			name: "header-context failure on a first attempt is re-queued",
			err:  errors.NewBlockHeaderContextError("unanchored run"),
			opts: &ValidateBlockOptions{},
			want: true,
		},
		{
			name: "header-context failure on a retry is not re-queued again",
			err:  errors.NewBlockHeaderContextError("unanchored run"),
			opts: &ValidateBlockOptions{IsRequeuedRetry: true},
			want: false,
		},
		{
			// The asymmetry this closes. A bare processing error fails identically every time —
			// model.Block.Valid reports the target-difficulty consensus check as one — so
			// re-queuing it is four guaranteed-futile validation passes ahead of real work.
			name: "a processing failure that is not a header-context failure is not re-queued",
			err:  errors.NewProcessingError("something else entirely"),
			opts: &ValidateBlockOptions{},
			want: false,
		},
		{
			name: "a storage failure is not re-queued",
			err:  errors.NewStorageError("the store is down"),
			opts: &ValidateBlockOptions{},
			want: false,
		},
		{
			name: "no failure is not re-queued",
			err:  nil,
			opts: &ValidateBlockOptions{},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldRequeueForHeaderContext(tc.err, tc.opts))
		})
	}
}

// TestShouldRequeueForHeaderContext_MatchesWrappedCause guards the property the narrow match
// depends on: ErrBlockHeaderContext wraps ErrProcessing so existing retryable handling keeps
// working, and Error.Is walks the chain by code. A wrapped header-context failure must therefore
// still be recognised, or the re-queue would silently stop firing the moment a caller added
// context to the error.
func TestShouldRequeueForHeaderContext_MatchesWrappedCause(t *testing.T) {
	wrapped := errors.NewServiceError("outer context",
		errors.NewBlockHeaderContextError("median-time-past window is not a linked chain"))

	require.True(t, errors.Is(wrapped, errors.ErrBlockHeaderContext), "precondition")
	require.True(t, shouldRequeueForHeaderContext(wrapped, &ValidateBlockOptions{}))
}
