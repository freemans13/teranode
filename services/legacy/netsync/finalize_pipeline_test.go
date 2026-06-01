package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestFinalizeError_RecordsFirstOnly verifies the pipeline's halt signal: the
// first finalization error is recorded and surfaced (so HandleBlockDirect stops
// ingesting further blocks), and subsequent errors do not overwrite it.
func TestFinalizeError_RecordsFirstOnly(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.NoError(t, sm.finalizeError(), "no error before any finalization")

	first := errors.NewProcessingError("first finalize failure")
	sm.setFinalizeError(first)
	require.ErrorIs(t, sm.finalizeError(), first)

	second := errors.NewProcessingError("second finalize failure")
	sm.setFinalizeError(second)
	require.ErrorIs(t, sm.finalizeError(), first, "first error must not be overwritten")
}

// TestEnqueueFinalize_AbortsOnShutdown verifies enqueueFinalize does not block
// forever when the manager is shutting down: with quit closed and the handoff
// channel never drained (finalizer not consuming), the send must abort via the
// quit arm so the blockQueue consumer can exit cleanly.
func TestEnqueueFinalize_AbortsOnShutdown(t *testing.T) {
	sm := &SyncManager{
		logger: ulogger.TestLogger{},
		quit:   make(chan struct{}),
	}

	// Pre-create a full, undrained channel and mark the finalizer as already
	// "started" so enqueueFinalize will not spawn finalizeLoop — isolating the
	// send/quit race to the select in enqueueFinalize.
	sm.finalizeCh = make(chan *finalizeJob, 1)
	sm.finalizeCh <- &finalizeJob{} // fill it
	sm.finalizeOnce.Do(func() {})    // mark Once consumed; ensureFinalizer becomes a no-op

	close(sm.quit)

	done := make(chan struct{})
	go func() {
		sm.enqueueFinalize(&finalizeJob{}) // channel full → must take the quit arm
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2_000_000_000, 10_000_000, "enqueueFinalize must abort on quit, not block")
}
