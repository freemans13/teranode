package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSampleBlockAssemblerMetrics_DrivesTheStallSignal drives the metrics
// updater's tick directly, with the subtree processor mocked so all four
// sampled inputs are controllable.
//
// The tick is otherwise unreachable: the updater goroutine waits on a
// hard-coded time.After(5 * time.Second), so covering a two-minute repeat
// cadence through it would mean a two-minute test. Calling the method with an
// explicit now instead makes the whole sequence deterministic and instant.
//
// The server is assembled by hand rather than through setupServer because that
// helper runs Init, which starts the real updater goroutine - it would race
// this test for the mock and consume its expectations on its own schedule.
//
// What this adds over the pure observeDequeueStall table is the wiring: that
// staleness really is derived from LastDequeueTime rather than a fresh clock
// read, that depth comes from QueueLength, and that the state one tick returns
// is what the next tick consumes.
func TestSampleBlockAssemblerMetrics_DrivesTheStallSignal(t *testing.T) {
	// The gauges the tick publishes are created behind a sync.Once that New
	// normally drives; without this they are nil and the first Set panics.
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}

	ba := &BlockAssembly{
		logger:         ulogger.TestLogger{},
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	// Sampled every tick, but plays no part in the stall decision.
	stp.On("TxCount").Return(uint64(42))
	stp.On("SubtreeCount").Return(7)

	// lastDequeue is the instant the consumer was last seen in its dequeue
	// branch. It stays put for the first three ticks - which is exactly what a
	// parked consumer looks like, since nothing else stamps it - and only moves
	// when the consumer resumes.
	lastDequeue := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	stp.On("LastDequeueTime").Return(lastDequeue).Times(3)
	stp.On("LastDequeueTime").Return(lastDequeue.Add(7 * time.Minute)).Once()

	// Deep queue for the first two ticks; the blocking handler drains it from
	// inside its own branch before the third.
	stp.On("QueueLength").Return(int64(10_000)).Twice()
	stp.On("QueueLength").Return(int64(0)).Twice()

	var state dequeueStallState

	// Tick 1, within the threshold: quiet.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(10*time.Second))
	require.False(t, state.stalled, "a deep queue with a live consumer is normal, not a stall")

	// Tick 2, past the threshold with work queued: the stall begins, backdated
	// to when the consumer actually stopped rather than to this tick.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(35*time.Second))
	require.True(t, state.stalled)
	require.Equal(t, lastDequeue, state.stalledSince,
		"staleness must be derived from LastDequeueTime, so backdating lands on the last dequeue exactly")

	// Tick 3, queue drained from inside the blocking handler. The consumer has
	// not moved, so this is still the same stall.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(65*time.Second))
	require.True(t, state.stalled, "an empty queue must not be mistaken for a recovered consumer")
	require.Equal(t, lastDequeue, state.stalledSince, "the incident must keep its original start instant")

	// Tick 4, the consumer resumes: it stamps on every loop iteration, so
	// staleness collapses even though the queue is still empty.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(7*time.Minute))
	require.False(t, state.stalled, "a fresh dequeue timestamp is the only thing that ends a stall")

	stp.AssertExpectations(t)
}
