package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	// Without this an unmatched expectation panics in place instead of failing
	// the test, and because the real updater runs this tick in a detached
	// goroutine that panic takes the whole package binary down with it.
	stp.Test(t)

	// The tick's whole product is its log lines, and which line it picks is a
	// decision with its own logic - startup versus a wedged consumer - so the
	// choice is asserted rather than merely reached.
	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
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

	stp.On("LastDequeueTime").Return(lastDequeue).Times(4)
	stp.On("LastDequeueTime").Return(lastDequeue.Add(7 * time.Minute)).Once()
	// Tick 6 reads a timestamp fractionally ahead of its own now - see there.
	stp.On("LastDequeueTime").Return(lastDequeue.Add(8*time.Minute + time.Second)).Once()

	// The consumer is running throughout: this sequence is a wedge, not startup.
	stp.On("ConsumerStarted").Return(true)

	// Deep queue for the first two ticks; the blocking handler drains it from
	// inside its own branch before the third.
	stp.On("QueueLength").Return(int64(10_000)).Twice()
	stp.On("QueueLength").Return(int64(0)).Times(4)

	var state dequeueStallState

	// Tick 1, within the threshold: quiet.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(10*time.Second))
	require.False(t, state.stalled, "a deep queue with a live consumer is normal, not a stall")
	require.Equal(t, float64(10), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"the gauge is the artifact an operator reads, so it must carry the real gap between now and the last dequeue")
	require.Equal(t, float64(10_000), testutil.ToFloat64(prometheusBlockAssemblerQueuedTransactions),
		"depth must come from QueueLength, so the two can be read against each other")

	// Tick 2, past the threshold with work queued: the stall begins, backdated
	// to when the consumer actually stopped rather than to this tick.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(35*time.Second))
	require.True(t, state.stalled)
	require.Equal(t, lastDequeue, state.stalledSince,
		"staleness must be derived from LastDequeueTime, so backdating lands on the last dequeue exactly")
	require.False(t, state.beforeConsumerStarted, "a running consumer that stops dequeuing is a wedge, not startup")
	require.Equal(t, float64(35), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds))
	require.True(t, logger.sawWarn("intake is growing unbounded"),
		"the rising edge of a genuine wedge warns immediately")

	// Tick 3, queue drained from inside the blocking handler. The consumer has
	// not moved, so this is still the same stall.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(65*time.Second))
	require.True(t, state.stalled, "an empty queue must not be mistaken for a recovered consumer")
	require.Equal(t, lastDequeue, state.stalledSince, "the incident must keep its original start instant")
	require.Equal(t, float64(65), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"the gauge must keep climbing while the consumer is parked, even with the queue drained from inside the blocking branch")

	// Tick 4, once the repeat cadence has elapsed since the rising edge: the
	// warning repeats, still on an empty queue, because the consumer is still
	// parked. A stall that goes quiet after one line would be worse than no
	// signal - it reads as resolved.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(35*time.Second+dequeueStallWarnRepeat))
	require.True(t, state.stalled)
	require.Equal(t, lastDequeue.Add(35*time.Second+dequeueStallWarnRepeat), state.lastWarn,
		"the repeat must re-arm the cadence, so warnings keep coming at a fixed rate rather than once")

	// Tick 5, the consumer resumes: it stamps on every loop iteration, so
	// staleness collapses even though the queue is still empty.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(7*time.Minute))
	require.False(t, state.stalled, "a fresh dequeue timestamp is the only thing that ends a stall")
	require.True(t, logger.sawInfo("consumer recovered"),
		"a wedge that recovers must be reported as a recovery, not as a consumer that finally started")

	// Tick 6: now is sampled in the updater before the timestamp is read, so a
	// consumer that stamps in that window yields a negative gap. The gauge must
	// floor at zero - a dashboard claiming the consumer last ran in the future
	// is nonsense, and "it just ran" is the honest reading.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(8*time.Minute))
	require.False(t, state.stalled)
	require.Equal(t, float64(0), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"a negative gap must publish as zero, never as a negative staleness")

	require.True(t, logger.sawWarn("still stalled"),
		"a stall that goes quiet after one line reads as resolved, so the repeat cadence must warn too")

	stp.AssertExpectations(t)
}

// TestSampleBlockAssemblerMetrics_StartupIsNotReportedAsUnboundedGrowth pins the
// classification, which is the difference between a signal an operator trusts
// and one they learn to ignore.
//
// The updater goroutine starts in BlockAssembly.Init and gRPC ingest comes up in
// BlockAssembly.Start, but BlockAssembler.Start only reaches
// subtreeProcessor.Start after loadUnminedTransactions, which takes minutes on a
// busy node while AddTx is already enqueueing (BlockAssembler.go says so at the
// DrainQueue call). So a non-empty queue with a long-stale dequeue timestamp is
// the ordinary startup path on every restart. Reporting that as "intake is
// growing unbounded" at warning level, on every restart, would erode trust in
// the one signal this whole change exists to add.
//
// It is still reported, at info: a loadUnminedTransactions that never returns
// looks exactly like this, and suppressing the window outright would make that
// failure silent.
//
// The assertions that catch a regression here are the negative ones: drop the
// consumerStarted branch and this test sees the unbounded-growth warning, while
// every assertion in the sibling test still passes.
func TestSampleBlockAssemblerMetrics_StartupIsNotReportedAsUnboundedGrowth(t *testing.T) {
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.Test(t)

	// The tick's whole product is its log lines, and which line it picks is a
	// decision with its own logic - startup versus a wedged consumer - so the
	// choice is asserted rather than merely reached.
	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	stp.On("TxCount").Return(uint64(0))
	stp.On("SubtreeCount").Return(0)
	stp.On("QueueLength").Return(int64(120_000))

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// The constructor seeded the timestamp at start, and nothing has moved it
	// since, because the consumer does not exist yet.
	stp.On("LastDequeueTime").Return(start).Twice()
	stp.On("ConsumerStarted").Return(false).Twice()

	var state dequeueStallState

	// Tick 1, well past the threshold: reported, but as startup.
	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second))
	require.True(t, state.stalled, "the condition is real and must be tracked, so its repeat cadence and end are reported")
	require.True(t, state.beforeConsumerStarted, "the cause must be latched, because by the closing edge the consumer has started either way")
	require.False(t, logger.sawWarn("intake is growing unbounded"),
		"routine startup must not be reported as unbounded growth")
	require.True(t, logger.sawInfo("consumer has not started yet"))

	// Tick 2, once the repeat cadence has elapsed: still startup, still info,
	// and it names the reload as the thing to suspect if it persists.
	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second+dequeueStallWarnRepeat))
	require.True(t, state.stalled)
	require.False(t, logger.sawWarn("still stalled"), "the repeat must stay at info while the consumer simply has not started")
	require.True(t, logger.sawInfo("still has not started"))

	// Tick 3: the consumer starts. SubtreeProcessor.Start re-seeds the
	// timestamp, so staleness collapses and the gap closes - and it must be
	// reported as the consumer arriving, not as a wedge that recovered, because
	// nothing was ever wedged.
	stp.On("LastDequeueTime").Return(start.Add(5 * time.Minute)).Once()
	stp.On("ConsumerStarted").Return(true).Once()

	state = ba.sampleBlockAssemblerMetrics(state, start.Add(5*time.Minute+time.Second))
	require.False(t, state.stalled)
	require.True(t, logger.sawInfo("consumer started after"),
		"the consumer arriving is not a recovery - nothing was ever wedged")
	require.False(t, logger.sawInfo("consumer recovered"),
		"reporting startup as a recovery would imply an incident that never happened")

	stp.AssertExpectations(t)
}
