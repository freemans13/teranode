package utxoset

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// batcherModeCallers is the concurrent SpendAndCreate load both tests drive. batcherModeSize is
// the batch cap configured for the store/create batcher, chosen small enough that the load
// forces several flushes rather than fitting in one.
const (
	batcherModeCallers = 2000
	batcherModeSize    = 500
)

// batcherFlushCount reads the current value of teranode_batcher_batches_total, summed across
// every trigger reason, for the named batcher.
//
// The counter lives on the process-wide registry every utxoset batcher reports through (see
// util/batchermetrics: one shared prometheus.Metrics provider, registered once). It is
// cumulative and shared with every other test in the package, so a test reads it before and
// after its own load and asserts on the delta rather than the absolute value.
func batcherFlushCount(t *testing.T, name string) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var total float64

	for _, mf := range mfs {
		if mf.GetName() != "teranode_batcher_batches_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "batcher" && lp.GetValue() == name {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}

	return total
}

// driveSpendAndCreateLoad fires n concurrent CreateOnly SpendAndCreate calls through s, each its
// own transaction, and waits for every one of them to land.
//
// CreateOnly skips the spend phase entirely (see runSpendAndCreateBatch), so every call can
// share mkTx's fixed input without any of them contending on it; only their outputs, and so
// their ids, need to differ, which distinct satoshis values already give them.
func driveSpendAndCreateLoad(t *testing.T, s *Store, ctx context.Context, n int, height uint32, baseSats uint64) {
	t.Helper()

	var wg sync.WaitGroup

	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			tx := mkTx(t, 1, baseSats+uint64(i)) //nolint:gosec // test id spread, no overflow at this scale

			if _, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly()); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

// TestBatcherModeGreedyAccumulateBoundsFlushCount is task 24's proof that
// StoreBatcherGreedyAccumulate, not a forced drain mode, now governs the store/create batcher:
// with drain off and greedy on, 2,000 concurrent creates against a batch cap of 500 must flush
// close to the theoretical minimum, because greedy only pulls what is already queued into the
// batch faster and never fires one early.
//
// Run three times (see the t.Run loop) because a bound this close to the minimum is the first
// place a scheduling fluke would show up as a flake.
func TestBatcherModeGreedyAccumulateBoundsFlushCount(t *testing.T) {
	bound := math.Ceil(float64(batcherModeCallers)/float64(batcherModeSize)) + 2

	for attempt := 1; attempt <= 3; attempt++ {
		t.Run(fmt.Sprintf("run%d", attempt), func(t *testing.T) {
			s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
				st.UtxoStore.StoreBatcherSize = batcherModeSize
				st.UtxoStore.StoreBatcherDurationMillis = 50
				st.UtxoStore.StoreBatcherDrainMode = false
				st.UtxoStore.StoreBatcherGreedyAccumulate = true
				st.UtxoStore.BatcherMaxConcurrent = 8
				st.BatcherBackground = true
			})

			before := batcherFlushCount(t, "utxoset_spend_and_create")

			driveSpendAndCreateLoad(t, s, ctx, batcherModeCallers, 100, uint64(attempt)*10_000_000)

			flushes := batcherFlushCount(t, "utxoset_spend_and_create") - before

			t.Logf("greedy accumulate: %v flushes for %d callers (bound %v)", flushes, batcherModeCallers, bound)

			require.LessOrEqualf(t, flushes, bound,
				"greedy accumulate should keep flushes near the size-driven minimum: got %v flushes for %d callers at batch size %d",
				flushes, batcherModeCallers, batcherModeSize)
		})
	}
}

// TestBatcherModeDrainProducesAdaptiveSmallBatches is the other half of the same proof: drain
// mode still exists and still behaves as before when an operator asks for it. With drain on
// (greedy off) the same 2,000-caller load must flush MORE than the size-driven bound above,
// because drain fires whatever has queued the moment a worker is free rather than waiting for
// the batch to fill.
func TestBatcherModeDrainProducesAdaptiveSmallBatches(t *testing.T) {
	bound := math.Ceil(float64(batcherModeCallers)/float64(batcherModeSize)) + 2

	for attempt := 1; attempt <= 3; attempt++ {
		t.Run(fmt.Sprintf("run%d", attempt), func(t *testing.T) {
			s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
				st.UtxoStore.StoreBatcherSize = batcherModeSize
				st.UtxoStore.StoreBatcherDurationMillis = 50
				st.UtxoStore.StoreBatcherDrainMode = true
				st.UtxoStore.StoreBatcherGreedyAccumulate = false
				st.UtxoStore.BatcherMaxConcurrent = 8
				st.BatcherBackground = true
			})

			before := batcherFlushCount(t, "utxoset_spend_and_create")

			driveSpendAndCreateLoad(t, s, ctx, batcherModeCallers, 100, uint64(1000+attempt)*10_000_000)

			flushes := batcherFlushCount(t, "utxoset_spend_and_create") - before

			t.Logf("drain mode: %v flushes for %d callers (bound %v)", flushes, batcherModeCallers, bound)

			require.Greaterf(t, flushes, bound,
				"drain mode should still produce adaptive, smaller-than-cap batches: got %v flushes for %d callers at batch size %d",
				flushes, batcherModeCallers, batcherModeSize)
		})
	}
}
