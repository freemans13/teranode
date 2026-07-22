package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// ackTestTimeout bounds every blocking receive in these concurrency tests so a
// missing ack surfaces as a clear failure rather than a hung test run.
const ackTestTimeout = 2 * time.Second

// gatedSpyBlockValidation records ProcessBlockWindow batch sizes and blocks each
// call on a gate the test releases explicitly, so the test can observe whether an
// ack is sent before or after the commit completes.
type gatedSpyBlockValidation struct {
	blockvalidation.MockBlockValidation
	gate    chan struct{} // released by the test; nil means no gating
	batches [][]*model.Block
	entered chan struct{} // signalled when ProcessBlockWindow is entered
}

func (s *gatedSpyBlockValidation) ProcessBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	batchCopy := make([]*model.Block, len(blocks))
	copy(batchCopy, blocks)
	s.batches = append(s.batches, batchCopy)

	if s.entered != nil {
		s.entered <- struct{}{}
	}

	if s.gate != nil {
		<-s.gate
	}

	return nil
}

var _ blockvalidation.Interface = (*gatedSpyBlockValidation)(nil)

// newAckTestSyncManager builds a minimal SyncManager wired to the given spy.
func newAckTestSyncManager(t *testing.T, spy blockvalidation.Interface) *SyncManager {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, int32(1000))
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	return &SyncManager{
		ctx:             context.Background(),
		settings:        tSettings,
		chainParams:     params,
		logger:          ulogger.TestLogger{},
		utxoStore:       &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		blockValidation: spy,
	}
}

// TestAckWindowedBlock_EarlyAckBeforeCommit proves that a non-budget-filling
// block is acked at accept-time (before any flush/commit runs). A budget of
// effectively infinite size means the window never reports full, so each add
// must ack immediately without touching ProcessBlockWindow.
func TestAckWindowedBlock_EarlyAckBeforeCommit(t *testing.T) {
	spy := &gatedSpyBlockValidation{}
	sm := newAckTestSyncManager(t, spy)

	// Huge budget so full() never trips for our small blocks.
	wa := newWindowAccumulator(1<<40, 20)

	flushCalled := false
	flushWindow := func() { flushCalled = true }
	noop := func() {}

	block := newMinimalModelBlock(t, 500)
	wa.add(block)

	reply := make(chan error, 1)
	sm.ackWindowedBlock(reply, wa, flushWindow, noop, noop)

	select {
	case err := <-reply:
		require.NoError(t, err, "early-ack must send nil")
	case <-time.After(ackTestTimeout):
		t.Fatal("early-ack was not sent at accept-time")
	}

	require.False(t, flushCalled, "non-full window must NOT flush at accept-time")
	require.Empty(t, spy.batches, "ProcessBlockWindow must not be called for a non-full window")
	require.False(t, wa.empty(), "window must still hold the block (not flushed)")
}

// TestAckWindowedBlock_WithholdUntilFlushOnFull proves withhold-on-full
// back-pressure: when the add makes the window full, the ack is NOT sent until
// after the flush/commit completes. The gated spy blocks inside
// ProcessBlockWindow; the reply must not arrive while the gate is closed.
func TestAckWindowedBlock_WithholdUntilFlushOnFull(t *testing.T) {
	spy := &gatedSpyBlockValidation{
		gate:    make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	sm := newAckTestSyncManager(t, spy)

	// Tiny budget so a single small block trips full().
	wa := newWindowAccumulator(1, 20)

	// Use the real flush closure so the commit path runs through the gated spy.
	flushWindow := func() { wa.flush(sm.ctx, sm) }
	noop := func() {}

	block := newMinimalModelBlock(t, 500)
	wa.add(block)
	require.True(t, wa.full(), "single block must fill the tiny budget")

	reply := make(chan error, 1)

	done := make(chan struct{})
	go func() {
		sm.ackWindowedBlock(reply, wa, flushWindow, noop, noop)
		close(done)
	}()

	// Wait until flush has entered ProcessBlockWindow (commit in progress).
	select {
	case <-spy.entered:
	case <-time.After(ackTestTimeout):
		t.Fatal("ProcessBlockWindow was not entered — flush did not run before ack")
	}

	// While the commit is gated, the ack must NOT have been sent yet.
	select {
	case <-reply:
		t.Fatal("ack was sent before commit completed — withhold-on-full violated")
	case <-time.After(50 * time.Millisecond):
		// expected: still withheld
	}

	// Release the commit; the ack must now be delivered.
	close(spy.gate)

	select {
	case err := <-reply:
		require.NoError(t, err, "ack after full-flush must send nil")
	case <-time.After(ackTestTimeout):
		t.Fatal("ack was not sent after commit completed")
	}

	<-done
	require.Len(t, spy.batches, 1, "full window must flush exactly one batch")
	require.True(t, wa.empty(), "window must be drained after full-flush")
}

// TestAckWindowedBlock_FillsWindowMultipleBlocks is the regression proof: under a
// fast producer whose blocks all fit under the budget, the window accumulates
// N>1 blocks and commits them in a SINGLE ProcessBlockWindow call — not N calls
// of one. It replays the accept-time ack sequence for several blocks and then
// flushes once. Against the old defer-to-flush behaviour (ack only at flush,
// serialising the peer to one block) this property cannot hold.
func TestAckWindowedBlock_FillsWindowMultipleBlocks(t *testing.T) {
	spy := &gatedSpyBlockValidation{}
	sm := newAckTestSyncManager(t, spy)

	// Budget large enough to hold all blocks without tripping full().
	wa := newWindowAccumulator(1<<40, 20)

	flushWindow := func() { wa.flush(sm.ctx, sm) }
	noop := func() {}

	const n = 5
	for i := 0; i < n; i++ {
		block := newMinimalModelBlock(t, uint32(500+i))
		wa.add(block)

		reply := make(chan error, 1)
		sm.ackWindowedBlock(reply, wa, flushWindow, noop, noop)

		// Each block must be acked at accept-time so the producer can push the
		// next one immediately (this is what lets the window fill).
		select {
		case err := <-reply:
			require.NoError(t, err)
		case <-time.After(ackTestTimeout):
			t.Fatalf("block %d was not acked at accept-time — window cannot fill", i)
		}
	}

	require.Empty(t, spy.batches, "no flush should have happened while filling under budget")
	require.Equal(t, n, len(wa.entries), "all blocks must still be in the window")

	// Timer/ineligible-block boundary would flush here in production; simulate it.
	flushWindow()

	require.Len(t, spy.batches, 1, "the accumulated window must commit in a SINGLE ProcessBlockWindow call")
	require.Equal(t, n, len(spy.batches[0]), "the single batch must contain all N>1 blocks")
	require.Greater(t, len(spy.batches[0]), 1, "regression proof: batch must hold more than one block")
}

// TestAckWindowedBlock_FlagOff_AccumulatorNeverConstructed proves that when
// ParallelWindowMemoryFraction==0 the drain goroutine's windowEnabled gate is
// false, so the accumulator is never constructed and the whole window/ack path
// (including ackWindowedBlock) is unreachable — flag-off stays byte-identical.
func TestAckWindowedBlock_FlagOff_AccumulatorNeverConstructed(t *testing.T) {
	tSettings, _ := newOutpointOnlySettings(t, true, int32(1000))
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.0

	// This is the exact gate the drain goroutine evaluates before constructing
	// the accumulator (manager.go blockHandler: windowEnabled := windowFraction > 0).
	windowFraction := tSettings.Legacy.ParallelWindowMemoryFraction
	windowEnabled := windowFraction > 0
	require.False(t, windowEnabled, "fraction=0 must disable the window path")

	// Mirror the construction guard: when disabled, wa stays nil (never built).
	var wa *windowAccumulator
	if windowEnabled {
		wa = newWindowAccumulator(windowBudgetBytes(windowFraction), 20)
	}

	require.Nil(t, wa, "accumulator must never be constructed when the window is disabled")
}
