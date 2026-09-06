package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// These tests pin the seam between quick validation and the quick window: a batch registers
// its transaction ids, a spend of a coin an in-flight PREDECESSOR is still creating waits on
// that predecessor's gate, and a spend that finds no coin although the parent was registered
// is a bug on our side rather than a bad block.

// windowHarness gives the one-wave harness a two-deep window whose committer is a no-op, so
// createAndSpendUTXOsForBatch can be driven directly for two consecutive blocks.
func windowHarness(t *testing.T, dbName string) (*BlockValidation, *applyRecorder, *quickWindow, func()) {
	t.Helper()

	initPrometheusMetrics()

	bv, rec, cleanup := newOneWaveHarness(t, dbName)
	bv.settings.BlockValidation.QuickWindowBlocks = 2
	bv.settings.BlockValidation.QuickValidateSkipUtxoLock = true

	ctx, cancel := context.WithCancel(context.Background())
	w := newQuickWindow(bv.logger, 2, 64, func(context.Context, *windowEntry) error { return nil })
	w.Start(ctx)
	bv.quickWindow = w

	return bv, rec, w, func() { cancel(); cleanup() }
}

// twoBlocks returns a parent and its child, at heights the window can admit in order.
func twoBlocks(t *testing.T) (*model.Block, *model.Block) {
	t.Helper()

	blocks := chainOf(t, 2)
	blocks[0].Height = 100
	blocks[0].ID = 100
	blocks[1].Height = 101
	blocks[1].ID = 101

	return blocks[0], blocks[1]
}

// Block 2 spends an output block 1 creates. With block 1's gate open, block 2's spend must wait;
// when block 1's batch closes its gate, block 2's spend proceeds and the coin is spent by block 2.
func TestWindow_SpendOfInFlightPredecessorWaitsForItsGate(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-gate")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "gate")
	p := spendOf(t, key, root, 0, 40_000) // in block 1
	c := spendOf(t, key, p, 0, 30_000)    // in block 2, spends p

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	batch1 := oneWaveBatchFor(t, bv, b1, []*bt.Tx{p})
	batch1.window = e1
	gate1, err := e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	batch1.gate = gate1
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	// Run block 2's batch first. It must not complete until block 1's gate closes.
	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	select {
	case err := <-done2:
		t.Fatalf("block 2 completed before block 1 created its coin: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, b1, batch1))
	require.NoError(t, <-done2)
	requireSpentBy(t, bv.utxoStore, p, 0, c)
}

// A failed predecessor releases the waiter with a service error and no spend is issued.
func TestWindow_FailedPredecessorFailsTheDependentBlockAsALocalFault(t *testing.T) {
	bv, rec, w, cleanup := windowHarness(t, "window-fail")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "fail")
	p := spendOf(t, key, root, 0, 40_000)
	c := spendOf(t, key, p, 0, 30_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	_, err = e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	e1.Fail(errors.NewProcessingError("block 1 broke"))

	err = <-done2
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)
	require.Zero(t, rec.spendCallsFor(c), "no spend may be issued for a block whose parent failed")
}

// Block 2 spends only old coins: it never waits, even with block 1 in flight.
func TestWindow_IndependentSuccessorDoesNotWait(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-indep")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "indep")
	p := spendOf(t, key, root, 0, 40_000)
	q := spendOf(t, key, root, 1, 40_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	_, err = e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{q})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*q.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	select {
	case err := <-done2:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("independent block 2 waited on block 1")
	}

	requireSpentBy(t, bv.utxoStore, root, 1, q)
}

// The store answering not-found for a REGISTERED parent is a gate bug: it must be reported as
// a local fault and counted, never as a peer fault.
func TestWindow_MissOnRegisteredParentIsReclassifiedAndCounted(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-miss")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "miss")
	p := spendOf(t, key, root, 0, 40_000)
	c := spendOf(t, key, p, 0, 30_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	// Block 1 registers p and CLOSES its gate without ever creating p (simulating a gate bug).
	g1, err := e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()
	g1.Close()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	before := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)

	err = bv.createAndSpendUTXOsForBatch(ctx, b2, batch2)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal))
}
