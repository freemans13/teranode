package aerospike

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
)

// This benchmark compares the current concurrent BatchPreviousOutputsDecorate
// against the previous sequential implementation, holding everything else equal.
//
// It uses a real go-batcher with the production defaults
// (OutpointBatcherSize=100 items, OutpointBatcherDurationMillis=10 ms)
// but stubs the per-batch flush callback so we don't need a running Aerospike.
// The callback signals every item's errCh after a configurable simulated
// per-batch latency, modelling the Aerospike batch round-trip.
//
// The thing under test is therefore the interaction between
// BatchPreviousOutputsDecorate's per-tx dispatch and the batcher's
// size-or-timer flush rule — exactly the contention point the PR addresses.

// stubbedBatcherStore builds a *Store that has only the fields needed for
// BatchPreviousOutputsDecorate + PreviousOutputsDecorate to run. The
// outpointBatcher uses the real batcher.NewWithPool with a fake flush fn
// that signals each item's errCh after `simulatedLatency`.
func stubbedBatcherStore(b testing.TB, concurrency int, simulatedLatency time.Duration) (*Store, func()) {
	b.Helper()

	tSettings := &settings.Settings{}
	tSettings.UtxoStore.BatchPreviousOutputsDecorateConcurrency = concurrency
	// Production defaults — see settings/settings.go:405-406.
	const outpointBatchSize = 100
	const outpointBatchDurationMillis = 10

	s := &Store{settings: tSettings}

	flush := func(items []*batchOutpoint) {
		// Model Aerospike batch round-trip cost: fixed per-call latency,
		// independent of batch size. This is the realistic shape — Aerospike
		// batch reads scale sub-linearly with batch size, so the per-batch
		// fixed cost is what dominates wall clock.
		time.Sleep(simulatedLatency)
		for _, item := range items {
			item.errCh <- nil
		}
	}

	bat := batcher.NewWithPool(
		outpointBatchSize,
		time.Duration(outpointBatchDurationMillis)*time.Millisecond,
		flush,
		true, // background dispatch — matches production
	)
	s.outpointBatcher = bat

	cleanup := func() {
		// Drain any in-flight items by triggering a final flush, then let
		// the batcher's worker exit naturally on test completion.
		bat.Trigger()
	}
	return s, cleanup
}

// sequentialBatchPreviousOutputsDecorate is a verbatim copy of the
// pre-PR implementation (the sequential per-tx loop) so the benchmark
// compares like-for-like against the current concurrent version.
func sequentialBatchPreviousOutputsDecorate(s *Store, ctx context.Context, txs []*bt.Tx) error {
	for _, tx := range txs {
		if err := s.PreviousOutputsDecorate(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// makeBenchmarkTxs builds `numTxs` synthetic txs each with `inputsPerTx` inputs.
// All inputs have nil PreviousTxScript so PreviousOutputsDecorate actually
// submits them to the batcher (it skips already-decorated inputs).
func makeBenchmarkTxs(b testing.TB, numTxs, inputsPerTx int) []*bt.Tx {
	b.Helper()
	txs := make([]*bt.Tx, numTxs)
	for i := range txs {
		tx := &bt.Tx{Version: 1}
		for j := 0; j < inputsPerTx; j++ {
			var h chainhash.Hash
			// Distinct outpoints so the batcher can't dedupe across inputs.
			h[0] = byte(i)
			h[1] = byte(j)
			h[2] = byte(i >> 8)
			input := &bt.Input{
				UnlockingScript:    &bscript.Script{},
				PreviousTxOutIndex: uint32(j),
			}
			_ = input.PreviousTxIDAdd(&h)
			tx.Inputs = append(tx.Inputs, input)
		}
		txs[i] = tx
	}
	return txs
}

// resetTxInputs wipes the PreviousTxScript on every input so the next
// benchmark iteration re-submits them (PreviousOutputsDecorate skips inputs
// that already have a script set). Without this, subsequent iterations are
// essentially no-ops.
//
// In our stubbed flush the items don't actually get decorated, so the
// PreviousTxScript stays nil and this reset is a defensive precaution rather
// than strictly necessary.
func resetTxInputs(txs []*bt.Tx) {
	for _, tx := range txs {
		for _, in := range tx.Inputs {
			in.PreviousTxScript = nil
		}
	}
}

// BenchmarkBatchPreviousOutputsDecorate compares sequential vs concurrent
// at several concurrency levels and block sizes, with a simulated Aerospike
// per-batch latency of 500 µs (typical for an in-region batch read).
//
// Block shape: 1000 txs × 3 inputs/tx = 3000 inputs. Average mainnet shape
// at the time of writing.
func BenchmarkBatchPreviousOutputsDecorate(b *testing.B) {
	const simLatency = 500 * time.Microsecond
	const numTxs = 1000
	const inputsPerTx = 3
	ctx := context.Background()

	for _, mode := range []string{"sequential", "concurrent_1", "concurrent_4", "concurrent_16", "concurrent_64"} {
		mode := mode
		b.Run(mode, func(b *testing.B) {
			var (
				concurrency int
				runFn       func(s *Store, txs []*bt.Tx) error
			)

			switch mode {
			case "sequential":
				concurrency = 1
				runFn = func(s *Store, txs []*bt.Tx) error {
					return sequentialBatchPreviousOutputsDecorate(s, ctx, txs)
				}
			default:
				_, err := fmt.Sscanf(mode, "concurrent_%d", &concurrency)
				if err != nil {
					b.Fatalf("parse concurrency: %v", err)
				}
				runFn = func(s *Store, txs []*bt.Tx) error {
					return s.BatchPreviousOutputsDecorate(ctx, txs)
				}
			}

			s, cleanup := stubbedBatcherStore(b, concurrency, simLatency)
			defer cleanup()

			txs := makeBenchmarkTxs(b, numTxs, inputsPerTx)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetTxInputs(txs)
				if err := runFn(s, txs); err != nil {
					b.Fatalf("decorate: %v", err)
				}
			}
		})
	}
}
