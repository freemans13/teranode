//go:build aerospike

package aerospike_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

// BenchmarkMergedOpsBatcher compares the three modes (off, single, split) of the
// merged-ops batcher under a mixed Create+Spend(+Get) workload at several
// concurrency levels.
//
// Run with:
//
//	go test -tags aerospike -bench BenchmarkMergedOpsBatcher \
//	    -benchtime=10s -run=^$ -count=1 ./stores/utxo/aerospike/
func BenchmarkMergedOpsBatcher(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	modes := []string{"off", "single", "split"}
	concurrencies := []int{32, 128, 512, 1024}

	for _, mode := range modes {
		for _, conc := range concurrencies {
			name := fmt.Sprintf("mode=%s/concurrency=%d", mode, conc)
			b.Run(name, func(b *testing.B) {
				runMergedOpsBench(b, mode, conc)
			})
		}
	}
}

func runMergedOpsBench(b *testing.B, mode string, concurrency int) {
	logger := ulogger.NewErrorTestLogger(b)
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.UtxoStore.MergedOpsBatcherMode = mode
	// Merged-ops sized to match production timing: size=512, duration=1ms.
	tSettings.UtxoStore.MergedOpsBatcherSize = 512
	tSettings.UtxoStore.MergedOpsBatcherDurationMillis = 1

	// Align per-op batcher config with production propagation pods on dev-scale-1.
	// Get: size=512, dur=1ms, drain=true
	tSettings.UtxoStore.GetBatcherSize = 512
	tSettings.UtxoStore.GetBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDrainMode = true
	// Store/Create: size=512, dur=1ms, drain=true. Aerospike code reads the
	// duration from tSettings.Aerospike.StoreBatcherDuration (time.Duration),
	// not from UtxoStore.StoreBatcherDurationMillis. Set both to be safe.
	tSettings.UtxoStore.StoreBatcherSize = 512
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.UtxoStore.StoreBatcherDrainMode = true
	tSettings.Aerospike.StoreBatcherDuration = 1 * time.Millisecond
	// Spend: size=512, dur=1ms, drain=false, concurrency=256
	tSettings.UtxoStore.SpendBatcherSize = 512
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.SpendBatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherConcurrency = 256
	// Locked: size=512, dur=1ms, drain=false
	tSettings.UtxoStore.LockedBatcherSize = 512
	tSettings.UtxoStore.LockedBatcherDurationMillis = 1
	tSettings.UtxoStore.LockedBatcherDrainMode = false
	// Per-batcher in-flight cap
	tSettings.UtxoStore.BatcherMaxConcurrent = 512

	store, ctx, deferFn := initAerospikeBench(b, tSettings, logger)
	defer deferFn()

	// Pre-seed: each worker gets a private pool of parent txs to spend.
	// Build enough parents that b.N total Create+Spend pairs can run without
	// reusing a parent. We don't know b.N up-front when seeding, so we seed
	// lazily inside each worker loop by creating a fresh parent + spending it.
	//
	// Strategy: in the worker loop:
	//   1. build parent (unique satoshis), Create(parent)
	//   2. build child spending parent.Outputs[0], Spend(child)
	//   3. occasional Get on parent hash
	//
	// Each iteration of b.N performs 1 Create + 1 Spend (+ 1 Get every 4 iters).

	var counter uint64
	b.ResetTimer()
	b.SetParallelism(concurrency)

	start := make(chan struct{})
	var wg sync.WaitGroup
	totalOps := b.N

	// Use manual worker pool to control concurrency precisely.
	workCh := make(chan int, concurrency*2)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			<-start
			for range workCh {
				idx := atomic.AddUint64(&counter, 1)
				if err := runCreateSpend(ctx, store, wID, idx); err != nil {
					b.Errorf("worker %d op %d: %v", wID, idx, err)
					return
				}
			}
		}(w)
	}

	close(start)
	for i := 0; i < totalOps; i++ {
		workCh <- i
	}
	close(workCh)
	wg.Wait()

	b.StopTimer()
	// Report ops/sec across the whole run. b.N == 1 Create + 1 Spend per
	// iteration; we report iterations/sec as the headline metric so it is
	// comparable across modes.
	if b.Elapsed() > 0 {
		opsPerSec := float64(b.N) / b.Elapsed().Seconds()
		b.ReportMetric(opsPerSec, "create+spend/s")
	}
}

// runCreateSpend builds a unique parent tx, creates it, then spends its
// first output via a child tx. The parent is made unique via a counter-driven
// satoshi tweak so each iteration touches a distinct key.
func runCreateSpend(ctx context.Context, store *teranode_aerospike.Store, workerID int, idx uint64) error {
	parent, err := bt.NewTxFromString(coinbaseTx.String())
	if err != nil {
		return err
	}
	// Make parent unique. Combine worker id + idx into the satoshi delta so
	// concurrent workers do not collide.
	parent.Outputs[0].Satoshis = parent.Outputs[0].Satoshis + uint64(workerID)*1_000_000_000 + idx

	if _, err := store.Create(ctx, parent, 0); err != nil {
		return fmt.Errorf("create: %w", err)
	}

	child := bt.NewTx()
	if err := child.From(
		parent.TxIDChainHash().String(), 0,
		parent.Outputs[0].LockingScript.String(),
		parent.Outputs[0].Satoshis,
	); err != nil {
		return fmt.Errorf("child.From: %w", err)
	}
	if err := child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1); err != nil {
		return fmt.Errorf("child.PayToAddress: %w", err)
	}

	if _, err := store.Spend(ctx, child, 1); err != nil {
		return fmt.Errorf("spend: %w", err)
	}

	// Sample a Get every 4 iterations to exercise the read path too.
	if idx%4 == 0 {
		if _, err := store.Get(ctx, parent.TxIDChainHash()); err != nil {
			return fmt.Errorf("get: %w", err)
		}
	}
	return nil
}
