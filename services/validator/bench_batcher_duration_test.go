//go:build aerospike

package validator

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// BenchmarkBatcherDuration tests impact of batcher duration on Validate() throughput
func BenchmarkBatcherDuration(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(b, err)
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, _ := container.Host(ctx)
	port, _ := container.ServicePort(ctx)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	// Test different durations
	durations := []int{1, 10, 100} // milliseconds

	// Test with 10K and 100K
	configs := []struct {
		totalTxs   int
		numChains  int
		chainDepth int
	}{
		{10000, 100, 100},    // 10K: 100 txs per level
		{100000, 1000, 100},  // 100K: 1000 txs per level
	}

	for _, cfg := range configs {
		for _, durationMs := range durations {
			// Set batcher durations
			tSettings.UtxoStore.SpendBatcherDurationMillis = durationMs
			tSettings.UtxoStore.StoreBatcherDurationMillis = durationMs
			tSettings.UtxoStore.GetBatcherDurationMillis = durationMs
			tSettings.Aerospike.StoreBatcherDuration = time.Duration(durationMs) * time.Millisecond

			// Test Validate() with NOT EXTENDED transactions
			b.Run(fmt.Sprintf("Validate_%dK_duration_%dms", cfg.totalTxs/1000, durationMs), func(b *testing.B) {
				b.StopTimer()

				aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=dur_%d_%d&externalStore=file://./data/dur_%d_%d",
					host, port, cfg.totalTxs, durationMs, cfg.totalTxs, durationMs))
				store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
				store.SetBlockHeight(100)
				v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

				txs, numChains, chainDepth, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, cfg.numChains, cfg.chainDepth)

				// STRIP extension data
				stripExtensionData(txs)

				b.ResetTimer()
				b.StartTimer()

				for i := 0; i < b.N; i++ {
					for level := 0; level < chainDepth; level++ {
						var wg sync.WaitGroup
						for chainIdx := 0; chainIdx < numChains; chainIdx++ {
							txIdx := level*numChains + chainIdx
							if txIdx >= len(txs) {
								break
							}
							tx := txs[txIdx]
							wg.Add(1)
							go func(t *bt.Tx) {
								defer wg.Done()
								v.Validate(ctx, t, 101, WithSkipScriptVerification(true))
							}(tx)
						}
						wg.Wait()
					}
				}

				b.StopTimer()
				b.ReportMetric(float64(cfg.totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
			})

			// Test ValidateMulti() for comparison
			b.Run(fmt.Sprintf("ValidateMulti_%dK_duration_%dms", cfg.totalTxs/1000, durationMs), func(b *testing.B) {
				b.StopTimer()

				aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=mdur_%d_%d&externalStore=file://./data/mdur_%d_%d",
					host, port, cfg.totalTxs, durationMs, cfg.totalTxs, durationMs))
				store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
				store.SetBlockHeight(100)
				v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

				txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, cfg.numChains, cfg.chainDepth)

				// STRIP extension data
				stripExtensionData(txs)

				opts := NewDefaultOptions()
				opts.SkipScriptVerification = true
				opts.SkipLevelOrganization = false
				opts.AutoExtendTransactions = true

				b.ResetTimer()
				b.StartTimer()

				for i := 0; i < b.N; i++ {
					v.ValidateMulti(ctx, txs, 101, opts)
				}

				b.StopTimer()
				b.ReportMetric(float64(cfg.totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
			})
		}
	}
}
