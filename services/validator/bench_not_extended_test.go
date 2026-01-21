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

// BenchmarkNotExtended tests performance when transactions are NOT pre-extended
// This forces both approaches to fetch parent transaction data from UTXO store
func BenchmarkNotExtended(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(b, err)
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, _ := container.Host(ctx)
	port, _ := container.ServicePort(ctx)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDurationMillis = 1
	tSettings.Aerospike.StoreBatcherDuration = 1 * time.Millisecond
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	// Test with 100K
	testSizes := []int{100000}

	// Test different concurrency levels
	concurrencyLevels := []int{1, 2, 4, 8}

	for _, totalTxs := range testSizes {
		numChains := totalTxs / 100 // 100 levels each
		chainDepth := 100

		// Validate() - NOT EXTENDED
		b.Run(fmt.Sprintf("Validate_%dK_NOT_EXTENDED", totalTxs/1000), func(b *testing.B) {
			b.StopTimer()

			aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=val_noext_%d&externalStore=file://./data/val_noext_%d", host, port, totalTxs, totalTxs))
			store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
			store.SetBlockHeight(100)
			v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

			txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)

			// STRIP extension data - force fetching from UTXO store
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
			b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})

		for _, concurrency := range concurrencyLevels {
			// ValidateMulti() - ONE CALL - NOT EXTENDED - with concurrent level processing
			b.Run(fmt.Sprintf("ValidateMulti_%dK_ONE_CALL_Concurrency_%d", totalTxs/1000, concurrency), func(b *testing.B) {
				b.StopTimer()

				aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=multi_c%d_%d&externalStore=file://./data/multi_c%d_%d",
					host, port, concurrency, totalTxs, concurrency, totalTxs))
				store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
				store.SetBlockHeight(100)
				v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

				txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)

				// STRIP extension data - force fetching from UTXO store
				stripExtensionData(txs)

				opts := NewDefaultOptions()
				opts.SkipScriptVerification = true
				opts.SkipLevelOrganization = false // Let ValidateMulti handle DAG
				opts.AutoExtendTransactions = true // Should help with in-block parents
				// ConcurrentLevels removed - levels must be processed sequentially

				b.ResetTimer()
				b.StartTimer()

				for i := 0; i < b.N; i++ {
					result, _ := v.ValidateMulti(ctx, txs, 101, opts)

					successCount := 0
					for _, r := range result.Results {
						if r.Success {
							successCount++
						}
					}
					b.Logf("ValidateMulti (Concurrency=%d) succeeded: %d/%d", concurrency, successCount, len(txs))
				}

				b.StopTimer()
				b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
			})
		}

		// ValidateMulti() - 100 CALLS - NOT EXTENDED
		b.Run(fmt.Sprintf("ValidateMulti_%dK_100_CALLS_NOT_EXTENDED", totalTxs/1000), func(b *testing.B) {
			b.StopTimer()

			aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=multi_100_noext_%d&externalStore=file://./data/multi_100_noext_%d", host, port, totalTxs, totalTxs))
			store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
			store.SetBlockHeight(100)
			v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

			txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)

			// STRIP extension data
			stripExtensionData(txs)

			opts := NewDefaultOptions()
			opts.SkipScriptVerification = true
			opts.SkipLevelOrganization = true   // Caller handles levels
			opts.AutoExtendTransactions = false // Can't extend without parent metadata

			levelSlices := make([][]*bt.Tx, chainDepth)
			for level := 0; level < chainDepth; level++ {
				levelTxs := make([]*bt.Tx, 0, numChains)
				for chainIdx := 0; chainIdx < numChains; chainIdx++ {
					txIdx := level*numChains + chainIdx
					if txIdx < len(txs) {
						levelTxs = append(levelTxs, txs[txIdx])
					}
				}
				levelSlices[level] = levelTxs
			}

			b.ResetTimer()
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				for level := 0; level < chainDepth; level++ {
					v.ValidateMulti(ctx, levelSlices[level], 101, opts)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})
	}
}
