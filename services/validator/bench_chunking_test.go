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

// BenchmarkChunking tests concurrent chunk processing within levels
func BenchmarkChunking(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(b, err)
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, _ := container.Host(ctx)
	port, _ := container.ServicePort(ctx)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.UtxoStore.SpendBatcherDurationMillis = 10 // Optimal from testing
	tSettings.UtxoStore.StoreBatcherDurationMillis = 10
	tSettings.UtxoStore.GetBatcherDurationMillis = 10
	tSettings.Aerospike.StoreBatcherDuration = 10 * time.Millisecond
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	// Test with 100K (1000 txs per level)
	totalTxs := 100000
	numChains := 1000
	chainDepth := 100

	// Test different chunk sizes
	chunkSizes := []int{0, 50, 100, 200, 500}

	// Baseline: Validate()
	b.Run("Validate_100K_baseline", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=val_base&externalStore=file://./data/val_base", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
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

	// Test ValidateMulti with different chunk sizes
	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("ValidateMulti_100K_chunk_%d", chunkSize), func(b *testing.B) {
			b.StopTimer()

			aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=chunk_%d&externalStore=file://./data/chunk_%d",
				host, port, chunkSize, chunkSize))
			store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
			store.SetBlockHeight(100)
			v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

			txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
			stripExtensionData(txs)

			opts := NewDefaultOptions()
			opts.SkipScriptVerification = true
			opts.SkipLevelOrganization = false
			opts.AutoExtendTransactions = true
			opts.BatchSize = chunkSize // Enable batching!

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
				b.Logf("ChunkSize=%d succeeded: %d/%d", chunkSize, successCount, len(txs))
			}

			b.StopTimer()
			b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})
	}
}
