//go:build aerospike

package validator

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// BenchmarkValidateMulti_BatchConcurrency tests the impact of BatchDirectConcurrency setting
// on ValidateMulti performance and connection usage
func BenchmarkValidateMulti_BatchConcurrency(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

	// Start Aerospike test container
	container, err := aeroTest.RunContainer(ctx)
	require.NoError(b, err)
	b.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(b, err)
	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	// Test configurations exploring different concurrency and chunk size combinations
	configs := []struct {
		name                   string
		chunkSize              int
		batchDirectConcurrency int // 0 means use ConnectionQueueSize (100)
	}{
		// Sequential processing with varying chunk sizes
		{"Sequential_Chunk100_BatchConc1", 100, 1},
		{"Sequential_Chunk200_BatchConc1", 200, 1},
		{"Sequential_Chunk250_BatchConc1", 250, 1}, // Sweet spot candidate
		{"Sequential_Chunk500_BatchConc1", 500, 1},
		{"Sequential_Chunk1000_BatchConc1", 1000, 1}, // One chunk per level

		// Minimal parallelism
		{"Minimal_Chunk100_BatchConc2", 100, 2},
		{"Minimal_Chunk100_BatchConc4", 100, 4},
		{"Minimal_Chunk150_BatchConc3", 150, 3},

		// Mid-range parallelism
		{"Midrange_Chunk100_BatchConc8", 100, 8},
		{"Midrange_Chunk125_BatchConc6", 125, 6},

		// Baseline - current default behavior
		{"Baseline_Chunk75_BatchConc100", 75, 0}, // 0 = use ConnectionQueueSize

		// Control - very small chunks with high parallelism
		{"HighParallel_Chunk50_BatchConc20", 50, 20},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			// Create settings with specific BatchDirectConcurrency
			tSettings := test.CreateBaseTestSettings(b)
			tSettings.BlockAssembly.Disabled = true

			// Optimal settings from previous testing
			tSettings.UtxoStore.SpendBatcherDurationMillis = 10
			tSettings.UtxoStore.StoreBatcherDurationMillis = 10
			tSettings.UtxoStore.GetBatcherDurationMillis = 10
			tSettings.UtxoStore.GetBatcherSize = 100
			tSettings.UtxoStore.SpendBatcherSize = 100
			tSettings.UtxoStore.StoreBatcherSize = 100
			tSettings.Validator.MultiBatchConcurrency = cfg.batchDirectConcurrency
			tSettings.Aerospike.StoreBatcherDuration = 10 * time.Millisecond

			aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=conctest_%s&externalStore=file://./data/conctest_%s",
				host, port, cfg.name, cfg.name))
			require.NoError(b, err)

			store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
			require.NoError(b, err)
			store.SetBlockHeight(100)

			// Connection tracking removed - simplified architecture

			v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
			require.NoError(b, err)

			// Generate 100K transactions: 100 levels x 1000 txs
			txs, _, _, err := generateChainedTransactionsWithSpecificStructure(ctx, store, 1000, 100)
			require.NoError(b, err)

			// Configure ValidateMulti options
			opts := NewDefaultOptions()
			opts.SkipScriptVerification = true
			opts.SkipLevelOrganization = false

			opts.BatchSize = cfg.chunkSize
			// MaxConcurrentChunks removed - concurrency now at validator level

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Connection tracking removed

				// Run ValidateMulti
				result, err := v.ValidateMulti(ctx, txs, 101, opts)
				require.NoError(b, err)

				successCount := 0
				for _, r := range result.Results {
					if r.Success {
						successCount++
					}
				}

				if successCount != len(txs) {
					b.Fatalf("Expected %d successes, got %d", len(txs), successCount)
				}
			}

			b.StopTimer()

			// Connection tracking removed - simplified architecture
			// Calculate metrics
			totalTxs := int64(len(txs) * b.N)
			elapsed := b.Elapsed()
			tps := float64(totalTxs) / elapsed.Seconds()

			// Report metrics
			b.ReportMetric(float64(len(txs)), "total_txs")
			b.ReportMetric(tps, "txs/sec")

			// Log configuration for reference
			b.Logf("Config: BatchSize=%d, BatchDirectConcurrency=%d, Levels=%d, TxsPerLevel=%d",
				cfg.chunkSize, cfg.batchDirectConcurrency, 100, 1000)
		})
	}
}
