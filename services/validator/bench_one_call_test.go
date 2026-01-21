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

// BenchmarkOneCallVsManyCalls tests if calling ValidateMulti 100 times vs once makes a difference
func BenchmarkOneCallVsManyCalls(b *testing.B) {
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

	// 100K transactions
	b.Run("ValidateMulti_100K_CalledOnceWithDAG", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=once_dag&externalStore=file://./data/once_dag", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, 1000, 100)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.SkipLevelOrganization = false // Let ValidateMulti do DAG internally!

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
			b.Logf("ONE CALL: %d succeeded", successCount)
		}

		b.StopTimer()
		b.ReportMetric(float64(100000*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	b.Run("ValidateMulti_100K_Called100Times", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=many_calls&externalStore=file://./data/many_calls", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, numChains, chainDepth, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, 1000, 100)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.SkipLevelOrganization = true // We handle levels

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
		b.ReportMetric(float64(100000*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	b.Run("Validate_100K_Concurrent", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=validate_conc&externalStore=file://./data/validate_conc", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, numChains, chainDepth, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, 1000, 100)

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
		b.ReportMetric(float64(100000*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})
}
