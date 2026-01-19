//go:build aerospike

package validator

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// BenchmarkPipelining compares sequential vs pipelined ValidateMulti
func BenchmarkPipelining(b *testing.B) {
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
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	txCount := 10000 // 10K txs = 100 levels × 100 txs

	// Test 1: Validate (baseline with natural pipelining)
	b.Run("Validate_10K", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=pipe_val&externalStore=file://./data/pipe_val", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, numChains, chainDepth, _ := generateChainedTransactionsWithLevels(ctx, store, txCount, 100)

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
		b.ReportMetric(float64(txCount*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	// Test 2: ValidateMulti Sequential (BatchDirect)
	b.Run("ValidateMulti_10K_Sequential", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=pipe_seq&externalStore=file://./data/pipe_seq", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, numChains, chainDepth, _ := generateChainedTransactionsWithLevels(ctx, store, txCount, 100)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.SkipLevelOrganization = true
		opts.UseIndividualBatchedCalls = false // Sequential BatchDirect

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
		b.ReportMetric(float64(txCount*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	// Test 3: ValidateMulti Pipelined (Individual batched calls)
	b.Run("ValidateMulti_10K_Pipelined", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=pipe_pipe&externalStore=file://./data/pipe_pipe", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, numChains, chainDepth, _ := generateChainedTransactionsWithLevels(ctx, store, txCount, 100)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.SkipLevelOrganization = true
		opts.UseIndividualBatchedCalls = true // PIPELINED!

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
		b.ReportMetric(float64(txCount*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})
}
