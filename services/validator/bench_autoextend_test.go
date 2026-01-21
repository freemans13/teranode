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

// BenchmarkAutoExtend tests if AutoExtendTransactions helps or hurts
func BenchmarkAutoExtend(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(b, err)
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, _ := container.Host(ctx)
	port, _ := container.ServicePort(ctx)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.UtxoStore.SpendBatcherDurationMillis = 10
	tSettings.UtxoStore.StoreBatcherDurationMillis = 10
	tSettings.UtxoStore.GetBatcherDurationMillis = 10
	tSettings.Aerospike.StoreBatcherDuration = 10 * time.Millisecond
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	totalTxs := 100000
	numChains := 1000
	chainDepth := 100

	// Test 1: NOT_EXT with AutoExtend=true (builds parent maps 99 times)
	b.Run("NOT_EXT_AutoExtend_TRUE_chunk_75", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=noext_auto_t&externalStore=file://./data/noext_auto_t", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
		stripExtensionData(txs)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.AutoExtendTransactions = true // BUILD PARENT MAPS
		opts.BatchSize = 75

		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			v.ValidateMulti(ctx, txs, 101, opts)
		}

		b.StopTimer()
		b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	// Test 2: NOT_EXT with AutoExtend=false (fetches from Aerospike instead)
	b.Run("NOT_EXT_AutoExtend_FALSE_chunk_75", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=noext_auto_f&externalStore=file://./data/noext_auto_f", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
		stripExtensionData(txs)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.AutoExtendTransactions = false // SKIP PARENT MAPS - Just fetch!
		opts.BatchSize = 75

		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			v.ValidateMulti(ctx, txs, 101, opts)
		}

		b.StopTimer()
		b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	// Test 3: EXTENDED with AutoExtend=true (builds maps wastefully)
	b.Run("EXTENDED_AutoExtend_TRUE_chunk_75", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=ext_auto_t&externalStore=file://./data/ext_auto_t", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
		// DON'T strip - keep extended

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.AutoExtendTransactions = true // Wasteful - builds maps then skips
		opts.BatchSize = 75

		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			v.ValidateMulti(ctx, txs, 101, opts)
		}

		b.StopTimer()
		b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})

	// Test 4: EXTENDED with AutoExtend=false (should be optimal!)
	b.Run("EXTENDED_AutoExtend_FALSE_chunk_75", func(b *testing.B) {
		b.StopTimer()

		aeroURL, _ := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=ext_auto_f&externalStore=file://./data/ext_auto_f", host, port))
		store, _ := aerospike.New(ctx, logger, tSettings, aeroURL)
		store.SetBlockHeight(100)
		v, _ := New(ctx, logger, tSettings, store, nil, nil, nil, nil)

		txs, _, _, _ := generateChainedTransactionsWithSpecificStructure(ctx, store, numChains, chainDepth)
		// DON'T strip

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.AutoExtendTransactions = false // SKIP MAPS - Already extended!
		opts.BatchSize = 75

		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			v.ValidateMulti(ctx, txs, 101, opts)
		}

		b.StopTimer()
		b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
	})
}
