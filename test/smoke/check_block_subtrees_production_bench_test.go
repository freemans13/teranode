// Package subtreevalidation production benchmarks with real Aerospike and validator.
//
// These benchmarks measure the actual performance of level-based vs pipelined strategies
// using production components:
//
// - Real Aerospike UTXO store (production backend)
// - Real validator with consensus rules
// - Real transaction chains with dependencies
// - Actual batch UTXO operations
//
// This validates the performance characteristics and strategy selection threshold.
//
// Run benchmarks:
//
//	go test -bench=BenchmarkProduction -benchmem -benchtime=10s -timeout=30m ./services/subtreevalidation
//
// Note: Requires Docker for Aerospike testcontainer
package smoke

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// setupProductionBenchmark creates a TestDaemon with Aerospike for benchmarking
func setupProductionBenchmark(b *testing.B) (*daemon.TestDaemon, func()) {
	b.Helper()

	// Start Aerospike container
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(b, err)

	// Convert b to *testing.T for TestDaemon
	t := &testing.T{}

	// Create TestDaemon with Aerospike
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			parsed, err := url.Parse(utxoStoreURL)
			require.NoError(b, err)
			s.UtxoStore.UtxoStore = parsed
		},
	})

	// Initialize blockchain
	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(b, err)

	cleanup := func() {
		td.Stop(t)
		_ = teardown()
	}

	return td, cleanup
}

// createTransactionChain creates a chain of transactions with specified depth and width
func createTransactionChain(b *testing.B, td *daemon.TestDaemon, depth int, width int) []*bt.Tx {
	b.Helper()

	// Get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(&testing.T{}, td.Ctx)

	allTxs := make([]*bt.Tx, 0, depth*width)
	txsPerLevel := make([][]*bt.Tx, depth)

	// Create transactions level by level
	// Level 0: parent transaction with width outputs
	// Level 1: width transactions, each spending one output from parent
	// Level 2+: width transactions, each spending from corresponding tx in prev level
	for level := 0; level < depth; level++ {
		txsPerLevel[level] = make([]*bt.Tx, 0, width)

		if level == 0 {
			// Level 0: Create ONE parent transaction with multiple outputs
			parentTx := td.CreateTransactionWithOptions(&testing.T{},
				transactions.WithInput(coinbaseTx, 0),
				transactions.WithP2PKHOutputs(width, 4_900_000_000/uint64(width)),
			)

			err := td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
			require.NoError(b, err)

			allTxs = append(allTxs, parentTx)
			txsPerLevel[level] = append(txsPerLevel[level], parentTx)
		} else if level == 1 {
			// Level 1: Each transaction spends one output from the parent
			parentTx := txsPerLevel[0][0]
			for i := 0; i < width; i++ {
				tx := td.CreateTransactionWithOptions(&testing.T{},
					transactions.WithInput(parentTx, uint32(i)),
					transactions.WithP2PKHOutputs(1, 400_000_000),
				)

				err := td.PropagationClient.ProcessTransaction(td.Ctx, tx)
				require.NoError(b, err)

				allTxs = append(allTxs, tx)
				txsPerLevel[level] = append(txsPerLevel[level], tx)
			}
		} else {
			// Level 2+: Each transaction spends from corresponding transaction in previous level
			for i := 0; i < width; i++ {
				parentTx := txsPerLevel[level-1][i]

				tx := td.CreateTransactionWithOptions(&testing.T{},
					transactions.WithInput(parentTx, 0),
					transactions.WithP2PKHOutputs(1, 300_000_000),
				)

				err := td.PropagationClient.ProcessTransaction(td.Ctx, tx)
				require.NoError(b, err)

				allTxs = append(allTxs, tx)
				txsPerLevel[level] = append(txsPerLevel[level], tx)
			}
		}
	}

	return allTxs
}

// BenchmarkProductionSmallBlock benchmarks with 41 transactions (level-based strategy)
// This tests the level-based strategy with real Aerospike and validator
func BenchmarkProductionSmallBlock(b *testing.B) {
	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 5
	const width = 10
	const totalTxs = 1 + (depth-1)*width // 1 parent + 4 levels × 10 = 41 transactions

	b.Logf("Benchmarking %d transactions (level-based strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses level-based for < 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount) // +1 for coinbase

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}

// BenchmarkProductionThresholdBlock benchmarks at the 100 tx threshold
// This tests the crossover point where strategy selection changes
func BenchmarkProductionThresholdBlock(b *testing.B) {
	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 11
	const width = 10
	const totalTxs = 1 + (depth-1)*width // 1 parent + 10 levels × 10 = 101 transactions

	b.Logf("Benchmarking %d transactions (at threshold, pipelined strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses pipelined for >= 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount)

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}

// BenchmarkProductionLargeBlock benchmarks with 191 transactions (pipelined strategy)
// This tests the pipelined strategy with real Aerospike and validator
func BenchmarkProductionLargeBlock(b *testing.B) {
	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 20
	const width = 10
	const totalTxs = 1 + (depth-1)*width // 1 parent + 19 levels × 10 = 191 transactions

	b.Logf("Benchmarking %d transactions (pipelined strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses pipelined for >= 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount)

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}

// BenchmarkProductionVeryLargeBlock benchmarks with 481 transactions
// This tests the pipelined strategy with a large block
func BenchmarkProductionVeryLargeBlock(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping very large benchmark in short mode")
	}

	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 25
	const width = 20
	const totalTxs = 1 + (depth-1)*width // 1 parent + 24 levels × 20 = 481 transactions

	b.Logf("Benchmarking %d transactions (pipelined strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses pipelined for >= 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount)

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}

// BenchmarkProductionWideBlock benchmarks with many parallel transactions
// This tests batch operations with minimal dependencies (wide tree)
func BenchmarkProductionWideBlock(b *testing.B) {
	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 2
	const width = 100
	const totalTxs = 1 + (depth-1)*width // 1 parent + 1 level × 100 = 101 transactions

	b.Logf("Benchmarking %d transactions in wide structure (pipelined strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses pipelined for >= 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount)

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}

// BenchmarkProductionDeepChain benchmarks with a deep transaction chain
// This tests pipelined strategy with sequential dependencies
func BenchmarkProductionDeepChain(b *testing.B) {
	td, cleanup := setupProductionBenchmark(b)
	defer cleanup()

	const depth = 100
	const width = 1
	const totalTxs = 1 + (depth-1)*width // 1 parent + 99 levels × 1 = 100 transactions

	b.Logf("Benchmarking %d transactions in deep chain (pipelined strategy expected)", totalTxs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create transaction chain
		txs := createTransactionChain(b, td, depth, width)

		b.StartTimer()

		// Mine block and validate (uses pipelined for >= 100 txs)
		block := td.MineAndWait(&testing.T{}, 1)
		require.NotNil(b, block)

		b.StopTimer()

		// Verify block was validated
		require.Equal(b, uint64(totalTxs+1), block.TransactionCount)

		// Clean up transactions for next iteration
		for _, tx := range txs {
			txHash := tx.TxIDChainHash()
			_ = td.UtxoStore.Delete(context.Background(), txHash)
		}
	}

	b.ReportMetric(float64(totalTxs), "txs/block")
	b.ReportMetric(float64(totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
}
