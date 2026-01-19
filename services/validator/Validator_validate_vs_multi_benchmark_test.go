//go:build aerospike

package validator

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// BenchmarkValidate_vs_ValidateMulti_1M_Txs compares the performance of:
// 1. Calling Validate(tx) 1 million times (with go-batcher internally batching to Aerospike)
// 2. Calling ValidateMulti([]tx) once with 1 million transactions (batch operations at level granularity)
//
// This benchmark demonstrates the performance difference between the two validation approaches
// when using Aerospike as the UTXO store. Script verification is skipped to focus on UTXO
// operations and coordination overhead.
func BenchmarkValidate_vs_ValidateMulti_1M_Txs(b *testing.B) {
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

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true // Disable block assembly for cleaner benchmark

	// Define transaction counts to test
	txCounts := []int{
		1000,    // 1K transactions
		10000,   // 10K transactions
		100000,  // 100K transactions
		1000000, // 1M transactions
	}

	for _, txCount := range txCounts {
		b.Run(fmt.Sprintf("Validate_%d_txs", txCount), func(b *testing.B) {
			b.StopTimer()

			// Create unique UTXO store for this sub-benchmark using a unique namespace/set
			aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo_validate_%d&externalStore=file://./data/external_bench_%d", host, port, txCount, txCount))
			require.NoError(b, err)

			store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
			require.NoError(b, err)
			store.SetBlockHeight(100000)

			// Create validator
			v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
			require.NoError(b, err)

			// Generate test transactions and fund their inputs
			txs, numChains, chainDepth, err := generateChainedTransactionsWithLevels(ctx, store, txCount, 100000)
			require.NoError(b, err)

			b.ResetTimer()
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				// Call Validate(tx) for each transaction, level by level
				// This is necessary because level N+1 depends on level N being validated first
				for level := 0; level < chainDepth; level++ {
					// Process all transactions at this level
					for chainIdx := 0; chainIdx < numChains; chainIdx++ {
						txIdx := level*numChains + chainIdx
						if txIdx >= len(txs) {
							break
						}
						tx := txs[txIdx]
						_, err := v.Validate(ctx, tx, 100001, WithSkipScriptVerification(true))
						if err != nil {
							b.Logf("Validation error for tx %s at level %d: %v", tx.TxID(), level, err)
						}
					}
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(txCount*b.N), "total_txs")
			b.ReportMetric(float64(txCount*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})

		b.Run(fmt.Sprintf("ValidateMulti_%d_txs", txCount), func(b *testing.B) {
			b.StopTimer()

			// Create unique UTXO store for this sub-benchmark using a unique namespace/set
			aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo_multi_%d&externalStore=file://./data/external_bench_multi_%d", host, port, txCount, txCount))
			require.NoError(b, err)

			store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
			require.NoError(b, err)
			store.SetBlockHeight(100000)

			// Create validator
			v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
			require.NoError(b, err)

			// Generate test transactions and fund their inputs
			txs, _, _, err := generateChainedTransactionsWithLevels(ctx, store, txCount, 100000)
			require.NoError(b, err)

			opts := NewDefaultOptions()
			opts.SkipScriptVerification = true // Skip dummy script validation for fair comparison
			opts.AutoExtendTransactions = true

			b.ResetTimer()
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				// Call ValidateMulti once with all transactions
				_, err := v.ValidateMulti(ctx, txs, 100001, opts)
				if err != nil {
					b.Logf("ValidateMulti error: %v", err)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(txCount*b.N), "total_txs")
			b.ReportMetric(float64(txCount*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})
	}
}

// generateChainedTransactionsWithLevels creates multiple transaction chains with dependency levels.
// This creates realistic transaction dependencies where transactions must be validated in order.
//
// For count transactions, the function creates chains such that:
// - Multiple independent chains run in parallel
// - Each chain has transactions that depend on the previous one
// - This forces level-by-level validation
//
// Example for 1M transactions:
// - 10,000 chains of 100 transactions each
// - Level 0: 10K txs (spending from 10K funding UTXOs)
// - Level 1: 10K txs (each spending from a level 0 tx)
// - Level 2: 10K txs (each spending from a level 1 tx)
// - ... up to Level 99
//
// All parent UTXOs are pre-created in the UTXO store so validation can succeed.
// Returns: transactions organized by level, number of chains, chain depth, error
func generateChainedTransactionsWithLevels(ctx context.Context, store utxo.Store, count int, blockHeight uint32) ([]*bt.Tx, int, int, error) {
	// Determine chain structure based on count
	// We want ~100 levels for good benchmarking
	var numChains, chainDepth int

	switch {
	case count <= 1000:
		// For small counts: 10 chains of 100 txs each
		numChains = 10
		chainDepth = count / numChains
	case count <= 10000:
		// For 10K: 100 chains of 100 txs each
		numChains = 100
		chainDepth = count / numChains
	case count <= 100000:
		// For 100K: 1000 chains of 100 txs each
		numChains = 1000
		chainDepth = count / numChains
	default:
		// For 1M+: 10,000 chains of 100 txs each
		numChains = 10000
		chainDepth = count / numChains
	}

	// Create funding transactions (one per chain)
	fundingTxs := make([]*bt.Tx, numChains)
	outputValue := uint64(1000000) // 0.01 BSV per output

	for i := 0; i < numChains; i++ {
		fundingTx := bt.NewTx()
		fundingTx.Version = 1
		fundingTx.LockTime = 0

		// Add a dummy input with unique previous txid to avoid duplicate funding transaction IDs
		// Use the chain index to create a unique hash
		prevTxHash := chainhash.Hash{}
		// Set the last 4 bytes to the chain index to make it unique
		prevTxHash[28] = byte(i)
		prevTxHash[29] = byte(i >> 8)
		prevTxHash[30] = byte(i >> 16)
		prevTxHash[31] = byte(i >> 24)

		fundingInput := &bt.Input{
			PreviousTxSatoshis: 100000000, // 1 BSV
			PreviousTxScript:   createP2PKHLockingScript(),
			UnlockingScript:    createP2PKHUnlockScript(),
			SequenceNumber:     0xffffffff,
			PreviousTxOutIndex: 0,
		}
		_ = fundingInput.PreviousTxIDAdd(&prevTxHash)
		fundingTx.Inputs = append(fundingTx.Inputs, fundingInput)

		// Single output for this chain
		fundingTx.Outputs = append(fundingTx.Outputs, &bt.Output{
			Satoshis:      outputValue,
			LockingScript: createP2PKHLockingScript(),
		})

		// Store in UTXO store
		_, err := store.Create(ctx, fundingTx, blockHeight)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to create funding tx %d: %w", i, err)
		}

		fundingTxs[i] = fundingTx
	}

	// Create transaction chains organized by level
	// We need to organize transactions so that allTxs[level*numChains + chainIdx]
	// gives us the transaction at 'level' in 'chainIdx' chain
	// This allows level-by-level processing in the Validate benchmark

	// First, create all chains as separate slices
	chains := make([][]*bt.Tx, numChains)

	for chainIdx := 0; chainIdx < numChains; chainIdx++ {
		chains[chainIdx] = make([]*bt.Tx, chainDepth)

		// Get funding tx for this chain
		prevTx := fundingTxs[chainIdx]
		prevTxHash := prevTx.TxIDChainHash()
		prevOutput := prevTx.Outputs[0]
		prevAmount := prevOutput.Satoshis

		// Create chain of transactions
		for level := 0; level < chainDepth; level++ {
			tx := bt.NewTx()
			tx.Version = 1
			tx.LockTime = 0

			// Spend output from previous transaction
			input := &bt.Input{
				PreviousTxSatoshis: prevAmount,
				PreviousTxScript:   prevOutput.LockingScript,
				UnlockingScript:    createP2PKHUnlockScript(),
				SequenceNumber:     0xffffffff,
				PreviousTxOutIndex: 0,
			}
			_ = input.PreviousTxIDAdd(prevTxHash)
			tx.Inputs = append(tx.Inputs, input)

			// Create output (slightly less to account for fees)
			outputAmount := prevAmount - 100 // 100 satoshi fee per tx
			tx.Outputs = append(tx.Outputs, &bt.Output{
				Satoshis:      outputAmount,
				LockingScript: createP2PKHLockingScript(),
			})

			chains[chainIdx][level] = tx

			// Update for next iteration
			prevTx = tx
			prevTxHash = tx.TxIDChainHash()
			prevOutput = tx.Outputs[0]
			prevAmount = outputAmount
		}
	}

	// Now reorganize into level-first order
	allTxs := make([]*bt.Tx, 0, count)
	for level := 0; level < chainDepth; level++ {
		for chainIdx := 0; chainIdx < numChains; chainIdx++ {
			allTxs = append(allTxs, chains[chainIdx][level])
		}
	}

	return allTxs, numChains, chainDepth, nil
}
