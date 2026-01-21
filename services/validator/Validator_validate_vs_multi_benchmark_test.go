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

	// CRITICAL: Configure optimal batcher settings from previous testing
	// 10ms provides the right balance for batching without excessive delay
	tSettings.UtxoStore.SpendBatcherDurationMillis = 10 // 10ms - optimal from testing
	tSettings.UtxoStore.StoreBatcherDurationMillis = 10 // 10ms - optimal from testing
	tSettings.UtxoStore.GetBatcherDurationMillis = 10   // 10ms - optimal from testing
	tSettings.Aerospike.StoreBatcherDuration = 10 * time.Millisecond

	// Use optimal batcher size from testing
	tSettings.UtxoStore.SpendBatcherSize = 100
	tSettings.UtxoStore.StoreBatcherSize = 100
	tSettings.UtxoStore.GetBatcherSize = 100

	// CRITICAL: Increase Aerospike batch size limit to avoid chunking

	// Test transaction counts with different chain structures
	type testConfig struct {
		totalTxs      int
		numChains     int
		chainDepth    int
		description   string
		chunkSize     int
		maxConcurrent int
	}

	testConfigs := []testConfig{
		{100000, 1000, 100, "100K_OPTIMAL_Chunk75_Conc8", 75, 8}, // Your exact config!
		{100000, 1000, 100, "100K_Chunk50_Conc16", 50, 16},
		{100000, 1000, 100, "100K_Chunk75_Conc16", 75, 16},
		{100000, 1000, 100, "100K_Chunk100_Conc16", 100, 16},
		{100000, 1000, 100, "100K_Chunk100_Conc32", 100, 32},
		{100000, 1000, 100, "100K_Chunk150_Conc16", 150, 16},
	}

	for _, cfg := range testConfigs {
		// Test Validate()
		b.Run(fmt.Sprintf("Validate_%s", cfg.description), func(b *testing.B) {
			b.StopTimer()

			aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=val_%s&externalStore=file://./data/val_%s", host, port, cfg.description, cfg.description))
			require.NoError(b, err)

			store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
			require.NoError(b, err)
			store.SetBlockHeight(100)

			v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
			require.NoError(b, err)

			// Generate with specific structure
			txs, numChains, chainDepth, err := generateChainedTransactionsWithSpecificStructure(ctx, store, cfg.numChains, cfg.chainDepth)
			require.NoError(b, err)

			b.ResetTimer()
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				// Call Validate(tx) for each transaction, level by level
				// PARALLELIZE within each level to utilize go-batcher effectively
				successCount := 0
				var successMutex sync.Mutex
				levelTimes := make([]time.Duration, chainDepth)

				for level := 0; level < chainDepth; level++ {
					levelStart := time.Now()
					// Process all transactions at this level IN PARALLEL
					var wg sync.WaitGroup
					for chainIdx := 0; chainIdx < numChains; chainIdx++ {
						txIdx := level*numChains + chainIdx
						if txIdx >= len(txs) {
							break
						}
						tx := txs[txIdx]
						wg.Add(1)
						go func(transaction *bt.Tx, lvl int) {
							defer wg.Done()
							txStart := time.Now()
							_, err := v.Validate(ctx, transaction, 101, WithSkipScriptVerification(true))
							duration := time.Since(txStart)
							if err == nil {
								successMutex.Lock()
								successCount++
								successMutex.Unlock()
							}
							// Log slow transactions
							if duration > 15*time.Millisecond && level == 0 {
								b.Logf("  Level %d, TX took %v, err=%v", lvl, duration, err)
							}
						}(tx, level)
					}
					wg.Wait() // Wait for this level to complete before starting next level
					levelTimes[level] = time.Since(levelStart)
					if level < 3 {
						b.Logf("Level %d: %v (%d txs)", level, levelTimes[level], numChains)
					}
				}

				// Calculate average time per level
				var totalLevelTime time.Duration
				for _, t := range levelTimes {
					totalLevelTime += t
				}
				avgPerLevel := totalLevelTime / time.Duration(len(levelTimes))

				b.Logf("Validate processed %d txs, %d succeeded, %d failed. Avg per level: %v",
					len(txs), successCount, len(txs)-successCount, avgPerLevel)
			}

			b.StopTimer()
			b.ReportMetric(float64(cfg.totalTxs*b.N), "total_txs")
			b.ReportMetric(float64(cfg.totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})

		b.Run(fmt.Sprintf("ValidateMulti_%s", cfg.description), func(b *testing.B) {
			b.StopTimer()

			aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=multi_%s&externalStore=file://./data/multi_%s", host, port, cfg.description, cfg.description))
			require.NoError(b, err)

			store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
			require.NoError(b, err)
			store.SetBlockHeight(100)

			v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
			require.NoError(b, err)

			txs, numChains, chainDepth, err := generateChainedTransactionsWithSpecificStructure(ctx, store, cfg.numChains, cfg.chainDepth)
			require.NoError(b, err)

			opts := NewDefaultOptions()
			opts.SkipScriptVerification = true
			opts.SkipLevelOrganization = true

			opts.BatchSize = cfg.chunkSize
			// MaxConcurrentChunks removed - using simplified architecture

			// PRE-BUILD level slices BEFORE timing starts
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
				// FAIR COMPARISON: Process level-by-level like Validate() does
				successCount := 0
				levelTimes := make([]time.Duration, chainDepth)

				for level := 0; level < chainDepth; level++ {
					levelStart := time.Now()

					// Call ValidateMulti with just this level's transactions (pre-built)
					result, _ := v.ValidateMulti(ctx, levelSlices[level], 101, opts)

					// Count successes
					for _, r := range result.Results {
						if r.Success {
							successCount++
						}
					}

					levelTimes[level] = time.Since(levelStart)
				}

				// Calculate average
				var totalLevelTime time.Duration
				for _, t := range levelTimes {
					totalLevelTime += t
				}
				avgPerLevel := totalLevelTime / time.Duration(len(levelTimes))

				b.Logf("ValidateMulti processed %d txs, %d succeeded, %d failed. Avg per level: %v",
					len(txs), successCount, len(txs)-successCount, avgPerLevel)
			}

			b.StopTimer()
			b.ReportMetric(float64(cfg.totalTxs*b.N), "total_txs")
			b.ReportMetric(float64(cfg.totalTxs*b.N)/b.Elapsed().Seconds(), "txs/sec")
		})
	}
}

// BenchmarkExtensionComparison - Disabled, keeping for reference
func BenchmarkExtensionComparison_DISABLED(b *testing.B) {
	b.Skip("Disabled - use main benchmark instead")
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(b)

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
	tSettings.BlockAssembly.Disabled = true
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDurationMillis = 1
	tSettings.UtxoStore.SpendBatcherSize = 5000
	tSettings.UtxoStore.StoreBatcherSize = 5000
	tSettings.UtxoStore.GetBatcherSize = 5000

	txCount := 10000

	// Test with NON-EXTENDED transactions (must fetch parent data)
	b.Run(fmt.Sprintf("Validate_%d_txs_NOT_EXTENDED", txCount), func(b *testing.B) {
		b.StopTimer()

		aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo_validate_noext_%d&externalStore=file://./data/noext_val_%d", host, port, txCount, txCount))
		require.NoError(b, err)

		store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
		require.NoError(b, err)
		store.SetBlockHeight(100)

		v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
		require.NoError(b, err)

		txs, numChains, chainDepth, err := generateChainedTransactionsWithLevels(ctx, store, txCount, 100)
		require.NoError(b, err)

		// STRIP extension data - force both to fetch from UTXO store
		stripExtensionData(txs)

		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			successCount := 0
			var successMutex sync.Mutex

			for level := 0; level < chainDepth; level++ {
				var wg sync.WaitGroup
				for chainIdx := 0; chainIdx < numChains; chainIdx++ {
					txIdx := level*numChains + chainIdx
					if txIdx >= len(txs) {
						break
					}
					tx := txs[txIdx]
					wg.Add(1)
					go func(transaction *bt.Tx) {
						defer wg.Done()
						_, err := v.Validate(ctx, transaction, 101, WithSkipScriptVerification(true))
						if err == nil {
							successMutex.Lock()
							successCount++
							successMutex.Unlock()
						}
					}(tx)
				}
				wg.Wait()
			}
			b.Logf("Validate (NOT_EXTENDED) processed %d txs, %d succeeded", len(txs), successCount)
		}

		b.StopTimer()
		// b.ReportMetric - disabled
	})

	b.Run("ValidateMulti_NOT_EXTENDED", func(b *testing.B) {
		b.Skip("Disabled")
		b.StopTimer()

		txCount := 10000
		aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo_multi_noext_%d&externalStore=file://./data/noext_multi_%d", host, port, txCount, txCount))
		require.NoError(b, err)

		store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
		require.NoError(b, err)
		store.SetBlockHeight(100)

		v, err := New(ctx, logger, tSettings, store, nil, nil, nil, nil)
		require.NoError(b, err)

		txs, numChains, chainDepth, err := generateChainedTransactionsWithLevels(ctx, store, txCount, 100)
		require.NoError(b, err)

		// STRIP extension data - force both to fetch from UTXO store
		stripExtensionData(txs)

		opts := NewDefaultOptions()
		opts.SkipScriptVerification = true
		opts.SkipLevelOrganization = true

		opts.BatchSize = 100 // Default for NOT_EXTENDED test

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
			successCount := 0

			for level := 0; level < chainDepth; level++ {
				result, _ := v.ValidateMulti(ctx, levelSlices[level], 101, opts)

				for _, r := range result.Results {
					if r.Success {
						successCount++
					}
				}
			}
			b.Logf("ValidateMulti (NOT_EXTENDED) processed %d txs, %d succeeded", len(txs), successCount)
		}

		b.StopTimer()
		// b.ReportMetric - disabled
	})
}

// generateChainedTransactionsWithSpecificStructure creates chains with exact structure
func generateChainedTransactionsWithSpecificStructure(ctx context.Context, store utxo.Store, numChains, chainDepth int) ([]*bt.Tx, int, int, error) {
	// Create funding transactions (one per chain) - PARALLEL for speed!
	fundingTxs := make([]*bt.Tx, numChains)
	outputValue := uint64(1000000) // 0.01 BSV per output

	var fundingWg sync.WaitGroup
	fundingErrs := make(chan error, numChains)

	for i := 0; i < numChains; i++ {
		fundingWg.Add(1)
		go func(chainIdx int) {
			defer fundingWg.Done()

			fundingTx := bt.NewTx()
			fundingTx.Version = 1
			fundingTx.LockTime = 0

			prevTxHash := chainhash.Hash{}
			prevTxHash[28] = byte(chainIdx)
			prevTxHash[29] = byte(chainIdx >> 8)
			prevTxHash[30] = byte(chainIdx >> 16)
			prevTxHash[31] = byte(chainIdx >> 24)

			fundingInput := &bt.Input{
				PreviousTxSatoshis: 100000000,
				PreviousTxScript:   createP2PKHLockingScript(),
				UnlockingScript:    createP2PKHUnlockScript(),
				SequenceNumber:     0xffffffff,
				PreviousTxOutIndex: 0,
			}
			_ = fundingInput.PreviousTxIDAdd(&prevTxHash)
			fundingTx.Inputs = append(fundingTx.Inputs, fundingInput)

			fundingTx.Outputs = append(fundingTx.Outputs, &bt.Output{
				Satoshis:      outputValue,
				LockingScript: createP2PKHLockingScript(),
			})

			fundingTxs[chainIdx] = fundingTx

			_, err := store.Create(ctx, fundingTx, 100)
			if err != nil {
				fundingErrs <- fmt.Errorf("failed to create funding tx %d: %w", chainIdx, err)
			}
		}(i)
	}

	fundingWg.Wait()
	close(fundingErrs)

	if err := <-fundingErrs; err != nil {
		return nil, 0, 0, err
	}

	// Create all chains in PARALLEL
	chains := make([][]*bt.Tx, numChains)

	var chainsWg sync.WaitGroup
	for chainIdx := 0; chainIdx < numChains; chainIdx++ {
		chainsWg.Add(1)
		go func(idx int) {
			defer chainsWg.Done()

			chains[idx] = make([]*bt.Tx, chainDepth)

			prevTx := fundingTxs[idx]
			prevTxHash := prevTx.TxIDChainHash()
			prevOutput := prevTx.Outputs[0]
			prevAmount := prevOutput.Satoshis

			for level := 0; level < chainDepth; level++ {
				tx := bt.NewTx()
				tx.Version = 1
				tx.LockTime = 0

				input := &bt.Input{
					PreviousTxSatoshis: prevAmount,
					PreviousTxScript:   prevOutput.LockingScript,
					UnlockingScript:    createP2PKHUnlockScript(),
					SequenceNumber:     0xffffffff,
					PreviousTxOutIndex: 0,
				}
				_ = input.PreviousTxIDAdd(prevTxHash)
				tx.Inputs = append(tx.Inputs, input)

				outputAmount := prevAmount - 100
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      outputAmount,
					LockingScript: createP2PKHLockingScript(),
				})

				chains[idx][level] = tx

				prevTx = tx
				prevTxHash = tx.TxIDChainHash()
				prevOutput = tx.Outputs[0]
				prevAmount = outputAmount
			}
		}(chainIdx)
	}

	chainsWg.Wait()

	// Reorganize into level-first order
	allTxs := make([]*bt.Tx, 0, numChains*chainDepth)
	for level := 0; level < chainDepth; level++ {
		for chainIdx := 0; chainIdx < numChains; chainIdx++ {
			allTxs = append(allTxs, chains[chainIdx][level])
		}
	}

	return allTxs, numChains, chainDepth, nil
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

	// Create funding transactions (one per chain) - PARALLEL for speed!
	fundingTxs := make([]*bt.Tx, numChains)
	outputValue := uint64(1000000) // 0.01 BSV per output

	// Create all funding txs in parallel
	var fundingWg sync.WaitGroup
	fundingErrs := make(chan error, numChains)

	for i := 0; i < numChains; i++ {
		fundingWg.Add(1)
		go func(chainIdx int) {
			defer fundingWg.Done()

			fundingTx := bt.NewTx()
			fundingTx.Version = 1
			fundingTx.LockTime = 0

			// Add a dummy input with unique previous txid to avoid duplicate funding transaction IDs
			// Use the chain index to create a unique hash
			prevTxHash := chainhash.Hash{}
			// Set the last 4 bytes to the chain index to make it unique
			prevTxHash[28] = byte(chainIdx)
			prevTxHash[29] = byte(chainIdx >> 8)
			prevTxHash[30] = byte(chainIdx >> 16)
			prevTxHash[31] = byte(chainIdx >> 24)

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

			fundingTxs[chainIdx] = fundingTx

			// Store in UTXO store (concurrent creates will batch!)
			_, err := store.Create(ctx, fundingTx, blockHeight)
			if err != nil {
				fundingErrs <- fmt.Errorf("failed to create funding tx %d: %w", chainIdx, err)
			}
		}(i)
	}

	fundingWg.Wait()
	close(fundingErrs)

	// Check for errors
	if err := <-fundingErrs; err != nil {
		return nil, 0, 0, err
	}

	// Create transaction chains organized by level
	// We need to organize transactions so that allTxs[level*numChains + chainIdx]
	// gives us the transaction at 'level' in 'chainIdx' chain
	// This allows level-by-level processing in the Validate benchmark

	// Create all chains in PARALLEL - each chain is independent!
	chains := make([][]*bt.Tx, numChains)

	var chainsWg sync.WaitGroup
	for chainIdx := 0; chainIdx < numChains; chainIdx++ {
		chainsWg.Add(1)
		go func(idx int) {
			defer chainsWg.Done()

			chains[idx] = make([]*bt.Tx, chainDepth)

			// Get funding tx for this chain
			prevTx := fundingTxs[idx]
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

				chains[idx][level] = tx

				// Update for next iteration
				prevTx = tx
				prevTxHash = tx.TxIDChainHash()
				prevOutput = tx.Outputs[0]
				prevAmount = outputAmount
			}
		}(chainIdx)
	}

	chainsWg.Wait()

	// Now reorganize into level-first order
	allTxs := make([]*bt.Tx, 0, count)
	for level := 0; level < chainDepth; level++ {
		for chainIdx := 0; chainIdx < numChains; chainIdx++ {
			allTxs = append(allTxs, chains[chainIdx][level])
		}
	}

	return allTxs, numChains, chainDepth, nil
}

// stripExtensionData removes PreviousTxSatoshis and PreviousTxScript from all transaction inputs
// This forces both Validate() and ValidateMulti() to fetch parent data from UTXO store
func stripExtensionData(txs []*bt.Tx) {
	for _, tx := range txs {
		if tx == nil {
			continue
		}

		// CRITICAL: Use reflection to clear the internal 'extended' field
		// Otherwise IsExtended() will return true even with nil PreviousTxScript!
		// Since we can't access private field, we need to ensure PreviousTxScript is nil
		// which will make IsExtended() return false

		for _, input := range tx.Inputs {
			if input == nil {
				continue
			}
			// Clear extension data
			input.PreviousTxSatoshis = 0
			input.PreviousTxScript = nil
		}

		// Try to invalidate any cached extended state by checking
		_ = tx.IsExtended() // This should now return false
	}
}
