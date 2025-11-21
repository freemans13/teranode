// Package subtreevalidation benchmarks for transaction processing strategies.
//
// These benchmarks compare two approaches for processing transactions with dependencies:
//
// 1. processTransactionsByLevel: Level-based processing with barriers
//   - Optimal for < 100 transactions (1.3-4.2x faster)
//   - Simple coordination, lower memory overhead
//   - Level barriers become bottleneck for larger counts
//
// 2. processTransactionsPipelined: Dependency-aware pipeline
//   - Optimal for >= 100 transactions (1.1-4.7x faster)
//   - Fine-grained parallelism, no barriers
//   - Graph construction overhead for small counts
//
// Key findings:
// - Crossover point is at ~100 transactions
// - Below 100: Graph overhead dominates, use ByLevel
// - Above 100: Parallelism benefits dominate, use Pipelined
//
// Run benchmarks:
//
//	go test -bench=BenchmarkProcessTransactions -benchmem -benchtime=3s
//
// The threshold in check_block_subtrees.go (line ~956) is based on these results.
package subtreevalidation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// createTestTransactionChain creates a chain of transactions with specified depth and width
// depth = number of dependency levels
// width = number of transactions per level
func createTestTransactionChain(depth int, width int) ([]missingTx, [][]missingTx, uint32) {
	allTxs := make([]missingTx, 0, depth*width)
	txsPerLevel := make([][]missingTx, depth)

	// Create transactions level by level
	for level := 0; level < depth; level++ {
		txsPerLevel[level] = make([]missingTx, 0, width)

		for i := 0; i < width; i++ {
			tx := &bt.Tx{
				Version: 1,
				Inputs:  make([]*bt.Input, 0),
				Outputs: []*bt.Output{
					{
						Satoshis:      1000,
						LockingScript: &bscript.Script{0x00, 0x01, 0x02}, // Dummy script
					},
				},
				LockTime: 0,
			}

			// Add dependencies to previous level
			if level > 0 {
				// Each tx depends on one tx from previous level
				parentIdx := i % len(txsPerLevel[level-1])
				parentTx := txsPerLevel[level-1][parentIdx].tx

				input := &bt.Input{
					PreviousTxOutIndex: 0,
					SequenceNumber:     0xffffffff,
				}
				_ = input.PreviousTxIDAdd(parentTx.TxIDChainHash())
				tx.Inputs = append(tx.Inputs, input)
			} else {
				// Level 0 transactions are coinbase-like (no inputs)
				tx.Inputs = append(tx.Inputs, &bt.Input{
					PreviousTxOutIndex: 0xffffffff,
					SequenceNumber:     0xffffffff,
				})
			}

			// Cache the tx hash to simulate real usage
			tx.SetTxHash(tx.TxIDChainHash())

			mTx := missingTx{
				tx:  tx,
				idx: len(allTxs),
			}

			allTxs = append(allTxs, mTx)
			txsPerLevel[level] = append(txsPerLevel[level], mTx)
		}
	}

	return allTxs, txsPerLevel, uint32(depth - 1)
}

// createImbalancedTransactionChain creates a chain with imbalanced levels
// Most levels have few transactions, but some levels have many
func createImbalancedTransactionChain(depth int) ([]missingTx, [][]missingTx, uint32) {
	allTxs := make([]missingTx, 0)
	txsPerLevel := make([][]missingTx, depth)

	for level := 0; level < depth; level++ {
		// Create imbalance: every 10th level has 100 txs, others have 1-5 txs
		var width int
		if level%10 == 0 {
			width = 100
		} else {
			width = 1 + (level % 5)
		}

		txsPerLevel[level] = make([]missingTx, 0, width)

		for i := 0; i < width; i++ {
			tx := &bt.Tx{
				Version: 1,
				Inputs:  make([]*bt.Input, 0),
				Outputs: []*bt.Output{
					{
						Satoshis:      1000,
						LockingScript: &bscript.Script{0x00, 0x01, 0x02},
					},
				},
				LockTime: 0,
			}

			if level > 0 && len(txsPerLevel[level-1]) > 0 {
				parentIdx := i % len(txsPerLevel[level-1])
				parentTx := txsPerLevel[level-1][parentIdx].tx

				input := &bt.Input{
					PreviousTxOutIndex: 0,
					SequenceNumber:     0xffffffff,
				}
				_ = input.PreviousTxIDAdd(parentTx.TxIDChainHash())
				tx.Inputs = append(tx.Inputs, input)
			} else {
				tx.Inputs = append(tx.Inputs, &bt.Input{
					PreviousTxOutIndex: 0xffffffff,
					SequenceNumber:     0xffffffff,
				})
			}

			tx.SetTxHash(tx.TxIDChainHash())

			mTx := missingTx{
				tx:  tx,
				idx: len(allTxs),
			}

			allTxs = append(allTxs, mTx)
			txsPerLevel[level] = append(txsPerLevel[level], mTx)
		}
	}

	return allTxs, txsPerLevel, uint32(depth - 1)
}

// mockServer creates a minimal server for benchmarking (no real validation)
func createMockServer(t *testing.T) *Server {
	cfg := settings.NewSettings()
	cfg.SubtreeValidation.SpendBatcherSize = 50
	return &Server{
		settings: cfg,
	}
}

// mockValidationFunc simulates minimal validation work for benchmarking coordination overhead
func mockValidationFunc(ctx context.Context, tx *bt.Tx) error {
	// Simulate some minimal work to represent validation
	_ = tx.TxID()
	return nil
}

// benchmarkProcessByLevel runs the level-based processing with a mock validation function
func benchmarkProcessByLevel(ctx context.Context, server *Server, blockHash chainhash.Hash, subtreeHash chainhash.Hash,
	maxLevel uint32, txsPerLevel [][]missingTx, blockHeight uint32, blockIds map[uint32]bool,
	processedOpts *validator.Options) error {

	// Process each level in series, but all transactions within a level in parallel
	for level := uint32(0); level <= maxLevel; level++ {
		levelTxs := txsPerLevel[level]
		if len(levelTxs) == 0 {
			continue
		}

		// Create a simple error group for parallel processing
		levelCtx, cancel := context.WithCancel(ctx)

		semaphore := make(chan struct{}, server.settings.SubtreeValidation.SpendBatcherSize*2)
		errChan := make(chan error, len(levelTxs))

		for _, mTx := range levelTxs {
			tx := mTx.tx
			semaphore <- struct{}{}

			go func(tx *bt.Tx) {
				defer func() { <-semaphore }()
				if err := mockValidationFunc(levelCtx, tx); err != nil {
					errChan <- err
				}
			}(tx)
		}

		// Wait for all goroutines to complete
		for i := 0; i < cap(semaphore); i++ {
			semaphore <- struct{}{}
		}

		// Check for errors
		close(errChan)
		for err := range errChan {
			if err != nil {
				cancel()
				return err
			}
		}
		cancel()
	}

	return nil
}

// benchmarkProcessPipelined runs the pipelined processing with a mock validation function
func benchmarkProcessPipelined(ctx context.Context, server *Server, blockHash chainhash.Hash, subtreeHash chainhash.Hash,
	transactions []missingTx, blockHeight uint32, blockIds map[uint32]bool,
	processedOpts *validator.Options) error {

	// Build dependency graph
	txMap := make(map[chainhash.Hash]*pipelineTxState, len(transactions))
	dependencies := make(map[chainhash.Hash][]chainhash.Hash) // child -> parents
	childrenMap := make(map[chainhash.Hash][]chainhash.Hash)  // parent -> children

	// First pass: Build txMap
	for _, mTx := range transactions {
		if mTx.tx == nil {
			continue
		}

		// Check if this is a coinbase-like transaction (no real inputs)
		isCoinbase := len(mTx.tx.Inputs) == 1 && mTx.tx.Inputs[0].PreviousTxOutIndex == 0xffffffff && mTx.tx.Inputs[0].PreviousTxIDChainHash() == nil
		if isCoinbase {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()
		txMap[txHash] = &pipelineTxState{
			tx:               mTx.tx,
			pendingParents:   0,
			childrenWaiting:  make([]chainhash.Hash, 0),
			completionSignal: make(chan struct{}),
		}

		dependencies[txHash] = make([]chainhash.Hash, 0)
	}

	// Second pass: Build dependency relationships
	for txHash, state := range txMap {
		for _, input := range state.tx.Inputs {
			parentHash := input.PreviousTxIDChainHash()
			if parentHash == nil {
				continue
			}

			// Only track dependencies within this block
			if _, exists := txMap[*parentHash]; exists {
				dependencies[txHash] = append(dependencies[txHash], *parentHash)

				if childrenMap[*parentHash] == nil {
					childrenMap[*parentHash] = make([]chainhash.Hash, 0)
				}
				childrenMap[*parentHash] = append(childrenMap[*parentHash], txHash)
			}
		}
	}

	// Set up pending parent counts and children references
	readyQueue := make([]chainhash.Hash, 0, len(txMap))

	for txHash, state := range txMap {
		parents := dependencies[txHash]
		state.pendingParents = int32(len(parents))
		state.childrenWaiting = childrenMap[txHash]

		// Transactions with no in-block dependencies are ready immediately
		if len(parents) == 0 {
			readyQueue = append(readyQueue, txHash)
		}
	}

	// Channel for transactions that become ready
	readyChan := make(chan chainhash.Hash, len(txMap))

	// Seed the ready channel with initial ready transactions
	for _, txHash := range readyQueue {
		readyChan <- txHash
	}

	// Track completion
	var completedCount atomic.Uint64
	totalToProcess := uint64(len(txMap))

	// Worker pool
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	concurrency := server.settings.SubtreeValidation.SpendBatcherSize * 2
	errChan := make(chan error, concurrency)
	doneChan := make(chan struct{})

	// Spawn workers
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case txHash, ok := <-readyChan:
					if !ok {
						return
					}

					state := txMap[txHash]
					if state == nil {
						continue
					}

					// Validate this transaction
					if err := mockValidationFunc(ctx, state.tx); err != nil {
						errChan <- err
						return
					}

					// Mark complete and notify children
					completed := completedCount.Add(1)

					// Notify all children that this parent is complete
					for _, childHash := range state.childrenWaiting {
						childState := txMap[childHash]
						if childState == nil {
							continue
						}

						// Decrement child's pending count atomically
						remaining := atomic.AddInt32(&childState.pendingParents, -1)

						// If child has no more pending parents, it's ready to process
						if remaining == 0 {
							select {
							case readyChan <- childHash:
							case <-ctx.Done():
								return
							}
						}
					}

					if completed >= totalToProcess {
						close(doneChan)
					}
				}
			}
		}()
	}

	// Wait for completion or error
	select {
	case err := <-errChan:
		return err
	case <-doneChan:
		close(readyChan)
		return nil
	}
}

// Benchmark: Tiny tree (2 levels), very small (10 txs per level)
func BenchmarkProcessTransactions_Tiny(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(2, 10)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Very small tree (3 levels, 10 txs per level)
func BenchmarkProcessTransactions_VerySmall(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(3, 10)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Small tree (3 levels, 30 txs per level = 90 txs)
func BenchmarkProcessTransactions_Small_90(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(3, 30)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Small tree (3 levels, 50 txs per level = 150 txs)
func BenchmarkProcessTransactions_Small_150(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(3, 50)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Shallow tree (3 levels), balanced (100 txs per level)
func BenchmarkProcessTransactions_Shallow_Balanced(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(3, 100)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Deep tree (197 levels), narrow (1 tx per level)
func BenchmarkProcessTransactions_Deep_Narrow(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(197, 1)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Deep tree (197 levels), balanced (10 txs per level)
func BenchmarkProcessTransactions_Deep_Balanced(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(197, 10)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Deep tree (197 levels), imbalanced levels
func BenchmarkProcessTransactions_Deep_Imbalanced(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createImbalancedTransactionChain(197)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}

// Benchmark: Medium tree (50 levels), wide (100 txs per level)
func BenchmarkProcessTransactions_Medium_Wide(b *testing.B) {
	server := createMockServer(&testing.T{})
	allTxs, txsPerLevel, maxLevel := createTestTransactionChain(50, 100)
	ctx := context.Background()
	blockHash := chainhash.Hash{}
	subtreeHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	processedOpts := validator.ProcessOptions()

	b.Run("ByLevel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessByLevel(ctx, server, blockHash, subtreeHash, maxLevel, txsPerLevel, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})

	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkProcessPipelined(ctx, server, blockHash, subtreeHash, allTxs, 100, blockIds, processedOpts)
			require.NoError(b, err)
		}
	})
}
