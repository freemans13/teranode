package smoke

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

// TestCheckBlockSubtreesSQLite tests block subtree validation with SQLite backend
func TestCheckBlockSubtreesSQLite(t *testing.T) {
	utxoStore := "sqlite:///test"

	t.Run("SmallBlockValidation", func(t *testing.T) {
		testSmallBlockValidation(t, utxoStore)
	})

	t.Run("LargeBlockValidation", func(t *testing.T) {
		testLargeBlockValidation(t, utxoStore)
	})

	t.Run("DeepChainValidation", func(t *testing.T) {
		testDeepChainValidation(t, utxoStore)
	})

	t.Run("WideTreeValidation", func(t *testing.T) {
		testWideTreeValidation(t, utxoStore)
	})

	t.Run("EmptyBlock", func(t *testing.T) {
		testEmptyBlock(t, utxoStore)
	})

	t.Run("MixedDependencies", func(t *testing.T) {
		testMixedDependencies(t, utxoStore)
	})
}

// TestCheckBlockSubtreesPostgres tests block subtree validation with PostgreSQL backend
func TestCheckBlockSubtreesPostgres(t *testing.T) {
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("SmallBlockValidation", func(t *testing.T) {
		testSmallBlockValidation(t, utxoStore)
	})

	t.Run("LargeBlockValidation", func(t *testing.T) {
		testLargeBlockValidation(t, utxoStore)
	})

	t.Run("DeepChainValidation", func(t *testing.T) {
		testDeepChainValidation(t, utxoStore)
	})

	t.Run("WideTreeValidation", func(t *testing.T) {
		testWideTreeValidation(t, utxoStore)
	})

	t.Run("EmptyBlock", func(t *testing.T) {
		testEmptyBlock(t, utxoStore)
	})

	t.Run("MixedDependencies", func(t *testing.T) {
		testMixedDependencies(t, utxoStore)
	})
}

// TestCheckBlockSubtreesAerospike tests block subtree validation with Aerospike backend
func TestCheckBlockSubtreesAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("SmallBlockValidation", func(t *testing.T) {
		testSmallBlockValidation(t, utxoStore)
	})

	t.Run("LargeBlockValidation", func(t *testing.T) {
		testLargeBlockValidation(t, utxoStore)
	})

	t.Run("DeepChainValidation", func(t *testing.T) {
		testDeepChainValidation(t, utxoStore)
	})

	t.Run("WideTreeValidation", func(t *testing.T) {
		testWideTreeValidation(t, utxoStore)
	})

	t.Run("EmptyBlock", func(t *testing.T) {
		testEmptyBlock(t, utxoStore)
	})

	t.Run("MixedDependencies", func(t *testing.T) {
		testMixedDependencies(t, utxoStore)
	})
}

// testSmallBlockValidation tests block validation with < 100 transactions (level-based strategy)
func testSmallBlockValidation(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity to get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	t.Logf("Got spendable coinbase: %s", coinbaseTx.TxIDChainHash().String())

	// Create a block with 50 transactions (triggers level-based strategy)
	// Structure: 1 parent with 10 outputs, then 9 children spending one output each,
	// creating multiple levels of dependencies
	const numTransactions = 50

	// Create parent transaction with 10 outputs
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(10, 500_000_000), // 5 BSV per output
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)
	t.Logf("Created parent transaction with 10 outputs: %s", parentTx.TxIDChainHash().String())

	// Create transactions spending from parent
	var level1Txs []*bt.Tx
	for i := 0; i < 10; i++ {
		childTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(parentTx, uint32(i)),
			transactions.WithP2PKHOutputs(4, 120_000_000), // 1.2 BSV per output
		)
		err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
		require.NoError(t, err)
		level1Txs = append(level1Txs, childTx)
	}
	t.Logf("Created %d level 1 transactions", len(level1Txs))

	// Create level 2 transactions (spending from level 1)
	txCount := 1 + 10 // parent + level1
	for _, parentL1Tx := range level1Txs {
		if txCount >= numTransactions {
			break
		}
		for outIdx := 0; outIdx < 4 && txCount < numTransactions; outIdx++ {
			childTx := td.CreateTransactionWithOptions(t,
				transactions.WithInput(parentL1Tx, uint32(outIdx)),
				transactions.WithP2PKHOutputs(1, 119_000_000), // Leave some for fees
			)
			err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
			require.NoError(t, err)
			txCount++
		}
	}
	t.Logf("Total transactions created: %d", txCount)

	// Mine a block to confirm all transactions
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has the expected number of transactions (+ coinbase)
	require.Equal(t, uint64(txCount+1), block.TransactionCount, "Block should contain all transactions plus coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Small block validation test passed (level-based strategy used)")
}

// testLargeBlockValidation tests block validation with >= 100 transactions (pipelined strategy)
func testLargeBlockValidation(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	t.Logf("Got spendable coinbase: %s", coinbaseTx.TxIDChainHash().String())

	// Create a block with 150 transactions (triggers pipelined strategy)
	const numTransactions = 150

	// Create parent transaction with 20 outputs
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(20, 250_000_000), // 2.5 BSV per output
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)
	t.Logf("Created parent transaction with 20 outputs: %s", parentTx.TxIDChainHash().String())

	// Create a large fan-out structure
	var level1Txs []*bt.Tx
	for i := 0; i < 20; i++ {
		childTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(parentTx, uint32(i)),
			transactions.WithP2PKHOutputs(7, 35_000_000), // 0.35 BSV per output
		)
		err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
		require.NoError(t, err)
		level1Txs = append(level1Txs, childTx)
	}
	t.Logf("Created %d level 1 transactions", len(level1Txs))

	// Create level 2 transactions
	txCount := 1 + 20 // parent + level1
	for _, parentL1Tx := range level1Txs {
		if txCount >= numTransactions {
			break
		}
		for outIdx := 0; outIdx < 7 && txCount < numTransactions; outIdx++ {
			childTx := td.CreateTransactionWithOptions(t,
				transactions.WithInput(parentL1Tx, uint32(outIdx)),
				transactions.WithP2PKHOutputs(1, 34_000_000),
			)
			err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
			require.NoError(t, err)
			txCount++
			if txCount >= numTransactions {
				break
			}
		}
	}
	t.Logf("Total transactions created: %d", txCount)

	// Mine a block to confirm all transactions
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has the expected number of transactions (+ coinbase)
	require.Equal(t, uint64(txCount+1), block.TransactionCount, "Block should contain all transactions plus coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Large block validation test passed (pipelined strategy used)")
}

// testDeepChainValidation tests block validation with a very deep transaction chain
func testDeepChainValidation(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	t.Logf("Got spendable coinbase: %s", coinbaseTx.TxIDChainHash().String())

	// Create a deep chain of 50 transactions (each spending from the previous)
	const chainDepth = 50

	previousTx := coinbaseTx
	for i := 0; i < chainDepth; i++ {
		newTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(previousTx, 0),
			transactions.WithP2PKHOutputs(1, 4_900_000_000-uint64(i*10_000_000)), // Gradually decrease amount
		)
		err = td.PropagationClient.ProcessTransaction(td.Ctx, newTx)
		require.NoError(t, err)

		if i%10 == 0 {
			t.Logf("Created transaction %d/%d: %s", i+1, chainDepth, newTx.TxIDChainHash().String())
		}

		previousTx = newTx
	}
	t.Logf("Created deep chain of %d transactions", chainDepth)

	// Mine a block to confirm all transactions
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has the expected number of transactions (+ coinbase)
	require.Equal(t, uint64(chainDepth+1), block.TransactionCount, "Block should contain all chain transactions plus coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Deep chain validation test passed")
}

// testWideTreeValidation tests block validation with a wide tree (many independent transactions)
func testWideTreeValidation(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	t.Logf("Got spendable coinbase: %s", coinbaseTx.TxIDChainHash().String())

	// Create a parent transaction with 100 outputs
	const numOutputs = 100

	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputs, 49_000_000), // 0.49 BSV per output
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)
	t.Logf("Created parent transaction with %d outputs: %s", numOutputs, parentTx.TxIDChainHash().String())

	// Create 100 independent child transactions (wide tree, all at same level)
	for i := 0; i < numOutputs; i++ {
		childTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(parentTx, uint32(i)),
			transactions.WithP2PKHOutputs(1, 48_000_000),
		)
		err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
		require.NoError(t, err)

		if i%20 == 0 {
			t.Logf("Created transaction %d/%d", i+1, numOutputs)
		}
	}
	t.Logf("Created wide tree with %d independent transactions", numOutputs)

	// Mine a block to confirm all transactions
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has the expected number of transactions (parent + children + coinbase)
	require.Equal(t, uint64(numOutputs+2), block.TransactionCount, "Block should contain parent, all children, and coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Wide tree validation test passed")
}

// testEmptyBlock tests validation of a block with only a coinbase transaction
func testEmptyBlock(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity (but don't create any transactions)
	td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Mine an empty block (only coinbase)
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Empty block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has only the coinbase transaction
	require.Equal(t, uint64(1), block.TransactionCount, "Block should contain only coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Empty block validation test passed")
}

// testMixedDependencies tests validation with complex mixed dependency patterns
func testMixedDependencies(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	t.Logf("Got spendable coinbase: %s", coinbaseTx.TxIDChainHash().String())

	// Create a complex dependency structure:
	// - Create parent with 10 outputs
	// - Create 2 intermediate transactions (parent1 and parent2) from the parent
	// - Create children with mixed dependencies (some depend on parent1, some on parent2, some on both)

	// Parent transaction with 10 outputs
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(10, 490_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)
	t.Logf("Created parent: %s", parentTx.TxIDChainHash().String())

	// Parent 1 with 5 outputs (spending from parentTx)
	parent1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(5, 190_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parent1)
	require.NoError(t, err)
	t.Logf("Created parent1: %s", parent1.TxIDChainHash().String())

	// Parent 2 with 5 outputs (spending from parentTx, different outputs)
	parent2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 2),
		transactions.WithInput(parentTx, 3),
		transactions.WithP2PKHOutputs(5, 190_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parent2)
	require.NoError(t, err)
	t.Logf("Created parent2: %s", parent2.TxIDChainHash().String())

	// Children with single parent dependencies
	child1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent1, 0),
		transactions.WithP2PKHOutputs(2, 90_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, child1)
	require.NoError(t, err)

	child2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent2, 0),
		transactions.WithP2PKHOutputs(2, 90_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, child2)
	require.NoError(t, err)

	// Child with multiple inputs from both parents (mixed dependency)
	childMixed := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent1, 1),
		transactions.WithInput(parent2, 1),
		transactions.WithP2PKHOutputs(1, 370_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childMixed)
	require.NoError(t, err)
	t.Logf("Created mixed dependency child: %s", childMixed.TxIDChainHash().String())

	// Grandchildren (level 3)
	grandchild1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child1, 0),
		transactions.WithInput(child2, 0),
		transactions.WithP2PKHOutputs(1, 170_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, grandchild1)
	require.NoError(t, err)

	grandchild2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(childMixed, 0),
		transactions.WithP2PKHOutputs(1, 360_000_000),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, grandchild2)
	require.NoError(t, err)

	t.Logf("Created complex dependency graph with 8 transactions")

	// Mine a block to confirm all transactions
	startTime := time.Now()
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block)
	validationTime := time.Since(startTime)

	t.Logf("Block validated successfully in %v", validationTime)
	t.Logf("Block height: %d, tx count: %d", block.Height, block.TransactionCount)

	// Verify the block has all transactions (8 + coinbase)
	// parentTx, parent1, parent2, child1, child2, childMixed, grandchild1, grandchild2, + coinbase = 9 total
	require.Equal(t, uint64(9), block.TransactionCount, "Block should contain all 8 transactions plus coinbase")

	// Verify subtrees are valid
	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// Verify merkle root
	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	t.Logf("✓ Mixed dependencies validation test passed")
}

// Helper function to wait for context with timeout
func waitWithTimeout(ctx context.Context, timeout time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return nil
	}
}
