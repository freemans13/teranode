package longest_chain

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/stretchr/testify/require"
)

func TestLongestChainSQLite(t *testing.T) {
	utxoStore := "sqlite:///test"

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})
}

func TestLongestChainPostgres(t *testing.T) {
	// start a postgres container
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})
}

func TestLongestChainAerospike(t *testing.T) {
	// start an aerospike container
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, utxoStore)
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateBlockWithOldTx(t, utxoStore)
	})
}

func testLongestChainSimple(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block101 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)

	td.VerifyInBlockAssembly(t, tx1)

	_, block102a := td.CreateTestBlock(t, block101, 10201, tx1)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102a, block102a.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlock(t, block102a, blockWait)
	td.WaitForBlockBeingMined(t, block102a)

	// 0 -> 1 ... 101 -> 102a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	_, block102b := td.CreateTestBlock(t, block101, 10202, tx2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlockBeingMined(t, block102b)

	time.Sleep(1 * time.Second) // give some time for the block to be processed

	//                   / 102a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)

	_, block103b := td.CreateTestBlock(t, block102b, 10302) // empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlock(t, block103b, blockWait)
	td.WaitForBlockBeingMined(t, block103b)

	//                   / 102a
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (*)

	// tx1 should now be back in block assembly and marked as not on longest chain in the utxo store
	td.VerifyInBlockAssembly(t, tx1) // added back to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyNotInBlockAssembly(t, tx2)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)

	_, block103a := td.CreateTestBlock(t, block102a, 10301) // empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "legacy", ""), "Failed to process block")

	_, block104a := td.CreateTestBlock(t, block103a, 10401) // empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlock(t, block104a, blockWait)
	td.WaitForBlockBeingMined(t, block104a)

	//                   / 102a -> 103a -> 104a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyInBlockAssembly(t, tx2) // added back to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)
}

func testLongestChainInvalidateBlock(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block101 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))

	td.VerifyInBlockAssembly(t, tx1)

	_, block102a := td.CreateTestBlock(t, block101, 10201, tx1)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102a, block102a.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlock(t, block102a, blockWait)
	td.WaitForBlockBeingMined(t, block102a)

	// 0 -> 1 ... 101 -> 102a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	_, err = td.BlockchainClient.InvalidateBlock(t.Context(), block102a.Hash())
	require.NoError(t, err)
	td.WaitForBlock(t, block101, blockWait)

	td.VerifyInBlockAssembly(t, tx1) // re-added to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
}

func testLongestChainInvalidateBlockWithOldTx(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block101 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	td.Settings.BlockValidation.OptimisticMining = true

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))

	td.VerifyInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)

	_, block102a := td.CreateTestBlock(t, block101, 10201, tx2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102a, block102a.Height, "legacy", ""), "Failed to process block")
	td.WaitForBlock(t, block102a, blockWait)
	td.WaitForBlockBeingMined(t, block102a)

	// 0 -> 1 ... 101 -> 102a (*)

	td.VerifyInBlockAssembly(t, tx1)    // not mined yet
	td.VerifyNotInBlockAssembly(t, tx2) // mined and removed from block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)

	// create a block with tx1 and tx2 that will be invalid as tx2 is already on block102a
	_, block103a := td.CreateTestBlock(t, block102a, 10301, tx1, tx2)
	// processing the block as "test" will allow us to do optimistic mining
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "test", "test"), "Failed to process block")

	time.Sleep(1000 * time.Millisecond) // give some time for the block to be processed and invalidated

	// 0 -> 1 ... 101 -> 102a (*)

	td.VerifyInBlockAssembly(t, tx1)    // re-added to block assembly
	td.VerifyNotInBlockAssembly(t, tx2) // removed as already mined in block102a
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)
}
