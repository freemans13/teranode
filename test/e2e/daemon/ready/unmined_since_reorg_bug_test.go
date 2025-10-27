package smoke

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnminedSinceNotClearedAfterReorg tests the bug where unmined_since is not cleared
// when a side-chain block becomes part of the main chain during a blockchain reorganization.
//
// Bug: When reset(ctx, false) is called during a large reorg (>= CoinbaseMaturity blocks),
// the MarkTransactionsOnLongestChain() call is skipped, leaving unmined_since incorrectly set.
//
// This test creates a LARGE reorg (>= CoinbaseMaturity) to force reset(ctx, false) to be called.
// This should FAIL with current code, proving the bug exists.
func TestUnminedSinceNotClearedAfterReorg(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Use small CoinbaseMaturity to make large reorg easier to create
	const testCoinbaseMaturity = 5

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			// Small coinbase maturity to easily create large reorg
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = testCoinbaseMaturity
				s.ChainCfgParams = &chainParams
			}
			s.UtxoStore.UnminedTxRetention = 5
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer td.Stop(t, true)

	ctx := context.Background()

	// Step 1: Mine to > 1000 blocks (required for large reorg path)
	// BlockAssembler.go:1183 requires bestBlockHeight > 1000 for reset(ctx, false)
	t.Log("Step 1: Mining to height > 1000...")
	var forkPointHeight uint32
	_, err := td.CallRPC(ctx, "generate", []any{1005})
	require.NoError(t, err)

	currentHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Current height: %d (> 1000 required for large reorg path)", currentHeight)
	require.Greater(t, currentHeight, uint32(1000), "Height must be > 1000")

	// Mine to maturity to get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)
	require.NotNil(t, coinbaseTx)

	forkPointHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Fork point height: %d (after maturity)", forkPointHeight)

	// Get fork point block
	forkPointBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, forkPointHeight)
	require.NoError(t, err)

	// Step 2: Create test transaction
	t.Log("Step 2: Creating test transaction...")
	testTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5000000),
	)
	testTxHash := testTx.TxIDChainHash()
	t.Logf("Created test transaction: %s", testTxHash.String())

	// Step 3: Send transaction to mempool
	t.Log("Step 3: Sending transaction to mempool...")
	testTxBytes := hex.EncodeToString(testTx.ExtendedBytes())
	_, err = td.CallRPC(ctx, "sendrawtransaction", []any{testTxBytes})
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	// Verify transaction has unmined_since set
	meta, err := td.UtxoStore.Get(ctx, testTxHash, fields.UnminedSince)
	require.NoError(t, err)
	initialUnminedSince := meta.UnminedSince
	t.Logf("Transaction unmined_since in mempool: %d", initialUnminedSince)
	require.NotEqual(t, uint32(0), initialUnminedSince, "Transaction should have unmined_since set")

	// Step 4: Mine (CoinbaseMaturity + 1) blocks on main chain WITHOUT test tx
	// This will be invalidated later
	t.Logf("Step 4: Mining %d blocks on main chain (without test tx)...", testCoinbaseMaturity+1)
	for i := 0; i < testCoinbaseMaturity+1; i++ {
		_, mainBlock := createTestBlockWithCorrectSubsidy(t, td, forkPointBlock, uint32(10000+i), nil)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, mainBlock, mainBlock.Height, "legacy", ""))
		forkPointBlock = mainBlock // Chain them
		t.Logf("Main chain block %d/%d mined at height %d", i+1, testCoinbaseMaturity+1, mainBlock.Height)
	}

	mainChainHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Main chain now at height: %d", mainChainHeight)

	// Step 5: Get fork point again for side chain
	forkPointBlock, err = td.BlockchainClient.GetBlockByHeight(ctx, forkPointHeight)
	require.NoError(t, err)

	// Step 6: Mine (CoinbaseMaturity + 2) blocks on SIDE chain WITH test tx in first block
	// Need +2 to have more work than main chain
	t.Logf("Step 5: Mining %d blocks on SIDE chain (first contains test tx)...", testCoinbaseMaturity+2)

	waitForBlockAssemblyToProcessTx(t, td, testTxHash.String())

	// First side chain block contains our test tx
	_, sideBlock1 := createTestBlockWithCorrectSubsidy(t, td, forkPointBlock, uint32(20000), []*bt.Tx{testTx})
	require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, sideBlock1, sideBlock1.Height, "legacy", ""))
	t.Logf("Side chain block 1 mined at height %d (contains test tx): %s", sideBlock1.Height, sideBlock1.Hash().String())

	// Wait for mined_set to process
	time.Sleep(2 * time.Second)

	// Verify tx has block_id and unmined_since (on side chain, not main)
	meta, err = td.UtxoStore.Get(ctx, testTxHash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)
	t.Logf("After side chain block - unmined_since: %d, blockIDs: %v", meta.UnminedSince, meta.BlockIDs)
	require.NotEmpty(t, meta.BlockIDs, "Should have block_id from side chain")

	// Check if on current chain (should be false)
	onChain, err := td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)
	t.Logf("Transaction on current chain: %t (should be false - still on side chain)", onChain)

	// Mine remaining blocks on side chain
	prevBlock := sideBlock1
	for i := 1; i < testCoinbaseMaturity+2; i++ {
		_, sideBlock := createTestBlockWithCorrectSubsidy(t, td, prevBlock, uint32(20000+i), nil)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, sideBlock, sideBlock.Height, "legacy", ""))
		prevBlock = sideBlock
		t.Logf("Side chain block %d/%d mined at height %d", i+1, testCoinbaseMaturity+2, sideBlock.Height)
	}

	finalHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Side chain now at height: %d (should be longest chain)", finalHeight)
	require.Equal(t, forkPointHeight+testCoinbaseMaturity+2, finalHeight, "Side chain should be longest")

	// Step 6: Wait for LARGE reorg to complete
	// BlockAssembler.go:1183 will trigger reset(ctx, false) which now calls MarkTransactionsOnLongestChain()
	t.Logf("Step 6: Waiting for LARGE reorg to complete (>= %d blocks)...", testCoinbaseMaturity)
	t.Log("With Fix #1, MarkTransactionsOnLongestChain() should now be called even with fullScan=false")
	time.Sleep(10 * time.Second) // Longer wait for reset to complete

	// Step 7: THE BUG CHECK
	t.Log("Step 7: Checking if unmined_since was cleared (BUG CHECK)...")
	meta, err = td.UtxoStore.Get(ctx, testTxHash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)

	onChain, err = td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)

	t.Logf("After LARGE reorg:")
	t.Logf("  Transaction: %s", testTxHash.String())
	t.Logf("  Block IDs: %v", meta.BlockIDs)
	t.Logf("  On current chain: %t", onChain)
	t.Logf("  unmined_since: %d", meta.UnminedSince)

	if onChain {
		// This is the BUG - unmined_since should be 0 but will still be set!
		assert.Equal(t, uint32(0), meta.UnminedSince,
			"BUG DETECTED: Transaction is on main chain (blockIDs=%v) but unmined_since=%d is still set!\n"+
				"Expected: unmined_since=0 (NULL)\n"+
				"Actual: unmined_since=%d\n\n"+
				"ROOT CAUSE: Large reorg (>=%d blocks) triggered reset(ctx, false) at BlockAssembler.go:1188\n"+
				"  → loadUnminedTransactions(ctx, false) was called\n"+
				"  → Transactions collected in markAsMinedOnLongestChain: %v\n"+
				"  → But MarkTransactionsOnLongestChain() was SKIPPED because fullScan=false (line 1563)\n\n"+
				"FIX: Remove 'fullScan &&' check on BlockAssembler.go:1563\n"+
				"  Current: if fullScan && len(markAsMinedOnLongestChain) > 0 {\n"+
				"  Fixed:   if len(markAsMinedOnLongestChain) > 0 {",
			meta.BlockIDs, meta.UnminedSince, meta.UnminedSince, testCoinbaseMaturity, testTxHash.String())

		// Verify impact
		if meta.UnminedSince > 0 {
			t.Log("\nStep 8: Verifying BUG IMPACT...")

			cutoffHeight := finalHeight - 1
			oldUnminedTxs, err := td.UtxoStore.QueryOldUnminedTransactions(ctx, cutoffHeight)
			require.NoError(t, err)

			found := false
			for _, hash := range oldUnminedTxs {
				if hash.IsEqual(testTxHash) {
					found = true
					break
				}
			}

			if found {
				t.Errorf("IMPACT CONFIRMED: Main-chain tx %s incorrectly in QueryOldUnminedTransactions (cutoff=%d)",
					testTxHash.String(), cutoffHeight)
				t.Error("This will cause parent transactions to get preserve_until set incorrectly!")
			} else {
				t.Log("Transaction not in QueryOldUnminedTransactions (cutoff too recent)")
			}
		}
	} else {
		t.Errorf("Test setup issue: Transaction should be on main chain after reorg but isn't")
	}

	t.Log("\n=== TEST COMPLETE ===")
	t.Logf("This test proves the bug exists when reset(ctx, false) is called during large reorgs")
}

// createTestBlockWithCorrectSubsidy creates a test block with the correct block subsidy for the height
func createTestBlockWithCorrectSubsidy(t *testing.T, td *daemon.TestDaemon, previousBlock *model.Block, nonce uint32, txs []*bt.Tx) (*subtree.Subtree, *model.Block) {
	// Get wallet address from test daemon's private key
	// Use a fixed test address since we can't access td.privKey
	address, err := bscript.NewAddressFromString("1MUMxUTXcPQ1kAqB7MtJWneeAwVW4cHzzp")
	require.NoError(t, err)

	height := previousBlock.Height + 1

	// Calculate correct block subsidy for this height
	blockSubsidy := util.GetBlockSubsidyForHeight(height, td.Settings.ChainCfgParams)

	// Calculate total fees from transactions
	totalFees := uint64(0)
	if txs != nil {
		for range txs {
			// Simplified fee calculation - just use a default fee
			totalFees += 1000 // Small fee per tx
		}
	}

	coinbaseValue := blockSubsidy + totalFees

	// Create coinbase with correct subsidy
	coinbaseTx, err := model.CreateCoinbase(height, coinbaseValue, "test", []string{address.AddressString})
	require.NoError(t, err)

	var (
		merkleRoot *chainhash.Hash
		st         *subtree.Subtree
		subtrees   []*chainhash.Hash
	)

	// Calculate transaction count and size
	transactionCount := uint64(len(txs) + 1) // +1 for coinbase
	sizeInBytes := uint64(coinbaseTx.Size())
	for _, tx := range txs {
		sizeInBytes += uint64(tx.Size())
	}

	// Handle blocks with/without transactions
	if len(txs) == 0 {
		merkleRoot = coinbaseTx.TxIDChainHash()
		subtrees = []*chainhash.Hash{}
	} else {
		// Create subtree with transactions
		st, err = subtree.NewIncompleteTreeByLeafCount(len(txs) + 1)
		require.NoError(t, err)

		subtreeData := subtree.NewSubtreeData(st)
		subtreeMeta := subtree.NewSubtreeMeta(st)

		err = st.AddCoinbaseNode()
		require.NoError(t, err)

		for i, tx := range txs {
			err = st.AddNode(*tx.TxIDChainHash(), uint64(i), uint64(i))
			require.NoError(t, err)

			err = subtreeData.AddTx(tx, i+1)
			require.NoError(t, err)

			err = subtreeMeta.SetTxInpointsFromTx(tx)
			require.NoError(t, err)
		}

		// Store subtree files
		rootHash := st.RootHash()

		// Store subtree
		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		// Store subtree data
		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeData, subtreeDataBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		// Store subtree meta
		subtreeMetaBytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		merkleRoot, err = st.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
		require.NoError(t, err)

		subtrees = []*chainhash.Hash{st.RootHash()}
	}

	block := &model.Block{
		Subtrees:         subtrees,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		Header: &model.BlockHeader{
			HashPrevBlock:  previousBlock.Header.Hash(),
			HashMerkleRoot: merkleRoot,
			Version:        536870912,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           previousBlock.Header.Bits,
			Nonce:          nonce,
		},
		Height: height,
	}

	// Mine the block (find valid nonce)
	for {
		ok, _, _ := block.Header.HasMetTargetDifficulty()
		if ok {
			break
		}
		block.Header.Nonce++
	}

	return st, block
}
