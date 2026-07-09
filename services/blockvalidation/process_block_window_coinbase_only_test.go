//go:build testtxmetacache

package blockvalidation

// ProcessBlockWindow coinbase-only (0-subtree) block tests.
//
// On live testnet/mainnet, early blocks are frequently coinbase-only: a single
// coinbase transaction, no regular txs, therefore zero subtrees (as prepareSubtrees
// produces len(block.Subtrees)==0). The proven serial path quickValidateBlock commits
// such a block with ZERO subtrees — it assigns the block ID idempotently and calls
// commitBlock, never building a placeholder subtree. ProcessBlockWindow must do the
// same in its concurrent pipeline.
//
// Three tests:
//
//  1. COINBASE-ONLY COMMITS — a real coinbase-only below-checkpoint block through
//     ProcessBlockWindow commits (block stored, block.ID != 0) with zero subtrees and
//     does NOT return "block has no subtrees". FAILS against the pre-fix code (RED).
//
//  2. PARITY WITH quickValidateBlock's no-subtree path — the same coinbase-only block
//     via ProcessBlockWindow yields the same committed state (existence, block.ID != 0,
//     zero subtrees) as the AssignBlockID+commitBlock sequence quickValidateBlock runs
//     on a separate store.
//
//  3. MIXED-SHAPE WINDOW — a window of [coinbase-only, single-subtree, multi-subtree]
//     blocks in ascending height all commit correctly and in order; the 0-subtree block
//     interleaves in the C1→C2→C3 barrier without breaking create→spend→commit.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// buildCoinbaseOnlyBlock builds a GENUINE coinbase-only block: one coinbase tx, no
// regular txs, therefore zero subtrees (exactly what prepareSubtrees yields for
// txCount==1). The merkle root of a single-tx block is the coinbase txid. The block
// is mined to valid PoW so commitBlock's AddBlock accepts it.
//
// This is the block shape that wedged the engine on live testnet: createBlockUTXOs
// hard-rejected len(block.Subtrees)==0.
func buildCoinbaseOnlyBlock(t *testing.T, prevHash *chainhash.Hash, height uint32, timestamp uint32) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	// Unique coinbase per height → distinct tx hash → distinct block hash.
	cb := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/coinbase-only-test/"),
		transactions.WithP2PKHOutputs(1, 50e8, privKey.PubKey()),
	)

	// Single-tx block: merkle root == coinbase txid.
	merkleRoot := cb.TxIDChainHash()

	ph := *prevHash
	blk := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &ph,
			HashMerkleRoot: merkleRoot,
			Timestamp:      timestamp,
			Bits:           *nBits,
		},
		Height:           height,
		CoinbaseTx:       cb,
		Subtrees:         nil, // coinbase-only → zero subtrees
		TransactionCount: 1,   // just the coinbase
	}
	mineBlockPoW(t, blk)
	return blk
}

// mineBlockPoW increments the nonce until the header meets the (very easy) test target.
func mineBlockPoW(t *testing.T, blk *model.Block) {
	t.Helper()
	for {
		if ok, _, _ := blk.Header.HasMetTargetDifficulty(); ok {
			return
		}
		blk.Header.Nonce++
		if blk.Header.Nonce > 5_000_000 {
			t.Fatal("failed to find valid PoW nonce within budget")
		}
	}
}

// ---------------------------------------------------------------------------
// Test 1: coinbase-only block commits through ProcessBlockWindow
// ---------------------------------------------------------------------------

func TestProcessBlockWindow_CoinbaseOnly_Commits(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "cbonly_commit")
	defer cancel()

	blk := buildCoinbaseOnlyBlock(t, bv.settings.ChainCfgParams.GenesisHash, 100, 1)
	require.Len(t, blk.Subtrees, 0, "test block must be genuinely coinbase-only (zero subtrees)")

	err := bv.ProcessBlockWindow(ctx, []*model.Block{blk}, "cbonly-peer")
	if err != nil {
		require.NotContains(t, err.Error(), "block has no subtrees",
			"engine must not reject a coinbase-only block as 'block has no subtrees'")
	}
	require.NoError(t, err, "coinbase-only block must commit through ProcessBlockWindow")

	// Block ID was assigned (mirrors quickValidateBlock's no-subtree path).
	require.NotZero(t, blk.ID, "coinbase-only block must have a block ID assigned")

	// Block was actually stored and committed with ZERO subtrees.
	require.Len(t, blk.Subtrees, 0, "committed coinbase-only block must retain zero subtrees (no placeholder)")

	stored, err := bv.blockchainClient.GetBlock(ctx, blk.Hash())
	require.NoError(t, err, "committed coinbase-only block must be retrievable")
	require.NotNil(t, stored)
	require.Len(t, stored.Subtrees, 0, "stored coinbase-only block must have zero subtrees")
}

// ---------------------------------------------------------------------------
// Test 2: parity with quickValidateBlock's no-subtree path
// ---------------------------------------------------------------------------

func TestProcessBlockWindow_CoinbaseOnly_ParityWithQuickValidate(t *testing.T) {
	bvWindow, ctxW, cancelW := newProcessWindowHarness(t, "cbonly_parity_window")
	defer cancelW()

	bvQuick, ctxQ, cancelQ := newProcessWindowHarness(t, "cbonly_parity_quick")
	defer cancelQ()

	// Both harnesses process the identical coinbase-only block (same height/timestamp →
	// same coinbase → same block hash).
	blkW := buildCoinbaseOnlyBlock(t, bvWindow.settings.ChainCfgParams.GenesisHash, 100, 1)
	blkQ := buildCoinbaseOnlyBlock(t, bvQuick.settings.ChainCfgParams.GenesisHash, 100, 1)
	require.Equal(t, blkW.Hash(), blkQ.Hash(), "both harnesses must build the identical block")

	// Window path.
	require.NoError(t, bvWindow.ProcessBlockWindow(ctxW, []*model.Block{blkW}, "parity-peer"))

	// Quick path: the exact no-subtree sequence from quickValidateBlock
	// (AssignBlockID → blockIDToUint32 → commitBlock).
	id, err := bvQuick.blockchainClient.AssignBlockID(ctxQ, blkQ.Hash())
	require.NoError(t, err)
	blkQ.ID, err = blockIDToUint32(id, blkQ.Hash().String())
	require.NoError(t, err)
	require.NoError(t, bvQuick.commitBlock(ctxQ, blkQ, "parity-peer", "quickValidateBlock"))

	// Parity: both assigned a non-zero ID, both committed zero subtrees, both retrievable.
	require.NotZero(t, blkW.ID)
	require.NotZero(t, blkQ.ID)
	require.Len(t, blkW.Subtrees, 0)
	require.Len(t, blkQ.Subtrees, 0)

	storedW, err := bvWindow.blockchainClient.GetBlock(ctxW, blkW.Hash())
	require.NoError(t, err)
	storedQ, err := bvQuick.blockchainClient.GetBlock(ctxQ, blkQ.Hash())
	require.NoError(t, err)

	require.Len(t, storedW.Subtrees, 0, "window path stored zero subtrees")
	require.Len(t, storedQ.Subtrees, 0, "quick path stored zero subtrees")
	require.Equal(t, storedQ.Hash(), storedW.Hash(), "both paths committed the same block")
}

// ---------------------------------------------------------------------------
// Test 3: mixed-shape window (coinbase-only, single-subtree, multi-subtree)
// ---------------------------------------------------------------------------

func TestProcessBlockWindow_MixedShapeWindow(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "mixed_shape")
	defer cancel()

	genesis := bv.settings.ChainCfgParams.GenesisHash
	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	// Block 0 (h=100): coinbase-only, ZERO subtrees.
	block0 := buildCoinbaseOnlyBlock(t, genesis, 100, 1)

	// Block 1 (h=101): single subtree with one regular tx.
	cb1 := transactions.Create(t,
		transactions.WithCoinbaseData(101, "/mixed-single/"),
		transactions.WithP2PKHOutputs(1, 50e8, privKey.PubKey()),
	)
	tx1 := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(cb1, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)
	ph1 := *block0.Hash()
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &ph1,
			Timestamp: 2, Bits: mustNBits(t),
		},
		Height:     101,
		CoinbaseTx: cb1,
	}
	prepareBlockInStore(t, bv, ctx, block1, cb1, []*bt.Tx{tx1})

	// Block 2 (h=102): multi-subtree — two subtrees, one regular tx each.
	cb2 := transactions.Create(t,
		transactions.WithCoinbaseData(102, "/mixed-multi/"),
		transactions.WithP2PKHOutputs(1, 50e8, privKey.PubKey()),
	)
	tx2a := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(cb2, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)
	tx2b := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(tx1, 1),
		transactions.WithP2PKHOutputs(1, 200),
		transactions.WithChangeOutput(),
	)
	ph2 := *block1.Hash()
	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &ph2,
			Timestamp: 3, Bits: mustNBits(t),
		},
		Height:     102,
		CoinbaseTx: cb2,
	}
	// Multi-subtree: subtree0 = [coinbase, tx2a] (leafCount 2, power of two — the
	// first-subtree invariant CheckMerkleRoot enforces); subtree1 = [tx2b] (final,
	// shorter than subtree0). This exercises the multi-subtree create path in C1.
	prepareMultiSubtreeBlockInStore(t, bv, ctx, block2, cb2, [][]*bt.Tx{{tx2a}, {tx2b}})

	require.Len(t, block0.Subtrees, 0, "block0 must be coinbase-only")
	require.Len(t, block1.Subtrees, 1, "block1 must have a single subtree")
	require.Len(t, block2.Subtrees, 2, "block2 must have two subtrees")

	blocks := []*model.Block{block0, block1, block2}
	require.NoError(t, bv.ProcessBlockWindow(ctx, blocks, "mixed-peer"),
		"mixed-shape window must commit all blocks")

	// All three committed with their original subtree shape, in order, with IDs assigned.
	for i, blk := range blocks {
		require.NotZero(t, blk.ID, "block %d (h=%d) must have a block ID", i, blk.Height)
		stored, err := bv.blockchainClient.GetBlock(ctx, blk.Hash())
		require.NoError(t, err, "block %d (h=%d) must be retrievable", i, blk.Height)
		require.Equal(t, len(blk.Subtrees), len(stored.Subtrees),
			"block %d (h=%d) subtree count must be preserved", i, blk.Height)
	}

	// tx1's change output was spent by tx2b → spender recorded (proves the barrier held
	// even though a 0-subtree block sat in the window).
	tx1H := *tx1.TxIDChainHash()
	m, err := bv.utxoStore.Get(ctx, &tx1H, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.Greater(t, len(m.SpendingDatas), 1)
	require.NotNil(t, m.SpendingDatas[1], "tx1[1] must be spent by tx2b")
	require.NotNil(t, m.SpendingDatas[1].TxID)
	require.Equal(t, *tx2b.TxIDChainHash(), *m.SpendingDatas[1].TxID, "tx1[1] must be spent by tx2b")
}

func mustNBits(t *testing.T) model.NBit {
	t.Helper()
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	return *nBits
}

// prepareMultiSubtreeBlockInStore writes N subtree + subtree-data files into
// bv.subtreeStore (one per element of txGroups), sets block.Subtrees, the
// TransactionCount and the header merkle root (computed with the same partitioned
// top-tree algorithm as model.Block.CheckMerkleRoot), and mines the block to valid
// PoW. Subtree 0 carries the coinbase placeholder at leaf 0; its leaf count must be a
// power of two (the first-subtree invariant CheckMerkleRoot enforces). Later subtrees
// carry only regular txs and the final one may be shorter.
func prepareMultiSubtreeBlockInStore(t *testing.T, bv *BlockValidation, ctx context.Context, block *model.Block, coinbaseTx *bt.Tx, txGroups [][]*bt.Tx) {
	t.Helper()
	require.GreaterOrEqual(t, len(txGroups), 2, "multi-subtree helper needs at least two subtrees")

	subtreeSlices := make([]*subtreepkg.Subtree, len(txGroups))
	subtreeRoots := make([]*chainhash.Hash, len(txGroups))
	var totalTxs uint64

	for gi, group := range txGroups {
		// Subtree 0 reserves leaf 0 for the coinbase placeholder.
		leafCount := len(group)
		if gi == 0 {
			leafCount++
		}

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(leafCount)
		require.NoError(t, err)
		if gi == 0 {
			require.NoError(t, subtree.AddCoinbaseNode())
		}
		for _, tx := range group {
			require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size()))) //nolint:gosec
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

		// subtree-data file: leaf 0 of subtree 0 is the coinbase tx, then the group txs.
		subtreeData := subtreepkg.NewSubtreeData(subtree)
		dataIdx := 0
		if gi == 0 {
			require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
			dataIdx = 1
		}
		for _, tx := range group {
			require.NoError(t, subtreeData.AddTx(tx, dataIdx))
			dataIdx++
		}
		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

		subtreeSlices[gi] = subtree
		root := *subtree.RootHash()
		subtreeRoots[gi] = &root
		totalTxs += uint64(subtree.Length()) //nolint:gosec
	}

	block.Subtrees = subtreeRoots
	block.SubtreeSlices = subtreeSlices
	block.TransactionCount = totalTxs
	block.Header.HashMerkleRoot = computeMultiSubtreeMerkleRoot(t, block, coinbaseTx, subtreeSlices)

	mineBlockPoW(t, block)

	// Independent oracle: the root we computed is exactly what the validator accepts.
	require.NoError(t, block.CheckMerkleRoot(ctx))
}

// computeMultiSubtreeMerkleRoot mirrors model.Block.CheckMerkleRoot's partitioned
// top-tree algorithm to produce the header merkle root for a multi-subtree block.
func computeMultiSubtreeMerkleRoot(t *testing.T, block *model.Block, coinbaseTx *bt.Tx, slices []*subtreepkg.Subtree) *chainhash.Hash {
	t.Helper()

	hashes := make([]chainhash.Hash, len(slices))
	for i, sub := range slices {
		if i == 0 {
			root, err := sub.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
			require.NoError(t, err)
			hashes[i] = *root
			continue
		}
		hashes[i] = *sub.RootHash()
	}

	targetHeight := slices[0].Height
	targetLength := slices[0].Length()
	if last := slices[len(slices)-1]; last.Length() < targetLength {
		lifted, err := last.RootHashPadded(targetHeight)
		require.NoError(t, err)
		hashes[len(hashes)-1] = *lifted
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(slices))
	require.NoError(t, err)
	for _, h := range hashes {
		require.NoError(t, top.AddNode(h, 1, 0))
	}

	root, err := chainhash.NewHash(top.RootHash()[:])
	require.NoError(t, err)
	return root
}
