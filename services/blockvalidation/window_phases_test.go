//go:build testtxmetacache

package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newWindowPhasesHarness builds a minimal in-memory BlockValidation for window_phases tests.
// Uses real sqlitememory UTXO store and blockchain client. No mocks.
func newWindowPhasesHarness(t *testing.T) (*BlockValidation, context.Context, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)

	// Engage the outpoint-only fast path so createBlockUTXOs can run.
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	// Place a high checkpoint so height 100 is firmly below it (RegressionNetParams has none).
	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params

	utxoStoreURL, err := url.Parse("sqlitememory:///window_phases_test_" + t.Name())
	require.NoError(t, err)
	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		utxoStore:                     utxoStore,
		subtreeStore:                  blobmemory.New(),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
	}
	t.Cleanup(func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
	})

	return bv, ctx, cancel
}

// buildWindowPhasesBlock builds a one-subtree block with:
//
//	coinbase + tx1 (spends coinbase[0]) + tx2 (spends tx1 change) + tx3 (spends tx2 change)
//
// It stores the subtree + subtree-data files in bv.subtreeStore and returns the ready block.
// Total non-coinbase inputs: 3 (one per regular tx).
func buildWindowPhasesBlock(t *testing.T, bv *BlockValidation, ctx context.Context) (*model.Block, []*bt.Tx) {
	t.Helper()

	// count=5 yields coinbase + 3 regular txs (loop runs count-2=3 times).
	txs := transactions.CreateTestTransactionChainWithCount(t, 5)
	coinbaseTx := txs[0]
	tx1 := txs[1]
	tx2 := txs[2]
	tx3 := txs[3]

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 0, uint64(tx1.Size()))) //nolint:gosec
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 0, uint64(tx2.Size()))) //nolint:gosec
	require.NoError(t, subtree.AddNode(*tx3.TxIDChainHash(), 0, uint64(tx3.Size()))) //nolint:gosec

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
	require.NoError(t, subtreeData.AddTx(tx1, 1))
	require.NoError(t, subtreeData.AddTx(tx2, 2))
	require.NoError(t, subtreeData.AddTx(tx3, 3))
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	prevHash := *bv.settings.ChainCfgParams.GenesisHash
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1000000,
			Bits:           *nBits,
			Nonce:          0,
		},
		Height:           100,
		CoinbaseTx:       coinbaseTx,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		TransactionCount: 4,
	}

	// Mine to valid PoW so validateSubtrees doesn't fail on header checks.
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
		if block.Header.Nonce > 2_000_000 {
			t.Fatal("failed to find a valid nonce within iteration budget")
		}
	}

	return block, []*bt.Tx{tx1, tx2, tx3}
}

// TestCreateBlockUTXOs_CreateHalf verifies the CREATE-only pass of createBlockUTXOs:
//   - block.ID is assigned (non-zero)
//   - every non-coinbase tx is present in the UTXO store with the correct BlockID
//   - the returned []windowSpend has exactly one entry per non-coinbase input
//   - each windowSpend carries non-zero parentTxHash and spendingTxHash
func TestCreateBlockUTXOs_CreateHalf(t *testing.T) {
	bv, ctx, cancel := newWindowPhasesHarness(t)
	defer cancel()

	block, regularTxs := buildWindowPhasesBlock(t, bv, ctx)

	spends, err := bv.createBlockUTXOs(ctx, block, true /* outpointOnly */, nil)
	require.NoError(t, err)

	// 1. block.ID must be non-zero.
	require.NotZero(t, block.ID, "block.ID must be assigned by createBlockUTXOs")

	// 2. Every non-coinbase tx must be in the UTXO store with matching BlockID.
	for _, tx := range regularTxs {
		meta, getErr := bv.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
		require.NoError(t, getErr, "tx %s must exist in UTXO store after createBlockUTXOs", tx.TxIDChainHash())
		require.NotNil(t, meta, "meta must not be nil for tx %s", tx.TxIDChainHash())
		require.NotEmpty(t, meta.BlockIDs, "BlockIDs must not be empty for tx %s", tx.TxIDChainHash())
		require.Equal(t, block.ID, meta.BlockIDs[0], "BlockID mismatch for tx %s", tx.TxIDChainHash())
	}

	// 3. len(spends) must equal total non-coinbase inputs (1 per regular tx in the chain).
	totalInputs := 0
	for _, tx := range regularTxs {
		totalInputs += len(tx.Inputs)
	}
	require.Equal(t, totalInputs, len(spends), "windowSpend count must match total non-coinbase inputs")

	// 4. Each windowSpend must have non-zero parentTxHash and spendingTxHash.
	for i, ws := range spends {
		require.NotEqual(t, chainhash.Hash{}, ws.parentTxHash, "windowSpend[%d].parentTxHash must be non-zero", i)
		require.NotEqual(t, chainhash.Hash{}, ws.spendingTxHash, "windowSpend[%d].spendingTxHash must be non-zero", i)
	}
}

// TestCreateBlockUTXOs_RejectsNonOutpointOnly verifies that createBlockUTXOs errors
// immediately when called with outpointOnly=false.
func TestCreateBlockUTXOs_RejectsNonOutpointOnly(t *testing.T) {
	bv, ctx, cancel := newWindowPhasesHarness(t)
	defer cancel()

	block := &model.Block{Height: 100}

	_, err := bv.createBlockUTXOs(ctx, block, false /* outpointOnly=false should error */, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "createBlockUTXOs called on non-outpoint-only block")
}

// newWindowParityHarness returns two independent BlockValidation instances (A and B) sharing
// no state. Both use separate sqlitememory UTXO stores and blockchain stores so parity
// assertions are meaningful (same logical block, different processing paths).
func newWindowParityHarness(t *testing.T) (bvA, bvB *BlockValidation, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()

	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	logger := ulogger.TestLogger{}

	newBV := func(suffix string) *BlockValidation {
		tSettings := testutil.CreateBaseTestSettings(t)
		tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

		params := *tSettings.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
		tSettings.ChainCfgParams = &params

		utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///window_parity_%s_%s", suffix, t.Name()))
		require.NoError(t, err)
		utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
		require.NoError(t, err)

		bv := &BlockValidation{
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              blockchainClient,
			utxoStore:                     utxoStore,
			subtreeStore:                  blobmemory.New(),
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		}
		t.Cleanup(func() {
			bv.blockExistsCache.Stop()
			bv.lastValidatedBlocks.Stop()
		})
		return bv
	}

	bvA = newBV("A")
	bvB = newBV("B")
	return bvA, bvB, ctx, cancel
}

// prepareBlockInStore writes the subtree and subtree-data files into bv.subtreeStore for
// the given block, updates block.Header.HashMerkleRoot and block.Subtrees, and mines
// the header to a valid PoW nonce. The block is then ready for processBlockSubtrees or
// createBlockUTXOs.
func prepareBlockInStore(t *testing.T, bv *BlockValidation, ctx context.Context, block *model.Block, coinbaseTx *bt.Tx, regularTxs []*bt.Tx) {
	t.Helper()

	allTxs := append([]*bt.Tx{coinbaseTx}, regularTxs...)
	leafCount := len(allTxs)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(leafCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	for _, tx := range regularTxs {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size()))) //nolint:gosec
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	for i, tx := range allTxs {
		require.NoError(t, subtreeData.AddTx(tx, i))
	}
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	block.Header.HashMerkleRoot = merkleRoot
	block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
	block.TransactionCount = uint64(leafCount) //nolint:gosec

	// Mine to valid PoW.
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
		if block.Header.Nonce > 5_000_000 {
			t.Fatal("failed to find valid PoW nonce within budget")
		}
	}
}

// buildParityChain builds a 3-block chain (deterministic tx hashes — same keys every time).
// The chain structure is:
//
//	block1 (height 100): coinbase1 + tx1a (spends coinbase1[0]) + tx1b (spends tx1a change[1])
//	block2 (height 101): coinbase2 + tx2a (spends tx1b change[1])
//	block3 (height 102): coinbase3 + tx3a (spends tx2a change[1])
//
// The deterministic private key used by CreateTestTransactionChainWithCount means both
// Path A and Path B will produce identical tx hashes, enabling cross-path spender assertions.
func buildParityChain(t *testing.T, genesisHash *chainhash.Hash) ([]*model.Block, [][]*bt.Tx) {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Use the same deterministic key as CreateTestTransactionChainWithCount so tx hashes
	// are reproducible — both Path A and Path B call this function and get identical bytes.
	privateKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	// CreateTestTransactionChainWithCount(t, 4) = coinbase + tx1a + tx1b (count-2=2 regular txs).
	chain1 := transactions.CreateTestTransactionChainWithCount(t, 4)
	cb1, tx1a, tx1b := chain1[0], chain1[1], chain1[2]

	// tx2a: spends tx1b output[1] (the change output index used by WithInput chain).
	tx2a := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(tx1b, 1),
		transactions.WithP2PKHOutputs(1, 500),
		transactions.WithChangeOutput(),
	)
	cb2 := transactions.CreateTestTransactionChainWithCount(t, 2)[0] // coinbase only

	// tx3a: spends tx2a output[1].
	tx3a := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(tx2a, 1),
		transactions.WithP2PKHOutputs(1, 200),
		transactions.WithChangeOutput(),
	)
	cb3 := transactions.CreateTestTransactionChainWithCount(t, 2)[0] // coinbase only

	// Use fixed dummy prev-hashes for block2/block3. These are below-checkpoint blocks;
	// processBlockSubtrees/createBlockUTXOs don't validate the chain linkage, only the
	// merkle root (set by prepareBlockInStore). Distinct timestamps → distinct block hashes.
	prevHash1 := *genesisHash
	var dummyHash2, dummyHash3 chainhash.Hash
	dummyHash2[0], dummyHash3[0] = 0x02, 0x03

	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &prevHash1,
			Timestamp: 1000000, Bits: *nBits,
		},
		Height: 100, CoinbaseTx: cb1,
	}

	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &dummyHash2,
			Timestamp: 1000001, Bits: *nBits,
		},
		Height: 101, CoinbaseTx: cb2,
	}

	block3 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &dummyHash3,
			Timestamp: 1000002, Bits: *nBits,
		},
		Height: 102, CoinbaseTx: cb3,
	}

	blocks := []*model.Block{block1, block2, block3}
	txsPerBlock := [][]*bt.Tx{
		{tx1a, tx1b},
		{tx2a},
		{tx3a},
	}
	return blocks, txsPerBlock
}

// TestSpendBlockUTXOs_FullParityWithProcessBlockSubtrees is the Task-2 parity gate.
// It runs the same 3-block synthetic chain through:
//
//	Path A: existing interleaved processBlockSubtrees (baseline)
//	Path B: de-interleaved createBlockUTXOs + spendBlockUTXOs (new path)
//
// and asserts that the resulting UTXO state is identical:
//   - every non-coinbase tx exists with the same relative BlockID grouping
//   - every consumed output's SpendingDatas[0].TxID matches across runs
//
// The transaction factory uses a deterministic private key, so hashes are identical
// across both paths, enabling cross-path spender-identity comparison.
func TestSpendBlockUTXOs_FullParityWithProcessBlockSubtrees(t *testing.T) {
	bvA, bvB, ctx, cancel := newWindowParityHarness(t)
	defer cancel()

	// Both harnesses use the same genesis hash (RegressionNetParams), so both paths
	// build identical tx chains (same deterministic keys → same tx hashes).
	genesisHash := bvA.settings.ChainCfgParams.GenesisHash

	blocksA, txsPerBlock := buildParityChain(t, genesisHash)
	blocksB, _ := buildParityChain(t, genesisHash) // identical hashes

	// Path A: interleaved processBlockSubtrees.
	for i, blk := range blocksA {
		prepareBlockInStore(t, bvA, ctx, blk, blk.CoinbaseTx, txsPerBlock[i])
		_, err := bvA.processBlockSubtrees(ctx, blk, true /* outpointOnly */)
		require.NoError(t, err, "Path A: processBlockSubtrees failed for block %d", i+1)
	}

	// Path B: de-interleaved createBlockUTXOs + spendBlockUTXOs.
	for i, blk := range blocksB {
		prepareBlockInStore(t, bvB, ctx, blk, blk.CoinbaseTx, txsPerBlock[i])

		spends, err := bvB.createBlockUTXOs(ctx, blk, true /* outpointOnly */, nil)
		require.NoError(t, err, "Path B: createBlockUTXOs failed for block %d", i+1)

		err = bvB.spendBlockUTXOs(ctx, blk, spends, true /* outpointOnly */, nil)
		require.NoError(t, err, "Path B: spendBlockUTXOs failed for block %d", i+1)
	}

	// Parity 1: block-rank grouping.
	// Build tx→blockRank map for each path using normalised BlockIDs (0=first seen, 1=second...).
	blockRankMap := func(bv *BlockValidation, txsPerBlock [][]*bt.Tx) map[chainhash.Hash]int {
		t.Helper()
		rankOf := make(map[uint32]int)
		result := make(map[chainhash.Hash]int)
		for _, txs := range txsPerBlock {
			for _, tx := range txs {
				h := *tx.TxIDChainHash()
				m, err := bv.utxoStore.Get(ctx, &h, fields.BlockIDs)
				require.NoError(t, err, "Get(BlockIDs) failed for tx %s", h)
				require.NotNil(t, m)
				require.NotEmpty(t, m.BlockIDs, "tx %s has no BlockIDs", h)
				bid := m.BlockIDs[0]
				if _, seen := rankOf[bid]; !seen {
					rankOf[bid] = len(rankOf)
				}
				result[h] = rankOf[bid]
			}
		}
		return result
	}

	rankA := blockRankMap(bvA, txsPerBlock)
	rankB := blockRankMap(bvB, txsPerBlock) // same tx hashes → same map keys
	require.Equal(t, rankA, rankB, "cross-path block-rank mismatch")

	// Parity 2: spender-identity for cross-block spends.
	// tx1b (txsPerBlock[0][1]) output[1] is spent by tx2a (txsPerBlock[1][0]).
	// tx2a (txsPerBlock[1][0]) output[1] is spent by tx3a (txsPerBlock[2][0]).
	// We check SpendingDatas[spentVout] rather than [0] because the sql store sizes
	// SpendingDatas by len(tx.Outputs) and indexes by output vout.
	type spentPair struct {
		parent    *bt.Tx
		spentVout int // which output of parent was spent
		spender   *bt.Tx
	}
	spentPairs := []spentPair{
		{txsPerBlock[0][1], 1, txsPerBlock[1][0]}, // tx1b[1] → tx2a
		{txsPerBlock[1][0], 1, txsPerBlock[2][0]}, // tx2a[1] → tx3a
	}

	for _, pair := range spentPairs {
		parentH := *pair.parent.TxIDChainHash()
		expectedSpender := *pair.spender.TxIDChainHash()
		vout := pair.spentVout

		for _, tc := range []struct {
			name string
			bv   *BlockValidation
		}{
			{"Path A", bvA},
			{"Path B", bvB},
		} {
			m, err := tc.bv.utxoStore.Get(ctx, &parentH, fields.Utxos)
			require.NoError(t, err, "%s: Get(Utxos) for spent tx %s", tc.name, parentH)
			require.NotNil(t, m, "%s: nil meta for spent tx %s", tc.name, parentH)
			require.True(t, len(m.SpendingDatas) > vout, "%s: SpendingDatas too short for tx %s vout %d (len=%d)", tc.name, parentH, vout, len(m.SpendingDatas))
			require.NotNil(t, m.SpendingDatas[vout], "%s: SpendingDatas[%d] nil for spent tx %s", tc.name, vout, parentH)
			require.NotNil(t, m.SpendingDatas[vout].TxID, "%s: SpendingDatas[%d].TxID nil for spent tx %s", tc.name, vout, parentH)
			require.Equal(t, expectedSpender, *m.SpendingDatas[vout].TxID,
				"%s: wrong spender for tx %s vout %d (expected %s got %s)", tc.name, parentH, vout, expectedSpender, m.SpendingDatas[vout].TxID)
		}
	}
}

// TestSpendBlockUTXOs_FailClosed verifies that spendBlockUTXOs:
//  1. Hard-fails (does not succeed) when a spend references an absent parent (the store
//     returns a non-retryable error for spending an output that was never created).
//  2. Hard-fails on a conflicting spend and does NOT retry it to success — the
//     fail-closed contract from spendBatchWithRetry must be preserved through spendBlockUTXOs.
func TestSpendBlockUTXOs_FailClosed(t *testing.T) {
	t.Run("absent parent hard-fails", func(t *testing.T) {
		bv, ctx, cancel := newWindowPhasesHarness(t)
		defer cancel()

		block := &model.Block{Height: 100, Header: model.GenesisBlockHeader}
		block.ID = 1

		// Craft a windowSpend whose parentTxHash does not exist in the store.
		var fakeparent, fakespender chainhash.Hash
		fakeparent[0] = 0xAB
		fakespender[0] = 0xCD

		spends := []windowSpend{
			{parentTxHash: fakeparent, vout: 0, spendingTxHash: fakespender, vin: 0},
		}

		err := bv.spendBlockUTXOs(ctx, block, spends, true /* outpointOnly */, nil)
		require.Error(t, err, "spendBlockUTXOs must error when parent tx does not exist")
	})

	t.Run("conflicting spend hard-fails and is not retried to success", func(t *testing.T) {
		// spendBlockUTXOs delegates to spendBatchWithRetry; ErrTxConflicting is classified
		// as hard-fail (not retryable) there. Wire up the spy store used by
		// TestSpendBatchWithRetry to confirm spendBlockUTXOs carries the same semantics.
		//
		// Technique: build a minimal windowSpend, reconstruct the minimal *bt.Tx that
		// spendBlockUTXOs will produce for it (via windowSpendsToTxs — an internal helper),
		// compute its hash, pre-register in the spy, then assert error + call count.

		var parentH, spenderH chainhash.Hash
		parentH[0], spenderH[0] = 0x11, 0x22

		// Build the minimal tx ourselves to learn its hash before calling spendBlockUTXOs.
		minimalTx := buildMinimalSpendTx(spenderH, []windowSpend{
			{parentTxHash: parentH, vout: 0, spendingTxHash: spenderH, vin: 0},
		})
		txHash := *minimalTx.TxIDChainHash()

		spy := &spendRetrySpyStore{
			failuresLeft: map[chainhash.Hash]int{txHash: 999},
			failErr:      map[chainhash.Hash]error{txHash: errors.NewTxConflictingError("injected conflict")},
		}
		spy.NullStore, _ = nullstore.NewNullStore()

		tSettings := testutil.CreateBaseTestSettings(t)
		params := *tSettings.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
		tSettings.ChainCfgParams = &params

		bv := &BlockValidation{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			utxoStore:         spy,
			spendRetryBackoff: time.Millisecond,
		}

		block := &model.Block{Height: 100, Header: model.GenesisBlockHeader, ID: 1}
		spends := []windowSpend{{parentTxHash: parentH, vout: 0, spendingTxHash: spenderH, vin: 0}}

		err := bv.spendBlockUTXOs(context.Background(), block, spends, true, nil)
		require.Error(t, err, "conflicting spend must hard-fail")
		require.Equal(t, int64(1), spy.spendCalls.Load(), "conflicting spend must not be retried")
	})
}
