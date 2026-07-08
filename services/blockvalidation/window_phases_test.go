//go:build testtxmetacache

package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
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

	spends, err := bv.createBlockUTXOs(ctx, block, true /* outpointOnly */)
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

	_, err := bv.createBlockUTXOs(ctx, block, false /* outpointOnly=false should error */)
	require.Error(t, err)
	require.Contains(t, err.Error(), "createBlockUTXOs called on non-outpoint-only block")
}
