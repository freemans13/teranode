// Package blockpersister provides functionality for persisting blockchain blocks and their associated data.
package blockpersister

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noOpCloser struct {
	io.Reader
}

func (noOpCloser) Close() error {
	return nil
}

func newReadCloserFromBytes(data []byte) io.ReadCloser {
	return noOpCloser{Reader: bytes.NewReader(data)}
}

// MockStore implements a mock storage interface for testing
type MockStore struct {
	txs      []*bt.Tx
	subtrees []*subtreepkg.Subtree
}

// newMockStore creates a new mock store with the given transactions
func newMockStore(txs []*bt.Tx) (*MockStore, error) {
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	if err != nil {
		return nil, err
	}

	for i, tx := range txs {
		if i == 0 {
			if err := subtree.AddCoinbaseNode(); err != nil {
				return nil, err
			}
		} else {
			if err := subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())); err != nil {
				return nil, err
			}
		}
	}

	return &MockStore{
		txs:      txs,
		subtrees: []*subtreepkg.Subtree{subtree},
	}, nil
}

func (m *MockStore) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	b, err := m.subtrees[0].Serialize()
	if err != nil {
		return nil, err
	}

	return newReadCloserFromBytes(b), nil
}

func (m *MockStore) BatchDecorate(ctx context.Context, hashes []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	for _, missing := range hashes {
		if missing.Idx >= len(m.txs) {
			continue
		}

		missing.Data = &meta.Data{
			Tx: m.txs[missing.Idx],
		}
	}

	return nil
}

func (m *MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "", nil
}

func (m *MockStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return nil, nil
}

func (m *MockStore) Delete(_ context.Context, hash *chainhash.Hash) error {
	return nil
}

func (m *MockStore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	return nil, nil
}

func (m *MockStore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return nil, nil
}

func (m *MockStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return nil, nil
}

func (m *MockStore) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return nil, errors.NewProcessingError("not implemented")
}

func (m *MockStore) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, nil
}

func (m *MockStore) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return nil, nil
}

func (m *MockStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return nil
}

func (m *MockStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return nil
}

func (m *MockStore) SetBlockHeight(height uint32) error {
	return nil
}

func (m *MockStore) GetBlockHeight() uint32 {
	return 0
}

func (m *MockStore) SetMedianBlockTime(height uint32) error {
	return nil
}

func (m *MockStore) GetMedianBlockTime() uint32 {
	return 0
}

func (m *MockStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockStore) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, nil
}

func (m *MockStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return nil
}

func (m *MockStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	return nil
}

func (m *MockStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return nil
}

func (m *MockStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return nil
}

// TestBlock validates the block persistence functionality by:
// - Creating a test block from mainnet block 100,000
// - Processing and validating its transactions
// - Ensuring proper storage and retrieval
func TestBlock(t *testing.T) {
	block, blockBytes, _, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	// remove the subtree data file if it exists, since we want to test the persistence
	err := subtreeStore.Del(t.Context(), mockUTXOStore.subtrees[0].RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	persister := New(context.Background(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	err = persister.persistBlock(context.Background(), mockUTXOStore.subtrees[0].RootHash(), blockBytes)
	require.NoError(t, err)

	newBlockBytes, err := blockStore.Get(context.Background(), mockUTXOStore.subtrees[0].RootHash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err)

	newBlockModel, err := model.NewBlockFromBytes(newBlockBytes)
	require.NoError(t, err)

	assert.Equal(t, block.Header.Hash().String(), newBlockModel.Header.Hash().String())

	subtreeBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)
	assert.Len(t, subtree.Nodes, 4) // 1 coinbase + 3 transactions

	// check all the transactions in the block
	subtreeDataBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, subtreeDataBytes)
	require.NoError(t, err)
	assert.Len(t, subtreeData.Txs, 4)

	// Verify UTXO additions file exists and contains data
	blockHash := block.Header.Hash()
	utxoAdditionsBytes, err := blockStore.Get(t.Context(), blockHash[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)
	assert.Greater(t, len(utxoAdditionsBytes), 36, "UTXO additions file should contain header (32 byte hash + 4 byte height) and data")

	// Verify UTXO deletions file exists and contains data
	utxoDeletionsBytes, err := blockStore.Get(t.Context(), blockHash[:], fileformat.FileTypeUtxoDeletions)
	require.NoError(t, err)
	assert.Greater(t, len(utxoDeletionsBytes), 36, "UTXO deletions file should contain header (32 byte hash + 4 byte height) and data")
}

func TestFileStorer(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	url, err := url.Parse("file://./data/blockstore")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(logger, url)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("test")
	fileType := fileformat.FileTypeDat

	// Delete the key if it exists.
	_ = blockStore.Del(ctx, key, fileType)

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	if _, err = storer.Write([]byte("hello")); err != nil {
		t.Errorf("error writing block to disk: %v", err)
	}

	if err = storer.Close(ctx); err != nil {
		t.Errorf("error closing block file: %v", err)
	}

	_, err = filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.ErrorIs(t, err, errors.NewBlobAlreadyExistsError(""))
}

func TestBlockMissingTxMeta(t *testing.T) {
	_, blockBytes, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	// use a mock store that has missing txs
	mockUTXOStoreWithMissingTxs, err := newMockStore(extendedTxs[1:])
	require.NoError(t, err)

	err = subtreeStore.Del(t.Context(), mockUTXOStore.subtrees[0].RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	persister := New(context.Background(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStoreWithMissingTxs, blockchainClient)

	err = persister.persistBlock(context.Background(), mockUTXOStore.subtrees[0].RootHash(), blockBytes)
	require.Error(t, err)
}

func setup(t *testing.T) (*model.Block, []byte, []*bt.Tx, *MockStore, *memory.Memory, *memory.Memory, *blockchain.LocalClient, *settings.Settings) {
	initPrometheusMetrics()

	// Take block 100,000 from mainnet
	blockBytes, err := hex.DecodeString("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d61900000000006657a9252aacd5c0b2940996ecff952228c3067cc38d4885efb5a4ac4247e9f337221b4d4c86041b0f2b57100401000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac000000000100000001032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a187000000008c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c9331586c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff0200e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c2f6b52de3d7c88ac000000000100000001c33ebff2a709f13d9f9a7569ab16a32786af7d7e2de09265e41c61d078294ecf010000008a4730440220032d30df5ee6f57fa46cddb5eb8d0d9fe8de6b342d27942ae90a3231e0ba333e02203deee8060fdc70230a7f5b4ad7d7bc3e628cbe219a886b84269eaeb81e26b4fe014104ae31c31bf91278d99b8377a35bbce5b27d9fff15456839e919453fc7b3f721f0ba403ff96c9deeb680e5fd341c0fc3a7b90da4631ee39560639db462e9cb850fffffffff0240420f00000000001976a914b0dcbf97eabf4404e31d952477ce822dadbe7e1088acc060d211000000001976a9146b1281eec25ab4e1e0793ff4e08ab1abb3409cd988ac0000000001000000010b6072b386d4a773235237f64c1126ac3b240c84b917a3909ba1c43ded5f51f4000000008c493046022100bb1ad26df930a51cce110cf44f7a48c3c561fd977500b1ae5d6b6fd13d0b3f4a022100c5b42951acedff14abba2736fd574bdb465f3e6f8da12e2c5303954aca7f78f3014104a7135bfe824c97ecc01ec7d7e336185c81e2aa2c41ab175407c09484ce9694b44953fcb751206564a9c24dd094d42fdbfdd5aad3e063ce6af4cfaaea4ea14fbbffffffff0140420f00000000001976a91439aa3d569e06a1d7926dc4be1193c99bf2eb9ee088ac00000000")
	require.NoError(t, err)

	// Check the number of transaction is 4
	assert.Equal(t, uint8(4), blockBytes[80])

	txs := make([]*bt.Tx, 4)

	// Read the transactions from the block
	reader := bytes.NewReader(blockBytes[81:])

	for i := 0; i < 4; i++ {
		txs[i] = &bt.Tx{}
		_, err = txs[i].ReadFrom(reader)
		require.NoError(t, err)
	}

	extendedTxs := make([]*bt.Tx, 4)
	extendedTxs[0] = txs[0] // Coinbase is the same as the original tx

	extendedTxs[1], err = bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a187000000008c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c9331586c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac0200e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	extendedTxs[2], err = bt.NewTxFromString("010000000000000000ef01c33ebff2a709f13d9f9a7569ab16a32786af7d7e2de09265e41c61d078294ecf010000008a4730440220032d30df5ee6f57fa46cddb5eb8d0d9fe8de6b342d27942ae90a3231e0ba333e02203deee8060fdc70230a7f5b4ad7d7bc3e628cbe219a886b84269eaeb81e26b4fe014104ae31c31bf91278d99b8377a35bbce5b27d9fff15456839e919453fc7b3f721f0ba403ff96c9deeb680e5fd341c0fc3a7b90da4631ee39560639db462e9cb850fffffffff00a3e111000000001976a91435fbee6a3bf8d99f17724ec54787567393a8a6b188ac0240420f00000000001976a914b0dcbf97eabf4404e31d952477ce822dadbe7e1088acc060d211000000001976a9146b1281eec25ab4e1e0793ff4e08ab1abb3409cd988ac00000000")
	require.NoError(t, err)

	extendedTxs[3], err = bt.NewTxFromString("010000000000000000ef010b6072b386d4a773235237f64c1126ac3b240c84b917a3909ba1c43ded5f51f4000000008c493046022100bb1ad26df930a51cce110cf44f7a48c3c561fd977500b1ae5d6b6fd13d0b3f4a022100c5b42951acedff14abba2736fd574bdb465f3e6f8da12e2c5303954aca7f78f3014104a7135bfe824c97ecc01ec7d7e336185c81e2aa2c41ab175407c09484ce9694b44953fcb751206564a9c24dd094d42fdbfdd5aad3e063ce6af4cfaaea4ea14fbbffffffff40420f00000000001976a914c4eb47ecfdcf609a1848ee79acc2fa49d3caad7088ac0140420f00000000001976a91439aa3d569e06a1d7926dc4be1193c99bf2eb9ee088ac00000000")
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		assert.Equal(t, extendedTxs[i].TxIDChainHash().String(), txs[i].TxIDChainHash().String())
	}

	mockUTXOStore, err := newMockStore(extendedTxs)
	require.NoError(t, err)

	subtreeStore := memory.New()
	subtree := mockUTXOStore.subtrees[0]

	// Create the .subtree file
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	// Create the .subtreeData file
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	// Add the transactions to the subtree data (skipping coinbase as it's handled specially)
	for i, tx := range extendedTxs[1:] { // Skip coinbase
		err = subtreeData.AddTx(tx, i+1) // +1 because index 0 is for coinbase
		require.NoError(t, err)
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	blockStore := memory.New()

	blockchainClient := &blockchain.LocalClient{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	var block model.Block

	block.Header, err = model.NewBlockHeaderFromBytes(blockBytes[:80])
	require.NoError(t, err)

	block.CoinbaseTx = extendedTxs[0]
	block.TransactionCount = uint64(len(extendedTxs))

	block.Subtrees = []*chainhash.Hash{mockUTXOStore.subtrees[0].RootHash()}

	b, err := block.Bytes()
	require.NoError(t, err)

	return &block, b, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings
}
