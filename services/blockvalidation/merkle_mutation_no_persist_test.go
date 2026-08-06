package blockvalidation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBlockValidation_BodyAttributableMerkleFault_DoesNotPersistInvalid is the
// service-level regression for Change 3 (issue 1424 / HARDEN-1424): a block that fails
// block.Valid on a body-attributable fault (here, a merkle-root mismatch — the same
// classification CVE-2012-2459 duplicate transactions get) must be rejected WITHOUT ever
// calling AddBlock with WithInvalid(true). Persisting the header's hash as invalid on a
// body-only fault would let an attacker replay the honest header with a doctored body
// (zero mining cost) to get the node to condemn a real block.
//
// The mock blockchain client deliberately does NOT register an expectation for AddBlock:
// if storeInvalidBlock is (re-)invoked, testify's mock panics on the unexpected call,
// failing the test loudly rather than silently. This is the strongest assertion the
// package's mock can express for "AddBlock was never called" — stronger than
// AssertNotCalled alone, which only inspects call history after the fact.
func TestBlockValidation_BodyAttributableMerkleFault_DoesNotPersistInvalid(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	txCount := 4
	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// Build a subtree with a coinbase placeholder plus txCount-1 real, parent-linked
	// transactions, exactly as the existing merkle-mismatch model-level test does —
	// this is what lets CheckBlockSubtrees (subtree validation) succeed so block.Valid
	// actually reaches CheckMerkleRoot instead of failing earlier.
	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

	fees := 0

	for i := 0; i < txCount-1; i++ {
		parentTx := newTx(uint32(i + 10000)) //nolint:gosec
		_, _, err = utxoStore.SpendAndCreate(ctx, parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
		require.NoError(t, err)

		tx := newTx(uint32(i), parentTx.TxIDChainHash()) //nolint:gosec

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx))

		fees += 100

		_, _, err = utxoStore.SpendAndCreate(ctx, tx, 0, utxostore.WithCreateOnly())
		require.NoError(t, err)
	}

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(fees)) //nolint:gosec

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		fmt.Sprintf("/subtree/%s", subtree.RootHash().String()),
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	nBits, _ := model.NewNBitFromString("207fffff")

	// Deliberately WRONG merkle root (zero hash): the real computed root for this
	// subtree/coinbase combination is never all-zero, so CheckMerkleRoot must reject
	// with "merkle root does not match" — a body-attributable fault classified via
	// errors.NewBlockInvalidBodyError (Change 2).
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		123123,
		0, 0,
	)
	require.NoError(t, err)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Maybe()
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Maybe()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 0}, nil).Maybe()
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	// Deliberately NOT registering AddBlock: see the function comment above.

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = bv.ValidateBlock(ctx, block, "http://localhost:8000")
	require.Error(t, err, "a body-attributable merkle-root mismatch must reject the block")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "rejection must be BlockInvalid (peer punishment keys on this)")
	require.Contains(t, err.Error(), "merkle root does not match")

	// Belt-and-suspenders: even if AddBlock somehow had a registered expectation in the
	// future, explicitly confirm it was never invoked with any arguments.
	mockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
