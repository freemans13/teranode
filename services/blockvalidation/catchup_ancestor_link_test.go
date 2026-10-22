package blockvalidation

import (
	"context"
	"encoding/binary"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// runAncestorLinkCatchup runs catchup() against a peer that offers [ancestorA, h1..h5], where h1 is
// built on firstParent. ancestorA is stored locally at height 100 with the local tip at 110, so the
// offer is a fork of depth 10. otherBlockB is a different block that is also stored locally. When
// firstParentIsAncestor is false, h1's previous hash names B rather than the common ancestor A.
//
// It returns the catchup error, how many /blocks/ requests reached the peer, and the blockchain
// mock so the caller can check whether any local block was un-stamped (step 5.5).
func runAncestorLinkCatchup(t *testing.T, firstParentIsAncestor bool) (error, int, *blockchain.Mock) {
	initPrometheusMetrics()

	const ancestorHeight, currentHeight = uint32(100), uint32(110)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	ancestorA := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(0),
		Timestamp:      uint32(time.Now().Unix() - 3600),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(ancestorA)

	otherBlockB := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(7),
		Timestamp:      uint32(time.Now().Unix() - 3500),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(otherBlockB)
	require.NotEqual(t, ancestorA.Hash().String(), otherBlockB.Hash().String())

	firstParent := otherBlockB
	if firstParentIsAncestor {
		firstParent = ancestorA
	}

	forkHeaders := testhelpers.CreateSyntheticChainFrom(firstParent, 5)

	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
	t.Cleanup(cleanup)

	mockUTXOStore.On("GetBlockHeight").Return(currentHeight)

	server.settings.ChainCfgParams.CoinbaseMaturity = 100
	server.settings.BlockValidation.SecretMiningThreshold = 1000 // keep the secret-mining gate out of the way

	perBlockWork := work.CalcBlockWork(binary.LittleEndian.Uint32(nBits.CloneBytes()))
	ancestorChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(ancestorHeight)))
	localChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(currentHeight)))

	targetBlock := &model.Block{
		Header: forkHeaders[len(forkHeaders)-1],
		Height: currentHeight + uint32(len(forkHeaders)),
	}

	// Both A and B are stored locally; B is registered first so the catch-all below does not shadow it.
	mockBlockchainClient.On("GetBlockExists", mock.Anything, ancestorA.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.On("GetBlockExists", mock.Anything, otherBlockB.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, ancestorA.Hash()).
		Return(ancestorA, &model.BlockHeaderMeta{Height: ancestorHeight, ID: 1, ChainWork: ancestorChainWork.Bytes()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewBlockNotFoundError("block not found")).Maybe()

	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(ancestorA, &model.BlockHeaderMeta{Height: currentHeight, ChainWork: localChainWork.Bytes()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{ancestorA.Hash()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{ancestorA}, []*model.BlockHeaderMeta{{Height: ancestorHeight, ID: 1}}, nil).Maybe()

	currentState := blockchain.FSMStateRUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&currentState, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{ancestorA}, []*model.BlockHeaderMeta{{Height: ancestorHeight}}, nil).Maybe()
	mockBlockchainClient.On("ClearBlockMinedSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchainClient.On("SendNotification", mock.Anything, mock.Anything).Return(nil).Maybe()

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	responseHeaders := append([]byte{}, ancestorA.Bytes()...)
	for _, h := range forkHeaders {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, responseHeaders))
	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(500, nil))

	err = server.catchup(context.Background(), targetBlock, "peer-ancestor-link", "http://peer")

	blocksRequests := 0
	for k, v := range httpmock.GetCallCountInfo() {
		if strings.Contains(k, "http://peer/blocks/") {
			blocksRequests += v
		}
	}

	return err, blocksRequests, mockBlockchainClient
}

// TestCatchup_FirstHeaderMustLinkToCommonAncestor covers bitcoin-sv/teranode#4975. The peer offers
// [A, h1..h5] where A is the stored common ancestor but h1's previous hash is a different stored
// block B. Catchup must abort before it un-stamps any local block (step 5.5) and before it fetches
// any block body, because every height it would assign (ancestor + 1 + index) is wrong.
//
// The control subtest proves that a correctly linked offer in the same setup reaches both the
// mined_set clear and the block fetch, so the zero counts in the second subtest are not vacuous.
func TestCatchup_FirstHeaderMustLinkToCommonAncestor(t *testing.T) {
	t.Run("control_linked_chain_reaches_block_fetch", func(t *testing.T) {
		_, blocksRequests, mockBlockchainClient := runAncestorLinkCatchup(t, true)

		require.Positive(t, blocksRequests, "a correctly linked chain must reach the block fetch, else the setup is vacuous")
		mockBlockchainClient.AssertCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
	})

	t.Run("first_header_parent_is_other_stored_block", func(t *testing.T) {
		err, blocksRequests, mockBlockchainClient := runAncestorLinkCatchup(t, false)

		require.Error(t, err, "catchup must abort when the first header does not link to the common ancestor")
		require.True(t, errors.Is(err, errors.ErrProcessing), "a mis-linked offer is a generic peer fault: %v", err)
		require.Contains(t, err.Error(), "does not link to common ancestor")
		require.Zero(t, blocksRequests, "no block may be fetched once the link to the ancestor is broken")
		mockBlockchainClient.AssertNotCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
		mockBlockchainClient.AssertNotCalled(t, "SendNotification", mock.Anything, mock.Anything)
	})
}
