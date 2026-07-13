package subtreeprocessor

// Tests for the moveForwardMetas pass-through on Reorg/reorgBlocks.
//
// The fast-path in moveForwardBlock (IBD path) fires when the block meta
// reports MinedSet=true + QuickValidated=true.  Historically the meta was
// obtained via a GetBlockHeader gRPC call on every block.  With the new
// moveForwardMetas slice, handleCatchUp can pre-supply the metas so that
// reorgBlocks passes them straight into moveForwardBlock — eliminating a
// round-trip per block.
//
// Discriminator:
//   - When metas are supplied (non-nil slice with MinedSet+QuickValidated):
//     the IBD fast-path fires, the fake subtree hash is never fetched from
//     the store, and GetBlockHeader is never called.
//   - When metas is nil: reorgBlocks passes nil bm into moveForwardBlock,
//     which falls back to GetBlockHeader.  Mocking GetBlockHeader to return
//     MinedSet+QuickValidated lets the fast-path fire via the gRPC route,
//     so the call count proves the fallback path was taken.

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	chaincfg "github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildCatchupSTPWithMock creates a fully started SubtreeProcessor wired with a
// real sqlite-memory UTXO store and a blockchain.Mock.  The settings include a
// checkpoint at ibdTestCheckpointHeight so that blocks at ibdTestBlockHeight
// satisfy model.BelowCheckpoint.  Always-on stubs are registered for
// SetBlockProcessedAt and GetBlockIsMined.
func buildCatchupSTPWithMock(t *testing.T) (*SubtreeProcessor, *blockchain.Mock) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Install a real checkpoint so BelowCheckpoint works for ibdTestBlockHeight.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: ibdTestCheckpointHeight}}
	settings.ChainCfgParams = &params

	u, err := url.Parse("sqlitememory:///catchup-meta-test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, u)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(1))

	subtreeStore := blob_memory.New()
	bcMock := &blockchain.Mock{}

	// SetBlockProcessedAt is called by finalizeBlockProcessing after each block.
	bcMock.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// GetBlockIsMined is needed by waitForBlockBeingMined in the catch-up path.
	bcMock.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, logger, settings, subtreeStore, bcMock, utxoStore, nil)
	require.NoError(t, err)
	stp.Start(ctx)

	return stp, bcMock
}

// buildCatchupBlocks builds n blocks that chain sequentially from prevBlockHeader
// (the package-level fixture), each carrying a fake subtree hash that does not
// exist in the blob store.  The IBD fast-path skips the store read; the full
// path would error, acting as the discriminator.
func buildCatchupBlocks(t *testing.T, n int) []*model.Block {
	t.Helper()

	blocks := make([]*model.Block, n)
	currentHeader := prevBlockHeader // package-level fixture in SubtreeProcessor_test.go

	for i := 0; i < n; i++ {
		fakeHash := chainhash.HashH([]byte(fmt.Sprintf("fake-subtree-catchup-meta-%d", i)))
		blocks[i] = &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  currentHeader.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      uint32(1234567890 + i),
				Bits:           model.NBit{},
				Nonce:          uint32(i),
			},
			Height:     ibdTestBlockHeight + uint32(i),
			Subtrees:   []*chainhash.Hash{&fakeHash},
			CoinbaseTx: coinbaseTx, // package-level fixture in SubtreeProcessor_test.go
		}
		currentHeader = blocks[i].Header
	}

	return blocks
}

// TestReorgCatchup_UsesPerBlockMetaSkipsGetBlockHeader verifies that when Reorg
// is called with a non-nil moveForwardMetas slice (MinedSet+QuickValidated per
// block), the IBD fast-path fires via the supplied meta and GetBlockHeader is
// never called.
func TestReorgCatchup_UsesPerBlockMetaSkipsGetBlockHeader(t *testing.T) {
	stp, bcMock := buildCatchupSTPWithMock(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	blocks := buildCatchupBlocks(t, 3)

	metas := []*model.BlockHeaderMeta{
		{MinedSet: true, QuickValidated: true},
		{MinedSet: true, QuickValidated: true},
		{MinedSet: true, QuickValidated: true},
	}

	require.NoError(t, stp.Reorg([]*model.Block{}, blocks, metas))
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}

// TestReorgCatchup_NilMetaFallsBackToGetBlockHeader verifies that when Reorg is
// called with nil moveForwardMetas, each block in the forward loop triggers a
// GetBlockHeader call (the existing gRPC fallback path).  The mock returns
// MinedSet+QuickValidated so the fast-path fires and the fake subtree hash is
// never read — but the call count proves the gRPC path was taken 3 times.
func TestReorgCatchup_NilMetaFallsBackToGetBlockHeader(t *testing.T) {
	stp, bcMock := buildCatchupSTPWithMock(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	blocks := buildCatchupBlocks(t, 3)

	// GetBlockHeader returns valid meta so the fast-path fires and no subtree
	// store error surfaces.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}, nil)

	require.NoError(t, stp.Reorg([]*model.Block{}, blocks, nil))
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 3)
}
