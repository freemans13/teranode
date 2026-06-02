package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newTestBlock builds a minimal bsvutil.Block whose hash is derived from a
// header with the given previous-block hash. No transactions are needed:
// deriveBlockHeight only inspects the header and the recorded height.
func newTestBlock(prev chainhash.Hash) *bsvutil.Block {
	merkle := chainhash.Hash{0xCC}
	hdr := wire.NewBlockHeader(1, &prev, &merkle, 0x1d00ffff, 0)
	return bsvutil.NewBlock(wire.NewMsgBlock(hdr))
}

// TestDeriveBlockHeight_PipelineUsesHeaderListNoStoreRead is the core unlock:
// with the finalization pipeline enabled and the height known from the
// in-memory header list, deriveBlockHeight returns that height WITHOUT a
// GetBlockHeader(prevBlock) round-trip — so it works even when the parent block
// has not yet been added to the blockchain (the v1 failure mode).
func TestDeriveBlockHeight_PipelineUsesHeaderListNoStoreRead(t *testing.T) {
	bc := &blockchain.Mock{} // no GetBlockHeader expectation: a call would panic

	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockFinalizationPipeline = true

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

	block := newTestBlock(chainhash.Hash{0xAA})
	blockHash := *block.Hash()

	sm.setHeaderHeight(blockHash, 500_000)

	h, err := sm.deriveBlockHeight(context.Background(), block, blockHash)
	require.NoError(t, err)
	require.Equal(t, uint32(500_000), h)
	require.Equal(t, int32(500_000), block.Height(), "block height set from header list")
	bc.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}

// TestDeriveBlockHeight_FallbackUsesStoreWhenFlagOff verifies that with the
// pipeline flag off the default behaviour is unchanged: height comes from
// GetBlockHeader(prevBlock) (previous height + 1).
func TestDeriveBlockHeight_FallbackUsesStoreWhenFlagOff(t *testing.T) {
	bc := &blockchain.Mock{}
	meta := &model.BlockHeaderMeta{Height: 499_999}
	bc.On("GetBlockHeader", mock.Anything, mock.Anything).Return((*model.BlockHeader)(nil), meta, nil)

	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockFinalizationPipeline = false

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

	block := newTestBlock(chainhash.Hash{0xAA})
	blockHash := *block.Hash()

	// Even with a header height recorded, the flag-off path must ignore it.
	sm.setHeaderHeight(blockHash, 123)

	h, err := sm.deriveBlockHeight(context.Background(), block, blockHash)
	require.NoError(t, err)
	require.Equal(t, uint32(500_000), h)
	bc.AssertCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}

// TestDeriveBlockHeight_FallbackWhenHeightUnknown verifies graceful fallback:
// flag on but the block's height was never recorded (e.g. not headers-first),
// so it must fall back to the store lookup rather than fail.
func TestDeriveBlockHeight_FallbackWhenHeightUnknown(t *testing.T) {
	bc := &blockchain.Mock{}
	meta := &model.BlockHeaderMeta{Height: 100}
	bc.On("GetBlockHeader", mock.Anything, mock.Anything).Return((*model.BlockHeader)(nil), meta, nil)

	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockFinalizationPipeline = true

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

	block := newTestBlock(chainhash.Hash{0xAA})
	blockHash := *block.Hash()

	h, err := sm.deriveBlockHeight(context.Background(), block, blockHash)
	require.NoError(t, err)
	require.Equal(t, uint32(101), h)
	bc.AssertCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}

// TestDeriveBlockHeight_PipelineRejectsMismatchedHeight verifies that when the
// wire block carries a height that disagrees with the trusted header-list
// height, the block is rejected as invalid.
func TestDeriveBlockHeight_PipelineRejectsMismatchedHeight(t *testing.T) {
	bc := &blockchain.Mock{}

	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockFinalizationPipeline = true

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

	block := newTestBlock(chainhash.Hash{0xAA})
	block.SetHeight(123) // disagrees with header-list height
	blockHash := *block.Hash()

	sm.setHeaderHeight(blockHash, 500_000)

	_, err := sm.deriveBlockHeight(context.Background(), block, blockHash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")
	bc.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}
