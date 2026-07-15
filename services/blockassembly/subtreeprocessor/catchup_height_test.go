package subtreeprocessor

// Tests for the per-block catch-up height signal (CatchupHeight), the truthful
// block-assembly height the legacy sync maturity gate polls. Before this, the
// polled height only advanced once per catch-up batch, so during a large
// catch-up it read stale-low by the whole batch size (~900 blocks on the
// 2026-07-15 mainnet freeze), tripping a false maturity-gate timeout.
//
// The load-bearing property is PER-BLOCK advance: CatchupHeight must rise as
// each block's moveForwardBlock completes, not jump once at batch commit.

import (
	"context"
	"net/url"
	"testing"

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

// TestCatchupHeight_AdvancesPerBlock drives a 3-block catch-up and samples
// CatchupHeight from inside the per-block SetBlockProcessedAt callback (which
// finalizeBlockProcessing invokes for block i BEFORE the height for block i is
// published). A batch-frozen implementation would sample [0,0,0]; the per-block
// implementation samples a strictly non-decreasing sequence that ends one block
// behind (the last publish lands after the last callback), and CatchupHeight
// after Reorg equals the final block's height.
func TestCatchupHeight_AdvancesPerBlock(t *testing.T) {
	// Inline setup (rather than buildCatchupSTPWithMock) so SetBlockProcessedAt's
	// SOLE registration carries the per-block sampling .Run — a second .On with
	// the same matcher is never selected by testify.
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: ibdTestCheckpointHeight}}
	settings.ChainCfgParams = &params

	u, err := url.Parse("sqlitememory:///catchup-height-perblock")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, u)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(1))

	bcMock := &blockchain.Mock{}
	bcMock.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, logger, settings, blob_memory.New(), bcMock, utxoStore, nil)
	require.NoError(t, err)
	stp.Start(ctx)
	stp.InitCurrentBlockHeader(prevBlockHeader)

	const n = 3
	blocks := buildCatchupBlocks(t, n)

	metas := make([]*model.BlockHeaderMeta, n)
	heights := make([]uint32, n)
	for i := 0; i < n; i++ {
		metas[i] = &model.BlockHeaderMeta{
			MinedSet:       true,
			QuickValidated: true,
		}
		heights[i] = ibdTestBlockHeight + uint32(i) //nolint:gosec
	}

	// Sample the published height as each block is finalized. finalizeBlockProcessing
	// calls SetBlockProcessedAt for block i BEFORE the height for block i is published,
	// so a per-block impl yields a rising sequence ending one block behind, while a
	// batch-frozen impl (publish once after the loop) yields all zeros.
	var samples []uint32
	bcMock.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) { samples = append(samples, stp.CatchupHeight()) }).
		Return(nil)

	require.NoError(t, stp.Reorg([]*model.Block{}, blocks, metas, heights))

	// After the batch, the hint holds the final block's height (no reset here —
	// BlockAssembler.setBestBlockHeader owns the reset).
	require.Equal(t, ibdTestBlockHeight+uint32(n-1), stp.CatchupHeight(),
		"CatchupHeight must reach the last catch-up block's height")

	// Per-block proof: the samples must be non-decreasing AND not all zero. A
	// batch-frozen implementation (store once, after the loop) yields all-zero
	// samples because the store happens after every SetBlockProcessedAt.
	require.Len(t, samples, n)
	require.NotEqual(t, []uint32{0, 0, 0}, samples, "height must advance during the batch, not only at commit")
	for i := 1; i < len(samples); i++ {
		require.GreaterOrEqual(t, samples[i], samples[i-1], "catch-up height must be monotonic")
	}
	require.Positive(t, samples[len(samples)-1], "later blocks must observe earlier blocks' published height")
}

// TestResetCatchupHeight_Zeroes verifies the reset the commit path relies on.
func TestResetCatchupHeight_Zeroes(t *testing.T) {
	stp, _ := buildCatchupSTPWithMock(t)
	stp.catchupHeight.Store(465571)
	require.Equal(t, uint32(465571), stp.CatchupHeight())
	stp.ResetCatchupHeight()
	require.Zero(t, stp.CatchupHeight())
}
