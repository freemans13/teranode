package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// makeBlocksWithDistinctCoinbases builds n gap blocks at ascending heights
// (1..n), each carrying one of the package's pre-defined distinct real
// coinbase transactions (coinbaseTx, coinbaseTx2, coinbaseTx3 - see
// SubtreeProcessor_test.go), so the store can be asserted on a per-coinbase
// basis.
func makeBlocksWithDistinctCoinbases(t *testing.T, n int) []*model.Block {
	t.Helper()

	require.LessOrEqual(t, n, 3, "only 3 distinct pre-defined coinbase txs are available")

	coinbases := []*bt.Tx{coinbaseTx, coinbaseTx2, coinbaseTx3}

	blocks := make([]*model.Block, 0, n)

	for i := 0; i < n; i++ {
		height := uint32(i + 1) //nolint:gosec // test data, small bounded value

		blocks = append(blocks, &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1234567890,
				Bits:           model.NBit{},
				Nonce:          height,
			},
			Height:     height,
			ID:         height,
			CoinbaseTx: coinbases[i],
			Subtrees:   []*chainhash.Hash{},
		})
	}

	return blocks
}

func TestReconcileCoinbases_CreatesAndIsIdempotent(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()
	settings := test.CreateBaseTestSettings(t)
	newSubtreeChan := make(chan NewSubtreeRequest, 10)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	blocks := makeBlocksWithDistinctCoinbases(t, 3) // heights 1,2,3

	for _, blk := range blocks {
		_, err := utxoStore.Get(ctx, blk.CoinbaseTx.TxIDChainHash())
		require.Error(t, err) // absent before
	}

	require.NoError(t, stp.ReconcileCoinbases(ctx, blocks))

	for _, blk := range blocks {
		_, err := utxoStore.Get(ctx, blk.CoinbaseTx.TxIDChainHash())
		require.NoError(t, err) // present after
	}

	require.NoError(t, stp.ReconcileCoinbases(ctx, blocks)) // idempotent
}

func TestReconcileCoinbases_EmptyGapBlocks_NoOp(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()
	settings := test.CreateBaseTestSettings(t)
	newSubtreeChan := make(chan NewSubtreeRequest, 10)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	require.NoError(t, stp.ReconcileCoinbases(ctx, nil))
}
