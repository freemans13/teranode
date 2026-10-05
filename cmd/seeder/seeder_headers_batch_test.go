package seeder

import (
	"context"
	"encoding/binary"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// headerTestBlock builds a block at height on prev with a BIP34 coinbase, so StoreBlock takes it.
func headerTestBlock(t *testing.T, height uint32, prev *chainhash.Hash, bits model.NBit) *model.Block {
	t.Helper()

	cb := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0xFFFFFFFF, SequenceNumber: 0xFFFFFFFF}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))
	in.UnlockingScript = bscript.NewFromBytes([]byte{3, byte(height), byte(height >> 8), byte(height >> 16)})
	cb.Inputs = append(cb.Inputs, in)
	cb.AddOutput(&bt.Output{Satoshis: 5_000_000_000, LockingScript: bscript.NewFromBytes([]byte{0x51})})

	return &model.Block{
		Header: &model.BlockHeader{
			Version: 2, Timestamp: 1_600_000_000 + height*600, Nonce: height,
			HashPrevBlock: prev, HashMerkleRoot: cb.TxIDChainHash(), Bits: bits,
		},
		Height: height, CoinbaseTx: cb, TransactionCount: 1,
	}
}

// writeHeadersFile writes a V2 .utxo-headers file holding the store's genesis and count blocks
// on top of it, as the UTXO persister writes one.
func writeHeadersFile(t *testing.T, store blockchain.Store, count int) string {
	t.Helper()

	genesis, _, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "test.utxo-headers")
	f, err := os.Create(path)
	require.NoError(t, err)

	defer func() { require.NoError(t, f.Close()) }()

	require.NoError(t, fileformat.NewHeader(fileformat.FileTypeUtxoHeaders).Write(f))

	blocks := []*model.Block{{Header: genesis, Height: 0}}
	prev := genesis.Hash()

	for h := 1; h <= count; h++ {
		b := headerTestBlock(t, uint32(h), prev, genesis.Bits) //nolint:gosec // test heights fit uint32
		blocks = append(blocks, b)
		prev = b.Hash()
	}

	tip := blocks[len(blocks)-1]
	_, err = f.Write(tip.Hash()[:])
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.LittleEndian, tip.Height))

	for _, b := range blocks {
		bi := &utxopersister.BlockIndex{Hash: b.Hash(), Height: b.Height, TxCount: 1, BlockHeader: b.Header, CoinbaseTx: b.CoinbaseTx}
		require.NoError(t, bi.Serialise(f))
	}

	return path
}

func newHeaderTestStore(t *testing.T) blockchain.Store {
	t.Helper()

	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := blockchain.NewStore(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	return s
}

// countingSeedStore passes StoreSeedHeaders through to the SQL store and counts the batches.
type countingSeedStore struct {
	blockchain.Store
	inner   seedHeaderStore
	batches int
}

func (c *countingSeedStore) StoreSeedHeaders(ctx context.Context, blocks []*model.Block, peerID string, opts ...blockchainoptions.StoreBlockOption) (bool, error) {
	c.batches++
	return c.inner.StoreSeedHeaders(ctx, blocks, peerID, opts...)
}

// plainStore hides StoreSeedHeaders, as a store without it would.
type plainStore struct{ blockchain.Store }

func TestProcessHeadersStoresHeadersInBatches(t *testing.T) {
	saved := seedHeaderBatchSize
	seedHeaderBatchSize = 7

	t.Cleanup(func() { seedHeaderBatchSize = saved })

	inner := newHeaderTestStore(t)
	file := writeHeadersFile(t, inner, 30)

	seedStore, ok := inner.(seedHeaderStore)
	require.True(t, ok, "the SQL store writes seed headers in batches")

	store := &countingSeedStore{Store: inner, inner: seedStore}
	require.NoError(t, processHeaders(context.Background(), ulogger.TestLogger{}, store, file))

	require.Equal(t, 5, store.batches, "30 headers in batches of 7")

	_, meta, err := inner.GetBestBlockHeader(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint32(30), meta.Height)
}

func TestProcessHeadersWithoutBatchSupportStoresEachHeader(t *testing.T) {
	inner := newHeaderTestStore(t)
	file := writeHeadersFile(t, inner, 12)

	require.NoError(t, processHeaders(context.Background(), ulogger.TestLogger{}, plainStore{inner}, file))

	_, meta, err := inner.GetBestBlockHeader(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint32(12), meta.Height)
}
