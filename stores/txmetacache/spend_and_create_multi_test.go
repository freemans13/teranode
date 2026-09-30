package txmetacache

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The wrapper must reach the inner store's SpendAndCreateMulti, never run the
// shared default over itself, and cache only created, unmined, non-conflicting
// records.
func TestTxMetaCache_SpendAndCreateMultiForwardsAndCaches(t *testing.T) {
	ctx := context.Background()

	txA := bt.NewTx()
	txA.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{0x51})})
	txB := bt.NewTx()
	txB.AddOutput(&bt.Output{Satoshis: 2, LockingScript: bscript.NewFromBytes([]byte{0x51})})
	txC := bt.NewTx()
	txC.AddOutput(&bt.Output{Satoshis: 3, LockingScript: bscript.NewFromBytes([]byte{0x51})})

	txs := []*bt.Tx{txA, txB, txC}
	results := []utxo.SpendAndCreateMultiResult{
		{Status: utxo.MultiTxCreated, Meta: &meta.Data{Fee: 11, SizeInBytes: 1}},
		{Status: utxo.MultiTxExisted},
		{Status: utxo.MultiTxCreated, Meta: &meta.Data{Fee: 33, Conflicting: true}},
	}

	inner := &utxo.MockUtxostore{}
	inner.On("SpendAndCreateMulti", mock.Anything, txs, uint32(100), mock.Anything).Return(results, nil).Once()

	c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, inner, Unallocated)
	require.NoError(t, err)

	got, err := c.(*TxMetaCache).SpendAndCreateMulti(ctx, txs, 100)
	require.NoError(t, err)
	require.Equal(t, results, got)

	inner.AssertExpectations(t)
	inner.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	cached, ok := c.(*TxMetaCache).GetMetaCached(ctx, *txA.TxIDChainHash())
	require.True(t, ok, "a created record is cached")
	require.Equal(t, uint64(11), cached.Fee)

	_, ok = c.(*TxMetaCache).GetMetaCached(ctx, *txB.TxIDChainHash())
	require.False(t, ok, "an existing record is not cached")

	_, ok = c.(*TxMetaCache).GetMetaCached(ctx, *txC.TxIDChainHash())
	require.False(t, ok, "a conflicting record is not cached")
}

// With WithTXIDs the cache keys on the supplied txids.
func TestTxMetaCache_SpendAndCreateMultiCachesUnderSuppliedTxIDs(t *testing.T) {
	ctx := context.Background()

	tx := bt.NewTx()
	tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{0x51})})
	ids := []chainhash.Hash{{0x42}}

	inner := &utxo.MockUtxostore{}
	inner.On("SpendAndCreateMulti", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]utxo.SpendAndCreateMultiResult{
		{Status: utxo.MultiTxCreated, Meta: &meta.Data{Fee: 7}},
	}, nil).Once()

	c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, inner, Unallocated)
	require.NoError(t, err)

	_, err = c.(*TxMetaCache).SpendAndCreateMulti(ctx, []*bt.Tx{tx}, 100, utxo.WithTXIDs(ids))
	require.NoError(t, err)

	_, ok := c.(*TxMetaCache).GetMetaCached(ctx, ids[0])
	require.True(t, ok)
}
