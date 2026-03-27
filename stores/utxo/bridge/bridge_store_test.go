package bridge

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// mockStore implements utxo.Store with only the methods needed for testing.
// Unimplemented methods delegate to the embedded utxo.Store (nil), which will
// panic on unexpected calls.
type mockStore struct {
	utxo.Store

	getFunc           func(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error)
	getMetaFunc       func(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error
	setMinedMultiFunc func(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error)
	batchDecorateFunc func(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error
}

func (m *mockStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	return m.getFunc(ctx, hash, f...)
}

func (m *mockStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	return m.getMetaFunc(ctx, hash, data)
}

func (m *mockStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return m.setMinedMultiFunc(ctx, hashes, info)
}

func (m *mockStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	return m.batchDecorateFunc(ctx, items, f...)
}

// Stub non-intercepted methods to satisfy the interface without panicking.
func (m *mockStore) GetBlockHeight() uint32            { return 0 }
func (m *mockStore) GetMedianBlockTime() uint32        { return 0 }
func (m *mockStore) GetBlockState() utxo.BlockState    { return utxo.BlockState{} }
func (m *mockStore) SetBlockHeight(_ uint32) error     { return nil }
func (m *mockStore) SetMedianBlockTime(_ uint32) error { return nil }

// ---- helpers ---------------------------------------------------------------

func txHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	return h
}

func txHashPtr(b byte) *chainhash.Hash {
	h := txHash(b)
	return &h
}

func blockHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[31] = b
	return h
}

// ---- tests -----------------------------------------------------------------

func TestBridgeStore_GetMergesBlockIDs(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x01)
	tx := txHashPtr(0xAA)
	bridgeInst.AddBlock(bh, 42, 100, []*chainhash.Hash{tx})

	inner := &mockStore{
		getFunc: func(_ context.Context, _ *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			return &meta.Data{BlockIDs: []uint32{10}}, nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, true)
	data, err := store.Get(context.Background(), tx, fields.BlockIDs)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{10, 42}, data.BlockIDs)
}

func TestBridgeStore_GetMetaMergesBlockIDs(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x02)
	tx := txHashPtr(0xBB)
	bridgeInst.AddBlock(bh, 42, 100, []*chainhash.Hash{tx})

	inner := &mockStore{
		getMetaFunc: func(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
			data.BlockIDs = []uint32{10}
			return nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, true)
	d := &meta.Data{}
	err := store.GetMeta(context.Background(), tx, d)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{10, 42}, d.BlockIDs)
}

func TestBridgeStore_DisabledPassesThrough(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x03)
	tx := txHashPtr(0xCC)
	bridgeInst.AddBlock(bh, 42, 100, []*chainhash.Hash{tx})

	inner := &mockStore{
		getFunc: func(_ context.Context, _ *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			return &meta.Data{BlockIDs: []uint32{10}}, nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, false)
	data, err := store.Get(context.Background(), tx, fields.BlockIDs)
	require.NoError(t, err)
	// Bridge is disabled — only store data, no bridge data.
	require.Equal(t, []uint32{10}, data.BlockIDs)
}

func TestBridgeStore_DeduplicatesBlockIDs(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x04)
	tx := txHashPtr(0xDD)
	// Bridge also has blockID=42
	bridgeInst.AddBlock(bh, 42, 100, []*chainhash.Hash{tx})

	inner := &mockStore{
		getFunc: func(_ context.Context, _ *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			// Store already has blockID=42
			return &meta.Data{BlockIDs: []uint32{42}}, nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, true)
	data, err := store.Get(context.Background(), tx, fields.BlockIDs)
	require.NoError(t, err)
	// Should appear only once
	require.Equal(t, []uint32{42}, data.BlockIDs)
}

func TestBridgeStore_SetMinedMultiMergesBridgeIDs(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x05)
	tx := txHashPtr(0xEE)
	// Bridge has this tx from a previous block (blockID=99)
	bridgeInst.AddBlock(bh, 99, 50, []*chainhash.Hash{tx})

	txH := txHash(0xEE)
	inner := &mockStore{
		setMinedMultiFunc: func(_ context.Context, _ []*chainhash.Hash, _ utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
			// Inner returns empty map (tx not yet written to DB)
			return map[chainhash.Hash][]uint32{}, nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, true)
	result, err := store.SetMinedMulti(context.Background(), []*chainhash.Hash{&txH}, utxo.MinedBlockInfo{BlockID: 77})
	require.NoError(t, err)
	// Bridge should have added blockID=99 into the result
	require.ElementsMatch(t, []uint32{99}, result[txH])
}

func TestBridgeStore_BatchDecoratesMergesBlockIDs(t *testing.T) {
	bridgeInst := NewMinedTxBridge(10)
	bh := blockHash(0x06)
	tx := txHashPtr(0xFF)
	bridgeInst.AddBlock(bh, 55, 200, []*chainhash.Hash{tx})

	txH := txHash(0xFF)
	inner := &mockStore{
		batchDecorateFunc: func(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
			for _, item := range items {
				item.Data = &meta.Data{BlockIDs: []uint32{10}}
			}
			return nil
		},
	}

	store := NewBridgeStore(inner, bridgeInst, true)
	items := []*utxo.UnresolvedMetaData{{Hash: txH, Data: &meta.Data{}}}
	err := store.BatchDecorate(context.Background(), items, fields.BlockIDs)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{10, 55}, items[0].Data.BlockIDs)
}

// Compile-time check: BridgeStore satisfies utxo.Store.
var _ utxo.Store = (*BridgeStore)(nil)
