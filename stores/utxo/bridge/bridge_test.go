package bridge

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func newHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	return h
}

func newHashPtr(b byte) *chainhash.Hash {
	h := newHash(b)
	return &h
}

func TestBridge_AddBlockAndLookup(t *testing.T) {
	bridge := NewMinedTxBridge(10)

	blockHash := newHash(0x01)
	tx1 := newHashPtr(0xAA)
	tx2 := newHashPtr(0xBB)
	missing := newHashPtr(0xCC)

	bridge.AddBlock(blockHash, 42, 100, []*chainhash.Hash{tx1, tx2})

	ids := bridge.GetBlockIDsForTx(tx1)
	require.NotNil(t, ids)
	require.Equal(t, []uint32{42}, ids)

	ids = bridge.GetBlockIDsForTx(tx2)
	require.NotNil(t, ids)
	require.Equal(t, []uint32{42}, ids)

	ids = bridge.GetBlockIDsForTx(missing)
	require.Nil(t, ids)
}

func TestBridge_MultipleBlocks(t *testing.T) {
	bridge := NewMinedTxBridge(10)

	blockHash1 := newHash(0x01)
	blockHash2 := newHash(0x02)
	sharedTx := newHashPtr(0xAA)

	bridge.AddBlock(blockHash1, 10, 100, []*chainhash.Hash{sharedTx})
	bridge.AddBlock(blockHash2, 20, 101, []*chainhash.Hash{sharedTx})

	ids := bridge.GetBlockIDsForTx(sharedTx)
	require.NotNil(t, ids)
	require.ElementsMatch(t, []uint32{10, 20}, ids)
}

func TestBridge_RemoveBlock(t *testing.T) {
	bridge := NewMinedTxBridge(10)

	blockHash := newHash(0x01)
	tx := newHashPtr(0xAA)

	bridge.AddBlock(blockHash, 42, 100, []*chainhash.Hash{tx})
	require.NotNil(t, bridge.GetBlockIDsForTx(tx))

	bridge.RemoveBlock(blockHash)
	require.Nil(t, bridge.GetBlockIDsForTx(tx))
}

func TestBridge_HasBlock(t *testing.T) {
	bridge := NewMinedTxBridge(10)

	blockHash := newHash(0x01)
	tx := newHashPtr(0xAA)

	require.False(t, bridge.HasBlock(blockHash))

	bridge.AddBlock(blockHash, 42, 100, []*chainhash.Hash{tx})
	require.True(t, bridge.HasBlock(blockHash))

	bridge.RemoveBlock(blockHash)
	require.False(t, bridge.HasBlock(blockHash))
}

func TestBridge_WarningThresholdNotEnforced(t *testing.T) {
	// warningThreshold is NOT a hard limit — bridge should accept all 3 blocks even with threshold=2
	bridge := NewMinedTxBridge(2)

	block1 := newHash(0x01)
	block2 := newHash(0x02)
	block3 := newHash(0x03)

	tx1 := newHashPtr(0xAA)
	tx2 := newHashPtr(0xBB)
	tx3 := newHashPtr(0xCC)

	bridge.AddBlock(block1, 1, 100, []*chainhash.Hash{tx1})
	bridge.AddBlock(block2, 2, 101, []*chainhash.Hash{tx2})
	bridge.AddBlock(block3, 3, 102, []*chainhash.Hash{tx3})

	require.Equal(t, 3, bridge.BlockCount())
	require.True(t, bridge.HasBlock(block1))
	require.True(t, bridge.HasBlock(block2))
	require.True(t, bridge.HasBlock(block3))
}
