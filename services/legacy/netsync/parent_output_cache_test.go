package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// makeTx builds a tx with the given output (satoshis, script) values. The script
// for output i is a distinct byte pattern so we can assert the right one is returned.
func makeParentTx(t *testing.T, sats []uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	for i, s := range sats {
		// distinct, non-trivial script per output
		scriptBytes := []byte{0x76, 0xa9, 0x14, byte(i), byte(i + 1), byte(i + 2)}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      s,
			LockingScript: bscript.NewFromBytes(scriptBytes),
		})
	}
	return tx
}

func TestParentOutputCache_PutGet(t *testing.T) {
	c, err := newParentOutputCache(8 * 1024 * 1024) // 8 MB off-heap
	require.NoError(t, err)

	parent := makeParentTx(t, []uint64{111, 222, 333})
	c.putTx(parent)

	parentHash := parent.TxIDChainHash()

	// Build a child input referencing parent output 1 and fill it from the cache.
	dst := make([]byte, 0, 64)
	for idx, wantSats := range []uint64{111, 222, 333} {
		in := &bt.Input{PreviousTxOutIndex: uint32(idx)}
		require.NoError(t, in.PreviousTxIDAdd(parentHash))

		ok := c.fillInput(in, &dst)
		require.True(t, ok, "expected cache hit for output %d", idx)
		require.NotNil(t, in.PreviousTxScript)
		require.Equal(t, wantSats, in.PreviousTxSatoshis, "satoshis for output %d", idx)
		require.Equal(t,
			[]byte{0x76, 0xa9, 0x14, byte(idx), byte(idx + 1), byte(idx + 2)},
			[]byte(*in.PreviousTxScript), "script for output %d", idx)
	}

	hits, misses := c.stats()
	require.Equal(t, uint64(3), hits)
	require.Equal(t, uint64(0), misses)
}

func TestParentOutputCache_Miss(t *testing.T) {
	c, err := newParentOutputCache(8 * 1024 * 1024)
	require.NoError(t, err)

	// Nothing cached → any lookup misses and leaves the input untouched.
	other := makeParentTx(t, []uint64{999})
	in := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, in.PreviousTxIDAdd(other.TxIDChainHash()))

	dst := make([]byte, 0, 64)
	ok := c.fillInput(in, &dst)
	require.False(t, ok)
	require.Nil(t, in.PreviousTxScript, "miss must not populate the input")

	_, misses := c.stats()
	require.Equal(t, uint64(1), misses)
}

// TestParentOutputCache_DstReuse verifies the script is copied out of the shared
// scratch buffer, so reusing dst across lookups can't corrupt an earlier result.
func TestParentOutputCache_DstReuse(t *testing.T) {
	c, err := newParentOutputCache(8 * 1024 * 1024)
	require.NoError(t, err)

	parent := makeParentTx(t, []uint64{111, 222})
	c.putTx(parent)
	parentHash := parent.TxIDChainHash()

	dst := make([]byte, 0, 64)

	in0 := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, in0.PreviousTxIDAdd(parentHash))
	require.True(t, c.fillInput(in0, &dst))
	script0 := []byte(*in0.PreviousTxScript)

	in1 := &bt.Input{PreviousTxOutIndex: 1}
	require.NoError(t, in1.PreviousTxIDAdd(parentHash))
	require.True(t, c.fillInput(in1, &dst)) // reuses dst

	// in0's script must be unaffected by the second fill.
	require.Equal(t, []byte{0x76, 0xa9, 0x14, 0, 1, 2}, script0)
	require.Equal(t, []byte{0x76, 0xa9, 0x14, 1, 2, 3}, []byte(*in1.PreviousTxScript))
}
