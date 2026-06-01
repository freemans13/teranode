package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// makeSatoshiTx builds a tx with the given output satoshis. Scripts are set (so
// the tx is realistic) but the satoshi cache must store ONLY satoshis.
func makeSatoshiTx(t *testing.T, sats []uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	for i, s := range sats {
		scriptBytes := []byte{0x76, 0xa9, 0x14, byte(i), byte(i + 1), byte(i + 2)}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      s,
			LockingScript: bscript.NewFromBytes(scriptBytes),
		})
	}

	return tx
}

func TestSatoshiCache_PutGet(t *testing.T) {
	c, err := newSatoshiCache(8 * 1024 * 1024) // 8 MB off-heap
	require.NoError(t, err)

	parent := makeSatoshiTx(t, []uint64{111, 222, 333})
	c.putTx(parent)

	parentHash := parent.TxIDChainHash()

	dst := make([]byte, 0, 16)
	for idx, want := range []uint64{111, 222, 333} {
		got, ok := c.satoshis(parentHash, uint32(idx), &dst)
		require.True(t, ok, "expected cache hit for output %d", idx)
		require.Equal(t, want, got, "satoshis for output %d", idx)
	}

	hits, misses := c.stats()
	require.Equal(t, uint64(3), hits)
	require.Equal(t, uint64(0), misses)
}

func TestSatoshiCache_Miss(t *testing.T) {
	c, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)

	other := makeSatoshiTx(t, []uint64{999})
	dst := make([]byte, 0, 16)

	got, ok := c.satoshis(other.TxIDChainHash(), 0, &dst)
	require.False(t, ok, "lookup with empty cache must miss")
	require.Equal(t, uint64(0), got, "miss must return zero satoshis")

	_, misses := c.stats()
	require.Equal(t, uint64(1), misses)
}

// TestSatoshiCache_NilSafe verifies the zero-overhead path: a nil cache (feature
// off) never panics and always reports a miss, so callers can fall through to
// the store without a nil check at every call site.
func TestSatoshiCache_NilSafe(t *testing.T) {
	var c *satoshiCache

	require.NotPanics(t, func() { c.putTx(makeSatoshiTx(t, []uint64{1})) })

	dst := make([]byte, 0, 16)
	got, ok := c.satoshis(&chainhash.Hash{}, 0, &dst)
	require.False(t, ok)
	require.Equal(t, uint64(0), got)

	hits, misses := c.stats()
	require.Equal(t, uint64(0), hits)
	require.Equal(t, uint64(0), misses)
}

// TestSatoshiCache_StatsReset verifies stats() returns and resets counters so
// per-block logging reflects only that block's lookups.
func TestSatoshiCache_StatsReset(t *testing.T) {
	c, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)

	parent := makeSatoshiTx(t, []uint64{5})
	c.putTx(parent)

	dst := make([]byte, 0, 16)
	_, _ = c.satoshis(parent.TxIDChainHash(), 0, &dst) // hit
	_, _ = c.satoshis(&chainhash.Hash{0xff}, 0, &dst)  // miss

	hits, misses := c.stats()
	require.Equal(t, uint64(1), hits)
	require.Equal(t, uint64(1), misses)

	// counters reset after the read
	hits, misses = c.stats()
	require.Equal(t, uint64(0), hits)
	require.Equal(t, uint64(0), misses)
}
