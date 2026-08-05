package model

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCheckDuplicateTransactionsAboveUint32 pins issue 1428: a block whose
// TransactionCount exceeds 2^32 must go through duplicate checking in 64-bit,
// not fail a uint32 narrowing with a retryable processing error. Post-Genesis
// BSV has no block-size limit and SV Node carries the count as a CompactSize,
// so such a block is consensus-valid; a conversion error here made block
// validation refetch and revalidate the same block forever, wedging the whole
// fleet deterministically.
func TestCheckDuplicateTransactionsAboveUint32(t *testing.T) {
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for i := byte(1); i <= 3; i++ {
		hash := chainhash.HashH([]byte{i, 0xdd})
		require.NoError(t, subtree.AddNode(hash, 1, 0))
	}

	block := &Block{
		Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		TransactionCount: math.MaxUint32 + 2, // the count the wire format allows but uint32 cannot hold
		SubtreeSlices:    []*subtreepkg.Subtree{subtree},
	}

	err = block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil)
	require.NoError(t, err, "a transaction count above 2^32 must not fail duplicate checking")

	// The pooled map must be releasable with the same 64-bit count.
	block.releaseTxMap()
	require.Nil(t, block.txMap)
}

// TestTxMapPool64Bit pins the pool API on 64-bit counts: any count must yield
// a usable map (counts above the largest size class allocate fresh with a
// clamped preallocation hint — the swiss maps grow on demand), and returning
// it with the same count must not panic.
func TestTxMapPool64Bit(t *testing.T) {
	for _, n := range []uint64{0, 1, 1 << 20, 1 << 31, math.MaxUint32 + 1, 1 << 40} {
		m := GetTxMap(n)
		require.NotNil(t, m, "count %d", n)

		hash := chainhash.HashH([]byte{byte(n), byte(n >> 8), 0xab})
		require.NoError(t, m.Put(hash, 1))
		require.True(t, m.Exists(hash))

		PutTxMap(m, n)
	}
}
