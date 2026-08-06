package model

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// These tests pin issue 1428 — a block whose TransactionCount exceeds 2^32
// must be handled in 64-bit, not fail a uint32 narrowing with a retryable
// processing error that made block validation refetch the same
// consensus-valid block forever.
//
// Deliberately NOT tested by materialising a >2^32-entry map: GetTxMap
// preallocates eagerly from its hint, and a single such call costs tens of
// GB of RSS (the same trap TestGetTxMap_OversizedAllocatesFresh documents
// removing after it dominated CI memory, issue 1051). The narrowing seam
// itself no longer exists at the type level — GetTxMap/PutTxMap take uint64 —
// so the pure classification and clamp logic carries the behavioural pin.

// TestTxMapClassIdx64Bit pins the size-class classification on 64-bit counts:
// everything at or below the largest class resolves to a pool class, and
// every count above it — including counts beyond uint32, which previously
// could not even be expressed — resolves to the allocate-fresh path.
func TestTxMapClassIdx64Bit(t *testing.T) {
	largestClass := uint64(txMapSizeClasses[len(txMapSizeClasses)-1])

	require.Equal(t, 0, txMapClassIdxFor(0))
	require.Equal(t, 0, txMapClassIdxFor(1))
	require.Equal(t, len(txMapSizeClasses)-1, txMapClassIdxFor(largestClass))

	for _, n := range []uint64{largestClass + 1, math.MaxUint32, math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, -1, txMapClassIdxFor(n), "count %d must take the allocate-fresh path", n)
	}
}

// TestTxMapAllocHintClamp pins the preallocation-hint bound: any count above
// the largest pooled size class caps the hint at that class (preallocation only
// — the map resizes on insert), everything else passes through unchanged.
func TestTxMapAllocHintClamp(t *testing.T) {
	largestClass := txMapSizeClasses[len(txMapSizeClasses)-1]

	require.Equal(t, uint32(0), txMapAllocHint(0))
	require.Equal(t, uint32(1<<20), txMapAllocHint(1<<20))
	require.Equal(t, largestClass, txMapAllocHint(uint64(largestClass)))

	for _, n := range []uint64{uint64(largestClass) + 1, math.MaxUint32, math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, largestClass, txMapAllocHint(n), "count %d must be bounded to the largest class", n)
	}
}

// TestTxMapAllocHintAvoidsConstructorOverflow pins the bound below the point
// where the map constructor's own per-bucket arithmetic wraps. It computes
// per-bucket size as (hint + hint/5) in uint32, so any hint above ~3.58e9
// overflows and preallocates an arbitrary amount unrelated to the count
// (MaxUint32 wraps to ~859M entries). Every hint this package can produce must
// stay in the range where that arithmetic is exact.
func TestTxMapAllocHintAvoidsConstructorOverflow(t *testing.T) {
	for _, n := range []uint64{0, 1 << 20, uint64(txMapSizeClasses[len(txMapSizeClasses)-1]), math.MaxUint32, 1 << 40, math.MaxUint64} {
		hint := txMapAllocHint(n)

		// The constructor's computation, in uint32 as it performs it, must equal
		// the same computation carried out without truncation.
		require.Equal(t, uint64(hint)+uint64(hint)/5, uint64(hint+hint/5),
			"hint %d (from count %d) overflows the constructor's per-bucket arithmetic", hint, n)
	}
}

// TestTxMapPoolRoundTrip64BitKey pins Get/Put with the 64-bit key on pooled
// (cheap) size classes: the map must be usable and returnable with the same
// uint64 count a block carries.
func TestTxMapPoolRoundTrip64BitKey(t *testing.T) {
	for _, n := range []uint64{0, 1, 1 << 12, 1 << 20} {
		m := GetTxMap(n)
		require.NotNil(t, m, "count %d", n)

		hash := chainhash.HashH([]byte{byte(n), byte(n >> 8), 0xab})
		require.NoError(t, m.Put(hash, 1))
		require.True(t, m.Exists(hash))

		PutTxMap(m, n)
	}
}

// TestCheckDuplicateTransactionsUsesFullCount drives the real duplicate-check
// path with a pooled-size count and verifies the pooled map is released with
// the same 64-bit key. The >2^32 seam is covered by the classification and
// clamp tests above (materialising such a map costs tens of GB — see the
// header comment).
func TestCheckDuplicateTransactionsUsesFullCount(t *testing.T) {
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for i := byte(1); i <= 3; i++ {
		hash := chainhash.HashH([]byte{i, 0xdd})
		require.NoError(t, subtree.AddNode(hash, 1, 0))
	}

	block := &Block{
		Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		TransactionCount: 4,
		SubtreeSlices:    []*subtreepkg.Subtree{subtree},
	}

	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil))

	// Hold the map across release: PutTxMap clears it only when the 64-bit key
	// resolves to a pool class, so a wrong or stale release key (which would
	// silently drop the map instead of pooling it) leaves the 3 entries behind.
	pooled, ok := block.txMap.(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.Equal(t, 3, pooled.Length())

	block.releaseTxMap()
	require.Nil(t, block.txMap)
	require.Equal(t, 0, pooled.Length(), "released map must be cleared and pooled, not dropped")
}
