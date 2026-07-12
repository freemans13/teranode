package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestResetSubtreeState_ReusesEmptySubtree proves Fix 2: when the current subtree
// carries no block transactions (only the coinbase placeholder), resetSubtreeState
// reuses the SAME underlying *Subtree object rather than allocating a fresh
// currentItemsPerFile-capacity one (1M leaves x 48 bytes ~= 48 MB, zeroed, on
// every move_forward during IBD).
//
// The invariant that must survive reuse: after any number of empty resets the
// current subtree is a correctly-sized subtree holding exactly the coinbase
// placeholder, and a subsequent AddNode (mining resuming) works. The test drives
// N resets and asserts (a) the object identity is preserved, (b) Length()==1 with
// the coinbase placeholder at index 0, (c) the capacity is unchanged, and (d) a
// real tx can still be added afterwards.
func TestResetSubtreeState_ReusesEmptySubtree(t *testing.T) {
	stp, _ := buildIBDFastPathSTP(t)

	// After construction the current subtree holds only the coinbase placeholder.
	first := stp.currentSubtree.Load()
	require.NotNil(t, first)
	require.Equal(t, 1, first.Length(), "fresh subtree has just the coinbase placeholder")
	require.False(t, first.IsMmapBacked(), "test settings are heap-backed (reuse only applies to heap)")

	initialCap := first.Size()

	// Repeated empty resets must all reuse the same object.
	const resets = 5
	for i := 0; i < resets; i++ {
		require.NoError(t, stp.resetSubtreeState(true, true), "reset %d", i)

		cur := stp.currentSubtree.Load()
		require.Same(t, first, cur, "reset %d must reuse the same subtree object (no fresh allocation)", i)
		require.Equal(t, 1, cur.Length(), "reset %d: subtree must hold exactly the coinbase placeholder", i)
		require.True(t, cur.Nodes[0].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash),
			"reset %d: node 0 must be the coinbase placeholder", i)
		require.Equal(t, initialCap, cur.Size(), "reset %d: capacity must be unchanged", i)
		require.Equal(t, uint64(0), cur.Fees, "reset %d: fees reset to zero", i)
		require.Equal(t, uint64(0), cur.SizeInBytes, "reset %d: size reset to zero", i)
		require.Nil(t, cur.ConflictingNodes, "reset %d: conflicting nodes cleared", i)
	}

	// Mining resumes: a real transaction must still be addable to the reused
	// subtree, landing at index 1 (after the coinbase placeholder).
	cur := stp.currentSubtree.Load()
	txHash := chainhash.HashH([]byte("some-real-tx-after-reuse"))
	require.NoError(t, cur.AddNode(txHash, 123, 456))
	require.Equal(t, 2, cur.Length(), "coinbase + one real tx")
	require.Equal(t, uint64(123), cur.Fees)
	require.Equal(t, uint64(456), cur.SizeInBytes)
	require.Equal(t, 1, cur.NodeIndex(txHash), "the reused subtree's node index must resolve the added tx")
}

// TestResetSubtreeState_AllocatesWhenNonEmpty proves the reuse guard is
// conservative: when the current subtree holds real transactions (Length() > 1),
// resetSubtreeState must NOT reuse it in place (a captured pre-reset pointer on
// the full path still needs to read those nodes for remainder recovery). It
// allocates a fresh object and the old one keeps its contents.
func TestResetSubtreeState_AllocatesWhenNonEmpty(t *testing.T) {
	stp, _ := buildIBDFastPathSTP(t)

	old := stp.currentSubtree.Load()
	// Add a real tx so Length() becomes 2 (coinbase + tx).
	txHash := chainhash.HashH([]byte("mined-tx"))
	require.NoError(t, old.AddNode(txHash, 10, 20))
	require.Equal(t, 2, old.Length())

	require.NoError(t, stp.resetSubtreeState(true, true))

	cur := stp.currentSubtree.Load()
	require.NotSame(t, old, cur, "a non-empty subtree must be replaced by a fresh allocation, not reused")
	require.Equal(t, 1, cur.Length(), "the fresh subtree holds only the coinbase placeholder")
	// The old object was Close()d and replaced; its captured contents are not
	// asserted here (Close may release heap-backed slices in future), but the
	// key property is that the live current subtree is a distinct, clean object.
}

// TestResetSubtreeState_NoReuseWhenDisallowed proves the reuse path is gated on
// the allowSubtreeReuse flag: the full moveForwardBlock path and the reorg paths
// pass false so that their captured originalCurrentSubtree stays a distinct
// object (the remainder handoff and the rollback contract depend on it). Even
// with an empty (coinbase-only) current subtree, allowSubtreeReuse=false must
// allocate a fresh object.
func TestResetSubtreeState_NoReuseWhenDisallowed(t *testing.T) {
	stp, _ := buildIBDFastPathSTP(t)

	old := stp.currentSubtree.Load()
	require.Equal(t, 1, old.Length(), "empty subtree, coinbase only")

	require.NoError(t, stp.resetSubtreeState(true, false))

	cur := stp.currentSubtree.Load()
	require.NotSame(t, old, cur,
		"allowSubtreeReuse=false must allocate a fresh subtree even when the current one is empty")
	require.Equal(t, 1, cur.Length(), "the fresh subtree holds only the coinbase placeholder")
}
