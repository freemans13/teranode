package model

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// randHash returns a deterministic non-placeholder hash seeded by n.
func randHash(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	h[1] = 0xAB
	return h
}

// subtreeWithHashes builds a *subtreepkg.Subtree whose Nodes contain
// exactly the hashes provided, in order.
func subtreeWithHashes(hashes ...chainhash.Hash) *subtreepkg.Subtree {
	st := &subtreepkg.Subtree{}
	for _, h := range hashes {
		h := h
		st.Nodes = append(st.Nodes, subtreepkg.Node{Hash: h})
	}
	return st
}

// TestCheckSubtreeSlicesForDuplicateTxs_Clean verifies that a set of
// slices with no duplicate hashes passes without error.
func TestCheckSubtreeSlicesForDuplicateTxs_Clean(t *testing.T) {
	slices := []*subtreepkg.Subtree{
		// First subtree: coinbase placeholder + two unique txs
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, randHash(1), randHash(2)),
		// Second subtree: two more unique txs
		subtreeWithHashes(randHash(3), randHash(4)),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err, "clean slices must pass dedup check")
}

// TestCheckSubtreeSlicesForDuplicateTxs_DuplicateAcrossSubtrees verifies that
// a hash present in two different subtrees is caught.
func TestCheckSubtreeSlicesForDuplicateTxs_DuplicateAcrossSubtrees(t *testing.T) {
	dup := randHash(0x42)

	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, dup, randHash(1)),
		subtreeWithHashes(dup, randHash(2)), // dup repeated
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err, "duplicate hash must be detected")

	var terr *errors.Error
	require.ErrorAs(t, err, &terr)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "error must be BlockInvalid")
}

// TestCheckSubtreeSlicesForDuplicateTxs_DuplicateWithinOneSubtree verifies that
// two identical hashes inside the same subtree are caught.
func TestCheckSubtreeSlicesForDuplicateTxs_DuplicateWithinOneSubtree(t *testing.T) {
	dup := randHash(0xBE)

	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, dup, dup),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
}

// TestCheckSubtreeSlicesForDuplicateTxs_CoinbasePlaceholderAllowed verifies that
// the coinbase placeholder appearing exactly once at position [0][0] is not
// treated as a duplicate.
func TestCheckSubtreeSlicesForDuplicateTxs_CoinbasePlaceholderAllowed(t *testing.T) {
	// Only one subtree: coinbase placeholder alone — trivially clean.
	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err, "lone coinbase placeholder must pass")
}

// TestCheckSubtreeSlicesForDuplicateTxs_EmptySlices verifies that nil/empty
// input returns no error.
func TestCheckSubtreeSlicesForDuplicateTxs_EmptySlices(t *testing.T) {
	require.NoError(t, CheckSubtreeSlicesForDuplicateTxs(nil))
	require.NoError(t, CheckSubtreeSlicesForDuplicateTxs([]*subtreepkg.Subtree{}))
}

// TestCheckSubtreeSlicesForDuplicateTxs_NilSubtreeSkipped verifies that a nil
// element inside the slice is skipped gracefully.
func TestCheckSubtreeSlicesForDuplicateTxs_NilSubtreeSkipped(t *testing.T) {
	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, randHash(1)),
		nil, // must be skipped, not panic
		subtreeWithHashes(randHash(2)),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err)
}
