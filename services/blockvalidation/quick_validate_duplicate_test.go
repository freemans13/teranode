package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// buildQuickPathSubtrees returns two subtree slices of capacity 2 whose leaves are the
// given transaction hashes, with a coinbase placeholder occupying the first position of
// the first subtree — the shape validateSubtrees sees on the quick-validation path.
func buildQuickPathSubtrees(t *testing.T, second, third chainhash.Hash) []*subtreepkg.Subtree {
	t.Helper()

	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(second, 1, 100))

	last, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, last.AddNode(third, 1, 100))

	return []*subtreepkg.Subtree{first, last}
}

// newQuickPathBlock wraps the given subtree slices in the minimum viable block for
// validateSubtrees: a header (for block.Hash() and the merkle-root comparison), a real
// coinbase, and matching Subtrees/SubtreeSlices lengths. The header's merkle root is
// deliberately unrelated to the body, so a block that gets past the duplicate check fails
// on the merkle root — which is what the second sub-test asserts.
func newQuickPathBlock(t *testing.T, slices []*subtreepkg.Subtree) *model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, len(slices))
	for i, st := range slices {
		subtreeHashes[i] = st.RootHash()
	}

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1,
			Bits:           *nBits,
			Nonce:          0,
		},
		CoinbaseTx:    coinbase,
		Subtrees:      subtreeHashes,
		SubtreeSlices: slices,
	}
}

// TestValidateSubtrees_RejectsDuplicateTransaction pins the CVE-2012-2459 duplicate check
// this PR adds to the quick-validation path (issue 1424). That path is default-on for every
// block at or below the highest checkpoint and previously ran no duplicate check at all,
// while the merkle root provably cannot detect the mutation.
//
// A package coverage profile showed the rejection branch had zero coverage: the existing
// quick-path tests execute the call but never drive a duplicate through it, so removing the
// check entirely left every test in the package passing. This test closes that gap — it
// fails if the check is removed or moved after CheckMerkleRoot.
//
// It calls validateSubtrees directly because that is the single seam all three quick-path
// pipelines funnel through, and it needs no BlockValidation dependencies: the function
// reads only the block's subtree slices.
func TestValidateSubtrees_RejectsDuplicateTransaction(t *testing.T) {
	ctx := context.Background()

	txA := chainhash.Hash{0x01}
	txB := chainhash.Hash{0x02}

	u := &BlockValidation{}

	t.Run("duplicate transaction is rejected", func(t *testing.T) {
		// txA appears in both subtrees — the duplicate a mutated body carries.
		block := newQuickPathBlock(t, buildQuickPathSubtrees(t, txA, txA))

		_, err := u.validateSubtrees(ctx, block, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.Contains(t, err.Error(), "CVE-2012-2459")

		// The error must reach the caller as a consensus rejection, not a transient
		// processing fault: the legacy unified route returns it straight out with no
		// fallback, and peer punishment keys on ErrBlockInvalid.
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a duplicate on the quick path must be BlockInvalid, so the peer is punished")
		require.False(t, errors.Is(err, errors.ErrProcessing),
			"it must not be wrapped as a processing error, which reads as transient")
	})

	t.Run("distinct transactions pass the duplicate check", func(t *testing.T) {
		// Same shape, no duplicate: this must get past the duplicate check and fail only
		// on the merkle root (the block header here carries an unrelated root), proving
		// the new check does not misfire on a legitimate body — in particular that the
		// coinbase placeholder in the first slot is not treated as a duplicate.
		block := newQuickPathBlock(t, buildQuickPathSubtrees(t, txA, txB))

		_, err := u.validateSubtrees(ctx, block, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "merkle root mismatch")
		require.NotContains(t, err.Error(), "duplicate transaction")
	})
}
