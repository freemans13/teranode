package bump

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/require"
)

// proofWithLevels builds an internally consistent proof with the given segment
// sizes and indices. Sibling hashes are arbitrary but distinct.
func proofWithLevels(t *testing.T, subtreeLevels, blockLevels, subtreeIndex, txIndex int) *merkleproof.MerkleProof {
	t.Helper()

	mkHash := func(b byte) chainhash.Hash {
		var h chainhash.Hash
		h[0] = b
		h[31] = 0x7f

		return h
	}

	subtreeProof := make([]chainhash.Hash, subtreeLevels)
	for i := range subtreeProof {
		subtreeProof[i] = mkHash(byte(i + 1))
	}

	blockProof := make([]chainhash.Hash, blockLevels)
	for i := range blockProof {
		blockProof[i] = mkHash(byte(100 + i))
	}

	return &merkleproof.MerkleProof{
		TxID:             mkHash(0xee),
		BlockHeight:      800000,
		SubtreeIndex:     subtreeIndex,
		TxIndexInSubtree: txIndex,
		SubtreeProof:     subtreeProof,
		BlockProof:       blockProof,
	}
}

// TestConvertToBUMPLargeBlockOffsets pins the 64-bit offset arithmetic (issue
// 1427): with 32 subtree levels, the subtree index lands entirely above bit 32
// of the global leaf offset. The previous uint32 computation shifted it to
// zero, silently emitting a proof whose offsets pointed at a different
// transaction; the offsets must come out exact in 64 bits.
func TestConvertToBUMPLargeBlockOffsets(t *testing.T) {
	const (
		subtreeLevels = 32
		blockLevels   = 2
		subtreeIndex  = 3
		txIndex       = 5
	)

	proof := proofWithLevels(t, subtreeLevels, blockLevels, subtreeIndex, txIndex)

	bump, err := ConvertToBUMP(proof)
	require.NoError(t, err)
	require.Len(t, bump.Path, subtreeLevels+blockLevels)

	wantGlobal := uint64(subtreeIndex)<<subtreeLevels | uint64(txIndex) // 12884901893, wraps to 5 in uint32

	// Level 0 holds the sibling and (odd offset) the appended txid node.
	level0 := bump.Path[0]
	require.Len(t, level0, 2)
	require.Equal(t, wantGlobal^1, level0[0].Offset)
	require.Equal(t, wantGlobal, level0[1].Offset)
	require.True(t, level0[1].TxID)

	// Every level's sibling offset derives from the full 64-bit global offset.
	for levelIdx := 1; levelIdx < subtreeLevels+blockLevels; levelIdx++ {
		require.Len(t, bump.Path[levelIdx], 1)
		require.Equal(t, (wantGlobal>>uint(levelIdx))^1, bump.Path[levelIdx][0].Offset, "level %d", levelIdx)
	}

	// The binary encoding must carry the large offset as a full 8-byte VarInt.
	bin, err := bump.EncodeBinary()
	require.NoError(t, err)

	var wantVarInt [9]byte
	wantVarInt[0] = 0xFF
	for i := 0; i < 8; i++ {
		wantVarInt[1+i] = byte(wantGlobal >> (8 * i))
	}

	require.True(t, bytes.Contains(bin, wantVarInt[:]), "encoded BUMP must contain the 64-bit VarInt of the txid offset")
}

// TestConvertToBUMPSubtreeProofSentinel pins the subtree-proof path: a proof of
// a subtree root (not of a transaction) carries TxIndexInSubtree == -1 with an
// empty SubtreeProof, and the leaf being proven is the subtree root itself at
// global offset == SubtreeIndex. The old 32-bit code folded the -1 sentinel to
// 0xFFFFFFFF, so every subtree-proof BUMP served by the asset server's fallback
// carried garbage offsets.
func TestConvertToBUMPSubtreeProofSentinel(t *testing.T) {
	proof := proofWithLevels(t, 0, 2, 3, -1)
	proof.TxID = chainhash.Hash{} // subtree proofs carry no transaction id

	bump, err := ConvertToBUMP(proof)
	require.NoError(t, err)
	require.Len(t, bump.Path, 2)

	// Leaf (the subtree root) sits at global offset 3; its sibling is 2.
	require.Len(t, bump.Path[0], 1)
	require.Equal(t, uint64(2), bump.Path[0][0].Offset)

	// Next level: (3>>1)^1 = 0.
	require.Len(t, bump.Path[1], 1)
	require.Equal(t, uint64(0), bump.Path[1][0].Offset)
}

// TestConvertToBUMPRejectsInconsistentIndices pins the guards that replace the
// old silent wrap: indices that cannot be represented in their segment of the
// flat leaf offset are rejected loudly.
func TestConvertToBUMPRejectsInconsistentIndices(t *testing.T) {
	t.Run("more than 64 levels", func(t *testing.T) {
		proof := proofWithLevels(t, 33, 32, 0, 0)

		_, err := ConvertToBUMP(proof)
		require.Error(t, err)
		require.Contains(t, err.Error(), "65 levels")
	})

	t.Run("negative subtree index", func(t *testing.T) {
		proof := proofWithLevels(t, 2, 1, -1, 0)

		_, err := ConvertToBUMP(proof)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative proof position")
	})

	t.Run("negative tx index", func(t *testing.T) {
		proof := proofWithLevels(t, 2, 1, 0, -3)

		_, err := ConvertToBUMP(proof)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative proof position")
	})

	t.Run("tx index beyond its subtree", func(t *testing.T) {
		// 2 subtree levels = 4 leaves; index 5 would smear a bit into the
		// subtree-index segment (the old code silently accepted this).
		proof := proofWithLevels(t, 2, 3, 0, 5)

		_, err := ConvertToBUMP(proof)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not fit in a subtree")
	})

	t.Run("subtree index beyond the block", func(t *testing.T) {
		// 1 block level = 2 subtrees; index 2 does not exist in this block.
		proof := proofWithLevels(t, 2, 1, 2, 0)

		_, err := ConvertToBUMP(proof)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not fit in a block")
	})
}
