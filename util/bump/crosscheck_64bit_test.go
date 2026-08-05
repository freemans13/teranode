package bump_test

import (
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/require"
)

// TestConvertToBUMP64BitCrosscheck verifies the 64-bit offset path against the
// go-bc reference implementation, independently of Teranode's own arithmetic:
// with 32 subtree levels the transaction's global leaf offset exceeds 2^32
// (the range the old uint32 computation silently wrapped, issue 1427). go-bc
// must parse the encoded BUMP and fold the proof to the same merkle root a
// manual sibling-by-sibling fold produces.
func TestConvertToBUMP64BitCrosscheck(t *testing.T) {
	const (
		subtreeLevels = 32
		blockLevels   = 2
		subtreeIndex  = 3
		txIndex       = 5
	)

	mkHash := func(b byte) chainhash.Hash {
		var h chainhash.Hash
		h[0] = b
		h[31] = 0x3c

		return h
	}

	txID := mkHash(0xee)

	subtreeProof := make([]chainhash.Hash, subtreeLevels)
	for i := range subtreeProof {
		subtreeProof[i] = mkHash(byte(i + 1))
	}

	blockProof := make([]chainhash.Hash, blockLevels)
	for i := range blockProof {
		blockProof[i] = mkHash(byte(100 + i))
	}

	proof := &merkleproof.MerkleProof{
		TxID:             txID,
		BlockHeight:      800000,
		SubtreeIndex:     subtreeIndex,
		TxIndexInSubtree: txIndex,
		SubtreeProof:     subtreeProof,
		BlockProof:       blockProof,
	}

	converted, err := bump.ConvertToBUMP(proof)
	require.NoError(t, err)

	binary, err := converted.EncodeBinary()
	require.NoError(t, err)

	// Manual fold: start at the leaf, combine with each sibling by offset
	// parity, exactly as any BRC-74 verifier must.
	offset := uint64(subtreeIndex)<<subtreeLevels | uint64(txIndex)
	working := txID

	fold := func(a, b chainhash.Hash) chainhash.Hash {
		combined := make([]byte, 64)
		copy(combined[:32], a[:])
		copy(combined[32:], b[:])

		return chainhash.DoubleHashH(combined)
	}

	for _, sibling := range append(append([]chainhash.Hash{}, subtreeProof...), blockProof...) {
		if offset%2 == 0 {
			working = fold(working, sibling)
		} else {
			working = fold(sibling, working)
		}

		offset >>= 1
	}

	expectedRoot := working.String()

	// go-bc reference implementation: parse the wire bytes and compute the
	// root from the txid.
	parsed, err := bc.NewBUMPFromBytes(binary)
	require.NoError(t, err)

	gotRoot, err := parsed.CalculateRootGivenTxid(txID.String())
	require.NoError(t, err)
	require.Equal(t, expectedRoot, gotRoot, "go-bc must reconstruct the same root through the >2^32 offsets")
}
