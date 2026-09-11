package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

// noopEmit discards completed subtrees. These tests are about what the builder
// accepts, not about what lands on disk.
func noopEmit(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
	return nil
}

// TestBlockStreamBuilder_RejectsADuplicateTransaction is the CVE-2012-2459 floor.
// The merkle root cannot catch this: the duplicate-last-when-odd rule means a
// mutated transaction list can produce the same root, the same header and the same
// block hash as the honest block. This scan is the only thing that catches it, and
// it is required below the checkpoint too, because a checkpoint anchors the block
// hash while the peer supplies the body.
func TestBlockStreamBuilder_RejectsADuplicateTransaction(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilderWithDedup(6, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	tx1, hash1 := streamTx(t, 1)
	require.NoError(t, b.AddTx(tx1, hash1))

	tx2, hash2 := streamTx(t, 2)
	require.NoError(t, b.AddTx(tx2, hash2))

	err = b.AddTx(tx1, hash1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate transaction",
		"the message must name the fault, or a later change could satisfy this test without enforcing the rule")
}

// TestBlockStreamBuilder_AcceptsDistinctTransactions is the control: the dedup
// must not reject an honest block.
func TestBlockStreamBuilder_AcceptsDistinctTransactions(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilderWithDedup(8, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	for i := 1; i < 8; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash), "transaction %d is distinct and must be accepted", i)
	}

	root, hashes, err := b.Finish()
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, hashes, 1)
}

// TestBlockStreamBuilder_WithoutAMapAcceptsDuplicates pins that the check is opt
// in, so a caller that has not supplied a map gets the previous behaviour rather
// than a silent nil dereference.
func TestBlockStreamBuilder_WithoutAMapAcceptsDuplicates(t *testing.T) {
	b, err := newBlockStreamBuilder(6, 8, coinbaseTx(t), noopEmit)
	require.NoError(t, err)

	tx1, hash1 := streamTx(t, 1)
	require.NoError(t, b.AddTx(tx1, hash1))
	require.NoError(t, b.AddTx(tx1, hash1), "with no map supplied the builder does not dedup")
}

// TestBlockStreamBuilder_TheCoinbaseIsNotDeduped pins the exemption. The coinbase
// occupies slot zero as a placeholder and is never offered to AddTx, so nothing
// should insert it; this guards against a future change that starts doing so and
// then trips over the placeholder on the next block.
func TestBlockStreamBuilder_TheCoinbaseIsNotDeduped(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilderWithDedup(4, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	for i := 1; i < 4; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	require.Equal(t, 3, seen.Length(), "only the three real transactions are inserted, never the coinbase")

	var placeholder chainhash.Hash
	for i := range placeholder {
		placeholder[i] = 0xff
	}

	require.False(t, seen.Exists(placeholder), "the coinbase placeholder must never be inserted")
}
