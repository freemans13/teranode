package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestUnspendRestoresACoinWithItsParentsBlockFactsFromMembership: the journal carries no
// block facts (they are mutable, and the journal payload is not); the restore reads them
// from tx_mined, where the parent's row is present for as long as any of its spends can be
// undone.
func TestUnspendRestoresACoinWithItsParentsBlockFactsFromMembership(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	h, b := coinFacts(t, s, ctx, parent)
	require.Equal(t, int32(700_100), h)
	require.Equal(t, int32(42), b)
}
