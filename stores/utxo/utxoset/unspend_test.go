package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
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

// TestUnspendOfAnAlreadyRestoredCoinIsANoOp pins the fix for the idempotent-replay gap: a
// second Unspend on an outpoint the first call already restored must succeed without
// creating a duplicate coin, even when the replayed request names a different spender than
// whatever actually did the restoring. This is exactly the shape BlockAssembler's
// conflict-intent WAL replay can produce -- a crash between a successful Unspend and its
// intent's completion record means replay calls Unspend again, and it may not remember (or
// may misremember) which spending transaction the original call used. Ownership only gates
// consuming the journal row; once the coin is live, the coin being unspent is the fact that
// matters, not who put it there.
func TestUnspendOfAnAlreadyRestoredCoinIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	realSpends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, realSpends))
	require.Equal(t, 1, coinCount(t, s, ctx, parent), "the coin must be restored exactly once")

	// A replayed Unspend naming a spender that never actually spent this outpoint.
	fakeSpender := chainhash.HashH([]byte("not-the-real-spender"))
	fakeSpends := []*utxo.Spend{{
		TxID:         parent.TxIDChainHash(),
		Vout:         0,
		SpendingData: spend.NewSpendingData(&fakeSpender, 0),
	}}

	require.NoError(t, s.Unspend(ctx, fakeSpends),
		"re-unspending an already-restored coin must be a no-op even under a different claimed spender")
	require.Equal(t, 1, coinCount(t, s, ctx, parent), "a replayed restore must not create a second coin")
}
