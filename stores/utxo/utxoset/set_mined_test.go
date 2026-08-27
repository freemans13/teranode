package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSetMinedRecordsTheBlockAndStopsWaiting is the ordinary path: a transaction that was in
// the mempool is mined, so it gains block membership and leaves the mempool set.
func TestSetMinedRecordsTheBlockAndStopsWaiting(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true,
	})
	require.NoError(t, err)

	require.Contains(t, got, *h, "every hash asked about must appear in the answer")
	require.Contains(t, got[*h], uint32(77), "and every answer must contain the block just recorded")

	r := readIdent(t, s, ctx, h[:])
	require.Nil(t, r.offChainSince, "mined on the longest chain means no longer waiting")
	require.Equal(t, packTriples(t, [3]uint32{77, 700_005, 2}), r.membership)
}

// TestSetMinedOnAReplayedBlockStillAnswers is the trap, and it is the reason this is two
// statements rather than one.
//
// The tempting shape is a single UPDATE that skips rows already carrying this block, with
// RETURNING to report what it touched. That returns nothing for a transaction that is
// already correctly mined, which is indistinguishable from the row not existing. The
// interface says every hash MUST appear in the answer, so the fused form turns every
// replayed block into a not-found error for every transaction in it.
func TestSetMinedOnAReplayedBlockStillAnswers(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err, "a replayed block must not report its transactions missing")
	require.Contains(t, got, *h)
	require.Contains(t, got[*h], uint32(77))

	r := readIdent(t, s, ctx, h[:])
	require.Equal(t, packTriples(t, [3]uint32{77, 700_005, 2}), r.membership,
		"and the same block must not be recorded twice")
}

// TestSetMinedReportsATransactionItDoesNotHold. The interface requires an implementation
// that cannot prove the postcondition to return an error rather than a partial map.
func TestSetMinedReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, known, 700_000)
	require.NoError(t, err)

	missing := mkTx(t, 1, 9_999)

	_, err = s.SetMinedMulti(ctx,
		[]*chainhash.Hash{known.TxIDChainHash(), missing.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, OnLongestChain: true})

	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"a hash the store does not hold must fail loudly, not come back as a silent gap in the map: got %v", err)
}

// TestUnsetMinedGivesTheTransactionAFreshClock covers the reorg path, and pins the fact that
// settled the merge question: a resurrected transaction gets a clock taken from the CURRENT
// tip, not its creation height. That is why the marker cannot be derived from created_height.
func TestUnsetMinedGivesTheTransactionAFreshClock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true}))
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	require.Nil(t, readIdent(t, s, ctx, h[:]).offChainSince)

	require.NoError(t, s.SetBlockHeight(5_000))

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 5, BlockHeight: 100, UnsetMined: true,
	})
	require.NoError(t, err)

	r := readIdent(t, s, ctx, h[:])
	require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the mempool set")
	require.Equal(t, int32(5_000), *r.offChainSince,
		"the clock comes from the current tip, not from created_height, which is why the two are different concepts")
	require.Empty(t, r.membership, "and the block it was un-mined from is no longer claimed")
}

// TestUnsetMinedToleratesATransactionItDoesNotHold, which the interface states explicitly.
func TestUnsetMinedToleratesATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	gone := mkTx(t, 1, 1_000)

	_, err := s.SetMinedMulti(ctx, []*chainhash.Hash{gone.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err, "un-mining may no-op for a transaction that no longer exists")
}
