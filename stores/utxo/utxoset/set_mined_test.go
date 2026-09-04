package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetMinedRecordsTheBlockAndStopsWaiting is the ordinary path: a transaction that was in
// the mempool is mined, so it gains block membership and leaves the mempool set -- which it
// now does by leaving the mempool TABLE, because a block on the longest chain naming a row
// that claims no other block settles it, and a settled transaction lives in tx_mined.
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

	require.False(t, identExists(t, s, ctx, tx), "mined on the longest chain means no longer waiting")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	m, err := s.Get(ctx, h)
	require.NoError(t, err)
	require.Equal(t, []uint32{77}, m.BlockIDs)
	require.Equal(t, []uint32{700_005}, m.BlockHeights)
	require.Equal(t, []int{2}, m.SubtreeIdxs)
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

	require.Equal(t, 1, minedRows(t, s, ctx, tx),
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
// The mined state is now reached by a stamp on a mempool arrival rather than by a create
// carrying block information. That create takes the block path, which writes no identity row,
// and un-mining is an identity-row operation: it puts a transaction BACK in the mempool set,
// which is only meaningful for one that was in it. At the tip that is the only shape a reorg
// ever sees, because everything but the coinbase arrives from the mempool first.
//
// The stamp here is a FORK stamp, and that is not a detail. A longest-chain stamp on a row
// claiming no other block settles it and moves it out of the identity table altogether, and
// bringing such a row back from tx_mined is its own step (the reverse move). The row this test
// needs is one that still holds an identity row and still claims a block, which is what a fork
// stamp leaves behind. The marker-clearing half of the stamp is pinned by
// TestLongestChainStampOnAMultiBlockRowClearsTheMarkerAndStays.
func TestUnsetMinedGivesTheTransactionAFreshClock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 5, BlockHeight: 100, SubtreeIdx: 0,
	})
	require.NoError(t, err)

	require.Equal(t, packTriples(t, [3]uint32{5, 100, 0}), readIdent(t, s, ctx, h[:]).membership)

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

// TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable: the retry path stamps a
// transaction the block path already created; the postcondition must be satisfied from
// tx_mined and the returned ids must include the stamped block.
func TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got[*tx.TxIDChainHash()])
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "same block stamped again appends nothing")
}

// TestSetMinedMultiAppendsASecondBlockAtTheSameHeight: a sibling block at the same height
// stamps the same transaction; membership records both, in order.
func TestSetMinedMultiAppendsASecondBlockAtTheSameHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100})
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got[*tx.TxIDChainHash()])
	require.Equal(t, 2, minedRows(t, s, ctx, tx))
}

// TestSetMinedMultiStillFailsForAnUnknownTransaction keeps the postcondition honest.
func TestSetMinedMultiStillFailsForAnUnknownTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true})
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestLongestChainStampMovesAMempoolRowIntoMembership: after the stamp the transaction has no
// identity row, one membership row, and Get still answers with its block.
func TestLongestChainStampMovesAMempoolRowIntoMembership(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, tx))

	// The create path writes fee NULL on purpose, so give the row one. A fee lost in the move
	// would only surface once block assembly rebuilt a candidate from an un-mined transaction.
	_, err = s.pool.Exec(ctx, `UPDATE tx_ident SET fee = 1234 WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "mined on the main chain: the mempool row is gone")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, []int{2}, got.SubtreeIdxs)
	require.Equal(t, uint64(uint32(tx.Size())), got.SizeInBytes, "the mempool payload travels with the row")
	require.NotNil(t, got.TxInpoints.ParentTxHashes)
	require.Equal(t, uint64(1_234), got.Fee, "and so does the fee block assembly would need back")
}

// TestForkStampAppendsAndMovesNothing: a block not on the longest chain records itself on the
// identity row and leaves the transaction in the mempool set.
func TestForkStampAppendsAndMovesNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.NotZero(t, got.UnminedSince, "still in the mempool set")
}

// TestLongestChainStampOnAMultiBlockRowClearsTheMarkerAndStays: two blocks name it, so no
// single block is "its" block; the marker clears, the row stays for a later un-mine or stamp
// to disambiguate.
func TestLongestChainStampOnAMultiBlockRowClearsTheMarkerAndStays(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got.BlockIDs)
	require.Zero(t, got.UnminedSince)
}
