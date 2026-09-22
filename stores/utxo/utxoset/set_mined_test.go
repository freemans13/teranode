package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetMinedRecordsTheBlockAndStopsWaiting is the ordinary path: a transaction seen before
// its block is mined, so it gains a containment row and its unmined marker clears. Its identity
// row STAYS. Nothing moves between tables any more: containment has one home, and the identity
// row lives until the deep stamp of build step 5 writes the block onto the UTXOs and deletes it.
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

	require.True(t, identExists(t, s, ctx, tx), "the identity row stays until the stamp")
	require.Nil(t, markerOf(t, s, ctx, tx), "mined on the longest chain means no longer waiting")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	m, err := s.Get(ctx, h)
	require.NoError(t, err)
	require.Equal(t, []uint32{77}, m.BlockIDs)
	require.Equal(t, []uint32{700_005}, m.BlockHeights)
	require.Equal(t, []int{2}, m.SubtreeIdxs)
	require.Zero(t, m.UnminedSince)

	h2, b2 := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h2, "recording mined touches no UTXO; the stamp writes the pair later")
	require.Equal(t, int32(0), b2)
}

// TestSetMinedOnAReplayedBlockStillAnswers: the insert does nothing on conflict, and the answer
// is read back from the rows, so a replayed block reports every transaction in it with the
// block recorded exactly once.
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
// that cannot prove the postcondition to return an error rather than a partial map. A
// transaction with neither an identity row nor a containment row has no payload to copy, so
// the insert writes nothing for it and the read-back misses it.
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
//
// The un-mine is a point delete of the one containment row named, and the marker is written on
// the identity row, which was there all along.
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
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	require.NoError(t, s.SetBlockHeight(5_000))

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 5, BlockHeight: 100, UnsetMined: true,
	})
	require.NoError(t, err)

	r := readIdent(t, s, ctx, h[:])
	require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the unmined set")
	require.Equal(t, int32(5_000), *r.offChainSince,
		"the clock comes from the current tip, not from created_height, which is why the two are different concepts")
	require.Equal(t, 0, minedRows(t, s, ctx, tx), "and the block it was un-mined from is no longer recorded")
}

// TestUnsetMinedToleratesATransactionItDoesNotHold, which the interface states explicitly.
func TestUnsetMinedToleratesATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	gone := mkTx(t, 1, 1_000)

	_, err := s.SetMinedMulti(ctx, []*chainhash.Hash{gone.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err, "un-mining may no-op for a transaction that no longer exists")
}

// TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable: the retry path records a
// transaction the block path already created; the postcondition must be satisfied from
// tx_mined and the returned ids must include the recorded block.
func TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got[*tx.TxIDChainHash()])
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "same block recorded again appends nothing")
}

// TestSetMinedMultiAppendsASecondBlockAtTheSameHeight: a sibling block at the same height
// records the same transaction; containment holds both, and the answer lists them in
// (mined_height, block_id) order.
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

// TestLongestChainRecordCopiesThePayloadOntoTheContainmentRow: the containment row carries
// everything a lookup needs once the identity row is gone, copied from the identity row at the
// moment of recording, and Get answers with both the payload and the block while both exist.
func TestLongestChainRecordCopiesThePayloadOntoTheContainmentRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, tx))

	// The create path writes fee NULL on purpose, so give the row one. A fee lost in the copy
	// would only surface once block assembly rebuilt a candidate from an un-mined transaction
	// after the stamp had deleted its identity row.
	_, err = s.pool.Exec(ctx, `UPDATE tx_ident SET fee = 1234 WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx), "mined on the main chain: the identity row stays")
	require.Nil(t, markerOf(t, s, ctx, tx), "with its marker clear")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, []int{2}, got.SubtreeIdxs)
	require.Equal(t, uint64(uint32(tx.Size())), got.SizeInBytes)
	require.NotNil(t, got.TxInpoints.ParentTxHashes)
	require.Equal(t, uint64(1_234), got.Fee)

	// And the containment row alone answers the same, which is what the stamp will leave.
	dropIdentityRow(t, s, ctx, tx)

	got, err = s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, uint64(uint32(tx.Size())), got.SizeInBytes, "the payload travels with the row")
	require.NotNil(t, got.TxInpoints.ParentTxHashes)
	require.Equal(t, uint64(1_234), got.Fee, "and so does the fee block assembly would need back")
}

// TestForkRecordInsertsContainmentAndKeepsTheMarker: a block not on the longest chain records
// itself as containment exactly as a main-chain block does, and leaves the transaction in the
// unmined set. There is no second code path for a fork block.
func TestForkRecordInsertsContainmentAndKeepsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "containment is recorded whichever chain the block is on")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.NotZero(t, got.UnminedSince, "still in the unmined set")
}

// TestLongestChainRecordAfterAForkRecordClearsTheMarker: two blocks contain it, one of them
// on the longest chain. Both are recorded, the marker clears, and nobody has to decide which
// block is right; the callers filter against the chain.
func TestLongestChainRecordAfterAForkRecordClearsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got.BlockIDs)
	require.Zero(t, got.UnminedSince)
}

// TestForkRecordTwiceRecordsTheBlockOnce: the insert does nothing on conflict, so a replayed
// fork block leaves one row and one entry in the answer.
func TestForkRecordTwiceRecordsTheBlockOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	info := utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3}

	first, err := s.SetMinedMulti(ctx, hashes(tx), info)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, first[*tx.TxIDChainHash()])

	second, err := s.SetMinedMulti(ctx, hashes(tx), info)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, second[*tx.TxIDChainHash()], "a replayed block is recorded once")

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs, "one row, not two")
	require.Equal(t, []uint32{700_100}, got.BlockHeights)
	require.Equal(t, []int{3}, got.SubtreeIdxs)
}

// TestUnMineDeletesTheOneRowAndSetsTheMarker: the block is taken back; its containment row
// goes, the identity row that was there all along gets the unmined marker at the CURRENT tip,
// and the UTXOs, which were at the sentinel from birth, are still there.
func TestUnMineDeletesTheOneRowAndSetsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, got.BlockIDs)
	require.Equal(t, uint32(700_150), got.UnminedSince, "a fresh clock from the current tip, not the creation height")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h)
	require.Equal(t, int32(0), b)
}

// TestUnMineIsAPointDeleteOnTheFullKey: un-mining ONE of the two blocks that contain a
// transaction removes that block's row and nothing else. The sibling block's row survives,
// because "block 43 contains this transaction" is still true and a chain switch cannot make it
// false.
//
// This reverses the earlier rule that an un-mine deleted every containment row so that the
// transaction lived in exactly one table. Containment has one home now and the identity row
// stays put through mining, so there is no second home to keep clear.
//
// The transaction was created by the block path, so its UTXOs carry block 42's pair from
// birth, and block 42 is the block being un-mined. Through build steps 2 to 4 the un-mine
// still runs the UTXO reset, NARROWED to UTXOs naming the un-mined block, so they go back to
// the sentinel: below the checkpoint every UTXO is born from a block, and leaving a stale pair
// on them with nothing to correct it would be worse than today. The design's test list says
// "no UTXO touched" for this test; its build order for step 2 says the narrowed reset stays,
// and the build order is what this step builds. A UTXO naming a SIBLING block would be left
// alone, which TestUnMineDoesNotResetAnotherTransactionsUTXO's colliding-row variant and the
// $5 narrowing in resetUTXOsSQL cover.
func TestUnMineIsAPointDeleteOnTheFullKey(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "an un-mine recreates no identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "the sibling's row survives the un-mine")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{43}, got.BlockIDs, "the un-mined block's row is gone, the sibling's stands")
	require.Zero(t, got.UnminedSince, "there is no identity row to carry a marker")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "the narrowed reset reaches a UTXO naming the un-mined block")
	require.Equal(t, int32(0), b)
}

// TestUnMineLeavesAUTXONamingASiblingBlockAlone is the other half of the narrowing: the reset
// is confined to UTXOs that name the un-mined block, so a UTXO born from the sibling keeps its
// pair.
func TestUnMineLeavesAUTXONamingASiblingBlockAlone(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(700_100), h, "the UTXO names block 43, which was not un-mined")
	require.Equal(t, int32(43), b)
}

// TestUnMineDoesNotResetAnotherTransactionsUTXO: the UTXO reset must recheck the full
// transaction id, not just the packed key it found the row by.
//
// ukey is a 96-bit prefix and non-unique by design, so two transactions in the same leaf can
// share one. Matching an UPDATE on (leaf, ukey) alone would reset a stranger's UTXO to the
// unconfirmed sentinel -- a UTXO that is spendable now reading as immature, or a mined UTXO
// reading as unmined -- which is why every other by-key write in this store rechecks txid.
func TestUnMineDoesNotResetAnotherTransactionsUTXO(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	// The stranger names the SAME block, so only the txid recheck can keep it out of the reset.
	other := insertCollidingUTXO(t, s, ctx, tx, 700_100, 42)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "the un-mined transaction's own UTXO is reset")
	require.Equal(t, int32(0), b)

	oh, ob := utxoFactsOf(t, s, ctx, other)
	require.Equal(t, int32(700_100), oh, "a UTXO sharing the packed key must be untouched")
	require.Equal(t, int32(42), ob)
}

// TestUnMineOfABlockTheTransactionDoesNotNameIsANoOp. An un-mine names a block, and a
// transaction with no containment row for THAT block was never mined into it, so there is
// nothing to take back. The interface tolerates the absence; it must not turn it into an
// un-settling of the block the transaction actually is in.
func TestUnMineOfABlockTheTransactionDoesNotNameIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	require.Equal(t, 1, minedRows(t, s, ctx, tx), "block 42's containment row stays")
	require.False(t, identExists(t, s, ctx, tx), "and no identity row appears")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(700_100), h, "its UTXO keeps block 42's pair")
	require.Equal(t, int32(42), b)
}
