package utxoset

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// hashBytes is the transaction id as the store stores it.
func hashBytes(tx *bt.Tx) []byte {
	h := tx.TxIDChainHash()

	return h[:]
}

// hashes wraps one transaction as the slice SetMinedMulti takes.
func hashes(tx *bt.Tx) []*chainhash.Hash {
	return []*chainhash.Hash{tx.TxIDChainHash()}
}

// spendPair creates a parent, creates a child that spends its only output, and spends it, so
// the parent has no live coin left and the spend journal names it.
func spendPair(t *testing.T, s *Store, ctx context.Context, h uint32) (parent, child *bt.Tx) {
	t.Helper()

	parent = mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, h)
	require.NoError(t, err)

	child = bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, h)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, child, h)
	require.NoError(t, err)

	return parent, child
}

func identExists(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_ident WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n > 0
}

func bodyExists(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_body WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n > 0
}

// TestSettledNeedsBothAMainChainBlockAndDepth pins the rule the reclaimer decides on.
//
// A transaction is SETTLED when nothing is missing from its record and its deepest block is
// far enough back that the node could not un-mine it even if asked. 288 blocks is that
// depth, and it is not a number this store chose: it is the point at which the subtree files
// a reorg would need are deleted, after which the un-mine path warns and skips.
func TestSettledNeedsBothAMainChainBlockAndDepth(t *testing.T) {
	s, ctx := newTestStore(t)

	deep := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, deep, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	recent := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, recent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 1_400, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := mkTx(t, 1, 3_000)
	_, err = s.Create(ctx, waiting, 1_000)
	require.NoError(t, err)

	// Tip 1,500: the first is 500 deep, the second only 100.
	got, err := s.settled(ctx, [][]byte{
		hashBytes(deep), hashBytes(recent), hashBytes(waiting),
	}, 1_500)
	require.NoError(t, err)

	require.Contains(t, got, string(hashBytes(deep)), "500 blocks deep, past the point the node could un-mine it")
	require.NotContains(t, got, string(hashBytes(recent)), "only 100 deep, a reorg could still take it back")
	require.NotContains(t, got, string(hashBytes(waiting)), "no main-chain block at all")
}

// TestSettledTakesTheDeepestBlockNotTheFirst.
//
// A transaction can name several blocks: one that lost a race, and the one that actually
// mined it. Taking the most convenient block would call it settled while the real one is
// still shallow enough to be undone. Taking the deepest is safe in the only direction that
// matters, because it can delay reclaim but never rush it.
func TestSettledTakesTheDeepestBlockNotTheFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 1_450, OnLongestChain: true},
	))
	require.NoError(t, err)

	got, err := s.settled(ctx, [][]byte{hashBytes(tx)}, 1_500)
	require.NoError(t, err)
	require.NotContains(t, got, string(hashBytes(tx)),
		"the shallow block at 1,450 decides it, not the deep one at 1,000")
}

// TestReclaimRemovesAParentWhoseCoinsAreGoneAndSpenderIsSettled is the whole point of the
// reclaimer: the identity table cannot be reclaimed by dropping a window, so rows have to be
// deleted individually when they are genuinely finished.
func TestReclaimRemovesAParentWhoseCoinsAreGoneAndSpenderIsSettled(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent, child := spendPair(t, s, ctx, 100)

	// Both are mined. A spender cannot be mined before its parent, so a buried spender
	// implies a buried parent; what the parent needs separately is only to be ON the main
	// chain rather than waiting.
	_, err := s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{
		BlockID: 8, BlockHeight: 110, OnLongestChain: true})
	require.NoError(t, err)

	// The child is mined and buried well beyond the depth at which it could be un-mined.
	_, err = s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{
		BlockID: 9, BlockHeight: 120, OnLongestChain: true})
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, parent), "fully spent, spender long settled, nothing can need it")
	require.True(t, identExists(t, s, ctx, child), "the child still holds a live coin")
}

// TestReclaimKeepsAParentWhoseSpenderIsStillWaiting. If the spending transaction is still in
// the mempool, a reorg or a conflict could still need the parent's coins restored.
func TestReclaimKeepsAParentWhoseSpenderIsStillWaiting(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent, _ := spendPair(t, s, ctx, 100)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent),
		"the spender has never been mined, so the parent's coins may still have to come back")
}

// TestReclaimKeepsAParentWhoseSpenderWasMinedTooRecently. Mined is not enough. Until the
// spender is deep enough that the node could not un-mine it, the parent is still needed.
func TestReclaimKeepsAParentWhoseSpenderWasMinedTooRecently(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent, child := spendPair(t, s, ctx, 100)

	_, err := s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{
		BlockID: 9, BlockHeight: 950, OnLongestChain: true})
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	// Tip 1,000, so the spender is only 50 blocks deep.
	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent), "50 blocks deep is still reversible")
}

// TestReclaimKeepsAParentThatStillHasALiveCoin. Even with every spend settled, a parent with
// an unspent output is needed by whoever eventually spends it.
func TestReclaimKeepsAParentThatStillHasALiveCoin(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 100)
	require.NoError(t, err)
	_, err = spendOnly(ctx, s, child, 100)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{
		BlockID: 9, BlockHeight: 120, OnLongestChain: true})
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)
	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent), "output 1 is still unspent and someone will want its parent")
}

// TestBodyWindowsBelowRetentionAreDropped. The transaction bytes are the one part with a
// horizon rather than a dependency, so their windows go wholesale.
func TestBodyWindowsBelowRetentionAreDropped(t *testing.T) {
	s, ctx := newTestStore(t)

	old := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, old, 100)
	require.NoError(t, err)

	recent := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, recent, 900)
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	// Tip 1,000, so anything filed below 712 is past the 288-block horizon.
	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.False(t, bodyExists(t, s, ctx, old), "filed at 100, far past the horizon")
	require.True(t, bodyExists(t, s, ctx, recent), "filed at 900, still inside it")

	require.True(t, identExists(t, s, ctx, old),
		"and the identity row survives its body: it is still needed while its coin is unspent")
}

// TestSettledRefusesBothSpellingsOfNoBlock pins the one divergence that dropping mh_max from the
// settled predicate could have introduced, and it would not have been caught by any test above.
//
// mh_max returned NULL for BOTH spellings of "this transaction names no block": a NULL
// membership, and the zero-length value unstampSQL leaves behind when overlay removes the last
// triple. Both were therefore refused. The NOT EXISTS that replaced it is TRUE over an empty
// membership, because there is no triple to disqualify it, so guarding on "membership IS NOT
// NULL" instead of on its length would have flipped that residue from refused to settled and
// deleted the identity row of a transaction no block contains.
func TestSettledRefusesBothSpellingsOfNoBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	// A positive control, so a query that returns nothing at all cannot pass this test.
	deep := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, deep, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	nullMembership := mkTx(t, 1, 4_000)
	_, err = s.Create(ctx, nullMembership, 1_000)
	require.NoError(t, err)

	emptyMembership := mkTx(t, 1, 5_000)
	_, err = s.Create(ctx, emptyMembership, 1_000)
	require.NoError(t, err)

	// Clear the off-chain marker on both, so the membership test is what decides them rather
	// than the condition in front of it.
	_, err = s.pool.Exec(ctx,
		`UPDATE tx_ident SET membership = NULL, off_chain_since = NULL WHERE txid = $1`,
		hashBytes(nullMembership))
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx,
		`UPDATE tx_ident SET membership = ''::bytea, off_chain_since = NULL WHERE txid = $1`,
		hashBytes(emptyMembership))
	require.NoError(t, err)

	got, err := s.settled(ctx, [][]byte{
		hashBytes(deep), hashBytes(nullMembership), hashBytes(emptyMembership),
	}, 10_000)
	require.NoError(t, err)

	require.Contains(t, got, string(hashBytes(deep)),
		"the control must settle, or this test would pass on a query that returns nothing")
	require.NotContains(t, got, string(hashBytes(nullMembership)),
		"a NULL membership names no block and can never be settled")
	require.NotContains(t, got, string(hashBytes(emptyMembership)),
		"an empty membership is the residue unstamp leaves, and it names no block either")
}

// TestSettledStillTakesTheDeepestBlockWithoutMhMax repeats the deepest-block rule against the
// rewritten predicate, because the rewrite asks the question inside out.
//
// mh_max reduced every triple to a maximum and compared once. The replacement asks whether any
// triple is above the cutoff, which is the same answer only if it examines all of them. A
// version that stopped at the first triple, or that read the height at the wrong offset, would
// still pass the single-block tests and settle a transaction whose shallow block is listed
// second.
func TestSettledStillTakesTheDeepestBlockWithoutMhMax(t *testing.T) {
	s, ctx := newTestStore(t)

	// Deep block first, shallow block second: the shallow one must decide it.
	shallowSecond := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, shallowSecond, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 1_450, OnLongestChain: true}))
	require.NoError(t, err)

	// And the same pair the other way round, so neither position is privileged.
	shallowFirst := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, shallowFirst, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 1_450, OnLongestChain: true},
		utxo.MinedBlockInfo{BlockID: 4, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.settled(ctx, [][]byte{hashBytes(shallowSecond), hashBytes(shallowFirst)}, 1_500)
	require.NoError(t, err)

	require.NotContains(t, got, string(hashBytes(shallowSecond)),
		"the block at 1,450 is only 50 deep and it is listed second")
	require.NotContains(t, got, string(hashBytes(shallowFirst)),
		"and the same pair listed the other way round must decide the same")
}

// TestSettledDoesNotWrapOnAHighHeight guards the cast the rewritten predicate inherited.
//
// In PostgreSQL 255::int << 24 wraps to a negative number, silently. An int4 version of the
// shift would read a high block height as negative, which is below every cutoff, so every
// transaction in the store would settle at once.
func TestSettledDoesNotWrapOnAHighHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0xFF00_0001, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.settled(ctx, [][]byte{hashBytes(tx)}, 1_500)
	require.NoError(t, err)

	require.NotContains(t, got, string(hashBytes(tx)),
		"a height above the signed 32-bit boundary must read as huge, not as negative")
}

// TestReclaimBatchKeepsEveryPairAndTracksTheMark pins what a batch records per parent.
//
// The work list arrives as one row per parent-spender pair. The per-parent spender list must
// stay whole, because it decides whether ALL of a parent's spends settled; deduplication of the
// spenders actually probed happens at judgement time, and only for parents that still need a
// probe. The applied mark is an AND across a parent's pairs: one unmarked spend means the
// parent takes the full path.
func TestReclaimBatchKeepsEveryPairAndTracksTheMark(t *testing.T) {
	b := newReclaimBatch()

	parentOne := []byte("parent-one")
	parentTwo := []byte("parent-two")
	spender := []byte("one-transaction-took-both")
	later := []byte("a-mempool-spender")

	b.add(parentOne, spender, true)
	b.add(parentTwo, spender, true)
	b.add(parentTwo, later, false)

	require.Len(t, b.parents, 2, "both parents have to be judged")
	require.Equal(t, [][]byte{spender}, b.spentBy[string(parentOne)])
	require.Equal(t, [][]byte{spender, later}, b.spentBy[string(parentTwo)],
		"the per-parent list decides whether all of a parent's spends settled and must stay whole")

	require.True(t, b.allApplied[string(parentOne)], "every spend of parent one was block-applied")
	require.False(t, b.allApplied[string(parentTwo)], "one unmarked spend puts parent two on the full path")
}

// TestReclaimBatchResetForgetsEverything is not defensive tidiness.
//
// Chunks cut on a parent boundary and the batch is reused across them. Anything reset kept
// would either be judged twice or, worse, carry a stale mark verdict from an earlier chunk
// into a parent that shares a key with nothing in this one.
func TestReclaimBatchResetForgetsEverything(t *testing.T) {
	b := newReclaimBatch()

	parent := []byte("a-parent")
	spender := []byte("a-spender")

	b.add(parent, spender, false)
	b.reset()

	require.Empty(t, b.parents)
	require.Empty(t, b.spentBy)
	require.Empty(t, b.allApplied)

	b.add(parent, spender, true)
	require.True(t, b.allApplied[string(parent)], "a fresh chunk starts from its own pairs, not the last chunk's verdict")
}

// TestReclaimDoesNotRefuseAParentWhoseSpenderSortsIntoAnEarlierChunk pins the leak that
// left about 83 percent of the mainnet identity table unreachable.
//
// A pays B and B pays C, all inside one journal partition, all settled. The work list is
// ordered by transaction id, which is a hash, so half the time B sorts before A. If the
// reclaimer deletes B's identity row as soon as B's chunk is judged, A's chunk then asks
// whether A's spender B is settled, finds no row, and refuses A. Nothing ever names A again,
// because this partition held A's last spend and it is dropped at the end of the session.
//
// The test forces the losing order by picking satoshi values until B's id sorts before A's,
// and a chunk size of one parent so the two are judged in separate chunks. Both A and B must
// go; C still holds a live coin and stays.
func TestReclaimDoesNotRefuseAParentWhoseSpenderSortsIntoAnEarlierChunk(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96
	s.reclaimChunkParents = 1

	const at = uint32(100)

	// Build A and B in memory until B's txid sorts before A's. mkTx is deterministic in its
	// satoshi argument, so varying it varies the id.
	var a, b *bt.Tx

	for sats := uint64(5_000); ; sats++ {
		a = mkTx(t, 1, sats)

		b = bt.NewTx()
		require.NoError(t, b.FromUTXOs(&bt.UTXO{
			TxIDHash: a.TxIDChainHash(), Vout: 0,
			LockingScript: a.Outputs[0].LockingScript, Satoshis: a.Outputs[0].Satoshis,
		}))
		b.AddOutput(&bt.Output{Satoshis: sats - 1_000, LockingScript: a.Outputs[0].LockingScript})

		if bytes.Compare(hashBytes(b), hashBytes(a)) < 0 {
			break
		}

		require.Less(t, sats, uint64(5_100), "could not find a chain where the spender sorts first")
	}

	_, err := s.Create(ctx, a, at)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(a), utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: at, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.Create(ctx, b, at)
	require.NoError(t, err)
	_, err = spendOnly(ctx, s, b, at)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(b), utxo.MinedBlockInfo{
		BlockID: 2, BlockHeight: at, OnLongestChain: true})
	require.NoError(t, err)

	c := spendOneOutput(t, s, ctx, b, 0, at)
	_, err = s.SetMinedMulti(ctx, hashes(c), utxo.MinedBlockInfo{
		BlockID: 3, BlockHeight: at, OnLongestChain: true})
	require.NoError(t, err)

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	reclaimed, _, err := s.reclaimFromPartition(ctx, partition, 1_000)
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, b), "B is fully spent by settled C")
	require.False(t, identExists(t, s, ctx, a),
		"A is fully spent by settled B; deleting B's row before judging A must not make A look unsettled")
	require.True(t, identExists(t, s, ctx, c), "C still holds a live coin")
	require.Equal(t, 2, reclaimed)
}

// spendWithoutIdentity spends one of parent's outputs by a transaction the store never creates,
// the way the block path does when a transaction with no spendable outputs is not stored, or a
// spender was reclaimed already. blockApplied marks the spend as recorded by the
// below-checkpoint block path, which is the outpoint-only option the validator refuses above
// the checkpoint.
func spendWithoutIdentity(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, height uint32,
	blockApplied bool) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err := spendOnly(ctx, s, child, height, utxo.WithSkipUTXOHashCheck(blockApplied))
	require.NoError(t, err)

	return child
}

// TestReclaimTrustsABlockAppliedSpendWhoseSpenderHasNoIdentityRow.
//
// Below the hardcoded checkpoint the block path records every spend with the outpoint-only
// mark, and a block there cannot be un-mined by rule. So a spend carrying the mark says its
// spender is in a main-chain block that will never be taken back, whether or not the spender
// still has, or ever had, an identity row. A transaction with no spendable outputs may never
// be stored at all, and a spender's own row may already have been reclaimed. Asking the
// identity table about such a spender finds nothing, and treating "no row" as "not settled"
// is what stranded the parents of every such spend forever.
func TestReclaimTrustsABlockAppliedSpendWhoseSpenderHasNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendWithoutIdentity(t, s, ctx, parent, 100, true)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, parent),
		"the only spend was block-applied below the checkpoint; the spender needs no row to prove it is buried")
}

// TestReclaimStillRefusesAnUnmarkedSpendWhoseSpenderHasNoIdentityRow keeps the fail-safe
// where it still means something. A spend recorded at the tip carries no mark, its spender
// may be a mempool transaction, and the only proof it is buried is its identity row.
func TestReclaimStillRefusesAnUnmarkedSpendWhoseSpenderHasNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendWithoutIdentity(t, s, ctx, parent, 100, false)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent),
		"an unmarked spend by a spender with no row proves nothing about depth")
}
