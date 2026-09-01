package utxoset

import (
	"context"
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

// TestReclaimBatchProbesEachSpenderOnce pins the deduplication of the settled check's argument.
//
// The work list arrives as one row per parent-spender pair, so a transaction that consumed
// several parents used to be probed once per parent. The parents beside it were already
// deduplicated.
func TestReclaimBatchProbesEachSpenderOnce(t *testing.T) {
	b := newReclaimBatch()

	parentOne := []byte("parent-one")
	parentTwo := []byte("parent-two")
	spender := []byte("one-transaction-took-both")

	b.add(parentOne, spender)
	b.add(parentTwo, spender)

	require.Len(t, b.spenders, 1,
		"a transaction that consumed two parents is one key to probe, not two")
	require.Len(t, b.parents, 2, "both parents still have to be judged")

	require.Equal(t, [][]byte{spender}, b.spentBy[string(parentOne)],
		"the per-parent list decides whether all of a parent's spends settled and must stay whole")
	require.Equal(t, [][]byte{spender}, b.spentBy[string(parentTwo)])
}

// TestReclaimBatchResetForgetsSpenders is not defensive tidiness.
//
// Chunks cut on a parent boundary and the batch is reused across them. If reset kept the
// deduplication set, every spender already seen in an earlier chunk would be silently left out
// of the next chunk's settled probe, come back unsettled because it was never asked about, and
// its parent would never be reclaimed. That leaks rather than deletes, so nothing would fail
// loudly and the leak would look like the workload.
func TestReclaimBatchResetForgetsSpenders(t *testing.T) {
	b := newReclaimBatch()

	parent := []byte("a-parent")
	spender := []byte("a-spender")

	b.add(parent, spender)
	b.reset()
	b.add(parent, spender)

	require.Len(t, b.spenders, 1,
		"the next chunk must probe this spender again; reset has to forget it")
}
