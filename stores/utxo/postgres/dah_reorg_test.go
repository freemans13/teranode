package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// spentProgressOfTx reads txs.spent_progress for tx (Setter-C counter).
func spentProgressOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()
	var p int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_progress FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&p))
	return p
}

// lastSpendHeightOfTx reads txs.last_spend_height for tx (nil when never folded).
func lastSpendHeightOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) *int64 {
	t.Helper()
	var h *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT last_spend_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&h))
	return h
}

// spendVoutOwned spends exactly one output (vout) of parentTx at spendHeight and
// returns the *utxo.Spend (carrying the stored spending_data ownership token) so a
// test can later Unspend that exact spend. Mirrors spendOneOutput but recovers the
// ownership token from the spends row it just created.
func spendVoutOwned(t *testing.T, store *Store, parentTx *bt.Tx, vout uint32, spendHeight uint32) *utxo.Spend {
	t.Helper()
	ctx := context.Background()

	child := getSpendingTx(t, parentTx, vout)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)

	parentHash := parentTx.TxIDChainHash()
	var sdBytes []byte
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spending_data FROM spends WHERE prev_tx_hash=$1 AND prev_output_idx=$2`,
		parentHash[:], int32(vout)).Scan(&sdBytes))
	sd, sdErr := spendpkg.NewSpendingDataFromBytes(sdBytes)
	require.NoError(t, sdErr)

	return &utxo.Spend{TxID: parentHash, Vout: vout, SpendingData: sd}
}

// TestUnspendDecrementsSpentProgressExactlyOne is the core reorg-counter test:
// a 2-spendable-output mined tx spent to completion (spent_progress folds to 2 and
// the sweep stamps delete_at_height). Unspending ONE spendable output must:
//   - decrement spent_progress by EXACTLY 1 (2 -> 1), and
//   - clear delete_at_height (the tx is no longer fully spent → not prune-eligible).
//
// Re-spending that output at a NEW height must let the fold reconverge (progress back
// to 2) and the sweep re-stamp with the new completion height.
func TestUnspendDecrementsSpentProgressExactlyOne(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // pre-mined, exactly 2 spendable outputs
	sp0 := spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // now fully spent at 101

	// Sweep folds both spends and stamps the fully-spent mined parent.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent), "both spendable outputs folded")
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before unspend")
	require.Equal(t, int64(101)+1+ret, *dah)

	// Unspend ONE spendable output.
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{sp0}))

	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent),
		"Unspend of one owned spendable output must decrement spent_progress by exactly 1")
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"Unspend must clear delete_at_height (no longer fully spent)")

	// pending_deletes must also be cleared for the revived parent.
	var pdCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pdCount))
	require.Zero(t, pdCount, "revived parent must be removed from pending_deletes")

	// Re-spend the same output at a NEW height; the fold must reconverge and re-stamp.
	// The sweep already folded vout1's spend at 101 (still present, still counted in
	// spent_progress=1). We rewind the watermark to just BELOW the new spend so the
	// forward-only fold picks up ONLY the new vout0 spend at 200 (progress 1 -> 2),
	// without double-folding the surviving vout1 spend. (A reorg that rewinds past a
	// SURVIVING spend double-counts it under the forward-only fold; that drift no longer
	// causes data loss — the stamp is authorised by a ground-truth recount and the
	// reconcile backstop heals the counter, see TestFoldStampRefusesDriftedCounter —
	// here we prove the Task-7 decrement/re-fold reconvergence in isolation.)
	require.NoError(t, store.SetBlockHeight(210))
	_ = spendVoutOwned(t, store, parent, 0, 200)

	require.NoError(t, store.RewindDAHWatermark(ctx, 199))
	_, err = procSweepUpTo(store, ctx, 210)
	require.NoError(t, err)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"re-spend must let the fold reconverge to spendable_count")
	dah2 := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah2, "reconverged fully-spent mined parent must be re-stamped")
	// GREATEST(lastSpendHeight, minedHeight)+1+ret. last_spend_height is now the
	// running max of the folded spends: 101 (vout1, still present) and 200 (re-spent
	// vout0) → 200. minedHeight is NULL for a pre-mined tx.
	require.Equal(t, int64(200)+1+ret, *dah2,
		"re-stamp uses the new completion height (max folded spend)")
}

// TestFoldStampRefusesDriftedCounter reproduces the IBD data-loss wedge observed on
// Hetzner mainnet (h63266, then h49925 on the reorg-re-fold "fix") and testnet (block
// …5e5ea) on 2026-07-02, and pins the real fix.
//
// A reorg calls RewindDAHWatermark(forkHeight) so the fold re-sweeps (forkHeight, tip].
// A spend still present in that range (surviving chain, never Unspent) is counted a
// SECOND time by the forward-only fold (spent_progress += n, no reset), so a
// partially-spent mined tx (vout0 spent, vout1 still a live UTXO) has its counter drift
// 1 -> 2 = spendable_count. The maintained counter is therefore UNTRUSTWORTHY for the
// irreversible prune stamp. The fix: the stamp is authorised by a GROUND-TRUTH recount
// of the tx's spendable spends, not the counter — so even with the counter drifted to
// spendable_count, delete_at_height stays NULL because vout1 is genuinely unspent. (The
// counter drift itself is left for the reconcile backstop to correct; it is now harmless.)
func TestFoldStampRefusesDriftedCounter(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	// Unique tx (not the shared fixed-hash helper) so the pending_deletes assertion below
	// cannot collide with a stale row left by another test (pending_deletes is keyed by hash
	// and is not truncated between tests).
	parent := newUniqueUnminedTxK(t, store, 2) // unique hash, exactly 2 spendable outputs
	mineTx(t, store, parent, 100)              // mined (so the DAH stamp's mined gate can pass)
	spendVouts(t, store, parent, 101, 0)       // PARTIAL: only vout0 spent; vout1 stays a live UTXO

	// First fold: counter reaches 1 (one spendable output spent), NOT stamped (partial).
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent), "one spendable output folded")
	require.Nil(t, dahOfTx(t, store, ctx, parent), "partially-spent tx must not be stamped")

	// Reorg: rewind the watermark BELOW the surviving vout0 spend (height 101) and re-fold.
	// The forward-only fold double-counts the still-present vout0 spend → counter drifts to 2.
	require.NoError(t, store.RewindDAHWatermark(ctx, 100))
	_, err = procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	// The counter has drifted to spendable_count (2), but GROUND TRUTH is that only vout0 is
	// spent (1 spend) — vout1 is a live UTXO. The verified stamp must REFUSE.
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"forward-only re-fold double-counts the surviving spend (drift left for reconcile to fix)")
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"verified stamp must refuse: ground-truth recount (1) != spendable_count (2); vout1 is unspent")
	var pd int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Zero(t, pd, "a drifted-but-not-fully-spent tx must not be queued for deletion")
}

// TestUnspendNonOwningDoesNotDecrement pins that a non-owning (mismatched
// SpendingData) Unspend deletes 0 rows and therefore does NOT decrement
// spent_progress — the live spend row and its counter contribution are untouched.
// Over-decrement here would cause premature pruning (live-UTXO loss). However,
// the parent's delete_at_height MUST STILL BE CLEARED by the non-owning call,
// consistent with the DAH housekeeping that unconditionally clears the stamp for
// all affected parents (even if the spend deletion was a no-op).
func TestUnspendNonOwningDoesNotDecrement(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent at 101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent))
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before non-owning Unspend")
	require.Equal(t, int64(101)+1+ret, *dah)

	// A non-owning Unspend: correct outpoint but WRONG spending_data token. It must
	// delete 0 rows and leave spent_progress at 2.
	bogus := spendpkg.NewSpendingData(parent.TxIDChainHash(), 999)
	nonOwning := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: bogus}
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{nonOwning}))

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"non-owning no-op Unspend must NOT decrement spent_progress")

	// The owned spend row must still be present.
	var stillThere int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1 AND prev_output_idx=0`,
		parent.TxIDChainHash()[:]).Scan(&stillThere))
	require.Equal(t, 1, stillThere, "the live owned spend row must survive a non-owning Unspend")

	// However, the parent's delete_at_height MUST be cleared by the non-owning call.
	// The DAH housekeeping in Unspend (line 839-856 of spend.go) runs for EVERY
	// affected parent, including non-owning no-op deletes, so any stale stamp is
	// cleared in the same transaction as the spend deletion. A future regression
	// (making the clear conditional on owning rows) would leave a live tx carrying
	// a prunable stamp — live-UTXO loss — and should be caught here.
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"non-owning Unspend must still clear delete_at_height (DAH housekeeping is unconditional)")
}

// TestUnspendSingleSpendableDecrementsByOne asserts the single-output happy path:
// a single owned spendable Unspend decrements spent_progress by exactly 1. The
// decrement query filters on out_spendables so only spendable spend rows count.
func TestUnspendSingleSpendableDecrementsByOne(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	sp0 := spendVoutOwned(t, store, parent, 0, 101) // partial: only vout 0

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent), "one spendable output folded")

	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{sp0}))
	require.Equal(t, 0, spentProgressOfTx(t, store, ctx, parent),
		"unspending the one owned spendable output returns spent_progress to 0")
}

// TestSetMinedMultiStampsViaCounter pins Site 1: the spent-before-mined ordering.
// A tx is fully spent while UNMINED. The sweep folds spent_progress to
// spendable_count but the mined gate keeps it unstamped. SetMinedMulti must then
// stamp delete_at_height DIRECTLY from the counter (spent_progress = spendable_count)
// with NO spends re-aggregation, using GREATEST(last_spend_height, mined)+1+ret.
func TestSetMinedMultiStampsViaCounter(t *testing.T) {
	store, ctx := setupTestStore(t)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined, 2 spendable outputs
	spendAllOutputs(t, store, tx, 50)      // fully spent while UNMINED at height 50

	// Fold the spends via the sweep so spent_progress reaches spendable_count while
	// the tx is still unmined (proc must NOT stamp — mined gate).
	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, tx), "sweep folds counter while unmined")
	require.Nil(t, dahOfTx(t, store, ctx, tx), "unmined tx must stay unstamped after fold")
	lsh := lastSpendHeightOfTx(t, store, ctx, tx)
	require.NotNil(t, lsh)
	require.Equal(t, int64(50), *lsh, "fold advanced last_spend_height to the spend height")

	// Now mine it: SetMinedMulti stamps via the counter (spent_progress=spendable_count).
	mineTx(t, store, tx, 60)
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "SetMinedMulti must stamp the fully-spent tx via the counter")
	// GREATEST(last_spend_height=50, mined=60)+1+ret = 61+ret
	require.Equal(t, int64(60)+1+ret, *dah, "DAH = GREATEST(last_spend_height, mined)+1+retention")

	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "mine-time stamp must be mirrored into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestSetMinedMultiPartialCounterDoesNotStamp pins that SetMinedMulti does NOT
// stamp when the counter has not reached spendable_count (partially spent), even
// though the tx is being mined. Under-stamping is safe (the sweep completes it
// later); over-stamping here would prune a tx with live UTXOs.
func TestSetMinedMultiPartialCounterDoesNotStamp(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := newUniqueUnminedTxK(t, store, 2)
	spendVouts(t, store, tx, 50, 0) // only vout 0 spent while unmined

	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, tx), "only one output folded")

	mineTx(t, store, tx, 60)
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"SetMinedMulti must NOT stamp a partially-spent tx (counter < spendable_count)")
}

// TestUnsetMinedClearsStampedDAH pins Site 3: unsetMinedMulti (reorg clearing
// mined) must clear a previously-stamped delete_at_height, consistent with Site 1.
func TestUnsetMinedClearsStampedDAH(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := newUniqueUnminedTxK(t, store, 2)
	spendAllOutputs(t, store, tx, 50)

	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)

	// Mine it (stamps via counter), confirm stamped.
	mineTx(t, store, tx, 60)
	require.NotNil(t, dahOfTx(t, store, ctx, tx), "must be stamped after mine")

	// Reorg it out: unsetMinedMulti (UnsetMined path) must clear the stamp.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:     60,
		BlockHeight: 60,
		UnsetMined:  true,
	})
	require.NoError(t, err)

	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"unsetMinedMulti must clear the previously-stamped delete_at_height")

	var pdCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&pdCount))
	require.Zero(t, pdCount, "unset-mined must remove the tx from pending_deletes")
}
