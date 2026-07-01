package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// setSpentProgressRaw forces txs.spent_progress to a wrong value directly, without
// touching the spends table — simulating counter drift (arithmetic bug, lost
// update, or reorg double-fold) that the maintained counter can never self-correct
// on its own (the forward-only fold only ever adds NEW spends).
func setSpentProgressRaw(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx, val int) {
	t.Helper()
	_, err := store.pool.Exec(ctx,
		`UPDATE txs SET spent_progress = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], val)
	require.NoError(t, err)
}

// setDAHRaw forces txs.delete_at_height directly (nil clears it).
func setDAHRaw(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx, val *int64) {
	t.Helper()
	_, err := store.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], val)
	require.NoError(t, err)
}

// reconcileAllPartitions runs the reconciliation pass over EVERY partition once,
// using a slice large enough to cover the small test data. Returns the total
// number of drifted rows corrected across all partitions.
func reconcileAllPartitions(t *testing.T, store *Store, ctx context.Context, safeTip int64, slice int) int64 {
	t.Helper()
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive
	var total int64
	for p := 0; p < numPartitions; p++ {
		n, err := store.reconcileSpentProgressPartition(ctx, p, safeTip, ret, slice)
		require.NoError(t, err)
		total += n
	}
	return total
}

// TestReconcileHealsDriftUp pins the core self-heal: a maintained spent_progress
// that has drifted ABOVE the true count (e.g. a reorg double-fold overshoot) is
// corrected back to the true count (recomputed from the spends table using the
// same spendable predicate the fold uses).
func TestReconcileHealsDriftUp(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100) // 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)    // one spendable output spent

	// Fold it: true spent_progress = 1.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent))

	// Corrupt the counter UP by 1 (drift the maintained value; spends unchanged).
	setSpentProgressRaw(t, store, ctx, parent, 2)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent), "precondition: drifted counter")

	// Reconcile the slice: it must recompute the true count (1) and correct it.
	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Positive(t, corrected, "reconcile must report the drift it corrected")

	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent),
		"reconcile must correct spent_progress to the true spendable-spend count")
}

// TestReconcileHealsDriftDown pins the opposite drift: a counter that has drifted
// BELOW the true count (a lost decrement/update) is corrected up to the true count.
func TestReconcileHealsDriftDown(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // both spendable outputs spent → true = 2

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent))

	setSpentProgressRaw(t, store, ctx, parent, 1) // drift DOWN
	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Positive(t, corrected)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"reconcile must correct spent_progress up to the true count")
}

// TestReconcileCorrectsLastSpendHeight pins that reconcile also recomputes
// last_spend_height = max(spent_at_height) over the tx's spendable spends.
func TestReconcileCorrectsLastSpendHeight(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(210))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 150)
	_ = spendVoutOwned(t, store, parent, 1, 200) // max spendable spend height = 200

	_, err := procSweepUpTo(store, ctx, 210)
	require.NoError(t, err)

	// Corrupt both counter and last_spend_height.
	setSpentProgressRaw(t, store, ctx, parent, 5)
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET last_spend_height = 999 WHERE hash = $1`, parent.TxIDChainHash()[:])
	require.NoError(t, err)

	reconcileAllPartitions(t, store, ctx, 210, 10000)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent))
	lsh := lastSpendHeightOfTx(t, store, ctx, parent)
	require.NotNil(t, lsh)
	require.Equal(t, int64(200), *lsh, "reconcile must recompute last_spend_height = max spendable spend height")
}

// TestReconcileCompletesAndStamps pins the drift-that-completes case: a
// fully-spent + mined tx whose spent_progress was corrupted DOWN to
// spendable_count-1 (so it is un-stamped) must, after reconcile, be corrected to
// spendable_count AND have delete_at_height stamped (mirroring the fold's stamp),
// catching a previously-missed completion. It must also be mirrored into
// pending_deletes.
func TestReconcileCompletesAndStamps(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // mined@100, 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent@101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent))
	require.NotNil(t, dahOfTx(t, store, ctx, parent), "precondition: fold stamped it")

	// Corrupt: knock progress down by 1 AND clear the stamp, simulating a missed
	// completion (counter drift left it below spendable_count and un-stamped).
	setSpentProgressRaw(t, store, ctx, parent, 1)
	setDAHRaw(t, store, ctx, parent, nil)
	require.Nil(t, dahOfTx(t, store, ctx, parent), "precondition: un-stamped")

	reconcileAllPartitions(t, store, ctx, 110, 10000)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"reconcile must correct progress up to spendable_count")
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "reconcile completing a fully-spent mined tx must stamp delete_at_height")
	require.Equal(t, int64(101)+1+ret, *dah,
		"stamped DAH = GREATEST(last_spend_height, mined)+1+retention")

	// Must be mirrored into pending_deletes (the only pruner path).
	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "reconcile stamp must be mirrored into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestReconcileDoesNotStampUnmined pins that reconcile respects the same mined
// gate as the fold: a fully-spent but UNMINED tx is corrected to spendable_count
// but NOT stamped (the mine path owns that stamp).
func TestReconcileDoesNotStampUnmined(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(60))

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined
	spendAllOutputs(t, store, tx, 50)

	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, tx))
	require.Nil(t, dahOfTx(t, store, ctx, tx))

	// Corrupt the counter; reconcile must fix it but leave it unstamped.
	setSpentProgressRaw(t, store, ctx, tx, 0)
	reconcileAllPartitions(t, store, ctx, 60, 10000)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, tx),
		"reconcile corrects the counter even while unmined")
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"reconcile must NOT stamp an unmined tx (mine path owns that stamp)")
}

// TestReconcileNoDriftIsNoOp pins that a correct counter is left untouched and
// reports zero corrections (no spurious writes on the hot table).
func TestReconcileNoDriftIsNoOp(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent))

	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Zero(t, corrected, "a correct counter must not be reported as drift")
	require.Equal(t, 1, spentProgressOfTx(t, store, ctx, parent), "unchanged")
}

// TestReconcileBounded pins that a single reconcile call processes AT MOST the
// slice size rows — never O(all txs). With many drifted txs and a tiny slice, one
// pass over a partition corrects at most `slice` of that partition's rows.
func TestReconcileBounded(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	// Create 8 fully-spent mined parents and corrupt every counter.
	const n = 8
	txs := make([]*bt.Tx, 0, n)
	for i := 0; i < n; i++ {
		tx := newUniqueUnminedTxK(t, store, 2)
		mineTx(t, store, tx, 100)
		spendAllOutputs(t, store, tx, 101)
		txs = append(txs, tx)
	}
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	for _, tx := range txs {
		setSpentProgressRaw(t, store, ctx, tx, 99) // gross drift on all
	}

	// One reconcile pass with slice=1 per partition corrects at most numPartitions
	// rows total (1 per partition) — far fewer than the 8 drifted rows if they land
	// in fewer partitions. Assert it did NOT correct all of them in one pass.
	corrected := int64(0)
	for p := 0; p < numPartitions; p++ {
		c, cerr := store.reconcileSpentProgressPartition(ctx, p, 110, ret, 1)
		require.NoError(t, cerr)
		require.LessOrEqual(t, c, int64(1), "slice=1 must bound corrections to 1 per partition per pass")
		corrected += c
	}
	require.LessOrEqual(t, corrected, int64(numPartitions),
		"one bounded pass must correct at most slice*numPartitions rows, never all history")

	// Draining with repeated bounded passes eventually heals every row.
	for round := 0; round < 100; round++ {
		remaining := 0
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT count(*) FROM txs WHERE spent_progress = 99`).Scan(&remaining))
		if remaining == 0 {
			break
		}
		for p := 0; p < numPartitions; p++ {
			_, cerr := store.reconcileSpentProgressPartition(ctx, p, 110, ret, 1)
			require.NoError(t, cerr)
		}
	}
	var stillDrifted int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE spent_progress = 99`).Scan(&stillDrifted))
	require.Zero(t, stillDrifted, "repeated bounded passes eventually heal all drift")
}

// TestReconcileHealsReorgDoubleFold is the reorg-rewind safety proof. It folds a
// range, then RewindDAHWatermark below a SURVIVING spend, re-folds (the pure
// forward-only fold DOUBLE-COUNTS the surviving spend, overshooting
// spendable_count so the completion gate never fires again — permanent leak),
// then reconciles and asserts spent_progress equals the TRUE count (not doubled)
// and the completion stamp is (re)applied correctly.
func TestReconcileHealsReorgDoubleFold(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // mined@100, 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent@101 → true count = 2

	// Fold + stamp normally.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent))
	require.NotNil(t, dahOfTx(t, store, ctx, parent))

	// Reorg: rewind the watermark BELOW the surviving spends' heights. The spends
	// themselves survive (this models a reorg that re-swept the range but where the
	// spends were re-applied at the same heights). Re-folding now RE-PROCESSES those
	// still-present spends → spent_progress double-counts to 4.
	require.NoError(t, store.RewindDAHWatermark(ctx, 100))
	_, err = procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, 4, spentProgressOfTx(t, store, ctx, parent),
		"precondition: pure forward-only re-fold double-counts the surviving spends")

	// Reconcile is authoritative for the rewound range: recompute from the spends
	// table → true count 2 (not 4), and the completion stamp survives / is corrected.
	reconcileAllPartitions(t, store, ctx, 110, 10000)

	require.Equal(t, 2, spentProgressOfTx(t, store, ctx, parent),
		"reconcile must heal the reorg double-fold back to the true count")
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "the fully-spent mined tx must remain/again be stamped after heal")
	require.Equal(t, int64(101)+1+ret, *dah)
}
