package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// newUniqueUnminedTxK creates an unmined tx with exactly k spendable P2PKH
// outputs (spendable_count = k). Used by the fold-forward proc tests that need a
// k>2 output tx so that spends can be split across bands.
func newUniqueUnminedTxK(t *testing.T, store *Store, k int) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := bt.NewTx()
	for i := 0; i < k; i++ {
		//nolint:gosec // test-only value
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	}
	_, err := store.Create(ctx, tx, 10) // unmined — no WithMinedBlockInfo
	require.NoError(t, err)
	return tx
}

// spendVouts spends exactly the given output indexes of parentTx at spendHeight
// (creating and Spend-ing a single child that consumes those vouts). Unlike
// spendAllOutputs it does not self-assert full spend, so it can be called
// repeatedly to spend different vouts in different bands.
func spendVouts(t *testing.T, store *Store, parentTx *bt.Tx, spendHeight uint32, vouts ...uint32) {
	t.Helper()
	ctx := context.Background()
	child := getSpendingTx(t, parentTx, vouts...)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)
}

// dahOfTx reads txs.delete_at_height for tx (nil when unstamped).
func dahOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) *int64 {
	t.Helper()
	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
	return dah
}

// progressOfTx reads txs.spent_progress for tx.
func progressOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()
	var p int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_progress FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&p))
	return p
}

// minWatermark reads the lowest per-partition watermark.
func minWatermark(t *testing.T, store *Store, ctx context.Context) int64 {
	t.Helper()
	var wm int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT COALESCE(MIN(last_swept_height), 0) FROM dah_part_watermark`).Scan(&wm))
	return wm
}

// TestDAHProc_FoldStampsOnCompletion is the core fold-forward test: a k=3
// spendable mined tx with 2 outputs spent in band1 and the 3rd in band2. After a
// sweep whose band ends before band2's spend height, only 2 of 3 outputs are
// folded (spent_progress=2) so NO stamp fires. After a second sweep whose band
// covers band2, the 3rd fold completes (spent_progress=3=spendable_count) and the
// stamp fires with delete_at_height = GREATEST(maxSpendHeight, minedHeight)+1+ret.
// The watermark must advance past both bands.
func TestDAHProc_FoldStampsOnCompletion(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Force a small, deterministic band so band1 (up to height 150) excludes the
	// band2 spend at height 200. band_heights=100 → first band is (0,100], but we
	// sweep incrementally by calling with a rising safe_tip below.
	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET band_heights = 50 WHERE id = 1`)
	require.NoError(t, err)

	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTxK(t, store, 3)
	mineTx(t, store, tx, 100)           // mined_at_height = 100
	spendVouts(t, store, tx, 150, 0, 1) // band1: 2 of 3 outputs spent at 150
	spendVouts(t, store, tx, 200, 2)    // band2: last output spent at 200

	// --- Sweep 1: safe_tip = 150, so only band1's spends are folded. ---
	require.NoError(t, store.SetBlockHeight(150))
	store.sweepAllPartitionsOnce(ctx, 150, int32(ret)) //nolint:gosec

	require.Equal(t, 2, progressOfTx(t, store, ctx, tx), "band1: 2 of 3 spendable outputs folded")
	require.Nil(t, dahOfTx(t, store, ctx, tx), "partial fold (2/3) must NOT stamp")
	require.Equal(t, int64(150), minWatermark(t, store, ctx), "watermark advanced to safe_tip after band1")

	// --- Sweep 2: safe_tip = 200, band2's spend at 200 is now folded → complete. ---
	require.NoError(t, store.SetBlockHeight(200))
	store.sweepAllPartitionsOnce(ctx, 200, int32(ret)) //nolint:gosec

	require.Equal(t, 3, progressOfTx(t, store, ctx, tx), "band2: all 3 spendable outputs folded")
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "complete fold (3/3) of a mined tx must stamp")
	// GREATEST(maxSpendHeight=200, minedHeight=100) = 200
	require.Equal(t, int64(200)+1+ret, *dah, "DAH = GREATEST(lastSpendHeight, minedHeight)+1+retention")
	require.Equal(t, int64(200), minWatermark(t, store, ctx), "watermark advanced past both bands")

	// The stamp must also be mirrored into pending_deletes.
	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "stamp must be upserted into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestDAHProc_SpentBeforeMinedNotStampedByProc pins the proc's division of labour
// for the spent-before-mined ordering: outputs are all spent while the tx is
// UNMINED. The fold-forward proc advances spent_progress but must NOT stamp
// delete_at_height while the tx is unmined (mined_at_height IS NULL / unmined_since
// set). Stamping the spent-before-mined tx is the mine path's job (Task 7 /
// SetMinedMulti), not the sweep's. This test asserts ONLY the proc's half: it
// leaves the unmined tx unstamped even after folding every spendable output.
func TestDAHProc_SpentBeforeMinedNotStampedByProc(t *testing.T) {
	store, ctx := setupTestStore(t)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined
	spendAllOutputs(t, store, tx, 50)      // all outputs spent while UNMINED

	require.NoError(t, store.SetBlockHeight(60))
	store.sweepAllPartitionsOnce(ctx, 60, ret)

	// The proc folds the spends (progress reaches spendable_count) but the mined
	// gate keeps it unstamped while unmined.
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"proc must NOT stamp a fully-spent-but-unmined tx (mine path owns that stamp)")

	// Now mine it: SetMinedMulti (the mine path) stamps the fully-spent tx.
	mineTx(t, store, tx, 60)
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "mine path must stamp the fully-spent tx on SetMinedMulti")
	require.Equal(t, int64(60)+1+int64(ret), *dah, "DAH = GREATEST(spend=50, mined=60)+1+retention")
}

// TestDAHProc_BoundedWorkAdvancesIncrementally pins that the proc advances the
// watermark in bounded bands rather than requiring the whole (0, safe_tip] range
// to drain in one CALL. With a tiny band and a max-bands-per-call budget smaller
// than the number of bands needed to reach safe_tip, a single CALL advances the
// watermark only by budget*band — proving per-call work is bounded and the
// watermark moves forward incrementally.
func TestDAHProc_BoundedWorkAdvancesIncrementally(t *testing.T) {
	store, ctx := setupTestStore(t)

	// band_heights=10, max 3 bands per CALL → one CALL advances at most 30 heights.
	_, err := store.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET band_heights = 10, max_windows_per_call = 3 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(1000)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	// One CALL per partition, single pass: watermark advances by exactly budget*band.
	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	wm := minWatermark(t, store, ctx)
	require.Equal(t, int64(30), wm,
		"one CALL advances at most max_bands*band = 3*10 = 30 heights (bounded per call)")
	require.Less(t, wm, safeTip, "the whole range must NOT drain in one CALL")

	// Repeated CALLs keep advancing forward until the safe_tip is reached — the
	// background driver loops CALLs while backlog>0, so a bounded number of CALLs
	// reaches the tip.
	for i := 0; i < 40 && minWatermark(t, store, ctx) < safeTip; i++ {
		store.sweepAllPartitionsOnce(ctx, safeTip, ret)
	}
	require.Equal(t, safeTip, minWatermark(t, store, ctx),
		"repeated bounded CALLs advance the watermark forward to safe_tip")
}

// TestDAHSweepProcBootstrap verifies the procedure is installed by store.New and
// that the control row records the version + seeded knobs, and that re-bootstrap
// is idempotent.
func TestDAHSweepProcBootstrap(t *testing.T) {
	store, ctx := setupTestStore(t) // store.New already bootstrapped the procedure

	var procCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_proc WHERE proname = 'dah_sweep_batch'`).Scan(&procCount))
	require.Equal(t, 1, procCount, "dah_sweep_batch procedure must be installed by store.New")

	var version, batchRows, maxWin int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT proc_version, batch_rows, max_windows_per_call FROM dah_sweep_control WHERE id = 1`,
	).Scan(&version, &batchRows, &maxWin))
	require.Equal(t, dahSweepProcVersion, version)
	require.Positive(t, batchRows)
	require.Positive(t, maxWin)

	// Idempotent: a second bootstrap is a no-op (version already current).
	require.NoError(t, store.bootstrapDAHSweepProc(ctx))
}

// TestDAHSweepProcStampsExpectedDAH pins the procedure's DAH semantics against the
// spec directly (mined+fully-spent → stamp completion+1+retention; partially-spent
// → NULL; unmined → NULL) and that it advances the watermark. This is the proc's
// behavioural contract now that it is the only sweep mechanism.
func TestDAHSweepProcStampsExpectedDAH(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	const safeTip = int64(105)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	fullySpent := newUniqueUnminedTx(t, store)
	mineTx(t, store, fullySpent, 100)
	spendAllOutputs(t, store, fullySpent, 101) // mined@100, fully spent@101 → stamp

	partiallySpent := newUniqueUnminedTx(t, store)
	mineTx(t, store, partiallySpent, 100)
	spendOneOutput(t, store, partiallySpent, 0, 101) // partially spent → NULL

	unmined := newUniqueUnminedTx(t, store) // unmined → NULL (unmined_since guard)

	// Drive all 8 partition CALLs in parallel (the production path).
	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec // small positive

	dahOf := func(tx *bt.Tx) *int64 {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		return dah
	}

	// completion_height = GREATEST(max spent_at_height=101, mined_at_height=100) = 101
	fsDAH := dahOf(fullySpent)
	require.NotNil(t, fsDAH, "fully-spent mined parent must be stamped")
	require.Equal(t, int64(101)+1+ret, *fsDAH, "DAH must be completion+1+retention")

	require.Nil(t, dahOf(partiallySpent), "partially-spent parent must stay NULL")
	require.Nil(t, dahOf(unmined), "unmined tx must stay NULL")

	var watermark int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&watermark))
	require.Equal(t, safeTip, watermark, "every partition's watermark must reach the safe tip")
}

// TestDAHSweepProcDrainsWideRangeInOneCall pins that a single CALL drains the
// entire (watermark, safe_tip] range regardless of height span — the row-targeted
// proc has no per-CALL window cap. Red against the old window-capped proc.
func TestDAHSweepProcDrainsWideRangeInOneCall(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Force the old proc's worst case: at most one window per CALL.
	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET max_windows_per_call = 1, batch_rows = 1000 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(2000)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// Fully-spent mined parents spread across heights far wider than one 256-window.
	heights := []int64{100, 400, 800, 1200, 1600, 1900}
	txs := make([]*bt.Tx, 0, len(heights))
	for _, h := range heights {
		tx := newUniqueUnminedTx(t, store)
		mineTx(t, store, tx, uint32(h-1))        //nolint:gosec // small positive height
		spendAllOutputs(t, store, tx, uint32(h)) //nolint:gosec // small positive height
		txs = append(txs, tx)
	}

	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec

	for i, tx := range txs {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		require.NotNilf(t, dah, "parent at height %d must be stamped in one CALL", heights[i])
		require.Equal(t, heights[i]+1+ret, *dah)
	}

	var minWM int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&minWM))
	require.Equal(t, safeTip, minWM, "watermark must reach safe tip in one CALL across the wide range")
}

// TestDAHSweepProcDenseHeightMultiPass pins that a single height holding more
// fully-spent parents than batch_rows is fully drained by repeated bounded passes
// within one CALL, and the watermark advances to safe_tip.
func TestDAHSweepProcDenseHeightMultiPass(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET batch_rows = 2 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(110)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// 7 fully-spent mined parents ALL at the same spend height (105) — well over batch_rows=2.
	txs := make([]*bt.Tx, 0, 7)
	for i := 0; i < 7; i++ {
		tx := newUniqueUnminedTx(t, store)
		mineTx(t, store, tx, 104)
		spendAllOutputs(t, store, tx, 105)
		txs = append(txs, tx)
	}

	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec

	for _, tx := range txs {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		require.NotNil(t, dah, "every fully-spent parent at the dense height must be stamped")
		require.Equal(t, int64(105)+1+ret, *dah)
	}

	var minWM int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&minWM))
	require.Equal(t, safeTip, minWM, "watermark must reach safe tip after dense-height multipass")
}

// TestDAHSweepProcRewindRerunIsResultIdempotent verifies that rewinding the watermark
// and re-sweeping the same range leaves the SAME stamp RESULT. The forward-only fold
// double-counts the surviving spends on the re-sweep (so the spent_progress counter
// drifts — that is expected and left for the reconcile backstop), but the ground-truth
// stamp gate means delete_at_height is unchanged: a genuinely fully-spent tx stays
// stamped at the identical height. The per-tx RESULT is the invariant, not the counter.
func TestDAHSweepProcRewindRerunIsResultIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(uint32(110)))
	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTx(t, store)
	mineTx(t, store, tx, 104)
	spendAllOutputs(t, store, tx, 105)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	firstDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, firstDAH, "fully-spent mined parent stamped on first sweep")

	// Rewind the watermark so the same (surviving) range is re-swept, then sweep again.
	require.NoError(t, store.RewindDAHWatermark(ctx, 0))
	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	secondDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, secondDAH, "genuinely fully-spent parent stays stamped after re-sweep")
	require.Equal(t, *firstDAH, *secondDAH, "rewind+re-sweep must yield the identical delete_at_height")
}

// TestDAHSweepProcSkipsWatermarkOnLockContention verifies that when the per-partition
// advisory lock is held elsewhere, the CALL does NOT advance the watermark (which would
// silently skip the unswept range). It then releases the lock and confirms a normal sweep
// drains and advances.
func TestDAHSweepProcSkipsWatermarkOnLockContention(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	tx := newUniqueUnminedTx(t, store)
	mineTx(t, store, tx, 104)
	spendAllOutputs(t, store, tx, 105)

	// Hold all 8 partition advisory locks in a separate connection's open transaction
	// so every dah_sweep_batch CALL misses pg_try_advisory_xact_lock.
	holder, err := store.pool.Acquire(ctx)
	require.NoError(t, err)
	holderTx, err := holder.Begin(ctx)
	require.NoError(t, err)
	_, err = holderTx.Exec(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock(20240684 + g) FROM generate_series(0,%d) g`, numPartitions-1))
	require.NoError(t, err)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	var wmDuringContention int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MAX(last_swept_height) FROM dah_part_watermark`).Scan(&wmDuringContention))
	require.Equal(t, int64(0), wmDuringContention, "watermark must NOT advance while the lock is held elsewhere")

	// Release the locks, sweep again: now it drains and advances.
	require.NoError(t, holderTx.Rollback(ctx))
	holder.Release()

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "after lock release the fully-spent parent must be stamped")

	var wmAfter int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&wmAfter))
	require.Equal(t, safeTip, wmAfter, "after lock release the watermark must reach safe tip")
}

// TestBootstrapInstallsV14WithoutLockTimeout verifies that the bootstrapped
// procedure has version 14 and does not contain the lock_timeout setting.
func TestBootstrapInstallsV14WithoutLockTimeout(t *testing.T) {
	store, ctx := setupTestStore(t)

	var version int
	err := store.pool.QueryRow(ctx,
		`SELECT proc_version FROM dah_sweep_control WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, 14, version)

	var src string
	err = store.pool.QueryRow(ctx,
		`SELECT prosrc FROM pg_proc WHERE proname = 'dah_sweep_batch'`).Scan(&src)
	require.NoError(t, err)
	require.NotContains(t, src, "lock_timeout")
}

func TestSweepOnePartitionRunsWithoutDeadline(t *testing.T) {
	store, ctx := setupTestStore(t)

	// safeTip=-1 is the documented no-op smoke value: the CALL returns immediately.
	// The contract under test: sweepOnePartition exists, takes NO timeout, and
	// surfaces the CALL error verbatim (nil here).
	err := store.sweepOnePartition(ctx, 0, -1, 0)
	require.NoError(t, err)
}

func TestPgSQLStateExtractsCode(t *testing.T) {
	require.Equal(t, "", pgSQLState(nil))
	require.Equal(t, "", pgSQLState(errors.NewProcessingError("not a pg error")))

	pgErr := &pgconn.PgError{Code: "40P01"}
	require.Equal(t, "40P01", pgSQLState(pgErr))
}

func TestDAHPartitionBacklog(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = 100 WHERE partition = 2`)
	require.NoError(t, err)

	backlog, err := store.dahPartitionBacklog(ctx, 2, 150)
	require.NoError(t, err)
	require.Equal(t, int64(50), backlog)

	backlog, err = store.dahPartitionBacklog(ctx, 2, 100)
	require.NoError(t, err)
	require.Zero(t, backlog)

	// Missing watermark row must be an ERROR (not silently "caught up") — the
	// old dahWatermarkBacklog returned 0 on any error, which idled the drain.
	_, err = store.dahPartitionBacklog(ctx, 99, 150)
	require.Error(t, err)
}
