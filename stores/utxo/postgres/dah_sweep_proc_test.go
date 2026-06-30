package postgres

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

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
	_, err = holderTx.Exec(ctx, `SELECT pg_advisory_xact_lock(20240684 + g) FROM generate_series(0,7) g`)
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
