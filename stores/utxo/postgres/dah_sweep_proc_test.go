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
