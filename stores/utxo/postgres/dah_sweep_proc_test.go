package postgres

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// snapshotDAH reads delete_at_height for every tx, keyed by hex hash. A nil DAH
// is represented by absence from the "set" map plus presence in the "all" map,
// so NULL-vs-value differences are caught.
func snapshotDAH(t *testing.T, store *Store, ctx context.Context) map[string]*int64 {
	t.Helper()

	rows, err := store.pool.Query(ctx, `SELECT hash, delete_at_height FROM txs`)
	require.NoError(t, err)
	defer rows.Close()

	out := make(map[string]*int64)
	for rows.Next() {
		var (
			h   []byte
			dah *int64
		)
		require.NoError(t, rows.Scan(&h, &dah))
		out[hex.EncodeToString(h)] = dah
	}
	require.NoError(t, rows.Err())

	return out
}

// TestDAHSweepProcBootstrap verifies that teranode can install the procedure on a
// vanilla connection and that the control row is seeded with the version + knobs.
func TestDAHSweepProcBootstrap(t *testing.T) {
	store, ctx := setupTestStore(t)

	unavailable, err := store.bootstrapDAHSweepProc(ctx)
	require.NoError(t, err)
	require.False(t, unavailable, "test postgres connection should be able to CREATE the procedure")

	// The procedure exists.
	var procCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_proc WHERE proname = 'dah_sweep_batch'`).Scan(&procCount))
	require.Equal(t, 1, procCount, "dah_sweep_batch procedure must be installed")

	// The control row records the installed version and seeded knobs.
	var (
		version, batchRows, maxWin int
	)
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT proc_version, batch_rows, max_windows_per_call FROM dah_sweep_control WHERE id = 1`,
	).Scan(&version, &batchRows, &maxWin))
	require.Equal(t, dahSweepProcVersion, version)
	require.Positive(t, batchRows)
	require.Positive(t, maxWin)

	// Idempotent: a second bootstrap is a no-op (version already current).
	unavailable, err = store.bootstrapDAHSweepProc(ctx)
	require.NoError(t, err)
	require.False(t, unavailable)
}

// TestDAHSweepProcEquivalence is the consensus-critical gate: the server-side
// procedure must stamp delete_at_height byte-for-byte identically to the
// in-process Go sweep on the same input. It builds a dataset covering the DAH
// decision branches (mined+fully-spent → stamp; mined+partially-spent → NULL;
// unmined → NULL), runs the Go sweep and snapshots DAH, resets DAH + watermark,
// runs the procedure, and asserts the snapshots are identical.
func TestDAHSweepProcEquivalence(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	const safeTip = int64(105)
	retention := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	// Branch coverage (newUniqueUnminedTx gives a distinct txid per call so these
	// independent parents don't collide on the PK):
	fullySpent := newUniqueUnminedTx(t, store) // mined + fully spent → stamped
	mineTx(t, store, fullySpent, 100)
	spendAllOutputs(t, store, fullySpent, 101)

	partiallySpent := newUniqueUnminedTx(t, store) // mined + partially spent → NULL
	mineTx(t, store, partiallySpent, 100)
	spendOneOutput(t, store, partiallySpent, 0, 101)

	_ = newUniqueUnminedTx(t, store) // unmined → NULL (unmined_since guard)

	// --- Go sweep ---
	n, err := store.sweepDAHUpTo(ctx, safeTip, 100000)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1, "Go sweep must stamp at least the fully-spent parent")
	goSnapshot := snapshotDAH(t, store, ctx)

	// --- Reset DAH state ---
	_, err = store.pool.Exec(ctx, `UPDATE txs SET delete_at_height = NULL`)
	require.NoError(t, err)
	_, err = store.pool.Exec(ctx, `UPDATE dah_watermark SET last_swept_height = 0 WHERE id = 1`)
	require.NoError(t, err)

	// Sanity: everything is NULL again.
	for h, dah := range snapshotDAH(t, store, ctx) {
		require.Nil(t, dah, "DAH should be NULL after reset for %s", h)
	}

	// --- Procedure ---
	unavailable, err := store.bootstrapDAHSweepProc(ctx)
	require.NoError(t, err)
	require.False(t, unavailable)

	// One CALL drains the whole (tiny) range: heights 100..105 fit one 4096 window.
	_, err = store.pool.Exec(ctx, `CALL dah_sweep_batch($1, $2)`, safeTip, retention)
	require.NoError(t, err, "CALL dah_sweep_batch must succeed (COMMIT-in-CALL works in the test pool)")

	procSnapshot := snapshotDAH(t, store, ctx)

	// --- Equivalence ---
	require.Equal(t, len(goSnapshot), len(procSnapshot), "same set of txs")
	for h, goDAH := range goSnapshot {
		procDAH, ok := procSnapshot[h]
		require.True(t, ok, "tx %s present in proc snapshot", h)
		if goDAH == nil {
			require.Nil(t, procDAH, "tx %s: Go left DAH NULL; proc must too", h)
		} else {
			require.NotNil(t, procDAH, "tx %s: Go stamped DAH=%d; proc must too", h, *goDAH)
			require.Equal(t, *goDAH, *procDAH, "tx %s: DAH must be byte-identical (Go vs proc)", h)
		}
	}

	// The watermark advanced to the safe tip.
	var watermark int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT last_swept_height FROM dah_watermark WHERE id = 1`).Scan(&watermark))
	require.Equal(t, safeTip, watermark, "proc must advance the watermark to the safe tip")
}
