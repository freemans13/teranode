package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestSchema_PendingUnmined_CreationAndBackfill verifies that createSchemaWithPoolFlag
// always creates the pending_unmined parent table, 8 hash-partition leaves, 8 per-leaf
// btree indexes on unmined_since, and runs the one-time guarded backfill (leaving the
// marker index behind).
func TestSchema_PendingUnmined_CreationAndBackfill(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Clean slate: drop pending_unmined if a prior run left it.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	// Also drop the marker so the backfill can re-run on a fresh table.
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)

	// pending_unmined is ALWAYS-ON (no flag needed).
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: 8 pending_unmined leaves exist.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_unmined_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 8, n, "8 pending_unmined leaves must be created")

	// Assert: 8 per-leaf btree indexes exist (px_pu_since_pNN).
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'px_pu_since_p%' AND relkind='i'`).Scan(&n))
	require.Equal(t, 8, n, "8 per-leaf btree indexes on unmined_since must be created")

	// Assert: backfill marker index exists (proof backfill ran).
	var hasMarker bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_pu_backfill_marker')`).Scan(&hasMarker))
	require.True(t, hasMarker, "backfill marker index must exist after first schema creation")
}

// TestSchema_PendingUnmined_BackfillExistingUnminedTx verifies that an existing unmined
// (non-conflicting) tx row in txs is backfilled into pending_unmined when the schema is
// initialised with the table not yet present.
func TestSchema_PendingUnmined_BackfillExistingUnminedTx(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Full clean slate so we can create only txs first.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS spends CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_part_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_sweep_control CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS conflict_intents CASCADE`)

	// Create only txs (no pending_unmined, no indexes yet) so we can seed a row before
	// the full schema init runs the backfill.
	_, err = pool.Exec(ctx, txsDDL)
	require.NoError(t, err, "create txs parent")
	_, err = pool.Exec(ctx, spendsDDL)
	require.NoError(t, err, "create spends parent")
	for i := 0; i < numPartitions; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS txs_p%02d PARTITION OF txs FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS spends_p%02d PARTITION OF spends FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
	}

	// Insert a non-conflicting tx row with unmined_since set.
	txHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	const unminedSinceVal = int32(100)
	_, err = pool.Exec(ctx,
		`INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since)
         VALUES ($1, 1, 0, 100, 50, false, $2)`,
		txHash, unminedSinceVal)
	require.NoError(t, err)

	// Verify the precondition: tx is in txs with unmined_since set.
	var unmined *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, txHash).Scan(&unmined))
	require.NotNil(t, unmined, "precondition: tx must have unmined_since in txs")
	require.Equal(t, unminedSinceVal, *unmined)

	// Now initialise the full schema (creates pending_unmined and runs the backfill).
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: the unmined tx was backfilled into pending_unmined with the correct unmined_since.
	var pu *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, txHash).Scan(&pu))
	require.NotNil(t, pu, "unmined tx must be backfilled into pending_unmined")
	require.Equal(t, unminedSinceVal, *pu, "unmined_since in pending_unmined must match txs value")
}

// TestSchema_PendingUnmined_BackfillSkipsConflicting verifies that a conflicting unmined tx
// is NOT backfilled into pending_unmined (the INSERT … WHERE conflicting=false guard).
func TestSchema_PendingUnmined_BackfillSkipsConflicting(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS spends CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_part_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_sweep_control CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS conflict_intents CASCADE`)

	_, err = pool.Exec(ctx, txsDDL)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, spendsDDL)
	require.NoError(t, err)
	for i := 0; i < numPartitions; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS txs_p%02d PARTITION OF txs FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS spends_p%02d PARTITION OF spends FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
	}

	// Insert a conflicting unmined tx (conflicting=true) — must NOT be backfilled.
	txHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33}
	const unminedSinceVal = int32(100)
	_, err = pool.Exec(ctx,
		`INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since)
         VALUES ($1, 1, 0, 100, 50, true, $2)`,
		txHash, unminedSinceVal)
	require.NoError(t, err)

	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: conflicting tx is NOT in pending_unmined.
	var exists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, txHash).Scan(&exists))
	require.False(t, exists, "conflicting tx must NOT be backfilled into pending_unmined")
}

// TestSchema_PendingUnmined_BackfillIdempotency verifies that calling createSchemaWithPoolFlag
// a second time is safe: the marker prevents the backfill from re-running and no duplicates
// are created.
func TestSchema_PendingUnmined_BackfillIdempotency(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)

	// First init: backfill runs, marker created.
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	var markerExists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_pu_backfill_marker')`).Scan(&markerExists))
	require.True(t, markerExists, "marker must exist after first init")

	// Second init: must succeed without error; backfill is skipped (marker already exists).
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert marker still exists exactly once (no duplication).
	var markerCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname='px_pu_backfill_marker'`).Scan(&markerCount))
	require.Equal(t, 1, markerCount, "idempotent re-init must not duplicate the marker")
}
