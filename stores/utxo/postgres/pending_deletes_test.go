package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestSchema_PendingDeletes_FlagOn(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_deletes CASCADE`)
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, true)) // flag ON

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_deletes_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 8, n, "8 pending_deletes leaves")

	var hasBrin bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.False(t, hasBrin, "BRIN dropped when flag on")
}

func TestSchema_PendingDeletes_FlagOff(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Ensure clean slate: drop pending_deletes if a prior FlagOn test left it.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_deletes CASCADE`)
	// Ensure BRIN is absent so we can confirm creation.
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_delete_at_height`)

	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false)) // flag OFF

	// No pending_deletes leaves should exist.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_deletes_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 0, n, "no pending_deletes leaves when flag off")

	// BRIN index must be present.
	var hasBrin bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.True(t, hasBrin, "BRIN present when flag off")
}
