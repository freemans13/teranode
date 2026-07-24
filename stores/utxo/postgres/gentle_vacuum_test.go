package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// leafReloptions returns the reloptions array of a leaf partition as a set for
// membership checks.
func leafReloptions(t *testing.T, pool *pgxpool.Pool, ctx context.Context, leaf string) map[string]bool {
	t.Helper()

	var opts []string

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT coalesce(reloptions, '{}') FROM pg_class WHERE relname=$1`, leaf).Scan(&opts))

	set := make(map[string]bool, len(opts))
	for _, o := range opts {
		set[o] = true
	}

	return set
}

// toastReloptions returns the reloptions of a leaf's TOAST relation (toast.* params
// are stored there with the prefix stripped).
func toastReloptions(t *testing.T, pool *pgxpool.Pool, ctx context.Context, leaf string) map[string]bool {
	t.Helper()

	var opts []string

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT coalesce(tc.reloptions, '{}')
		   FROM pg_class c JOIN pg_class tc ON tc.oid = c.reltoastrelid
		  WHERE c.relname=$1`, leaf).Scan(&opts))

	set := make(map[string]bool, len(opts))
	for _, o := range opts {
		set[o] = true
	}

	return set
}

// TestGentleVacuum_ProfileApplied verifies the utxostore_postgresGentleVacuum
// override: with it OFF the aggressive default lands (txs cost_delay=0), with it ON
// the throttled profile lands on the leaf AND its TOAST (cost_delay=20). This is the
// permanence contract — the store re-applies these on every startup, so a deployment
// that reclaims out-of-band keeps its throttle instead of reverting to aggressive.
func TestGentleVacuum_ProfileApplied(t *testing.T) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	logger := ulogger.TestLogger{}

	// AGGRESSIVE (default): cost_delay=0 on txs leaves.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs, spends CASCADE`)
	require.NoError(t, createSchemaInternalFF(ctx, pool, nil, logger, 50, false))

	aggr := leafReloptions(t, pool, ctx, "txs_p00")
	require.True(t, aggr["autovacuum_vacuum_cost_delay=0"],
		"default profile must keep txs autovacuum unthrottled (cost_delay=0)")
	require.True(t, aggr["autovacuum_vacuum_cost_limit=8000"],
		"default profile must keep txs cost_limit=8000")

	// GENTLE: cost_delay=20 on txs leaves AND their toast.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs, spends CASCADE`)
	require.NoError(t, createSchemaInternalFF(ctx, pool, nil, logger, 50, true))

	gentle := leafReloptions(t, pool, ctx, "txs_p00")
	require.True(t, gentle["autovacuum_vacuum_cost_delay=20"],
		"gentle profile must throttle the txs leaf (cost_delay=20)")
	require.True(t, gentle["autovacuum_vacuum_cost_limit=150"],
		"gentle profile must set txs cost_limit=150")
	require.False(t, gentle["autovacuum_vacuum_cost_delay=0"],
		"gentle profile must NOT leave the aggressive cost_delay=0")

	gentleToast := toastReloptions(t, pool, ctx, "txs_p00")
	require.True(t, gentleToast["autovacuum_vacuum_cost_delay=20"],
		"gentle profile must also throttle the TOAST relation (where raw_tx lives)")

	// spends leaves are throttled too.
	spends := leafReloptions(t, pool, ctx, "spends_p00")
	require.True(t, spends["autovacuum_vacuum_cost_delay=20"],
		"gentle profile must throttle the spends leaves")
}
