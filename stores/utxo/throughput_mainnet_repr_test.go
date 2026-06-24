package utxo_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCollectReprSample_ReturnsShape(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("no postgres")
	}
	defer pool.Close()
	require.NoError(t, resetReprStats(ctx, pool))
	s, err := collectReprSample(ctx, pool)
	require.NoError(t, err)
	require.GreaterOrEqual(t, s.bufHitPct, 0.0)
	require.LessOrEqual(t, s.bufHitPct, 100.0)
	require.GreaterOrEqual(t, s.liveRows, int64(0))
}

type reprSample struct {
	atUnix            int64
	liveRows          int64
	txsBytes          int64
	spendsBytes       int64
	sliceMs, doomedMs float64
	deleteMs          float64
	bufHitPct         float64
}

func resetReprStats(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `SELECT pg_stat_reset()`)
	return err
}

func collectReprSample(ctx context.Context, pool *pgxpool.Pool) (reprSample, error) {
	var s reprSample
	row := pool.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(pg_total_relation_size(c.oid)) FILTER (WHERE c.relname LIKE 'txs%'),0),
		  COALESCE(SUM(pg_total_relation_size(c.oid)) FILTER (WHERE c.relname LIKE 'spends%'),0),
		  COALESCE(SUM(c.reltuples::bigint) FILTER (WHERE c.relname LIKE 'txs_p%'),0)
		FROM pg_class c WHERE c.relkind IN ('r','p')`)
	if err := row.Scan(&s.txsBytes, &s.spendsBytes, &s.liveRows); err != nil {
		return s, err
	}
	// per-class exec time
	classRow := pool.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE 'WITH slice%'),0),
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE '%doomed%'),0),
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE 'DELETE FROM %'
		           OR query LIKE 'WITH del%'),0)
		FROM pg_stat_statements`)
	if err := classRow.Scan(&s.sliceMs, &s.doomedMs, &s.deleteMs); err != nil {
		return s, err
	}
	hitRow := pool.QueryRow(ctx, `
		SELECT COALESCE(100.0*blks_hit/NULLIF(blks_hit+blks_read,0),0)
		FROM pg_stat_database WHERE datname=current_database()`)
	if err := hitRow.Scan(&s.bufHitPct); err != nil {
		return s, err
	}
	return s, nil
}
