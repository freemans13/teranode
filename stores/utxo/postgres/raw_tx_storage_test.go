package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestRawTxForcedOutOfLine verifies the WAL-reduction storage tuning: after schema
// creation, txs.raw_tx is STORAGE EXTERNAL and each txs leaf carries
// toast_tuple_target=128. Together these push the ~1 KB write-once raw_tx blob
// out-of-line into TOAST, so the SetMined/DAH row rewrites stop re-logging the
// untouched blob — measured ~63% less WAL per stamp on mainnet (4.6 KB -> 1.7 KB).
// spends has no toastable blob, so it must NOT get the toast_tuple_target override.
func TestRawTxForcedOutOfLine(t *testing.T) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	logger := ulogger.TestLogger{}

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs, spends CASCADE`)
	require.NoError(t, createSchemaInternalFF(ctx, pool, nil, logger, 50, false))

	// raw_tx must be EXTERNAL (out-of-line, uncompressed) — attstorage 'e'. SET STORAGE
	// on the parent recurses, so the leaf reflects it.
	var storage string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT attstorage::text FROM pg_attribute
		WHERE attrelid = 'txs_p00'::regclass AND attname = 'raw_tx'`).Scan(&storage))
	require.Equal(t, "e", storage, "raw_tx must be STORAGE EXTERNAL on the txs leaf")

	// Each txs leaf must carry toast_tuple_target=128 so its ~1 KB rows actually toast
	// (STORAGE EXTERNAL alone only toasts rows over the 2048-byte default target).
	require.True(t, leafReloptions(t, pool, ctx, "txs_p00")["toast_tuple_target=128"],
		"txs leaf must set toast_tuple_target=128 to force raw_tx out-of-line")

	// spends has no large blob column, so it must NOT get toast_tuple_target.
	require.False(t, leafReloptions(t, pool, ctx, "spends_p00")["toast_tuple_target=128"],
		"spends must not get toast_tuple_target (no toastable blob)")
}
