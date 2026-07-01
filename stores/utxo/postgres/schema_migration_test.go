package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchema_SpentProgressColumns asserts that:
//  1. Both counter columns (spent_progress, last_spend_height) exist on txs.
//  2. Neither column is covered by any btree index on txs — HOT-update safety.
func TestSchema_SpentProgressColumns(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx := context.Background()

	// --- assertion 1: both columns must be present ---
	var n int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_progress', 'last_spend_height')`).Scan(&n))
	require.Equal(t, 2, n, "txs must have both spent_progress and last_spend_height columns")

	// --- assertion 2: neither column may appear in any btree index on txs ---
	// Checks pg_index + pg_attribute across all partitions inheriting from txs.
	var idx int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_index i
		JOIN pg_class c  ON c.oid = i.indrelid
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_am am    ON am.oid = (SELECT relam FROM pg_class WHERE oid = i.indexrelid)
		WHERE (c.relname = 'txs' OR c.relname LIKE 'txs_p%')
		  AND am.amname = 'btree'
		  AND a.attname IN ('spent_progress', 'last_spend_height')`).Scan(&idx))
	require.Zero(t, idx, "spent_progress and last_spend_height must NOT be covered by any btree index on txs (HOT safety)")
}
