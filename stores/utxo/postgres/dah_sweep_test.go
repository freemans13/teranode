package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDAHSchemaObjectsExist(t *testing.T) {
	store, ctx := setupTestStore(t)

	for _, q := range []struct{ name, sql string }{
		{"spends.spent_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='spends_p00' AND column_name='spent_at_height'`},
		{"txs.mined_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='txs_p00' AND column_name='mined_at_height'`},
		{"brin spends", `SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_spent_at_height_brin'`},
		{"brin txs", `SELECT 1 FROM pg_indexes WHERE indexname='txs_p00_mined_at_height_brin'`},
		{"dah_watermark table", `SELECT 1 FROM information_schema.tables WHERE table_name='dah_watermark'`},
		{"dah_watermark seed row", `SELECT last_swept_height FROM dah_watermark WHERE id = 1`},
	} {
		var ok int
		err := store.pool.QueryRow(ctx, q.sql).Scan(&ok)
		require.NoError(t, err, "missing schema object: %s", q.name)
	}
}
