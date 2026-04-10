package queue

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testDSN = "postgresql://teranode:teranode@localhost:5432/teranode_test"

func setupTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()

	// Clean slate -- drop all tables (including old v3 tables if present)
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgresqueue"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Stop()
	})

	return store, ctx
}

func TestSchemaCreation(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Verify all 7 snapshot tables exist by querying them
	tables := []string{"transactions", "inputs", "outputs", "spends", "tx_state", "block_ids", "conflicting_children"}
	for _, table := range tables {
		var count int
		err := store.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		require.NoError(t, err, "table %s should exist", table)
	}

	// Verify partitions exist (64 per table = 448 total)
	for _, table := range tables {
		var partCount int
		err := store.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_inherits
			JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
			WHERE parent.relname = $1
		`, table).Scan(&partCount)
		require.NoError(t, err, "should be able to count partitions for %s", table)
		require.Equal(t, 64, partCount, "table %s should have 64 partitions", table)
	}

	// Verify tx_state partial indexes exist
	var idxCount int
	err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_indexes
		WHERE indexname IN ('px_unmined_since', 'px_delete_at_height')
	`).Scan(&idxCount)
	require.NoError(t, err)
	require.Equal(t, 2, idxCount, "tx_state should have 2 partial indexes")
}

func TestHealth(t *testing.T) {
	store, ctx := setupTestStore(t)

	code, name, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, code)
	require.Equal(t, "Queue UTXO Store", name)
}

func TestBlockState(t *testing.T) {
	store, _ := setupTestStore(t)

	require.Equal(t, uint32(0), store.GetBlockHeight())
	require.Equal(t, uint32(0), store.GetMedianBlockTime())

	err := store.SetBlockHeight(12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), store.GetBlockHeight())

	err = store.SetMedianBlockTime(1700000000)
	require.NoError(t, err)
	require.Equal(t, uint32(1700000000), store.GetMedianBlockTime())

	state := store.GetBlockState()
	require.Equal(t, uint32(12345), state.Height)
	require.Equal(t, uint32(1700000000), state.MedianTime)
}
