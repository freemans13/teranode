package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newSQLiteStoreProductionBatchers opens a sqlitememory store with the batchers
// as production runs them: drain mode off, so a spend waits for the batcher's
// timer or a full batch rather than firing at once. The other SQLite tests turn
// drain mode on, which hides how the store behaves between those two.
func newSQLiteStoreProductionBatchers(t *testing.T) utxostore.Store {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherDrainMode = false

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return db
}

// The shared SpendAndCreateMulti suite holds on SQLite with production batchers
// and the full per-level concurrency: overlapping spend transactions that fail
// one another with a table-lock error are retried, and every outcome matches.
func TestSpendAndCreateMultiSQLiteProductionBatchers(t *testing.T) {
	spendAndCreateMultiSuite(t, newSQLiteStoreProductionBatchers)
}

// A level is written as wide on SQLite as on Postgres. Written one transaction
// at a time, each spend waited out the batcher timer alone: 300 independent
// transactions took 3.1s against 56ms as concurrent SpendAndCreate calls, and
// on regtest every block goes down this path. Serial writing has a floor the
// machine cannot lower, 300 waits on the 10ms timer, so the bound is set well
// under that floor and well over the concurrent time under the race detector.
func TestSpendAndCreateMultiSQLiteWritesALevelConcurrently(t *testing.T) {
	ctx := context.Background()
	db := newSQLiteStoreProductionBatchers(t)

	const width = 300

	w := tests.BuildMultiWorkload(t, 0x61, 1, width)
	w.StoreRoots(t, db, 99)

	start := time.Now()

	results, err := db.SpendAndCreateMulti(ctx, w.Txs, 100, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)

	elapsed := time.Since(start)

	for i, r := range results {
		require.Equal(t, utxostore.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}

	// 300 serial waits on the 10ms batcher timer are 3s at the very least;
	// concurrently the level is one or two batches, 55ms, or 1.1s under -race.
	require.Less(t, elapsed, 2*time.Second, "a level of %d independent transactions took %v: written serially", width, elapsed)
}
