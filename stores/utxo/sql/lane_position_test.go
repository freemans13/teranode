package sql

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startLanePositionPostgres starts a Postgres container and returns its URL.
func startLanePositionPostgres(t *testing.T) *url.URL {
	t.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pgContainer.Terminate(ctx))
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	return dbURL
}

func lanePositionSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true
	tSettings.Pruner.UTXODefensiveEnabled = false

	return tSettings
}

// sqlLaneRules: every ErrTxExists the SQL store returns proves the position unwritten (see
// markRefusedCreate), with a block or without, and a list goes through SpendAndCreate.
var sqlLaneRules = tests.LaneReturnRules{Exists: true, MinedExists: true, ListExists: true}

// sqlLaneRows counts the transaction rows carrying any of positions, the one column the store
// writes a position to.
func sqlLaneRows(s *Store) tests.LanePositionCounter {
	return func(t testing.TB, positions []uint64) int {
		t.Helper()

		total := 0

		for _, p := range positions {
			var n int
			require.NoError(t, s.db.QueryRowContext(context.Background(),
				`SELECT count(*) FROM transactions WHERE lane_position = $1`, lane.ToSigned(p)).Scan(&n))

			total += n
		}

		return total
	}
}

func TestLanePositionContractSQLite(t *testing.T) {
	db, _ := setup(context.Background(), t)

	tests.LanePositionContract(t, db)
	t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, db, "sql") })
	t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, db, sqlLaneRules, sqlLaneRows(db)) })
}

// Every Postgres create path writes the lane position: the create batcher's
// pipelined CTEs, a single CTE, and the plain per-row INSERT.
func TestLanePositionContractPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	variants := []struct {
		name   string
		tweaks func(*settings.Settings)
	}{
		{"create batcher", func(*settings.Settings) {}},
		{"single CTE", func(s *settings.Settings) { s.UtxoStore.StoreBatcherSize = 1 }},
		{"plain insert", func(s *settings.Settings) {
			s.UtxoStore.StoreBatcherSize = 1
			s.UtxoStore.BatchSQLOperations = false
		}},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			dbURL := startLanePositionPostgres(t)

			tSettings := lanePositionSettings(t)
			v.tweaks(tSettings)

			db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
			require.NoError(t, err)

			tests.LanePositionContract(t, db)
			t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, db, "sql") })
			t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, db, sqlLaneRules, sqlLaneRows(db)) })
		})
	}
}

// BatchDecorate returns the lane position when the caller names it, and a Get
// that does not name it leaves the field zero.
func TestLanePositionBatchDecorateAndUnnamedField(t *testing.T) {
	ctx := context.Background()
	db, _ := setup(ctx, t)

	w := tests.BuildMultiWorkload(t, 0xe0, 1, 1)
	w.StoreRoots(t, db, 10)

	want := lane.Pack(200, 12345)

	_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 11, utxostore.WithLanePosition(want))
	require.NoError(t, err)

	hash := w.Txs[0].TxIDChainHash()

	items := []*utxostore.UnresolvedMetaData{{Hash: *hash, Idx: 0}}
	require.NoError(t, db.BatchDecorate(ctx, items, fields.LanePosition))
	require.NoError(t, items[0].Err)
	require.Equal(t, want, items[0].Data.LanePosition)

	md, err := db.Get(ctx, hash, fields.Fee)
	require.NoError(t, err)
	require.Equal(t, lane.None, md.LanePosition, "not read unless named")
}

const oldSQLiteTransactionsTable = `
CREATE TABLE transactions (
	 id               INTEGER PRIMARY KEY AUTOINCREMENT
	,hash             BLOB NOT NULL
	,version          BIGINT NOT NULL
	,lock_time        BIGINT NOT NULL
	,fee              BIGINT NOT NULL
	,size_in_bytes    BIGINT NOT NULL
	,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
	,frozen           BOOLEAN DEFAULT FALSE NOT NULL
	,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
	,locked           BOOLEAN DEFAULT FALSE NOT NULL
	,delete_at_height BIGINT
	,unmined_since    BIGINT
	,preserve_until   BIGINT
	,inserted_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

const oldPostgresTransactionsTable = `
CREATE TABLE transactions (
	 id               BIGSERIAL PRIMARY KEY
	,hash             BYTEA NOT NULL
	,version          BIGINT NOT NULL
	,lock_time        BIGINT NOT NULL
	,fee              BIGINT NOT NULL
	,size_in_bytes    BIGINT NOT NULL
	,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
	,frozen           BOOLEAN DEFAULT FALSE NOT NULL
	,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
	,locked           BOOLEAN DEFAULT FALSE NOT NULL
	,delete_at_height BIGINT
	,unmined_since    BIGINT
	,preserve_until   BIGINT
	,inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

// requireOldRowReadsNoLanePosition checks a store opened on a database whose
// transactions table predates lane_position: the old row reads lane.None and
// stays NULL (no backfill), and a new record keeps its lane position.
func requireOldRowReadsNoLanePosition(t *testing.T, inspect *sql.DB, store *Store, oldHash chainhash.Hash) {
	t.Helper()

	ctx := context.Background()

	md, err := store.Get(ctx, &oldHash, fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.None, md.LanePosition)

	var nulls int
	require.NoError(t, inspect.QueryRow(`SELECT count(*) FROM transactions WHERE lane_position IS NULL`).Scan(&nulls))
	require.Equal(t, 1, nulls, "the old row is not backfilled")

	w := tests.BuildMultiWorkload(t, 0xe1, 1, 1)
	w.StoreRoots(t, store, 10)

	want := lane.Pack(255, 77)

	_, _, err = store.SpendAndCreate(ctx, w.Txs[0], 11, utxostore.WithLanePosition(want))
	require.NoError(t, err)

	md, err = store.Get(ctx, w.Txs[0].TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, want, md.LanePosition)
}

func TestLanePositionMigrationSQLite(t *testing.T) {
	tSettings := lanePositionSettings(t)
	tSettings.DataFolder = t.TempDir()

	dbURL, err := url.Parse("sqlite:///lane_old_schema")
	require.NoError(t, err)

	// Build a database as an earlier release left it: no lane_position column.
	oldDB, err := sql.Open("sqlite", filepath.Join(tSettings.DataFolder, "lane_old_schema.db"))
	require.NoError(t, err)

	_, err = oldDB.Exec(oldSQLiteTransactionsTable)
	require.NoError(t, err)

	oldHash := chainhash.HashH([]byte("old row"))
	_, err = oldDB.Exec(`INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes) VALUES (?, 1, 0, 5, 100)`, oldHash[:])
	require.NoError(t, err)

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)

	requireOldRowReadsNoLanePosition(t, oldDB, store, oldHash)
	require.NoError(t, oldDB.Close())

	// Opening again finds the column already there.
	_, err = New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
}

func TestLanePositionMigrationPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	oldDB, err := sql.Open("pgx", dbURL.String())
	require.NoError(t, err)

	_, err = oldDB.Exec(oldPostgresTransactionsTable)
	require.NoError(t, err)

	oldHash := chainhash.HashH([]byte("old row"))
	_, err = oldDB.Exec(`INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes) VALUES ($1, 1, 0, 5, 100)`, oldHash[:])
	require.NoError(t, err)

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)

	requireOldRowReadsNoLanePosition(t, oldDB, store, oldHash)
	require.NoError(t, oldDB.Close())
}

// On a database from before lane_position, an open transaction on transactions
// keeps the ADD COLUMN from getting its lock. The store retries within
// laneColumnMigrationLimit and then refuses to open, rather than waiting with
// no deadline while every query on transactions queues behind it; a reader on
// another connection is never held for long. Once the transaction ends, the
// next attempt adds the column.
func TestLanePositionMigrationPostgresBoundsTheLockWait(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	oldDB, err := sql.Open("pgx", dbURL.String())
	require.NoError(t, err)

	defer func() { require.NoError(t, oldDB.Close()) }()

	_, err = oldDB.Exec(oldPostgresTransactionsTable)
	require.NoError(t, err)

	saved := laneColumnMigrationLimit
	t.Cleanup(func() { laneColumnMigrationLimit = saved })

	holder, err := oldDB.Begin()
	require.NoError(t, err)

	_, err = holder.Exec(`SELECT count(*) FROM transactions`)
	require.NoError(t, err)

	// A reader on its own connection, polling while the store retries. Each
	// read may queue behind one ALTER attempt, so at most about the 200ms
	// lock_timeout.
	readerDone := make(chan time.Duration)

	go func() {
		var worst time.Duration

		for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
			started := time.Now()

			var n int
			if err := oldDB.QueryRow(`SELECT count(*) FROM transactions`).Scan(&n); err != nil {
				worst = time.Hour
				break
			}

			worst = max(worst, time.Since(started))
			time.Sleep(20 * time.Millisecond)
		}
		readerDone <- worst
	}()

	laneColumnMigrationLimit = time.Second
	started := time.Now()

	_, err = New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not add lane_position column")
	require.Less(t, time.Since(started), 30*time.Second)
	require.Less(t, <-readerDone, time.Second)

	// The holder ends while the store is retrying: the column goes in.
	laneColumnMigrationLimit = time.Minute

	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = holder.Rollback()
	}()

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)

	var columns int
	require.NoError(t, oldDB.QueryRow(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'lane_position'`).Scan(&columns))
	require.Equal(t, 1, columns)
	require.NotNil(t, store)
}

// A conflicting create on the single-CTE path commits its record, then writes the parent links in a
// transaction of its own. When that second transaction dies on a deadlock, Create retries, and the
// retry meets the record the first attempt committed. That ErrTxExists is this create's own record,
// which carries the position, so the position must not go back for reuse.
func TestLanePositionNotReturnedAfterAConflictLinkDeadlock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	tSettings := lanePositionSettings(t)
	tSettings.UtxoStore.StoreBatcherSize = 1

	db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	require.Nil(t, db.createBatcher, "the test needs the single-CTE path")
	require.True(t, db.settings.UtxoStore.BatchSQLOperations, "the test needs the single-CTE path")

	deadlocks := 0
	conflictingChildrenFault = func() error {
		deadlocks++
		return &pgconn.PgError{Code: usql.PgErrDeadlockDetected, Message: "deadlock detected"}
	}

	t.Cleanup(func() { conflictingChildrenFault = nil })

	source := tests.NewReturningLanePositions(lane.Pack(46, 1), lane.Pack(46, 2))
	db.SetLanePositionSource(source)
	t.Cleanup(func() { db.SetLanePositionSource(nil) })

	tx := tests.BuildMultiWorkload(t, 0xb5, 1, 1).Roots[0]

	_, err = db.Create(context.Background(), tx, 960, utxostore.WithConflicting(true))
	require.ErrorIs(t, err, errors.ErrTxExists, "the retry meets the record the first attempt committed")
	require.Equal(t, 1, deadlocks, "only the first attempt reached the conflict links")

	require.Empty(t, source.Returned(), "the record carries the position")

	md, err := db.Get(context.Background(), tx.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(46, 1), md.LanePosition)
}
