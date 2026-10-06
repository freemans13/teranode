package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
	sqlite3 "modernc.org/sqlite/lib"
)

// TestIsDeadlock_SQLiteSharedCacheTableLockIsRetryable produces the error two
// pooled connections raise against each other on the sqlitememory engine and
// asserts that both retry classifiers, isDeadlock for the spend batch and
// isLockError for create, treat it as retryable.
//
// InitSQLiteDB opens sqlitememory as a shared-cache in-memory database with a
// pool of five connections. A writer that wants a table another connection's
// transaction holds waits for it, and when the two wait for each other the
// engine breaks the cycle with SQLITE_LOCKED and the message "database table is
// locked: database is deadlocked", not SQLITE_BUSY's "database is locked". The
// legacy historical replay (services/legacy/netsync,
// TestLegacyHistoricalTestnetSync/default-settings) hits this collision when
// subtree validation runs a block's creates and spend batches in parallel over
// this store.
//
// This is a regression guard on the real engine error, not coverage of the
// extended-code mask. On modernc.org/sqlite v1.54.0 the cycle is reported with
// the plain primary code 6, not an extended LOCKED_SHAREDCACHE (262), so both
// classifiers already matched it before the mask was added to isLockError, and
// this test stays green with the mask removed.
// TestIsSQLiteLockCode_ExtendedCodesAreStillLocks below is the test that pins
// the mask. String fixtures for the same message are in spend_order_test.go and
// parent_outputs_test.go; this test exists because only a real *sqlite.Error
// reaches the typed code arms.
func TestIsDeadlock_SQLiteSharedCacheTableLockIsRetryable(t *testing.T) {
	ctx := context.Background()

	db, err := usql.Open("sqlite", "file:is_deadlock_sqlite_test?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	db.SetMaxOpenConns(5)

	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)`)
	require.NoError(t, err)

	// A shared-cache read lock is held for the rest of the transaction, so a
	// transaction that has read a table keeps every other writer of it waiting.
	// That is the spend batch's shape: its SELECT over outputs holds the table
	// while a create's transaction wants to write it.
	begin := func(table string) *sql.Tx {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		txn, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)

		var n int
		require.NoError(t, txn.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&n))

		return txn
	}

	// Each transaction holds a read lock on one table and then wants to write
	// the other's, so each waits for the other and the engine breaks the cycle.
	holdsT := begin("t")
	holdsU := begin("u")

	type outcome struct {
		txn *sql.Tx
		err error
	}

	results := make(chan outcome, 2)

	go func() {
		_, err := holdsT.ExecContext(ctx, `INSERT INTO u (v) VALUES (2)`)
		results <- outcome{txn: holdsT, err: err}
	}()
	go func() {
		_, err := holdsU.ExecContext(ctx, `INSERT INTO t (v) VALUES (2)`)
		results <- outcome{txn: holdsU, err: err}
	}()

	var refused outcome

	select {
	case refused = <-results:
	case <-time.After(30 * time.Second):
		t.Fatal("neither writer was refused within 30s: the engine did not report the lock cycle")
	}

	require.Error(t, refused.err, "one writer of the cycle must be refused")
	require.Contains(t, refused.err.Error(), "table is locked", "the engine must have raised the shared-cache table lock, not something else: %v", refused.err)

	// Release the refused transaction so the other writer's wait ends, then
	// release that one too. Neither outcome is the claim.
	require.NoError(t, refused.txn.Rollback())

	select {
	case other := <-results:
		_ = other.txn.Rollback()
	case <-time.After(30 * time.Second):
		t.Fatal("the surviving writer did not finish once the refused transaction rolled back")
	}

	require.True(t, isDeadlock(refused.err), "the spend batch must retry a shared-cache table lock, got a non-retryable classification for: %v", refused.err)
	require.True(t, isLockError(refused.err), "the create path must retry the same shared-cache table lock as the spend path, got a non-retryable classification for: %v", refused.err)
}

// TestIsSQLiteLockCode_ExtendedCodesAreStillLocks pins the primary-code
// comparison that isDeadlock and isLockError share. isLockError's *sqlite.Error
// arm used to compare the whole code against SQLITE_BUSY and SQLITE_LOCKED and
// return without reaching its string fallback, so a BUSY_SNAPSHOT (517) from a
// WAL snapshot conflict or a LOCKED_SHAREDCACHE (262) was not retried while the
// plain codes were. Reverting isSQLiteLockCode to a whole-code compare fails
// the three extended rows below.
func TestIsSQLiteLockCode_ExtendedCodesAreStillLocks(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want bool
	}{
		{"busy", sqlite3.SQLITE_BUSY, true},
		{"locked", sqlite3.SQLITE_LOCKED, true},
		{"busy_snapshot 517", sqlite3.SQLITE_BUSY_SNAPSHOT, true},
		{"busy_recovery 261", sqlite3.SQLITE_BUSY_RECOVERY, true},
		{"locked_sharedcache 262", sqlite3.SQLITE_LOCKED_SHAREDCACHE, true},
		{"error 1", sqlite3.SQLITE_ERROR, false},
		{"constraint 19", sqlite3.SQLITE_CONSTRAINT, false},
		{"constraint_unique 2067", sqlite3.SQLITE_CONSTRAINT_UNIQUE, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSQLiteLockCode(tc.code))
		})
	}
}
