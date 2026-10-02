package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

// TestIsDeadlock_SQLiteSharedCacheTableLockIsRetryable produces the error two
// pooled connections raise against each other on the sqlitememory engine and
// asserts the spend batch's retry classifier recognises it.
//
// InitSQLiteDB opens sqlitememory as a shared-cache in-memory database with a
// pool of five connections. A writer that wants a table another connection's
// transaction holds waits for it, and when the two wait for each other the
// engine breaks the cycle with SQLITE_LOCKED and the message "database table is
// locked: database is deadlocked", not SQLITE_BUSY's "database is locked".
// isDeadlock matched only the latter string, so sendSpendBatch's retry, written
// for exactly this collision, never fired on it: the batch was aborted and every
// item got "[Spend] batch aborted due to previous DB error". Create's isLockError
// in the same file matches the code, so the store retried its creates and not
// its spends. Found by the legacy historical replay (services/legacy/netsync,
// TestLegacyHistoricalTestnetSync/default-settings), where subtree validation
// validates a block's transactions in parallel over this store and a create's
// transaction and a spend batch's transaction each want the other's table.
//
// The error is produced for real rather than written as a string, because the
// SQLite arm of isDeadlock matches the result code on a *sqlite.Error, which a
// string fixture cannot carry. A plain-string error falls through to the
// "database is locked" substring fallback in both the old and the new
// isDeadlock, and "database table is locked: database is deadlocked" does not
// contain it, so a string fixture is red on both and proves nothing about the
// fix. Only isLockError's "deadlock" substring fallback, the secondary assert
// below, would accept a string.
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
	require.True(t, isLockError(refused.err), "the create path already classifies it as a lock error; the two must agree")
}
