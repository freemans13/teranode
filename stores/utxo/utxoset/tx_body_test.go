package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTxBodyIsRangePartitionedOnCreatedHeight pins the shape that makes the body window
// droppable.
//
// The body is the only part of a transaction whose life is bounded by a horizon rather than
// by its coins. Everything else on tx_ident is pinned for as long as any output is unspent,
// at any age, because the validator reads the parent's block ids and heights for every input.
// Keeping the body there too would make the transaction archive permanent for the whole
// pinned population, measured at 136 GB of out-of-line storage today against roughly 95 GB of
// free space on the mainnet box, and projecting past the disk at the tip.
//
// Range partitioning on created_height is what turns reclaim into dropping a file rather than
// deleting rows: a drop returned 883 MB to the operating system for 228 KB of crash-recovery
// journal, where the equivalent delete plus vacuum returned 128 KB for 1.75 GB.
func TestTxBodyIsRangePartitionedOnCreatedHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	var strategy string
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT partstrat::text FROM pg_partitioned_table WHERE partrelid = 'tx_body'::regclass`).Scan(&strategy))
	require.Equal(t, "r", strategy, "range partitioned, so a whole window can be dropped at once")
}

// TestTxBodyHasASentinelPartitionBelowEveryLiveWindow covers the case that takes transaction
// intake down completely if it is missing.
//
// A deep invalidation drops the tip below the lowest live partition, and a mempool create
// files at tip plus one. With no partition covering that height the insert is refused
// outright, so the node stops accepting transactions. The catch-all must be an explicit range
// from MINVALUE, never a DEFAULT partition: on 18.6 a default partition makes
// DETACH PARTITION ... CONCURRENTLY impossible on every other partition of the table, and the
// concurrent form is what keeps the drop from blocking readers.
func TestTxBodyHasASentinelPartitionBelowEveryLiveWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	var isDefault bool
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
              JOIN pg_inherits i ON i.inhrelid = c.oid
             WHERE i.inhparent = 'tx_body'::regclass
               AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT')`).Scan(&isDefault))
	require.False(t, isDefault, "a DEFAULT partition blocks concurrent detach on every sibling")

	// A create far below any live window must still land somewhere.
	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 1)
	require.NoError(t, err, "a create below the lowest live window must not be refused")

	h := tx.TxIDChainHash()

	var body []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT raw_tx FROM tx_body WHERE txid = $1`, h[:]).Scan(&body))
	require.Equal(t, tx.Bytes(), body)
}

// TestCreateWritesTheBody is the round trip. The body is the one thing that cannot be
// reconstructed: unlocking scripts exist only in the transaction, and the node keeps almost
// no blocks.
func TestCreateWritesTheBody(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 4_242)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	var body []byte
	var filedAt int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT created_height, raw_tx FROM tx_body WHERE txid = $1`, h[:]).Scan(&filedAt, &body))

	require.Equal(t, tx.Bytes(), body)
	require.Equal(t, int32(700_000), filedAt,
		"the body is filed by the height tx_ident records, so the two agree on where to look")
}

// TestCreateDoesNotWriteASecondBodyForADuplicate: the claim gates the body too, or a
// re-applied block writes the bytes again for every transaction in it.
func TestCreateDoesNotWriteASecondBodyForADuplicate(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100)
	require.Error(t, err)

	h := tx.TxIDChainHash()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]).Scan(&n))
	require.Equal(t, 1, n, "a duplicate must not file a second body, at a second height")
}
