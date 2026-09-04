package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCoinRowsCarryTheirParentsBlockFacts pins the two columns the read path relies on once
// a transaction's membership window has been dropped: the height and block of the
// transaction that made the coin. Both fixed width, placed before the variable-length
// script so alignment costs nothing.
func TestCoinRowsCarryTheirParentsBlockFacts(t *testing.T) {
	s, ctx := newTestStore(t)

	var names []string
	rows, err := s.pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		 WHERE table_name = 'utxo' ORDER BY ordinal_position`)
	require.NoError(t, err)

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	rows.Close()

	require.Contains(t, names, "mined_height")
	require.Contains(t, names, "block_id")

	idx := func(n string) int {
		for i, x := range names {
			if x == n {
				return i
			}
		}
		return -1
	}
	require.Less(t, idx("block_id"), idx("script"), "fixed-width columns go before the script so the row needs no padding for them")
}

// TestMembershipTableIsKeyedByTransactionIdFirst pins the key order. PostgreSQL needs the
// partition key inside a partitioned table's primary key but not at its head, and a
// height-leading key cannot be probed by transaction id without a skip scan over every
// height in the partition.
func TestMembershipTableIsKeyedByTransactionIdFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 700_000))

	var def string
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		 WHERE tablename = 'tx_mined' AND indexname = 'tx_mined_pkey'`).Scan(&def))
	require.Contains(t, def, "(txid, mined_height, block_id)")

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'tx_mined'::regclass`).Scan(&n))
	require.Equal(t, 1, n, "one 288-block window for height 700,000")
}

// TestDroppedMembershipWindowsCannotComeBack pins the floor: once a window is dropped, a
// create for a height inside it fails instead of recreating the window, because a block
// re-offered after its window retired would otherwise claim every transaction in it afresh.
func TestDroppedMembershipWindowsCannotComeBack(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	floor, err := s.txMinedFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), floor, "window 0 (heights 0-287) was dropped, so the floor is window 1")

	err = s.ensureTxMinedPartition(ctx, 100)
	require.Error(t, err, "recreating a dropped window would let a stale block double its coins")

	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000), "a live window is still fine")
}
