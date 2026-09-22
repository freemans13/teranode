package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestUTXORowsCarryTheirParentsBlockFacts pins the two columns the read path relies on once
// a transaction's containment window has been dropped: the height and block of the
// transaction that made the UTXO. Both fixed width, placed before the variable-length
// script so alignment costs nothing.
func TestUTXORowsCarryTheirParentsBlockFacts(t *testing.T) {
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
	require.Error(t, err, "recreating a dropped window would let a stale block double its UTXOs")

	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000), "a live window is still fine")
}

// TestInterimGuardRefusesADropWhileAnIdentityRowExists is the design's ST-29: on a build with
// containment changed and no deep stamp yet, a window due to drop is refused while tx_ident
// holds any row, the refusal is counted, and the partition stays attached. Delete the identity
// row and the same window drops.
//
// The soak cannot show this, because below the checkpoint tx_ident is empty and the guard never
// fires there, so this is what proves the guard works.
func TestInterimGuardRefusesADropWhileAnIdentityRowExists(t *testing.T) {
	s, ctx := newTestStore(t)

	// A block-path transaction puts window 0 on the table.
	filler := mkTx(t, 1, 1_111)
	_, err := s.Create(ctx, filler, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// A transaction seen before its block, recorded in the same window. Its UTXO is at (0,0)
	// and only the deep stamp will ever change that, so the window is the one place its
	// block lives.
	seen := mkTx(t, 1, 5_000)
	_, err = s.Create(ctx, seen, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(seen), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	before := testutil.ToFloat64(interimDropRefused)

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err, "a refusal is a logged skip, not an error the pruner would repeat every block")
	require.Equal(t, 0, dropped)
	require.Equal(t, before+1, testutil.ToFloat64(interimDropRefused), "and it is counted")
	require.Equal(t, 1, minedRows(t, s, ctx, seen), "the window is still attached")

	floor, err := s.txMinedFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floor, "and the floor has not moved")

	dropIdentityRow(t, s, ctx, seen)

	dropped, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "with tx_ident empty the same window drops")
	require.Equal(t, 0, minedRows(t, s, ctx, seen))
}

// TestInterimDropRaisesAllThreeFloorsTogether: the floor row carries an ordering constraint,
// 288 x floor <= stamp_complete_floor <= stamp_fence, and the interim drop has to satisfy it
// on a database that has never had a stamp. So it raises all three. When the deep stamp of
// build step 5 starts on such a database its pass begins at stamp_complete_floor, which is
// exactly the dropped floor.
func TestInterimDropRaisesAllThreeFloorsTogether(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 400))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 5_000))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 2, dropped, "windows 0 and 1 go, window 17 stays")

	var floor, fence, complete int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT floor, stamp_fence, stamp_complete_floor FROM tx_mined_floor WHERE id = 0`).
		Scan(&floor, &fence, &complete))

	require.Equal(t, int32(2), floor, "a window number: the highest dropped plus one")
	require.Equal(t, int32(2*TxMinedPartitionBlocks), complete, "a height: the dropped windows' upper bound")
	require.Equal(t, int32(2*TxMinedPartitionBlocks), fence)
}

// TestFloorRowRefusesAFloorAboveTheFence pins the constraint itself, so that a later build which
// raised floor alone would be refused by postgres rather than silently leaving the three values
// out of order.
func TestFloorRowRefusesAFloorAboveTheFence(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.pool.Exec(ctx, `UPDATE tx_mined_floor SET floor = 3 WHERE id = 0`)
	requireCheckViolation(t, err, "288 x floor above stamp_complete_floor must be refused")
}

// TestDroppingAWindowClearsTheEnsureCache pins the interaction between the two halves of
// window management, which is invisible until they disagree.
//
// ensureTxMinedPartition remembers the last window it created and returns early when the next
// create lands in the same one. That memory can outlive the window itself: a fork stamp at an
// old height caches it, the pruner then drops it, and a later stamp at that height hits the
// cache, skips the floor read, and the INSERT fails with "no partition of relation" instead of
// the loud, explained refusal the floor exists to give.
func TestDroppingAWindowClearsTheEnsureCache(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	require.Error(t, s.ensureTxMinedPartition(ctx, 100),
		"the cached window is gone, so the floor has to be re-read and the create refused")
}
