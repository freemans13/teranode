package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetServesAMinedTransactionFromTheMembershipTable: no identity row exists, so the
// block ids, heights and subtree index come from tx_mined, and the body from its window.
func TestGetServesAMinedTransactionFromTheMembershipTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, []uint32{700_100}, got.BlockHeights)
	require.Equal(t, []int{3}, got.SubtreeIdxs)
	require.NotNil(t, got.Tx, "the body is inside its window")
	require.Equal(t, uint32(0), got.UnminedSince)
}

// TestGetServesAnOldParentFromItsCoinOnceTheWindowIsGone: the membership window was dropped,
// the transaction still has a live coin, and the coin's block facts are the answer. Fee,
// size, inputs and subtree index are zero, which is what a pruned SV Node can say too.
func TestGetServesAnOldParentFromItsCoinOnceTheWindowIsGone(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, []uint32{100}, got.BlockHeights)
	require.Equal(t, []int{0}, got.SubtreeIdxs)
	require.Equal(t, uint64(0), got.Fee)
	require.Nil(t, got.TxInpoints.ParentTxHashes)
}

// TestGetReportsNotFoundForAFullySpentTransactionPastItsWindow is aerospike's behaviour after
// its delete-at-height and what the shared suite's pruning test requires.
func TestGetReportsNotFoundForAFullySpentTransactionPastItsWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 100)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	_, err = s.Get(ctx, parent.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestBatchDecorateFollowsTheSameOrder: one mempool row, one membership row, one coin-only
// parent and one unknown, in a single call.
func TestBatchDecorateFollowsTheSameOrder(t *testing.T) {
	s, ctx := newTestStore(t)

	mempool := mkTx(t, 1, 5_001)
	_, err := s.Create(ctx, mempool, 700_200)
	require.NoError(t, err)

	mined := mkTx(t, 1, 5_002)
	_, err = s.Create(ctx, mined, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	old := mkTx(t, 1, 5_003)
	_, err = s.Create(ctx, old, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	unknown := mkTx(t, 1, 5_004)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *mempool.TxIDChainHash()}, {Hash: *mined.TxIDChainHash()},
		{Hash: *old.TxIDChainHash()}, {Hash: *unknown.TxIDChainHash()},
	}
	require.NoError(t, s.BatchDecorate(ctx, items, fields.BlockIDs))

	require.NoError(t, items[0].Err)
	require.Empty(t, items[0].Data.BlockIDs)
	require.NoError(t, items[1].Err)
	require.Equal(t, []uint32{42}, items[1].Data.BlockIDs)
	require.NoError(t, items[2].Err)
	require.Equal(t, []uint32{7}, items[2].Data.BlockIDs)
	require.True(t, errors.Is(items[3].Err, errors.ErrTxNotFound))
}
