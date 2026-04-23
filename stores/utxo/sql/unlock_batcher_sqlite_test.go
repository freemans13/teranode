package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// spendAllOutputs spends every output of tx by creating a new spending
// transaction for each output and calling store.Spend. It is used in DAH
// tests to make a transaction fully spent before unlocking.
func spendAllOutputs(t *testing.T, ctx context.Context, store *Store, tx *bt.Tx, spendHeight uint32) {
	t.Helper()
	txHash := *tx.TxIDChainHash()
	for i, out := range tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(
			txHash.String(), uint32(i),
			out.LockingScript.String(),
			out.Satoshis,
		))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
		_, err := store.Spend(ctx, spendTx, spendHeight)
		require.NoError(t, err)
	}
}

// TestUnlockBatcher_SQLite_Wired verifies the unlock batcher is constructed
// for the SQLite engine when LockedBatcherSize > 1.
func TestUnlockBatcher_SQLite_Wired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	// Verify the precondition this test is asserting on, so the test fails
	// loudly if the default changes rather than silently bypassing the check.
	require.Greater(t, store.settings.UtxoStore.LockedBatcherSize, 1,
		"test precondition: LockedBatcherSize must be > 1 for this wiring check")
	require.NotNil(t, store.unlockBatcher,
		"unlockBatcher must be initialised for the sqlite engine when LockedBatcherSize > 1")
}

// TestUnlockBatcher_SQLite_DAH verifies the batched unlock path correctly
// recalculates delete_at_height for a fully-spent, mined, on-longest-chain
// transaction on SQLite.
func TestUnlockBatcher_SQLite_DAH(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)
	require.NotNil(t, store.unlockBatcher,
		"unlockBatcher must be initialised so this test exercises the batched unlock path")
	require.NoError(t, store.SetBlockHeight(1000))

	_, err := store.Create(ctx, tests.ParentTx, 999)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, 1000,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: 1000, SubtreeIdx: 0, OnLongestChain: true,
		}),
	)
	require.NoError(t, err)

	txHash := *tests.Tx.TxIDChainHash()

	// Spend all outputs via the normal Spend path. PR #729's DAH-on-Spend
	// logic also recomputes DAH here; SetLocked(true) then clears it, and
	// SetLocked(false) through the batcher should restore it.
	spendAllOutputs(t, ctx, store, tests.Tx, 1001)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{txHash}, true))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{txHash}, false))

	var dah *int64
	err = store.db.QueryRowContext(ctx,
		"SELECT delete_at_height FROM transactions WHERE hash = $1",
		txHash[:]).Scan(&dah)
	require.NoError(t, err)
	require.NotNil(t, dah, "DAH must be set after batched unlock of fully-spent mined tx")
	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.Equal(t, int64(store.blockHeight.Load()+1+retention), *dah)
}
