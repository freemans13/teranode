package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupSQLiteBatched builds a sqlitememory store with the create batcher
// enabled (StoreBatcherSize > 1) so the SQLite create-batch path runs.
func setupSQLiteBatched(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true
	tSettings.UtxoStore.StoreBatcherSize = 8
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5

	storeURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	require.NotNil(t, store.createBatcher,
		"createBatcher must be initialised for sqlite when StoreBatcherSize > 1")

	return store, ctx
}

// TestCreateBatcher_SQLite_Basic creates two distinct txs through the batcher
// and verifies each is retrievable afterwards.
func TestCreateBatcher_SQLite_Basic(t *testing.T) {
	store, ctx := setupSQLiteBatched(t)

	txs := []struct {
		tx *bt.Tx
	}{{tests.ParentTx}, {tests.Tx}}

	for _, c := range txs {
		_, err := store.Create(ctx, c.tx, 1000)
		require.NoError(t, err)
	}

	for _, c := range txs {
		meta, err := store.Get(ctx, c.tx.TxIDChainHash())
		require.NoError(t, err)
		require.NotNil(t, meta)
	}
}

// TestCreateBatcher_SQLite_Duplicate verifies the batched path returns
// ErrTxExists when the same tx is created twice.
func TestCreateBatcher_SQLite_Duplicate(t *testing.T) {
	store, ctx := setupSQLiteBatched(t)

	_, err := store.Create(ctx, tests.Tx, 1000)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, 1000)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists),
		"second Create of same tx should return ErrTxExists, got: %v", err)
}

// TestCreateBatcher_SQLite_Mined verifies that txs created with
// MinedBlockInfo through the batcher have their block_ids populated.
func TestCreateBatcher_SQLite_Mined(t *testing.T) {
	store, ctx := setupSQLiteBatched(t)

	_, err := store.Create(ctx, tests.Tx, 1000,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: 1000, SubtreeIdx: 0, OnLongestChain: true,
		}),
	)
	require.NoError(t, err)

	meta, err := store.Get(ctx, tests.Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{100}, meta.BlockIDs)
}

// TestSharedSuite_SQLite_Batched runs the shared stores/utxo/tests suite
// against a SQLite store with the Create and Unlock batchers enabled.
// If any test in this suite fails, the batched path has behavioural drift
// from the unbatched path — that's a regression, not a perf concern.
func TestSharedSuite_SQLite_Batched(t *testing.T) {
	store, ctx := setupSQLiteBatched(t)

	t.Run("Store", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.Store(t, store)
	})
	t.Run("Spend", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.Spend(t, store)
	})
	t.Run("Restore", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.Restore(t, store)
	})
	t.Run("Freeze", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.Freeze(t, store)
	})
	t.Run("ReAssign", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.ReAssign(t, store)
	})
	t.Run("SetMined", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.SetMined(t, store)
	})
	t.Run("Conflicting", func(t *testing.T) {
		_ = store.Delete(ctx, tests.TXHash)
		tests.Conflicting(t, store)
	})
}
