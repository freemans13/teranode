package utxocheck

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCheckAllTxsExistInUTXO(t *testing.T) {
	ctx := context.Background()

	makeHash := func(i int) chainhash.Hash {
		var h chainhash.Hash
		copy(h[:], fmt.Sprintf("tx%032d", i))
		return h
	}

	t.Run("empty hashes returns true", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, nil, 1000)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("all txs exist returns true", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{makeHash(1), makeHash(2), makeHash(3)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, item := range batch {
					item.Data = &meta.Data{Creating: false}
				}
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("missing tx returns false", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{makeHash(1), makeHash(2), makeHash(3)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				batch[0].Data = &meta.Data{Creating: false}
				// batch[1] left nil — missing tx
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("creating tx returns false", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{makeHash(1)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				batch[0].Data = &meta.Data{Creating: true}
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("BatchDecorate error returns false with error", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{makeHash(1)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Return(fmt.Errorf("store error"))

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.Error(t, err)
		assert.False(t, result)
	})

	t.Run("coinbase placeholder is skipped", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, makeHash(1)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				require.Len(t, batch, 1, "coinbase should be filtered out")
				batch[0].Data = &meta.Data{Creating: false}
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("batching works correctly", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := make([]chainhash.Hash, 5)
		for i := range hashes {
			hashes[i] = makeHash(i)
		}

		callCount := 0
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, item := range batch {
					item.Data = &meta.Data{Creating: false}
				}
				callCount++
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 2)
		require.NoError(t, err)
		assert.True(t, result)
		assert.Equal(t, 3, callCount, "5 items with batch size 2 should create 3 batches")
	})

	t.Run("item error returns false", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{makeHash(1)}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				batch[0].Err = fmt.Errorf("not found")
			}).
			Return(nil)

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("only coinbase hashes returns true without calling store", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}
		hashes := []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}

		result, err := CheckAllTxsExistInUTXO(ctx, mockStore, hashes, 1000)
		require.NoError(t, err)
		assert.True(t, result)
		// BatchDecorate should not be called since only coinbase hashes
		mockStore.AssertNotCalled(t, "BatchDecorate")
	})

}
