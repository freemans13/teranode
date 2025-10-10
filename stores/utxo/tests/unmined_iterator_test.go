package tests

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnminedTxIteratorSQLite(t *testing.T) {
	t.Cleanup(func() {
		_ = os.Remove("./data/test.db")
		_ = os.Remove("./data/test.db-shm")
		_ = os.Remove("./data/test.db-wal")
	})

	utxoStore := "sqlite:///test"
	testUnminedTxIterator(t, utxoStore)
}

func TestUnminedTxIteratorPostgres(t *testing.T) {
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	testUnminedTxIterator(t, utxoStore)
}

func TestUnminedTxIteratorAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	testUnminedTxIterator(t, utxoStore)
}

func testUnminedTxIterator(t *testing.T, utxoStoreURL string) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	settings := test.CreateBaseTestSettings(t)

	// Parse the URL and set it in settings
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err)

	settings.UtxoStore.UtxoStore = parsedURL

	store, err := factory.NewStore(ctx, logger, settings, "test", false)
	require.NoError(t, err)

	tx1, err := bt.NewTxFromString("010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000")
	require.NoError(t, err)

	tx2 := tx1.Clone()
	tx2.Version++

	t.Run("empty store", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, tx1.TxIDChainHash()))
		require.NoError(t, store.Delete(ctx, tx2.TxIDChainHash()))

		it, err := store.GetUnminedTxIterator(false)
		require.NoError(t, err)

		var count int

		for {
			unminedTransaction, err := it.Next(ctx)
			require.NoError(t, err)

			if unminedTransaction == nil {
				break
			}

			count++
		}

		assert.Equal(t, 0, count, "should not find any unmined transactions")
	})

	t.Run("mixed mined/unmined", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, tx1.TxIDChainHash()))
		require.NoError(t, store.Delete(ctx, tx2.TxIDChainHash()))

		currentBlockHeight := uint32(1)

		err = store.SetBlockHeight(currentBlockHeight)
		require.NoError(t, err)

		tx1Meta, err := store.Create(ctx, tx1, currentBlockHeight)
		require.NoError(t, err)

		_, err = store.Create(ctx, tx2, currentBlockHeight, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{
				BlockID:     1,
				BlockHeight: 1,
				SubtreeIdx:  1,
			},
		))
		require.NoError(t, err)

		it, err := store.GetUnminedTxIterator(false)
		require.NoError(t, err)

		var count int

		for {
			unminedTransaction, err := it.Next(ctx)
			require.NoError(t, err)

			if unminedTransaction == nil {
				break
			}

			assert.Equal(t, tx1.TxIDChainHash(), unminedTransaction.Hash)
			assert.Equal(t, tx1Meta.Fee, unminedTransaction.Fee)
			assert.Equal(t, tx1Meta.SizeInBytes, unminedTransaction.Size)
			assert.Len(t, unminedTransaction.TxInpoints.ParentTxHashes, 1)
			assert.Greater(t, unminedTransaction.CreatedAt, 0)
			assert.NotNil(t, unminedTransaction.BlockIDs)

			count++
		}

		assert.Equal(t, 1, count, "should find one unmined transaction")
	})

	t.Run("all mined", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, tx1.TxIDChainHash()))
		require.NoError(t, store.Delete(ctx, tx2.TxIDChainHash()))

		_, err = store.Create(ctx, tx1, 0, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{
				BlockID:     2,
				BlockHeight: 2,
				SubtreeIdx:  2,
			},
		))
		require.NoError(t, err)

		_, err = store.Create(ctx, tx2, 0, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{
				BlockID:     2,
				BlockHeight: 2,
				SubtreeIdx:  2,
			},
		))
		require.NoError(t, err)

		it, err := store.GetUnminedTxIterator(false)
		require.NoError(t, err)

		var count int

		for {
			unminedTransaction, err := it.Next(ctx)
			require.NoError(t, err)

			if unminedTransaction == nil {
				break
			}

			count++
		}

		assert.Equal(t, 0, count, "should not find any unmined transactions")
	})

	t.Run("iterator Close cancels context and marks done", func(t *testing.T) {
		it, err := store.GetUnminedTxIterator(false)
		require.NoError(t, err)

		assert.NoError(t, it.Close())
	})
}
