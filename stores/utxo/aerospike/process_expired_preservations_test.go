package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestProcessExpiredPreservations verifies the invariant that delete_at_height is
// only stamped at preservation expiry when the transaction is genuinely safe to
// drop (mined, on the longest chain, AND fully spent). ProcessExpiredPreservations
// writes the deleteAtHeight bin directly (bypassing the Lua setDeleteAtHeight
// self-heal), so without an eligibility check it would stamp a transaction that
// still has live outputs — and the DAH pruner deletes purely on that stamp.
func TestProcessExpiredPreservations(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// ProcessExpiredPreservations selects expired records via a secondary index on
	// preserveUntil. The store does not create this index itself (the method is not
	// yet wired into the pruner cycle), so create it explicitly for the test.
	writePolicy := aerospike.NewWritePolicy(0, 0)
	_, err := client.CreateIndex(writePolicy, store.GetNamespace(), store.GetName(),
		"test_preserveUntilIndex", fields.PreserveUntil.String(), aerospike.NUMERIC)
	require.NoError(t, err)

	const currentHeight = uint32(200)
	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	expiredPreserveUntil := uint32(currentHeight - 10)

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// processExpiryUntilProcessed runs ProcessExpiredPreservations, retrying to absorb
	// secondary-index build lag, until the record's preserveUntil bin is cleared
	// (proving the expired record was actually found and processed by the query).
	processExpiryUntilProcessed := func(t *testing.T) {
		t.Helper()
		require.Eventually(t, func() bool {
			require.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight))
			rec, getErr := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
			require.NoError(t, getErr)
			return rec.Bins[fields.PreserveUntil.String()] == nil
		}, 15*time.Second, 250*time.Millisecond, "expired preservation should be processed")
	}

	t.Run("ineligible_unmined_unspent_tx_is_not_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0) // unmined, unspent
		require.NoError(t, err)

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, expiredPreserveUntil))

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Nil(t, rec.Bins[fields.DeleteAtHeight.String()], "unmined/unspent tx must NOT be stamped for deletion")
	})

	t.Run("eligible_mined_fully_spent_tx_is_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Fully spend, then mine on the longest chain → eligible.
		_, err = store.Spend(ctx, spendTxAll, 1)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
		})
		require.NoError(t, err)

		// Preserve (clears the DAH set at mining time, sets preserveUntil).
		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, expiredPreserveUntil))

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Equal(t, int(currentHeight+retention), rec.Bins[fields.DeleteAtHeight.String()],
			"eligible tx must be stamped with currentHeight+retention")
	})

	t.Run("partial_spend_parent_is_not_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Mine on the longest chain but spend only output 0 — the parent still has
		// live outputs, so it is NOT fully spent and must not be stamped.
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
		})
		require.NoError(t, err)

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, expiredPreserveUntil))

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Nil(t, rec.Bins[fields.DeleteAtHeight.String()],
			"partially-spent parent must NOT be stamped — it still has live UTXOs")
	})
}
