package postgres

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestFreezeUTXOOnlyBlocksThatOutput is a regression test for the per-output freeze
// fix: freezing a single output of a multi-output tx must NOT make the other
// outputs unspendable. Previously FreezeUTXOs also set the transaction-level
// `frozen` column, which the spend-validation CTE gates on (`tx_frozen`), so a
// single frozen output blocked every output of the tx.
func TestFreezeUTXOOnlyBlocksThatOutput(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parentTx := makeThreeOutputTx(t) // vout0 and vout2 spendable; vout1 OP_RETURN
	_, err := store.Create(ctx, parentTx, 100)
	require.NoError(t, err)
	parentHash := parentTx.TxIDChainHash()

	// Freeze ONLY vout0.
	uh0, err := util.UTXOHashFromOutput(parentHash, parentTx.Outputs[0], 0)
	require.NoError(t, err)
	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{
		{TxID: parentHash, Vout: 0, UTXOHash: uh0},
	}, nil))

	// The transaction-level frozen flag must NOT have been set by a per-output freeze.
	var txFrozen bool
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT frozen FROM txs WHERE hash = $1`, parentHash[:]).Scan(&txFrozen))
	require.False(t, txFrozen, "per-output freeze must not set the tx-level frozen flag")

	// Spending the sibling output (vout2) must still succeed.
	spend2 := getSpendingTx(t, parentTx, 2)
	_, err = store.Create(ctx, spend2, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spend2, 101)
	require.NoError(t, err, "freezing vout0 must not block spending vout2")

	// Spending the frozen output itself must fail with a frozen error.
	spend0 := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spend0, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spend0, 101)
	require.Error(t, err, "spending the frozen vout0 must fail")
	require.True(t, errors.Is(err, errors.ErrFrozen), "expected ErrFrozen, got: %v", err)
}

// TestUnspendRequiresOwnership is a regression test for the ownership-checked
// Unspend fix (matches the aerospike/sql gold standard): a non-owning caller (one
// whose SpendingData token does not match the stored spender) must NOT delete the
// live spend, and a nil token must be rejected outright.
func TestUnspendRequiresOwnership(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parentTx := testExtendedTx(t)
	_, err := store.Create(ctx, parentTx, 100)
	require.NoError(t, err)
	parentHash := parentTx.TxIDChainHash()

	spendTx := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spendTx, 100)
	require.NoError(t, err)
	spends, err := store.Spend(ctx, spendTx, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	// Non-owning Unspend: a token for a DIFFERENT spender (parentHash stands in for
	// some other tx) must be a no-op for the live spend row.
	wrongOwner := &utxo.Spend{
		TxID:         parentHash,
		Vout:         0,
		UTXOHash:     spends[0].UTXOHash,
		SpendingData: spendpkg.NewSpendingData(parentHash, 0),
	}
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{wrongOwner}))

	got, err := store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, got.SpendingDatas[0], "a non-owning Unspend must not clear the live spend")

	// Nil SpendingData is a hard error.
	require.Error(t, store.Unspend(ctx, []*utxo.Spend{
		{TxID: parentHash, Vout: 0, UTXOHash: spends[0].UTXOHash},
	}), "Unspend without a SpendingData token must error")

	// The real owner can Unspend.
	require.NoError(t, store.Unspend(ctx, spends))
	got, err = store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.Nil(t, got.SpendingDatas[0], "the owning Unspend must clear the spend")
}

// TestUnsetMinedClearsDAH is a regression test for the reorg data-loss fix:
// when a block is invalidated and a fully-spent+mined tx falls off the longest
// chain, unsetMinedMulti must clear delete_at_height so the pruner does not delete
// the now-unconfirmed tx at the stale stamp height.
func TestUnsetMinedClearsDAH(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100) // mined into block_id 100
	spendAllOutputs(t, store, parent, 101)          // fully spent at 101
	parentHash := parent.TxIDChainHash()

	// Sweep stamps DAH for the fully-spent mined parent.
	_, err := store.sweepDAHUpTo(ctx, 110, 100000)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, parentHash[:]).Scan(&dah))
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before the reorg")

	// Reorg: invalidate block 100 (the only block this tx is in).
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID:     100,
		BlockHeight: 100,
		UnsetMined:  true,
	})
	require.NoError(t, err)

	var (
		dahAfter     *int64
		unminedSince *int64
	)
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height, unmined_since FROM txs WHERE hash = $1`, parentHash[:]).
		Scan(&dahAfter, &unminedSince))
	require.Nil(t, dahAfter, "unset-mined must clear delete_at_height so the tx is not pruned")
	require.NotNil(t, unminedSince, "unset-mined with no remaining block_ids must set unmined_since")
}

// TestFreezeUTXOsAbsentTxReturnsTxNotFound is a regression test: freezing an output of
// a transaction that does not exist (or an out-of-range vout) must report ErrTxNotFound,
// so callers can distinguish "the UTXO does not exist" from "the store failed", rather
// than the opaque storage error returned before.
func TestFreezeUTXOsAbsentTxReturnsTxNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)

	var txid, uh chainhash.Hash
	txid[0] = 0xAB // a transaction that was never created
	err := store.FreezeUTXOs(ctx, []*utxo.Spend{
		{TxID: &txid, Vout: 0, UTXOHash: &uh},
	}, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "absent tx must yield ErrTxNotFound, got: %v", err)
}
