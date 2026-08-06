package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendSpendableInHoldYieldsFrozenNotLocked pins that the alert system's
// spendableIn height gate (set by ReAssignUTXO after FreezeUTXOs) classifies
// as ErrFrozen, not ErrTxLocked, when a spend arrives before the held height.
// Conflating the two would make the validator retry a freeze that will not
// clear for up to ReAssignedUtxoSpendableAfterBlocks blocks (default 1000) --
// see the spendableIn branches in sql.go's trySendSpendBatchBulk/PerRow.
//
// There is no existing alert-system test in this package to reuse --
// FreezeUTXOs/ReAssignUTXO are exercised only by the e2e/sequential suites --
// so this drives the real alert-system API directly: FreezeUTXOs then
// ReAssignUTXO. ReAssignUTXO is called with the SAME UTXOHash the output
// already has, so the stored utxo_hash is unchanged and the real spending tx
// (built normally against the original output) still matches it, isolating
// the test to exactly the spendableIn gate rather than also changing which
// UTXO is being spent.
//
// This exercises trySendSpendBatchPerRow (the sqlitememory/non-bulk path);
// trySendSpendBatchBulk is Postgres-only (BatchSQLOperations && engine ==
// "postgres") and requires a TestContainers Postgres instance to reach, which
// is out of scope here. Both branches carry the identical spendableIn check
// and comment (see sql.go), so the fix and this pin apply to both.
func TestSpendSpendableInHoldYieldsFrozenNotLocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(spendTx)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	err = utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spends[0]}, utxoStore.settings)
	require.NoError(t, err)

	// Reassign to the same UTXOHash: this is what stamps spendableIn (and
	// clears frozen) without altering which UTXO the real spendTx matches.
	err = utxoStore.ReAssignUTXO(ctx, spends[0], spends[0], utxoStore.settings)
	require.NoError(t, err)

	// spendableIn = GetBlockHeight() (0, unset by this test) + the default
	// ReAssignedUtxoSpendableAfterBlocks (1000); blockHeight 1 is comfortably
	// below that, so the hold must still be in effect.
	_, err = utxoStore.Spend(ctx, spendTx, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "a spendableIn hold must classify as ErrFrozen, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrTxLocked), "a spendableIn hold must not be classified as ErrTxLocked -- that's the parent's own in-flight commit, a different condition")
}
