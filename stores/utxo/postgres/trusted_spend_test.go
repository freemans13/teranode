package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestTrustedSpendIgnoresUTXOHashAndExtension verifies the trusted-connect spend
// path: Spend with IgnoreUTXOHash records the spend by outpoint for a NON-extended
// tx (no PreviousTxScript / PreviousTxSatoshis), without the utxo-hash guard. This
// is the legacy-IBD-below-checkpoint fast path that lets the decorate be skipped.
func TestTrustedSpendIgnoresUTXOHashAndExtension(t *testing.T) {
	store, ctx := setupTestStore(t)

	parent := newMinedSingleOutputTx(t, store, 100)

	child := getSpendingTx(t, parent, 0)
	_, err := store.Create(ctx, child, 101)
	require.NoError(t, err)

	// Force the child non-extended (as it would be when the decorate is skipped):
	// clear the previous-output data the normal spend path needs for the utxo hash.
	for _, in := range child.Inputs {
		in.PreviousTxScript = nil
		in.PreviousTxSatoshis = 0
	}
	require.False(t, child.IsExtended(), "child must be non-extended for this test")

	// Trusted spend must succeed despite the tx not being extended.
	_, err = store.Spend(ctx, child, 101, utxo.IgnoreFlags{IgnoreUTXOHash: true})
	require.NoError(t, err, "trusted spend must not require an extended tx or a utxo-hash match")

	// The parent's output must now be recorded as spent.
	var spendCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1`, parent.TxIDChainHash()[:]).Scan(&spendCount))
	require.Equal(t, 1, spendCount, "trusted spend must record the spend by outpoint")

	// spent_at_height must be recorded (Worker 2 deferred-DAH relies on it).
	var spentAtHeight int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_at_height FROM spends WHERE prev_tx_hash=$1`, parent.TxIDChainHash()[:]).Scan(&spentAtHeight))
	require.Equal(t, int64(101), spentAtHeight)

	// Idempotency: re-running the trusted spend is a no-op (ON CONFLICT DO NOTHING).
	_, err = store.Spend(ctx, child, 101, utxo.IgnoreFlags{IgnoreUTXOHash: true})
	require.NoError(t, err)
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1`, parent.TxIDChainHash()[:]).Scan(&spendCount))
	require.Equal(t, 1, spendCount, "re-spend must remain idempotent")
}

// TestNormalSpendRequiresExtension is the contrast case: the validated spend path
// DOES require an extended tx (it derives the expected utxo hash from the input's
// previous-output data). Confirms the trusted flag is what unlocks the non-extended
// fast path, not a general relaxation.
func TestNormalSpendRequiresExtension(t *testing.T) {
	store, ctx := setupTestStore(t)

	parent := newMinedSingleOutputTx(t, store, 100)
	child := getSpendingTx(t, parent, 0)
	_, err := store.Create(ctx, child, 101)
	require.NoError(t, err)

	for _, in := range child.Inputs {
		in.PreviousTxScript = nil
		in.PreviousTxSatoshis = 0
	}
	require.False(t, child.IsExtended())

	// Without IgnoreUTXOHash, the spend path computes the utxo hash from the input's
	// previous-output data, which is absent → it must error rather than silently spend.
	_, err = store.Spend(ctx, child, 101)
	require.Error(t, err, "validated spend must reject a non-extended tx")
}
