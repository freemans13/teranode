package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// A transaction can be submitted in extended format, carrying its own copy of every coin it
// spends: the satoshis and the locking script. The validator does NOT re-derive those when
// they arrive, by design -- it validates against what the transaction brought. So the
// submitter's claim is what the inflation check sums and what script verification runs
// against, and nothing else contradicts it before the coin is consumed.
//
// The other two stores catch a false claim at the moment of the spend, by comparing a hash
// derived from the carried copy against the hash stored on the coin. This store returns the
// coin's real satoshis and script from the DELETE, in the same round trip, so it can compare
// the values themselves and needs no hash at all.
//
// These tests pin that comparison. Without it a submitter can assert what a coin is worth and
// who may move it, and be believed.

// tamperedSatoshis: the claim inflates the coin's value.
func TestSpendRejectsAnInflatedSatoshiClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	child.Inputs[0].PreviousTxSatoshis = 5_000_000_000

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrUtxoHashMismatch)

	// Nothing was committed, so the coin is still there for its rightful spender.
	honest := spendOutput(t, parent, 0, 2)

	_, honestSpends, err := s.SpendAndCreate(ctx, honest, 101)
	require.NoError(t, err, "the rejected claim must not have consumed the coin")
	require.NoError(t, honestSpends[0].Err)
}

// tamperedScript: the claim rewrites who may move the coin.
func TestSpendRejectsARewrittenLockingScriptClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	other, err := bscript.NewFromHexString("76a914111111111111111111111111111111111111111188ac")
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	child.Inputs[0].PreviousTxScript = other

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrUtxoHashMismatch)

	honest := spendOutput(t, parent, 0, 2)

	_, honestSpends, err := s.SpendAndCreate(ctx, honest, 101)
	require.NoError(t, err, "the rejected claim must not have consumed the coin")
	require.NoError(t, honestSpends[0].Err)
}

// The batched Spend path shares one statement with SpendAndCreate, so it must share the
// comparison too. It reports per-input rather than as a returned error, which is this store's
// documented contract for Spend.
func TestBatchedSpendRejectsAFalseClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	child.Inputs[0].PreviousTxSatoshis = 9_999

	spends, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrUtxoHashMismatch)
}

// An honest extended transaction must be unaffected. This is the whole steady-state path, so a
// comparison that fired here would reject every spend on the node.
func TestSpendAcceptsAnHonestClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)
}

// An input that carries NO claim cannot be lying about one. That is the below-checkpoint
// outpoint-only path: the transaction arrives un-decorated, the spend is what decorates it,
// and script validation is switched off in the same breath.
func TestSpendAcceptsAnInputThatCarriesNoClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	child.Inputs[0].PreviousTxSatoshis = 0
	child.Inputs[0].PreviousTxScript = nil

	_, spends, err := s.SpendAndCreate(ctx, child, 101, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	// The spend is still the decorate fetch: the input comes back carrying the truth.
	require.Equal(t, uint64(5_000), child.Inputs[0].PreviousTxSatoshis)
	require.NotNil(t, child.Inputs[0].PreviousTxScript)
}

// WithSkipUTXOHashCheck is the existing, gated opt-out the other two stores honour. This store
// ignored it entirely; it must now mean the same thing here.
func TestSkipUTXOHashCheckDisablesTheComparison(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	child.Inputs[0].PreviousTxSatoshis = 1

	_, spends, err := s.SpendAndCreate(ctx, child, 101,
		utxo.WithSkipUTXOHashCheck(true), utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)
}

// The replay path decorates from the journal instead of the coin row, so it is a second place
// the store hands a caller's claim back unexamined. A replay is by definition the same
// transaction spending again, so an honest one must still succeed -- and a doctored one must
// not slip through the second door.
func TestReplayedSpendRejectsAFalseClaim(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	spends, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// Same transaction, same coin: a replay, which must succeed.
	replay, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.NoError(t, replay[0].Err, "a replay of our own spend must not be a double spend")

	// Now the same replay with a doctored claim.
	child.Inputs[0].PreviousTxSatoshis = 5_000_000_000

	doctored, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.ErrorIs(t, doctored[0].Err, errors.ErrUtxoHashMismatch)
}

// One false input must not consume the honest ones. The comparison happens after the DELETE
// has already taken every row it could, so the guarantee comes entirely from SpendAndCreate
// running the whole thing in one database transaction and rolling back on any per-input error.
// If that ever stopped being true, a submitter could burn coins it does not own by lying about
// only the last input.
func TestOneFalseInputLeavesTheHonestCoinsUntouched(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	for vout := uint32(0); vout < 3; vout++ {
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      parent.Outputs[vout].Satoshis,
		}))
	}

	child.AddOutput(&bt.Output{Satoshis: 1_000, LockingScript: parent.Outputs[0].LockingScript})

	// Only the last input lies.
	child.Inputs[2].PreviousTxSatoshis = 5_000_000_000

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.Error(t, err)
	require.Len(t, spends, 3)
	require.NoError(t, spends[0].Err)
	require.NoError(t, spends[1].Err)
	require.ErrorIs(t, spends[2].Err, errors.ErrUtxoHashMismatch)

	// All three coins are still live.
	for vout := uint32(0); vout < 3; vout++ {
		resp, gErr := s.GetSpend(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: vout})
		require.NoError(t, gErr)
		require.Equal(t, int(utxo.Status_OK), resp.Status,
			"output %d must survive a transaction rejected for lying about output 2", vout)
	}
}
