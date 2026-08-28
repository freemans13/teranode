package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestCounterConflictingSurvivesABodyThatHasAgedOut is a crash test for shared code, driven
// through the one store that can actually produce the state.
//
// This store bounds serialized transaction bytes by a retention horizon and drops them by
// window, so a metadata read returns a record whose transaction is nil for anything older.
// That is the ordinary steady state rather than an error, and a transaction that lost a
// double-spend race is kept indefinitely while its bytes age out, so the combination is not
// exotic.
//
// The shared walk that finds who else spent a transaction's inputs read those inputs off the
// transaction body. With the body gone that is a nil pointer dereference, in a worker with
// nothing above it to recover, so it takes the whole process down. It is reachable from an
// incoming subtree, which means from the network.
//
// The inputs are on the identity record too, as stored inpoints, and those never age out.
func TestCounterConflictingSurvivesABodyThatHasAgedOut(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// Age the bytes out, exactly as the pruner does once the horizon passes.
	dropped, err := s.dropTxBodyWindowsBelow(ctx, 100_000)
	require.NoError(t, err)
	require.Positive(t, dropped, "the body windows must actually have been dropped")

	ch := child.TxIDChainHash()

	got, err := s.Get(ctx, ch)
	require.NoError(t, err)
	require.Nil(t, got.Tx, "the body is gone, which is the state under test")
	require.NotEmpty(t, got.TxInpoints.ParentTxHashes, "but what it spends is still known")

	// The call that used to panic here.
	//
	// It still cannot produce the full answer on this store, because naming who spent a
	// PARENT's output needs spending data the store does not yet put on a metadata read: the
	// coin row is destroyed on spend and the answer lives in the journal. That is separate
	// work. What this pins is that the walk gets past reading the transaction's OWN inputs,
	// which is where it used to die, and fails with a diagnosable error instead of taking the
	// process down.
	_, err = utxo.GetCounterConflictingTxHashes(ctx, s, *ch, 1_000)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is out of range",
		"the failure must be about the parent's spending data, not about reading the inputs")
	require.NotContains(t, err.Error(), "cannot read what the transaction spends",
		"the inputs must have come from the stored inpoints, which do not age out")
}
