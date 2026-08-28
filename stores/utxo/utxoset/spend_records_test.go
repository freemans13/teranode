package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendReturnsRecordsThisStoreCanRestore.
//
// Conflict resolution undoes its own spends when it fails part way, and it does that by handing
// the records this store returned straight back to this store's Unspend. Unspend refuses a
// record that cannot name the transaction that took the coin, deliberately, because restoring
// on the outpoint alone could resurrect a coin a different transaction now owns.
//
// So a record without a spender is not merely incomplete. It made every conflict-resolution
// failure escalate to the manual-intervention message, whatever had actually gone wrong,
// because the rollback itself could never succeed.
func TestSpendReturnsRecordsThisStoreCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	spends, err := s.Spend(ctx, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	require.NotNil(t, spends[0].SpendingData,
		"the record must name the transaction that took the coin")
	require.Equal(t, child.TxIDChainHash().String(), spends[0].SpendingData.TxID.String())

	// The proof: hand them back unmodified, which is exactly what a rollback does.
	require.NoError(t, s.Unspend(ctx, spends, false),
		"a record this store produced must be one this store can restore")
}

// TestSpendAndCreateReturnsRecordsThisStoreCanRestore is the same rule on the combined call,
// which is the one conflict resolution actually uses.
func TestSpendAndCreateReturnsRecordsThisStoreCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	require.NotNil(t, spends[0].SpendingData)
	require.Equal(t, child.TxIDChainHash().String(), spends[0].SpendingData.TxID.String())

	require.NoError(t, s.Unspend(ctx, spends, false))
}

// TestSpendNamesTheRightSpenderPerTransactionInOneBatch. A batch carries many transactions, and
// each record must name ITS OWN spender. Naming the batch's first, or last, would restore coins
// to the wrong owner on a rollback.
func TestSpendNamesTheRightSpenderPerTransactionInOneBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	first := spendOutput(t, parent, 0, 1)
	second := spendOutput(t, parent, 1, 2)

	require.NotEqual(t, first.TxIDChainHash().String(), second.TxIDChainHash().String())

	items := []*spendItem{
		{tx: first, blockHeight: 101, done: make(chan spendResult, 1)},
		{tx: second, blockHeight: 101, done: make(chan spendResult, 1)},
	}

	s.sendSpendBatch(items)

	results := make([]spendResult, 0, len(items))
	for _, it := range items {
		results = append(results, <-it.done)
	}

	for i, want := range []string{first.TxIDChainHash().String(), second.TxIDChainHash().String()} {
		require.NoError(t, results[i].err)
		require.Len(t, results[i].spends, 1)
		require.NotNil(t, results[i].spends[0].SpendingData)
		require.Equal(t, want, results[i].spends[0].SpendingData.TxID.String(),
			"record %d must name its own spender, not another transaction's", i)
	}

	// And both are restorable together, which is what a batch rollback does.
	all := make([]*utxo.Spend, 0, 2)
	for _, r := range results {
		all = append(all, r.spends...)
	}

	require.NoError(t, s.Unspend(ctx, all, false))
}
