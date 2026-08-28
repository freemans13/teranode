package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// spendOutput builds a transaction that spends one output of parent and adds n outputs of its
// own, so that varying n gives distinct transactions.
func spendOutput(t *testing.T, parent *bt.Tx, vout uint32, nOut int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))

	for i := 0; i < nOut; i++ {
		tx.AddOutput(&bt.Output{
			Satoshis:      uint64(1_000 + i),
			LockingScript: parent.Outputs[vout].LockingScript,
		})
	}

	return tx
}

// TestSetConflictingStopsTheCoinsBeingSpent is the whole point of the flag.
//
// It has to reach BOTH rows. The identity row is what a metadata read shows; the coin row is
// what the spend path reads, and the spend path never looks at the identity row. Setting only
// one leaves a transaction reporting itself conflicting while its coins stay spendable.
func TestSetConflictingStopsTheCoinsBeingSpent(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, true)
	require.NoError(t, err)

	meta, err := s.Get(ctx, ph)
	require.NoError(t, err)
	require.True(t, meta.Conflicting, "the identity row must report it")

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 200)
	require.Error(t, err, "a rejected spend is now a returned error, and rolled back")
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxConflicting),
		"the coin row must refuse the spend, got %v", spends[0].Err)
}

// TestClearingConflictingRestoresSpendability. Conflict resolution promotes a winner by
// clearing the flag and then spending through it, so a one-way flag would strand the winner.
func TestClearingConflictingRestoresSpendability(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, true)
	require.NoError(t, err)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, false)
	require.NoError(t, err)

	meta, err := s.Get(ctx, ph)
	require.NoError(t, err)
	require.False(t, meta.Conflicting, "the flag must clear on the identity row")

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err, "and on the coin row, or the winner can never be spent")
}

// TestSetConflictingNotesTheContestOnItsParents.
//
// A transaction that loses a double-spend race is kept rather than discarded, because
// resolving the race later has to find it. Finding it means asking the PARENT whose coin was
// contested, so the parent carries the list. Without it there is no route from a contested
// coin to the transactions competing for it.
func TestSetConflictingNotesTheContestOnItsParents(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	ch := child.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ch}, true)
	require.NoError(t, err)

	pmeta, err := s.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, pmeta.ConflictingChildren, 1)
	require.Equal(t, ch.String(), pmeta.ConflictingChildren[0].String(),
		"the parent must name the transaction contesting its coin")

	// Offering the same transaction again must not grow the list.
	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ch}, true)
	require.NoError(t, err)

	pmeta, err = s.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, pmeta.ConflictingChildren, 1, "a repeat must not be noted twice")
}

// TestSetConflictingReturnsOnlySpendsItsOwnUnspendCanRestore is the correction the review
// caught, and it is the one that decides whether conflict resolution can run at all.
//
// The first return value feeds straight into this store's Unspend, which fails unless EVERY
// record it is given comes back. So a record for an input that was never actually spent, or
// whose undo record has aged out, is not a harmless extra: it makes the whole restore fail.
// Only inputs with a live undo record naming this transaction may be reported.
func TestSetConflictingReturnsOnlySpendsItsOwnUnspendCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	// Only now is the coin actually taken, so only now is there anything to restore.
	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1, "exactly the one input it actually spent")

	require.NotNil(t, affected[0].SpendingData, "Unspend refuses a record with no spender")
	require.Equal(t, child.TxIDChainHash().String(), affected[0].SpendingData.TxID.String())

	// The real proof: hand it straight back to Unspend, which is what conflict resolution
	// does with it.
	require.NoError(t, s.Unspend(ctx, affected, false),
		"every record returned must be one this store can restore")
}

// TestSetConflictingReportsNothingToRestoreForAnUnspentTransaction. Same rule from the other
// side: a transaction whose inputs were never taken must report NO restorable spends, or the
// restore fails on records that were never spends.
func TestSetConflictingReportsNothingToRestoreForAnUnspentTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Empty(t, affected, "nothing was spent, so there is nothing to restore")
}

// TestSetConflictingCascadesDownTheChain exercises the second return value, which drives the
// walk that marks a loser's descendants.
//
// The coin row is destroyed the moment it is spent, so the undo journal is the only place this
// store can answer "who took this coin". This is the test that fails if the key range bound or
// the full-id recheck is wrong.
func TestSetConflictingCascadesDownTheChain(t *testing.T) {
	s, ctx := newTestStore(t)

	root := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, root, 100)
	require.NoError(t, err)

	child := spendOutput(t, root, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	grandchild := spendOutput(t, child, 0, 1)
	_, err = s.Create(ctx, grandchild, 102)
	require.NoError(t, err)

	spends, err = spendOnly(ctx, s, grandchild, 102)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// One level at a time: marking the root must name the child as the next level.
	_, next, err := s.SetConflicting(ctx, []chainhash.Hash{*root.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, child.TxIDChainHash().String(), next[0].String())

	// And the whole walk, through the shared recursive helper every store is held to.
	_, marked, err := utxo.MarkConflictingRecursively(ctx, s, []chainhash.Hash{*root.TxIDChainHash()})
	require.NoError(t, err)

	got := make(map[string]bool, len(marked))
	for _, h := range marked {
		got[h.String()] = true
	}

	require.True(t, got[root.TxIDChainHash().String()], "the root")
	require.True(t, got[child.TxIDChainHash().String()], "its child")
	require.True(t, got[grandchild.TxIDChainHash().String()], "and its grandchild")
}

// TestSetConflictingReportsATransactionItDoesNotHold, matching both reference stores: an
// absent hash is an error, not a silent skip.
func TestSetConflictingReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, known, 100)
	require.NoError(t, err)

	missing := mkTx(t, 1, 9_999)

	_, _, err = s.SetConflicting(ctx,
		[]chainhash.Hash{*known.TxIDChainHash(), *missing.TxIDChainHash()}, true)

	require.Error(t, err, "a hash the store does not hold must fail loudly")
	require.Contains(t, err.Error(), missing.TxIDChainHash().String())
}

// TestSetConflictingOnAnEmptyListIsANoOp, which is how the cascade terminates.
func TestSetConflictingOnAnEmptyListIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	affected, next, err := s.SetConflicting(ctx, nil, true)
	require.NoError(t, err)
	require.Empty(t, affected)
	require.Empty(t, next)
}

// TestNotingAConflictingChildTestsMembershipOnA32ByteBoundary is a defect test for the
// statement create.go uses when it stores a transaction already known to be conflicting.
//
// The column is a concatenation of 32-byte transaction ids, and the reader unpacks it that way
// and rejects a length that is not a multiple of 32. A plain substring search can therefore
// match bytes that STRADDLE two neighbouring entries, read that as already-present, and
// silently skip a real append. The parent then never names one of the transactions contesting
// its coin, and conflict resolution has no route to it.
//
// The straddling value is planted directly, because no amount of test data will produce one by
// chance from real transaction ids.
func TestNotingAConflictingChildTestsMembershipOnA32ByteBoundary(t *testing.T) {
	s, ctx := newTestStore(t)

	var parent, childA, childB, straddler [32]byte

	for i := 0; i < 32; i++ {
		parent[i] = byte(i)
		childA[i] = byte(0xa0 + i)
		childB[i] = byte(0xb0 + i)
	}

	// Exactly the second half of A followed by the first half of B, so it appears in A||B at
	// offset 16 and at no 32-byte boundary.
	copy(straddler[:16], childA[16:])
	copy(straddler[16:], childB[:16])

	_, err := s.pool.Exec(ctx, `
        INSERT INTO tx_ident (leaf, txid, created_height, conflicting_children)
        VALUES ($1, $2, 100, $3)`,
		LeafFor(parent[:]), parent[:], append(append([]byte{}, childA[:]...), childB[:]...))
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, noteConflictSQL, LeafFor(parent[:]), parent[:], straddler[:])
	require.NoError(t, err)

	var got []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT conflicting_children FROM tx_ident WHERE txid = $1`, parent[:]).Scan(&got))

	require.Len(t, got, 96,
		"a value that only appears straddling two entries is NOT present, and must be appended")
	require.Equal(t, straddler[:], got[64:96], "and appended at the end, on the boundary")
}
