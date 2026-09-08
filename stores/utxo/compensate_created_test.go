package utxo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// ghostChainFixture is P -> C -> D -> E plus a sibling S that also spends P.
// C is the transaction the store rejects as a pruned replay.
type ghostChainFixture struct {
	parent, child, grandchild, greatGrandchild, sibling *bt.Tx
}

func newGhostChainFixture(t *testing.T) ghostChainFixture {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	spendOf := func(prev *bt.Tx, vout uint32) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(prev.TxID(), vout, prev.Outputs[vout].LockingScript.String(), prev.Outputs[vout].Satoshis))
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", prev.Outputs[vout].Satoshis-500))

		return tx
	}

	child := spendOf(parent, 0)
	grandchild := spendOf(child, 0)
	greatGrandchild := spendOf(grandchild, 0)
	sibling := spendOf(parent, 1)

	return ghostChainFixture{parent: parent, child: child, grandchild: grandchild, greatGrandchild: greatGrandchild, sibling: sibling}
}

func hashesOf(txs ...*bt.Tx) []*chainhash.Hash {
	out := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		out = append(out, tx.TxIDChainHash())
	}

	return out
}

func createdSet(txs ...*bt.Tx) func(*chainhash.Hash) bool {
	set := make(map[chainhash.Hash]struct{}, len(txs))
	for _, tx := range txs {
		set[*tx.TxIDChainHash()] = struct{}{}
	}

	return func(h *chainhash.Hash) bool {
		_, ok := set[*h]

		return ok
	}
}

func TestPrunedReplayGhosts(t *testing.T) {
	f := newGhostChainFixture(t)
	block := []*bt.Tx{f.child, f.grandchild, f.greatGrandchild, f.sibling}
	rejected := hashesOf(f.child)

	t.Run("nothing rejected means nothing to remove", func(t *testing.T) {
		require.Nil(t, PrunedReplayGhosts(block, nil, createdSet(block...)))
	})

	t.Run("a rejected transaction is a ghost even when this attempt did not create it", func(t *testing.T) {
		// The leftover of an earlier attempt whose compensating delete failed
		// answers ErrTxExists to the create phase. It is still a ghost.
		ghosts := PrunedReplayGhosts(block, rejected, createdSet())
		require.ElementsMatch(t, hashesOf(f.child), hashesOf(ghosts...))
	})

	t.Run("dependents this attempt created are ghosts, transitively", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(block...))
		require.ElementsMatch(t, hashesOf(f.child, f.grandchild, f.greatGrandchild), hashesOf(ghosts...))
		require.Equal(t, f.child.TxIDChainHash(), ghosts[0].TxIDChainHash(), "the rejected transaction comes first")
	})

	t.Run("a dependent the store already held is left alone, and so is its subtree", func(t *testing.T) {
		// D pre-existed: a pruned parent says nothing about whether a still
		// unpruned child is legitimate. E was created here but spends D, not a
		// ghost, so it is not one either.
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(f.child, f.greatGrandchild, f.sibling))
		require.ElementsMatch(t, hashesOf(f.child), hashesOf(ghosts...))
	})

	t.Run("an unrelated sibling is never a ghost", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(block...))
		require.NotContains(t, hashesOf(ghosts...), f.sibling.TxIDChainHash())
	})

	t.Run("order of transactions does not matter", func(t *testing.T) {
		reversed := []*bt.Tx{f.sibling, f.greatGrandchild, f.grandchild, f.child}
		ghosts := PrunedReplayGhosts(reversed, rejected, createdSet(reversed...))
		require.ElementsMatch(t, hashesOf(f.child, f.grandchild, f.greatGrandchild), hashesOf(ghosts...))
	})

	t.Run("duplicate rejections collapse", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, hashesOf(f.child, f.child), createdSet())
		require.Len(t, ghosts, 1)
	})
}

// flakyDeleteStore fails DeleteComplete a fixed number of times before
// succeeding, and records every Unspend and decorate it is asked for. Only
// those three methods are ever called on it.
type flakyDeleteStore struct {
	Store
	failuresLeft atomic.Int32
	calls        atomic.Int32
	mu           sync.Mutex
	deleted      []chainhash.Hash
	unspent      []*Spend
	decorated    []chainhash.Hash
}

func (s *flakyDeleteStore) DeleteComplete(_ context.Context, hash *chainhash.Hash) error {
	s.calls.Add(1)

	if s.failuresLeft.Add(-1) >= 0 {
		return errors.NewStorageError("transient")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleted = append(s.deleted, *hash)

	return nil
}

func (s *flakyDeleteStore) Unspend(_ context.Context, spends []*Spend, _ ...bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unspent = append(s.unspent, spends...)

	return nil
}

func (s *flakyDeleteStore) PreviousOutputsDecorate(_ context.Context, tx *bt.Tx) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decorated = append(s.decorated, *tx.TxIDChainHash())

	for _, input := range tx.Inputs {
		if input.PreviousTxScript == nil {
			input.PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
			input.PreviousTxSatoshis = 1
		}
	}

	return nil
}

func TestDeleteCreatedRetriesATransientFailure(t *testing.T) {
	f := newGhostChainFixture(t)
	ctx := context.Background()

	t.Run("a failure inside the budget is retried to success", func(t *testing.T) {
		store := &flakyDeleteStore{}
		store.failuresLeft.Store(deleteCreatedAttempts - 1)

		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child}, 1))
		require.Equal(t, int32(deleteCreatedAttempts), store.calls.Load())
		require.Equal(t, []chainhash.Hash{*f.child.TxIDChainHash()}, store.deleted, "end state: the record is gone")
	})

	t.Run("a failure past the budget is reported", func(t *testing.T) {
		store := &flakyDeleteStore{}
		store.failuresLeft.Store(deleteCreatedAttempts)

		err := DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child}, 1)
		require.Error(t, err)
		require.Equal(t, int32(deleteCreatedAttempts), store.calls.Load())
		require.Empty(t, store.deleted, "end state: the record is still there, and the caller was told")
	})

	t.Run("nothing to delete is a no-op", func(t *testing.T) {
		store := &flakyDeleteStore{}
		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, nil, 1))
		require.Zero(t, store.calls.Load())
	})
}

// TestDeleteCreatedReleasesSurvivingSpends: a descendant ghost spent both the
// recreated pruned transaction and an unrelated, valid output. Deleting its
// record alone leaves that output recorded as spent by a transaction that no
// longer exists. The spend of the surviving parent must be released, with the
// ghost named as the spender so the store's own match protects any other
// spender; the spend of the ghost parent is skipped because that record goes.
func TestDeleteCreatedReleasesSurvivingSpends(t *testing.T) {
	f := newGhostChainFixture(t)
	ctx := context.Background()

	// D spends C:0 (a ghost) and P:1 (survives).
	descendant := bt.NewTx()
	require.NoError(t, descendant.From(f.child.TxID(), 0, f.child.Outputs[0].LockingScript.String(), f.child.Outputs[0].Satoshis))
	require.NoError(t, descendant.From(f.parent.TxID(), 1, f.parent.Outputs[1].LockingScript.String(), f.parent.Outputs[1].Satoshis))
	require.NoError(t, descendant.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))

	t.Run("extended inputs are released without a decorate", func(t *testing.T) {
		store := &flakyDeleteStore{}
		store.failuresLeft.Store(0)

		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child, descendant}, 1))

		require.Empty(t, store.decorated, "extended inputs carry the parent script; no decorate needed")
		require.Len(t, store.unspent, 2, "C's spend of P:0 and D's spend of P:1; D's spend of C:0 is skipped")

		wantHash, err := util.UTXOHashFromOutput(f.parent.TxIDChainHash(), f.parent.Outputs[1], 1)
		require.NoError(t, err)

		var released *Spend

		for _, spend := range store.unspent {
			if spend.Vout == 1 {
				released = spend
			}
		}

		require.NotNil(t, released)
		require.Equal(t, f.parent.TxIDChainHash(), released.TxID)
		require.Equal(t, wantHash, released.UTXOHash)
		require.Equal(t, descendant.TxIDChainHash(), released.SpendingData.TxID, "the ghost is named as the spender so the store's match protects any other spender")
		require.Equal(t, 1, released.SpendingData.Vin)

		require.ElementsMatch(t, []chainhash.Hash{*f.child.TxIDChainHash(), *descendant.TxIDChainHash()}, store.deleted,
			"end state: both records are gone, after the release")
	})

	t.Run("outpoint-only inputs are decorated first", func(t *testing.T) {
		bare := bt.NewTx()
		bare.Inputs = append(bare.Inputs, &bt.Input{PreviousTxOutIndex: 1, PreviousTxSatoshis: 0})
		require.NoError(t, bare.Inputs[0].PreviousTxIDAdd(f.parent.TxIDChainHash()))
		require.NoError(t, bare.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))

		store := &flakyDeleteStore{}
		store.failuresLeft.Store(0)

		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{bare}, 1))
		require.Equal(t, []chainhash.Hash{*bare.TxIDChainHash()}, store.decorated)
		require.Len(t, store.unspent, 1)
	})
}
