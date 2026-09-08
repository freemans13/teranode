package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

type replayPruneCase struct {
	name                                                     string
	defensive, markerFailure, deleteFailure, perRow, upgrade bool
}

func TestPrunedChildReplay(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range []replayPruneCase{
			{name: "normal"}, {name: "defensive", defensive: true},
			{name: "marker_failure", markerFailure: true},
			{name: "delete_failure", deleteFailure: true},
			{name: "per_row", perRow: true},
			{name: "schema_upgrade", upgrade: true},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var store *Store
				if backend == "postgres" {
					store, ctx = setupPostgresStore(t)
				} else {
					store, _ = setup(ctx, t)
				}
				store.settings.Pruner.UTXODefensiveEnabled = tc.defensive
				if tc.perRow {
					store.settings.UtxoStore.BatchSQLOperations = false
				}
				testPrunedChildReplay(t, ctx, store, tc)
			})
		}
	}
}

func testPrunedChildReplay(t *testing.T, ctx context.Context, store *Store, tc replayPruneCase) {
	t.Helper()
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)
	require.NoError(t, store.SetBlockHeight(1000))
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	// A successful spend whose create has not happened yet must remain retryable.
	_, err = store.Spend(ctx, child, 1000)
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)
	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	require.ErrorIs(t, err, errors.ErrTxExists)
	if tc.upgrade {
		// Model an existing database created by the previous schema.
		_, err = store.db.ExecContext(ctx, "DROP TABLE deleted_children")
		require.NoError(t, err)
		if store.engine == "postgres" {
			err = createPostgresSchema(store.db)
		} else {
			err = createSqliteSchema(store.db)
		}
		require.NoError(t, err)
	}
	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	if tc.markerFailure || tc.deleteFailure {
		table, operation := "deleted_children", "INSERT"
		if tc.deleteFailure {
			table, operation = "transactions", "DELETE"
		}
		if store.engine == "postgres" {
			_, err = store.db.ExecContext(ctx, `CREATE FUNCTION reject_prune() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected prune failure'; END $$`)
			require.NoError(t, err)
			_, err = store.db.ExecContext(ctx, "CREATE TRIGGER reject_prune BEFORE "+operation+" ON "+table+" FOR EACH ROW EXECUTE FUNCTION reject_prune()")
		} else {
			_, err = store.db.ExecContext(ctx, "CREATE TRIGGER reject_prune BEFORE "+operation+" ON "+table+" BEGIN SELECT RAISE(ABORT, 'injected prune failure'); END")
		}
		require.NoError(t, err)
		_, err = svc.Prune(ctx, 1300, "injected-failure")
		require.Error(t, err)
		var count int
		require.NoError(t, store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE hash = $1 AND unmined_since IS NULL", child.TxIDChainHash()[:]).Scan(&count))
		require.Equal(t, 1, count, "failed pruning retains the mined child")
		require.NoError(t, store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deleted_children").Scan(&count))
		require.Zero(t, count, "deletion failure must roll back marker writes too")
		if store.engine == "postgres" {
			_, err = store.db.ExecContext(ctx, "DROP TRIGGER reject_prune ON "+table)
		} else {
			_, err = store.db.ExecContext(ctx, "DROP TRIGGER reject_prune")
		}
		require.NoError(t, err)
	}
	n, err := svc.Prune(ctx, 1300, "replay-regression")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	var exists bool
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
	require.False(t, exists)
	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	// Assert the dedicated code and the message, not the ErrUtxoError aggregate
	// that wraps every batched spend failure — that wrapper is satisfied by any
	// spend error and would pass with the childPruned branch removed.
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "normal pruning must prevent recreation of a confirmed child")
	require.Contains(t, err.Error(), "spending transaction was pruned")
	require.NotErrorIs(t, err, errors.ErrSpent, "a pruned replay must not enter conflicting-transaction processing")
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
	require.False(t, exists)
	// Spend the remaining parent output, then prune the parent. Its marker must
	// not block defensive pruning and must disappear with the parent row.
	sibling := bt.NewTx()
	require.NoError(t, sibling.From(parent.TxID(), 1, parent.Outputs[1].LockingScript.String(), parent.Outputs[1].Satoshis))
	sibling.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, sibling.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, sibling, 1300)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{sibling.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1300, BlockHeight: 1300, OnLongestChain: true})
	require.NoError(t, err)
	n, err = svc.Prune(ctx, 1700, "prune-parent")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	var markers int
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deleted_children").Scan(&markers))
	require.Zero(t, markers, "markers must be bounded by surviving parents")
}

// TestPrunedReplayRollsBackSiblingSpends pins the consumer side of the pruned-replay
// rejection. A replayed child spends two parents: one still carries the pruner's
// marker and is rejected, the other does not and commits a fresh spend. The
// transaction can never exist, so that fresh spend has to be rolled back —
// otherwise the sibling output is permanently recorded as spent by an absent
// transaction and a genuine later spender gets ErrUtxoSpent naming a txid that
// is nowhere in the store.
//
// The end state asserted is the sibling output's spending_data read back from the
// store, not the fact that Unspend was called.
func TestPrunedReplayRollsBackSiblingSpends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *Store

			if backend == "postgres" {
				store, ctx = setupPostgresStore(t)
			} else {
				store, _ = setup(ctx, t)
			}

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)
			require.NoError(t, store.SetBlockHeight(1000))

			parents := make([]*bt.Tx, 2)

			for i := range parents {
				parent := bt.NewTx()
				// Distinct funding outpoints so the two parents get distinct txids.
				require.NoError(t, parent.From("111111111111111111111111111111111111111111111111111111111111111"+string(rune('1'+i)), 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)

				parents[i] = parent
			}

			child := bt.NewTx()
			for _, parent := range parents {
				require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for i := range child.Inputs {
				child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, _, err := store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parents[0].TxIDChainHash(), parents[1].TxIDChainHash(), child.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
			require.NoError(t, err)

			grandchild := bt.NewTx()
			require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
			grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
			_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			svc, err := store.GetPrunerService()
			require.NoError(t, err)
			n, err := svc.Prune(ctx, 1300, "sibling-rollback")
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			// Asymmetry fixture: parents[1] loses its marker, exactly the state a
			// node that was already pruning before this change inherits for every
			// child the old code removed. Its output is freed too, so the replay
			// commits a fresh spend there rather than an idempotent re-spend.
			_, err = store.db.ExecContext(ctx,
				"DELETE FROM deleted_children WHERE parent_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parents[1].TxIDChainHash()[:])
			require.NoError(t, err)
			_, err = store.db.ExecContext(ctx,
				"UPDATE outputs SET spending_data = NULL WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parents[1].TxIDChainHash()[:])
			require.NoError(t, err)

			_, _, err = store.SpendAndCreate(ctx, child, 1200)
			require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

			var siblingSpent bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT spending_data IS NOT NULL FROM outputs WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parents[1].TxIDChainHash()[:]).Scan(&siblingSpent))
			require.False(t, siblingSpent, "the rejected replay must not leave the sibling output spent by a transaction that does not exist")

			var childExists bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&childExists))
			require.False(t, childExists, "the pruned child must not be recreated")
		})
	}
}

// TestPrunedReplayRejectedAfterUnspend pins the marker's lifetime against the
// rollback path. Unspend clears the parent output's spending_data and
// deliberately leaves deleted_children alone, so a check nested inside "still
// spent by exactly this child" would silently stop firing at the moment a
// compensating rollback depends on it. The check keys on the incoming spender
// instead, so it survives.
func TestPrunedReplayRejectedAfterUnspend(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *Store

			if backend == "postgres" {
				store, ctx = setupPostgresStore(t)
			} else {
				store, _ = setup(ctx, t)
			}

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)
			require.NoError(t, store.SetBlockHeight(1000))

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err = store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
			require.NoError(t, err)

			grandchild := bt.NewTx()
			require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
			grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
			_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			svc, err := store.GetPrunerService()
			require.NoError(t, err)
			n, err := svc.Prune(ctx, 1300, "unspend-then-replay")
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			// Roll the parent output's spend back through the real Unspend, the
			// path unspendWithRetry takes after a failed create.
			utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
			require.NoError(t, err)
			require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			}}))

			var stillSpent bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT spending_data IS NOT NULL FROM outputs WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parent.TxIDChainHash()[:]).Scan(&stillSpent))
			require.False(t, stillSpent, "fixture: the unspend must actually have cleared spending_data")

			var markers int
			require.NoError(t, store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deleted_children").Scan(&markers))
			require.Equal(t, 1, markers, "Unspend must leave the replay marker in place")

			_, _, err = store.SpendAndCreate(ctx, child, 1200)
			require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned,
				"the marker must still reject the pruned child once its spend has been rolled back")

			var exists bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
			require.False(t, exists)
		})
	}
}
