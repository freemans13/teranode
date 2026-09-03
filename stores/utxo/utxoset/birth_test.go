package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// mkDataTx builds a transaction whose only output is OP_FALSE OP_RETURN data, which is
// provably unspendable in every era, so the store writes no coin row for it. The payload
// varies the transaction id.
func mkDataTx(t *testing.T, payload string) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.AddOpReturnOutput([]byte(payload)))

	return tx
}

func birthRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) (n int, maxHeight int) {
	t.Helper()

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(created_height), -1) FROM tx_birth WHERE txid = $1`,
		hashBytes(tx)).Scan(&n, &maxHeight))

	return n, maxHeight
}

// TestOnlyATransactionWithNoSpendableOutputsGetsABirthRow.
//
// The reclaimer learns about a transaction from a spend of one of its outputs, and a
// transaction with no spendable outputs is never anyone's parent, so nothing would ever name
// it. The birth ledger is the work list for exactly that class and nothing else.
func TestOnlyATransactionWithNoSpendableOutputsGetsABirthRow(t *testing.T) {
	s, ctx := newTestStore(t)

	data := mkDataTx(t, "just data")
	_, err := s.Create(ctx, data, 100)
	require.NoError(t, err)

	normal := mkTx(t, 1, 5_000)
	_, err = s.Create(ctx, normal, 100)
	require.NoError(t, err)

	n, h := birthRows(t, s, ctx, data)
	require.Equal(t, 1, n, "no coin row was written, so the birth ledger must name it")
	require.Equal(t, 100, h)

	n, _ = birthRows(t, s, ctx, normal)
	require.Equal(t, 0, n, "a transaction with a coin is found by the spend of that coin")
}

// TestReclaimRemovesAZeroOutputTransactionOnceSettled. The row exists while the mempool, the
// stamp postcondition and the block persister need it; once the transaction is mined on the
// main chain, buried past the depth the node could un-mine it, and its birth window has aged
// past journal retention, nothing can ask for it again.
func TestReclaimRemovesAZeroOutputTransactionOnceSettled(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	data := mkDataTx(t, "settled data")
	_, err := s.Create(ctx, data, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, data), "mined, deep, no coins: nothing can need it")

	n, _ := birthRows(t, s, ctx, data)
	require.Equal(t, 0, n, "its birth window has retired with it")
}

// TestReclaimKeepsAndRequeuesAnUnminedZeroOutputTransaction. A data transaction still in the
// mempool is exactly what block assembly needs the row for. It stays, and it is re-queued
// into the current window so it is judged again once it has had time to be mined.
func TestReclaimKeepsAndRequeuesAnUnminedZeroOutputTransaction(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	data := mkDataTx(t, "waiting data")
	_, err := s.Create(ctx, data, 100)
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, data), "unmined, so block assembly may still mine it")

	n, h := birthRows(t, s, ctx, data)
	require.Equal(t, 1, n, "re-queued once, into the window of the height it was judged at")
	require.Equal(t, 1_000, h)
}
