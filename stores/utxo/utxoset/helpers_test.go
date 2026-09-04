package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// hashBytes is the transaction id as the store stores it.
func hashBytes(tx *bt.Tx) []byte {
	h := tx.TxIDChainHash()

	return h[:]
}

// hashes wraps one transaction as the slice SetMinedMulti takes.
func hashes(tx *bt.Tx) []*chainhash.Hash {
	return []*chainhash.Hash{tx.TxIDChainHash()}
}

func identExists(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_ident WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n > 0
}

// spendOneOutput builds a transaction taking one of parent's outputs and applies the spend at
// height, leaving the spender unmined.
func spendOneOutput(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout uint32,
	height uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	_, err := s.Create(ctx, child, height)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, child, height)
	require.NoError(t, err)

	return child
}

// createDirect writes one transaction through the single create path, in a transaction of its
// own, whatever batcher the store is configured with.
//
// createIn needs a pgx.Tx because the claim's advisory lock is transaction-scoped, so a test
// that wants the single path for setup has to supply one. Setup that went through s.Create
// instead would wait on a flush window and depend on the code under test.
func createDirect(s *Store, ctx context.Context, tx *bt.Tx, height uint32) error {
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}

	if _, err := s.createIn(ctx, dbTx, tx, height); err != nil {
		_ = dbTx.Rollback(ctx)

		return err
	}

	return dbTx.Commit(ctx)
}
