package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// A list whose parent already exists falls back to the per-transaction default after step 1
// has committed every outside spend. A child that the fallback then rejects must not keep the
// outside spends step 1 made for it: its independent input is unspent afterwards and another
// transaction can take it.
func TestSpendAndCreateMultiFallbackRestoresTheOutsideSpendsOfARejectedChild(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const height = 1000

	w := tests.BuildMultiWorkload(t, 0xb0, 1, 2)
	w.StoreRoots(t, s, height-1)
	rootA, rootB := w.Roots[0], w.Roots[1]

	// The parent exists before the list runs, which sends the list to the fallback, and a
	// thief has taken the output the child spends, which the fallback finds and step 1 cannot.
	parent := payFrom(t, 1, []*bt.Tx{rootA}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, parent, height-1)
	require.NoError(t, err)

	thief := payFrom(t, 2, []*bt.Tx{parent}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, thief, height-1)
	require.NoError(t, err)

	child := payFrom(t, 3, []*bt.Tx{parent, rootB}, []uint32{0, 0}) // rootB:0 is the independent input

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status)
	require.Equal(t, utxo.MultiTxFailed, results[1].Status, "%v", results[1].Err)
	require.ErrorIs(t, results[1].Err, errors.ErrSpent)

	_, err = s.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the rejected child has no record")

	utxoHash, err := util.UTXOHashFromOutput(rootB.TxIDChainHash(), rootB.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: rootB.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "rootB:0 is still marked spent by the rejected child")

	other := payFrom(t, 4, []*bt.Tx{rootB}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, other, height)
	require.NoError(t, err, "rootB:0 is not spendable by another transaction")
}
