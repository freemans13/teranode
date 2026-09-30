package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// parentOutputsChild builds a child spending (parent[i], vout[i]) for each i, with
// forged previous-output fields on every input, so a test can tell whether the
// validator overwrote them from the store.
func parentOutputsChild(t *testing.T, parents []chainhash.Hash, vouts []uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for i := range parents {
		in := &bt.Input{
			PreviousTxOutIndex: vouts[i],
			PreviousTxScript:   bscript.NewFromBytes([]byte{0x51}), // forged OP_TRUE
			PreviousTxSatoshis: 21_000_000_00000000,
			UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
		}
		require.NoError(t, in.PreviousTxIDAdd(&parents[i]))
		tx.Inputs = append(tx.Inputs, in)
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))

	return tx
}

func parentOutputsValidator(store utxostore.Store) *Validator {
	return &Validator{settings: settings.NewSettings(), utxoStore: store}
}

func TestGetUtxoBlockHeightsAndExtendTx_UsesParentOutputs(t *testing.T) {
	ctx := context.Background()

	p0 := chainhash.Hash{0x01}
	p1 := chainhash.Hash{0x02}
	tx := parentOutputsChild(t, []chainhash.Hash{p0, p1, p0}, []uint32{3, 0, 1})

	s0 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x00})
	s1 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x01})
	s2 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x02})

	store := &utxostore.MockUtxostore{}
	store.On("ParentOutputsForValidation", mock.Anything, []utxostore.Outpoint{
		{TxID: p0, Vout: 3}, {TxID: p1, Vout: 0}, {TxID: p0, Vout: 1},
	}).Return([]utxostore.ParentOutput{
		{Status: utxostore.ParentOutputMined, Satoshis: 100, LockingScript: s0, Height: 120},
		{Status: utxostore.ParentOutputNotMined, Satoshis: 200, LockingScript: s1},
		{Status: utxostore.ParentOutputMined, Satoshis: 300, LockingScript: s2, Height: 120},
	}, nil).Once()

	heights, err := parentOutputsValidator(store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
	require.NoError(t, err)
	require.Equal(t, []uint32{120, unconfirmedParentHeight, 120}, heights)

	// Every input is overwritten from the store answer, never trusted as supplied
	// (GHSA-v76m-6vc7-g7c7).
	require.Equal(t, uint64(100), tx.Inputs[0].PreviousTxSatoshis)
	require.Same(t, s0, tx.Inputs[0].PreviousTxScript)
	require.Equal(t, uint64(200), tx.Inputs[1].PreviousTxSatoshis)
	require.Same(t, s1, tx.Inputs[1].PreviousTxScript)
	require.Equal(t, uint64(300), tx.Inputs[2].PreviousTxSatoshis)
	require.Same(t, s2, tx.Inputs[2].PreviousTxScript)

	store.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
	store.AssertExpectations(t)
}

func TestGetUtxoBlockHeightsAndExtendTx_ParentOutputFailures(t *testing.T) {
	ctx := context.Background()
	p0 := chainhash.Hash{0x01}

	cases := []struct {
		name   string
		answer utxostore.ParentOutput
		want   error
	}{
		{"missing transaction is a missing parent", utxostore.ParentOutput{Status: utxostore.ParentOutputTxNotFound}, errors.ErrTxMissingParent},
		{"no such index is invalid", utxostore.ParentOutput{Status: utxostore.ParentOutputNoSuchIndex}, errors.ErrTxInvalid},
		{"a store fault is a processing error", utxostore.ParentOutput{Err: errors.NewStorageError("timeout")}, errors.ErrProcessing},
		{"an unknown answer is a processing error", utxostore.ParentOutput{}, errors.ErrProcessing},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := parentOutputsChild(t, []chainhash.Hash{p0}, []uint32{0})

			store := &utxostore.MockUtxostore{}
			store.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return([]utxostore.ParentOutput{tc.answer}, nil).Once()

			_, err := parentOutputsValidator(store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
			require.ErrorIs(t, err, tc.want)
		})
	}

	t.Run("a call-level error is a processing error", func(t *testing.T) {
		tx := parentOutputsChild(t, []chainhash.Hash{p0}, []uint32{0})

		store := &utxostore.MockUtxostore{}
		store.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(nil, errors.NewServiceUnavailableError("down")).Once()

		_, err := parentOutputsValidator(store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
		require.ErrorIs(t, err, errors.ErrProcessing)
	})

	t.Run("a short answer list is a processing error", func(t *testing.T) {
		tx := parentOutputsChild(t, []chainhash.Hash{p0}, []uint32{0})

		store := &utxostore.MockUtxostore{}
		store.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return([]utxostore.ParentOutput{}, nil).Once()

		_, err := parentOutputsValidator(store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
		require.ErrorIs(t, err, errors.ErrProcessing)
	})
}

// Prefetched parents are read from the map and never sent to the store; they
// report the lowest recorded height, as the store does.
func TestGetUtxoBlockHeightsAndExtendTx_PrefetchedAndStoreMix(t *testing.T) {
	ctx := context.Background()

	p0 := chainhash.Hash{0x01}
	p1 := chainhash.Hash{0x02}
	tx := parentOutputsChild(t, []chainhash.Hash{p0, p1}, []uint32{0, 0})

	prefetched := map[chainhash.Hash]*meta.Data{
		p0: {BlockHeights: []uint32{126, 125}, Tx: prefetchParentTx(1000)},
	}

	store := &utxostore.MockUtxostore{}
	store.On("ParentOutputsForValidation", mock.Anything, []utxostore.Outpoint{{TxID: p1, Vout: 0}}).Return([]utxostore.ParentOutput{
		{Status: utxostore.ParentOutputMined, Satoshis: 7, LockingScript: bscript.NewFromBytes([]byte{0x6a}), Height: 90},
	}, nil).Once()

	heights, err := parentOutputsValidator(store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)
	require.Equal(t, []uint32{125, 90}, heights)
	require.Equal(t, uint64(1000), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, uint64(7), tx.Inputs[1].PreviousTxSatoshis)
	store.AssertExpectations(t)
}

// extendTransaction reads through the same method, and overwrites every input.
func TestExtendTransaction_UsesParentOutputs(t *testing.T) {
	ctx := context.Background()

	p0 := chainhash.Hash{0x01}
	tx := parentOutputsChild(t, []chainhash.Hash{p0}, []uint32{2})
	s := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x05})

	store := &utxostore.MockUtxostore{}
	store.On("ParentOutputsForValidation", mock.Anything, []utxostore.Outpoint{{TxID: p0, Vout: 2}}).Return([]utxostore.ParentOutput{
		{Status: utxostore.ParentOutputNotMined, Satoshis: 55, LockingScript: s},
	}, nil).Once()

	require.NoError(t, parentOutputsValidator(store).extendTransaction(ctx, tx))
	require.Equal(t, uint64(55), tx.Inputs[0].PreviousTxSatoshis)
	require.Same(t, s, tx.Inputs[0].PreviousTxScript)
	require.True(t, tx.IsExtended())
	store.AssertNotCalled(t, "PreviousOutputsDecorate", mock.Anything, mock.Anything)
}
