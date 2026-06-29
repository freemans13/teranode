package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// TxMetaDataFromTxNoFee builds tx metadata without computing fees. Used by minimal (below-checkpoint)
// create where inputs are not decorated, so GetFees would error. Fee is set to 0; TxInpoints (parent
// outpoints, used by the pruner) and size are computed exactly as in TxMetaDataFromTx.
func TxMetaDataFromTxNoFee(tx *bt.Tx) (*meta.Data, error) {
	var txInpoints subtree.TxInpoints
	var err error
	if tx.IsCoinbase() {
		txInpoints = subtree.TxInpoints{}
	} else {
		txInpoints, err = subtree.NewTxInpointsFromTx(tx)
		if err != nil {
			return nil, err
		}
	}

	s := meta.Data{
		Tx:         tx,
		TxInpoints: txInpoints,
		BlockIDs:   make([]uint32, 0),
		Fee:        0,
		IsCoinbase: tx.IsCoinbase(),
		LockTime:   tx.LockTime,
	}
	if len(tx.Inputs) > 0 {
		s.SizeInBytes = uint64(tx.Size())
	}
	return &s, nil
}

func TxMetaDataFromTx(tx *bt.Tx) (*meta.Data, error) {
	fee, err := GetFees(tx)
	if err != nil {
		return nil, err
	}

	var txInpoints subtree.TxInpoints
	if tx.IsCoinbase() {
		// For coinbase transactions, we do not have inputs, so we create an empty TxInpoints.
		txInpoints = subtree.TxInpoints{}
	} else {
		txInpoints, err = subtree.NewTxInpointsFromTx(tx)
		if err != nil {
			return nil, err
		}
	}

	s := meta.Data{
		Tx:         tx,
		TxInpoints: txInpoints,
		BlockIDs:   make([]uint32, 0),
		Fee:        fee,
		IsCoinbase: tx.IsCoinbase(),
		LockTime:   tx.LockTime,
	}

	// For partially populated utxos, we will have no inputs and possibly some nil outputs.
	// Therefore, we do not call tx.Size() as it will panic.
	if len(tx.Inputs) > 0 {
		s.SizeInBytes = uint64(tx.Size())
	}

	return &s, nil
}
