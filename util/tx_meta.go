package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

func TxMetaDataFromTx(tx *bt.Tx) (*meta.Data, error) {
	// GetFees needs PreviousTxSatoshis on every input, i.e. an extended tx. On the
	// trusted-connect legacy-IBD path below the checkpoint the previous-output
	// decorate is skipped, so the tx is NOT extended and the fee can be neither
	// computed nor meaningfully validated (it is unused for historical blocks —
	// they are not mined or fee-ranked). Record fee 0 for non-extended txs; all
	// normal paths pass extended txs and compute/validate the fee as before.
	var fee uint64
	if tx.IsExtended() {
		var feeErr error
		fee, feeErr = GetFees(tx)
		if feeErr != nil {
			return nil, feeErr
		}
	}

	var err error
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
