package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
)

// GetFees calculates the transaction fees for a Bitcoin transaction.
// For coinbase transactions, returns the sum of all output values.
// For regular transactions, returns input_sum - output_sum.
func GetFees(btTx *bt.Tx) (uint64, error) {
	if btTx.IsCoinbase() {
		fees := uint64(0)

		// fmt.Printf("coinbase tx: %s\n", btTx.String())
		for _, output := range btTx.Outputs {
			if output.Satoshis > 0 {
				fees += output.Satoshis
			}
		}

		return fees, nil
	}

	// SAO - there are some transactions (e.g. d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc) that have 0 satoshi inputs and
	// therefore look like they are not extended.  So for now, we will not check if the tx is extended.
	// if !btTx.IsExtended() {
	// 	return 0, fmt.Errorf("cannot get fees for non extended tx")
	// }

	if len(btTx.Inputs) == 0 {
		return 0, nil
	}

	inputFees := uint64(0)
	outputFees := uint64(0)

	for _, input := range btTx.Inputs {
		inputFees += input.PreviousTxSatoshis
	}

	for _, output := range btTx.Outputs {
		if output.Satoshis > 0 {
			outputFees += output.Satoshis
		}
	}

	if inputFees < outputFees {
		return 0, errors.NewProcessingError("input fees (%d) less than output fees (%d) for tx %s", inputFees, outputFees, btTx.TxID())
	}

	return inputFees - outputFees, nil
}
