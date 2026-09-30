package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// ParentOutputsForValidation implements utxo.Store.
//
// Placeholder until the netted implementation lands: every outpoint gets a
// retryable error, which callers treat as a store fault and retry, never as a
// verdict on a transaction.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	for i := range answers {
		answers[i] = utxo.ParentOutput{Err: errors.NewProcessingError("[utxoset] ParentOutputsForValidation not implemented yet")}
	}

	return answers, nil
}

// SpendAndCreateMulti implements utxo.Store through the shared default until the
// netted implementation lands.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, 1, txs, blockHeight, opts...)
}
