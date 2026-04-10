package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// Spend marks UTXOs consumed by the given transaction as spent.
// TODO: implement direct INSERT with validation CTE in Task 5.
func (s *Store) Spend(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// Unspend reverses a previous spend operation.
// TODO: implement in Task 8.
func (s *Store) Unspend(_ context.Context, _ []*utxo.Spend, _ ...bool) error {
	return errors.NewProcessingError("not implemented")
}
