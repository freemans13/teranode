package queue

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// TODO: implement in Task 10.
func (s *Store) FreezeUTXOs(_ context.Context, _ []*utxo.Spend, _ *settings.Settings) error {
	return errors.NewProcessingError("not implemented")
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// TODO: implement in Task 10.
func (s *Store) UnFreezeUTXOs(_ context.Context, _ []*utxo.Spend, _ *settings.Settings) error {
	return errors.NewProcessingError("not implemented")
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// TODO: implement in Task 10.
func (s *Store) ReAssignUTXO(_ context.Context, _ *utxo.Spend, _ *utxo.Spend, _ *settings.Settings) error {
	return errors.NewProcessingError("not implemented")
}
