package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// GetCounterConflicting returns the counter conflicting transactions for a given hash.
// TODO: implement in Task 6.
func (s *Store) GetCounterConflicting(_ context.Context, _ chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// GetConflictingChildren returns the children of the given conflicting transaction.
// TODO: implement in Task 6.
func (s *Store) GetConflictingChildren(_ context.Context, _ chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// SetConflicting marks transactions as conflicting or not conflicting.
// TODO: implement direct UPDATE tx_state in Task 6.
func (s *Store) SetConflicting(_ context.Context, _ []chainhash.Hash, _ bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, errors.NewProcessingError("not implemented")
}

// SetLocked marks transactions as locked for spending.
// TODO: implement direct UPDATE tx_state in Task 6.
func (s *Store) SetLocked(_ context.Context, _ []chainhash.Hash, _ bool) error {
	return errors.NewProcessingError("not implemented")
}
