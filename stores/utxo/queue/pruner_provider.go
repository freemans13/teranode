package queue

import (
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// Ensure Store implements the pruner.PrunerServiceProvider interface.
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// GetPrunerService returns a pruner service for the queue store.
// TODO: implement in Task 11.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	return nil, errors.NewProcessingError("not implemented")
}
