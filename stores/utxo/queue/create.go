package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// Create stores a new transaction's outputs as UTXOs.
// TODO: implement direct INSERT via unnest in Task 3.
func (s *Store) Create(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, error) {
	return nil, errors.NewProcessingError("not implemented")
}
