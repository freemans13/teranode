package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// Get retrieves UTXO metadata for a given transaction hash.
// TODO: implement JOINs for spends + tx_state in Task 4.
func (s *Store) Get(_ context.Context, _ *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// GetMeta retrieves only the metadata for a transaction.
// TODO: implement in Task 4.
func (s *Store) GetMeta(_ context.Context, _ *chainhash.Hash, _ *meta.Data) error {
	return errors.NewProcessingError("not implemented")
}

// GetSpend retrieves the spend status for a specific UTXO.
// TODO: implement in Task 4.
func (s *Store) GetSpend(_ context.Context, _ *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// BatchDecorate efficiently fetches metadata for multiple transactions.
// TODO: implement in Task 4.
func (s *Store) BatchDecorate(_ context.Context, _ []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	return errors.NewProcessingError("not implemented")
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
// TODO: implement in Task 4.
func (s *Store) PreviousOutputsDecorate(_ context.Context, _ *bt.Tx) error {
	return errors.NewProcessingError("not implemented")
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs
// across multiple transactions in bulk.
// TODO: implement in Task 4.
func (s *Store) BatchPreviousOutputsDecorate(_ context.Context, _ []*bt.Tx) error {
	return errors.NewProcessingError("not implemented")
}
