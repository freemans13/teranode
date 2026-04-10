package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// PreserveTransactions marks transactions to be preserved from deletion.
// TODO: implement in Task 11.
func (s *Store) PreserveTransactions(_ context.Context, _ []chainhash.Hash, _ uint32) error {
	return errors.NewProcessingError("not implemented")
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
// TODO: implement in Task 11.
func (s *Store) ProcessExpiredPreservations(_ context.Context, _ uint32) error {
	return errors.NewProcessingError("not implemented")
}
