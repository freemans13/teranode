package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Delete removes a transaction and all its associated data.
// TODO: implement updated for new tables in Task 8.
func (s *Store) Delete(_ context.Context, _ *chainhash.Hash) error {
	return errors.NewProcessingError("not implemented")
}
