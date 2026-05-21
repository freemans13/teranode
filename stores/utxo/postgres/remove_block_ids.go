package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveBlockIDs trims the supplied block IDs from each transaction's
// block_ids array column. Idempotent: array_remove silently no-ops when the
// element is absent.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	txn, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("failed to begin tx", err)
	}

	defer func() {
		_ = txn.Rollback(ctx)
	}()

	const q = `UPDATE txs SET block_ids = array_remove(block_ids, $1) WHERE hash = $2`

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}

		for _, blockID := range r.BlockIDs {
			if _, err = txn.Exec(ctx, q, int32(blockID), r.TxHash[:]); err != nil {
				return errors.NewStorageError("failed to remove block_id", err)
			}
		}
	}

	if err = txn.Commit(ctx); err != nil {
		return errors.NewStorageError("failed to commit remove block_ids tx", err)
	}

	return nil
}
