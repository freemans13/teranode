package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveBlockIDs trims the supplied block IDs from each transaction's
// block_ids array column, dropping the matching positions from the parallel
// block_heights and subtree_idxs arrays so all three stay index-aligned.
// Idempotent: a block ID that is absent matches nothing.
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

	// Trim the supplied block IDs from the parallel block_ids / block_heights /
	// subtree_idxs arrays in a SINGLE atomic UPDATE, dropping the SAME positions
	// from all three arrays so they stay index-aligned. A Get zips these three
	// arrays by position (get.go:139); the previous array_remove(block_ids, ...)
	// touched only block_ids, leaving block_heights and subtree_idxs one element
	// too long and silently misaligning every subsequent read. This mirrors the
	// UNNEST WITH ORDINALITY re-aggregation used by unsetMinedMulti.
	// Idempotent: a block ID that is absent matches nothing.
	//
	// The three arrays are decoded ONCE: a single correlated row-subquery unnests
	// them together and re-aggregates the kept positions, assigned to all three
	// columns via a multi-column SET — rather than re-running the same unnest three
	// times, one per SET column, as before. (A FROM/LATERAL subquery cannot
	// reference the UPDATE target, but a SET subquery can, so this stays one pass.)
	const q = `
		UPDATE txs t SET
			(block_ids, block_heights, subtree_idxs) = (
				SELECT COALESCE(array_agg(e.bid ORDER BY e.ord), '{}'::int[]),
				       COALESCE(array_agg(e.bh  ORDER BY e.ord), '{}'::int[]),
				       COALESCE(array_agg(e.si  ORDER BY e.ord), '{}'::int[])
				FROM unnest(t.block_ids, t.block_heights, t.subtree_idxs) WITH ORDINALITY AS e(bid, bh, si, ord)
				WHERE e.bid <> ALL($1::int[])
			)
		WHERE t.hash = $2`

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}

		if len(r.BlockIDs) == 0 {
			continue
		}

		ids := make([]int32, len(r.BlockIDs))
		for i, id := range r.BlockIDs {
			ids[i] = int32(id)
		}

		if _, err = txn.Exec(ctx, q, ids, r.TxHash[:]); err != nil {
			return errors.NewStorageError("failed to remove block_ids", err)
		}
	}

	if err = txn.Commit(ctx); err != nil {
		return errors.NewStorageError("failed to commit remove block_ids tx", err)
	}

	return nil
}
