package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Delete removes a transaction and all its associated data from all tables
// in a single pgx transaction.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[Delete] begin: %v", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Delete in dependency order: children tables first, then parent tables.
	deleteStatements := []string{
		`DELETE FROM spends WHERE prev_tx_hash = $1`,
		`DELETE FROM block_ids WHERE tx_hash = $1`,
		`DELETE FROM outputs WHERE tx_hash = $1`,
		`DELETE FROM inputs WHERE tx_hash = $1`,
		`DELETE FROM conflicting_children WHERE tx_hash = $1`,
		`DELETE FROM conflicting_children WHERE child_tx_hash = $1`,
		`DELETE FROM tx_state WHERE tx_hash = $1`,
		`DELETE FROM transactions WHERE hash = $1`,
	}

	for _, stmt := range deleteStatements {
		if _, err = pgxTx.Exec(ctx, stmt, hash[:]); err != nil {
			return errors.NewStorageError("[Delete] failed for %s: %v", hash, err)
		}
	}

	return pgxTx.Commit(ctx)
}

// setDAH sets or clears the delete_at_height field in tx_state based on
// whether all outputs are spent, the transaction has block_ids, and is on the
// longest chain (unmined_since IS NULL).
//
// This mirrors the aerospike Lua setDeleteAtHeight logic:
//   - If preserve_until is set: don't touch delete_at_height
//   - If all outputs are spent AND has block_ids AND on longest chain:
//     set delete_at_height = blockHeight + retention (bump forward if smaller)
//   - Otherwise: clear delete_at_height
func (s *Store) setDAH(ctx context.Context, hash *chainhash.Hash) error {
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		return nil
	}

	// Check preserve_until first: if set, don't touch DAH.
	var preserveUntil *int64
	err := s.pool.QueryRow(ctx,
		`SELECT preserve_until FROM tx_state WHERE tx_hash = $1`,
		hash[:],
	).Scan(&preserveUntil)
	if err != nil {
		return errors.NewStorageError("[setDAH] query preserve_until for %s: %v", hash, err)
	}
	if preserveUntil != nil {
		return nil
	}

	// Check if all outputs are spent.
	var allSpent bool
	err = s.pool.QueryRow(ctx, `
		SELECT NOT EXISTS(
			SELECT 1 FROM outputs o
			WHERE o.tx_hash = $1
			AND NOT EXISTS (SELECT 1 FROM spends sp WHERE sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx)
		) AS all_spent`,
		hash[:],
	).Scan(&allSpent)
	if err != nil {
		return errors.NewStorageError("[setDAH] check all_spent for %s: %v", hash, err)
	}

	// Check if has block_ids and is on longest chain.
	var hasBlockIDs bool
	err = s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM block_ids WHERE tx_hash = $1)`,
		hash[:],
	).Scan(&hasBlockIDs)
	if err != nil {
		return errors.NewStorageError("[setDAH] check block_ids for %s: %v", hash, err)
	}

	var onLongestChain bool
	err = s.pool.QueryRow(ctx,
		`SELECT unmined_since IS NULL FROM tx_state WHERE tx_hash = $1`,
		hash[:],
	).Scan(&onLongestChain)
	if err != nil {
		return errors.NewStorageError("[setDAH] check unmined_since for %s: %v", hash, err)
	}

	newDAH := int64(s.blockHeight.Load() + 1 + retention)

	if allSpent && hasBlockIDs && onLongestChain {
		// Set delete_at_height, but only bump forward (never decrease).
		_, err = s.pool.Exec(ctx, `
			UPDATE tx_state
			SET delete_at_height = CASE
				WHEN delete_at_height IS NULL OR delete_at_height < $2 THEN $2
				ELSE delete_at_height
			END
			WHERE tx_hash = $1`,
			hash[:], newDAH,
		)
		if err != nil {
			return errors.NewStorageError("[setDAH] set DAH for %s: %v", hash, err)
		}
	} else {
		// Clear delete_at_height since conditions are not met.
		_, err = s.pool.Exec(ctx, `
			UPDATE tx_state SET delete_at_height = NULL WHERE tx_hash = $1`,
			hash[:],
		)
		if err != nil {
			return errors.NewStorageError("[setDAH] clear DAH for %s: %v", hash, err)
		}
	}

	return nil
}
