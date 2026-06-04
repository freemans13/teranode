package postgres

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Delete removes a transaction and all its associated data from all 3 tables
// in a single pgx transaction.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[Delete] begin: %v", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Delete in dependency order: children tables first, then parent table.
	deleteStatements := []string{
		`DELETE FROM spends WHERE prev_tx_hash = $1`,
		`DELETE FROM outputs WHERE tx_hash = $1`,
		`DELETE FROM txs WHERE hash = $1`,
	}

	for _, stmt := range deleteStatements {
		if _, err = pgxTx.Exec(ctx, stmt, hash[:]); err != nil {
			return errors.NewStorageError("[Delete] failed for %s: %v", hash, err)
		}
	}

	return pgxTx.Commit(ctx)
}

// setDAH sets or clears the delete_at_height field in txs based on
// whether all outputs are spent, the transaction has block_ids, and is on the
// longest chain (unmined_since IS NULL).
func (s *Store) setDAH(ctx context.Context, hash *chainhash.Hash) error {
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		return nil
	}

	// Check preserve_until first: if set, don't touch DAH.
	var preserveUntil *int64
	err := s.pool.QueryRow(ctx,
		`SELECT preserve_until FROM txs WHERE hash = $1`,
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
			WHERE o.tx_hash = $1 AND o.spendable
			AND NOT EXISTS (SELECT 1 FROM spends sp WHERE sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx)
		) AS all_spent`,
		hash[:],
	).Scan(&allSpent)
	if err != nil {
		return errors.NewStorageError("[setDAH] check all_spent for %s: %v", hash, err)
	}

	// Check if has block_ids and is on longest chain — read from txs arrays.
	var blockIDs []int32
	var onLongestChain bool
	err = s.pool.QueryRow(ctx,
		`SELECT block_ids, (unmined_since IS NULL) FROM txs WHERE hash = $1`,
		hash[:],
	).Scan(&blockIDs, &onLongestChain)
	if err != nil {
		return errors.NewStorageError("[setDAH] check block_ids for %s: %v", hash, err)
	}

	hasBlockIDs := len(blockIDs) > 0
	newDAH := int64(s.blockHeight.Load() + 1 + retention)

	if allSpent && hasBlockIDs && onLongestChain {
		// Set delete_at_height, but only bump forward (never decrease).
		_, err = s.pool.Exec(ctx, `
			UPDATE txs
			SET delete_at_height = CASE
				WHEN delete_at_height IS NULL OR delete_at_height < $2 THEN $2
				ELSE delete_at_height
			END
			WHERE hash = $1`,
			hash[:], newDAH,
		)
		if err != nil {
			return errors.NewStorageError("[setDAH] set DAH for %s: %v", hash, err)
		}
	} else {
		// Clear delete_at_height since conditions are not met.
		_, err = s.pool.Exec(ctx, `
			UPDATE txs SET delete_at_height = NULL WHERE hash = $1`,
			hash[:],
		)
		if err != nil {
			return errors.NewStorageError("[setDAH] clear DAH for %s: %v", hash, err)
		}
	}

	return nil
}
