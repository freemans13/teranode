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
		return errors.NewStorageError("[Delete] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Delete in dependency order: children tables first, then parent table.
	deleteStatements := []string{
		`DELETE FROM spends WHERE prev_tx_hash = $1`,
		`DELETE FROM txs WHERE hash = $1`,
	}

	for _, stmt := range deleteStatements {
		if _, err = pgxTx.Exec(ctx, stmt, hash[:]); err != nil {
			return errors.NewStorageError("[Delete] failed for %s", hash, err)
		}
	}

	if err = pgxTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[Delete] commit", err)
	}

	return nil
}
