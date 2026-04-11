package queue

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// PreserveTransactions marks transactions to be preserved from deletion.
// Sets preserve_until to the given height and clears delete_at_height.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	totalAffected := int64(0)

	for i := 0; i < len(txIDs); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txIDs) {
			end = len(txIDs)
		}

		chunk := make([][]byte, end-i)
		for j, txID := range txIDs[i:end] {
			id := txID
			chunk[j] = id[:]
		}

		inClause, args := buildINClauseLocal(chunk, 2)
		q := fmt.Sprintf(`UPDATE utxos SET preserve_until = $1, delete_at_height = NULL WHERE hash IN %s`, inClause)

		allArgs := append([]interface{}{int64(preserveUntilHeight)}, args...)
		result, err := s.pool.Exec(ctx, q, allArgs...)
		if err != nil {
			return errors.NewStorageError("[PreserveTransactions] failed to preserve chunk: %v", err)
		}

		totalAffected += result.RowsAffected()
	}

	s.logger.Debugf("[PreserveTransactions] preserved %d out of %d transactions", totalAffected, len(txIDs))

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
// Sets delete_at_height = currentHeight + retention and clears preserve_until.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	deleteAtHeight := currentHeight + s.settings.GetUtxoStoreBlockHeightRetention()

	result, err := s.pool.Exec(ctx, `
		UPDATE utxos SET delete_at_height = $1, preserve_until = NULL
		WHERE preserve_until IS NOT NULL AND preserve_until <= $2
	`, int64(deleteAtHeight), int64(currentHeight))
	if err != nil {
		return errors.NewStorageError("failed to process expired preservations", err)
	}

	rowsAffected := result.RowsAffected()
	s.logger.Infof("[ProcessExpiredPreservations] processed %d expired preservations at height %d", rowsAffected, currentHeight)

	return nil
}
