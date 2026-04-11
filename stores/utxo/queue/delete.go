package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Delete removes a transaction from the utxos table. One table, one DELETE.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM utxos WHERE hash = $1`, hash[:])
	if err != nil {
		return errors.NewStorageError("[Delete] failed for %s: %v", hash, err)
	}

	// Evict from cache.
	s.cache.Remove(*hash)

	return nil
}

// setDAH sets or clears the delete_at_height field in utxos based on
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
		`SELECT preserve_until FROM utxos WHERE hash = $1`,
		hash[:],
	).Scan(&preserveUntil)
	if err != nil {
		return errors.NewStorageError("[setDAH] query preserve_until for %s: %v", hash, err)
	}
	if preserveUntil != nil {
		return nil
	}

	// Check if all outputs are spent using the spending_data array.
	// All outputs are spent when every element in spending_data is non-NULL
	// and spent_count equals the array length.
	var spentCount int
	var numOutputs int
	var blockIDs []int32
	var onLongestChain bool
	err = s.pool.QueryRow(ctx, `
		SELECT spent_count, COALESCE(array_length(spending_data, 1), 0),
		       block_ids, (unmined_since IS NULL)
		FROM utxos WHERE hash = $1`,
		hash[:],
	).Scan(&spentCount, &numOutputs, &blockIDs, &onLongestChain)
	if err != nil {
		return errors.NewStorageError("[setDAH] check all_spent for %s: %v", hash, err)
	}

	allSpent := numOutputs > 0 && spentCount >= numOutputs
	hasBlockIDs := len(blockIDs) > 0
	newDAH := int64(s.blockHeight.Load() + 1 + retention)

	if allSpent && hasBlockIDs && onLongestChain {
		// Set delete_at_height, but only bump forward (never decrease).
		_, err = s.pool.Exec(ctx, `
			UPDATE utxos
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
			UPDATE utxos SET delete_at_height = NULL WHERE hash = $1`,
			hash[:],
		)
		if err != nil {
			return errors.NewStorageError("[setDAH] clear DAH for %s: %v", hash, err)
		}
	}

	return nil
}
