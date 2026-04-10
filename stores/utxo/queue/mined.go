package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// minedChunkSize is the maximum number of hashes per bulk INSERT into block_ids.
const minedChunkSize = 500

// SetMinedMulti updates the block ID for multiple transactions that have been mined.
// Normal path: bulk INSERT into block_ids + UPDATE tx_state (locked, unmined_since).
// UnsetMined path (reorg): DELETE from block_ids for the given blockID.
// Returns a map of each hash to its list of block_ids.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	startTime := time.Now()
	defer func() {
		if prometheusDirectMinedDuration != nil {
			prometheusDirectMinedDuration.Observe(time.Since(startTime).Seconds())
		}
	}()

	if len(hashes) == 0 {
		return make(map[chainhash.Hash][]uint32), nil
	}

	if minedBlockInfo.UnsetMined {
		return s.unsetMinedMulti(ctx, hashes, minedBlockInfo.BlockID)
	}

	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	// Process in chunks for the INSERT into block_ids.
	for i := 0; i < len(hashes); i += minedChunkSize {
		end := i + minedChunkSize
		if end > len(hashes) {
			end = len(hashes)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if err := s.setMinedChunk(ctx, hashes[i:end], minedBlockInfo); err != nil {
			return nil, errors.NewStorageError("[SetMinedMulti] chunk %d-%d: %v", i, end-1, err)
		}
	}

	// Fetch block_ids for all hashes after the transaction is committed.
	for _, hash := range hashes {
		blockIDs, err := s.fetchBlockIDs(ctx, hash)
		if err != nil {
			return nil, err
		}
		resultMap[*hash] = blockIDs
	}

	return resultMap, nil
}

// setMinedChunk processes a single chunk of hashes within a single pgx transaction:
// 1. Bulk INSERT into block_ids via unnest.
// 2. Bulk UPDATE tx_state (locked=false, optionally clear unmined_since).
func (s *Store) setMinedChunk(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) error {
	hashBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		hashBytes[i] = h[:]
	}

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Step 1: Bulk INSERT into block_ids.
	_, err = pgxTx.Exec(ctx, `
		INSERT INTO block_ids (tx_hash, block_id, block_height, subtree_idx)
		SELECT unnest($1::bytea[]), $2, $3, $4
		ON CONFLICT (tx_hash, block_id) DO NOTHING`,
		hashBytes, int64(info.BlockID), int64(info.BlockHeight), int64(info.SubtreeIdx),
	)
	if err != nil {
		return errors.NewStorageError("[SetMinedMulti] INSERT block_ids: %v", err)
	}

	// Step 2: Update tx_state.
	inClause, inArgs := buildINClauseLocal(hashBytes, 1)
	if info.OnLongestChain {
		q := fmt.Sprintf(`UPDATE tx_state SET locked = false, unmined_since = NULL WHERE tx_hash IN %s`, inClause)
		if _, err = pgxTx.Exec(ctx, q, inArgs...); err != nil {
			return errors.NewStorageError("[SetMinedMulti] UPDATE tx_state (onLongestChain): %v", err)
		}
	} else {
		q := fmt.Sprintf(`UPDATE tx_state SET locked = false WHERE tx_hash IN %s`, inClause)
		if _, err = pgxTx.Exec(ctx, q, inArgs...); err != nil {
			return errors.NewStorageError("[SetMinedMulti] UPDATE tx_state: %v", err)
		}
	}

	return pgxTx.Commit(ctx)
}

// unsetMinedMulti handles the reorg path: DELETE from block_ids for the given blockID.
// If no block_ids remain after deletion, sets unmined_since to current block height.
func (s *Store) unsetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (map[chainhash.Hash][]uint32, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	for _, hash := range hashes {
		_, err = pgxTx.Exec(ctx,
			`DELETE FROM block_ids WHERE tx_hash = $1 AND block_id = $2`,
			hash[:], int64(blockID),
		)
		if err != nil {
			return nil, errors.NewStorageError("[UnsetMined] DELETE block_ids for %s: %v", hash, err)
		}
	}

	// If after deletion there are no remaining block_ids, set unmined_since.
	currentBlockHeight := int64(s.blockHeight.Load())
	for _, hash := range hashes {
		var count int
		err = pgxTx.QueryRow(ctx,
			`SELECT COUNT(*) FROM block_ids WHERE tx_hash = $1`, hash[:],
		).Scan(&count)
		if err != nil {
			return nil, errors.NewStorageError("[UnsetMined] COUNT block_ids for %s: %v", hash, err)
		}
		if count == 0 {
			_, err = pgxTx.Exec(ctx,
				`UPDATE tx_state SET unmined_since = $2 WHERE tx_hash = $1`,
				hash[:], currentBlockHeight,
			)
			if err != nil {
				return nil, errors.NewStorageError("[UnsetMined] UPDATE unmined_since for %s: %v", hash, err)
			}
		}
	}

	if err = pgxTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[UnsetMined] commit: %v", err)
	}

	// Fetch remaining block_ids.
	for _, hash := range hashes {
		blockIDs, fetchErr := s.fetchBlockIDs(ctx, hash)
		if fetchErr != nil {
			return nil, fetchErr
		}
		resultMap[*hash] = blockIDs
	}

	return resultMap, nil
}

// fetchBlockIDs returns the list of block_ids for a transaction, ordered by block_id.
func (s *Store) fetchBlockIDs(ctx context.Context, hash *chainhash.Hash) ([]uint32, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT block_id FROM block_ids WHERE tx_hash = $1 ORDER BY block_id`,
		hash[:],
	)
	if err != nil {
		return nil, errors.NewStorageError("[fetchBlockIDs] query for %s: %v", hash, err)
	}
	defer rows.Close()

	var blockIDs []uint32
	for rows.Next() {
		var bid int64
		if err := rows.Scan(&bid); err != nil {
			return nil, errors.NewStorageError("[fetchBlockIDs] scan for %s: %v", hash, err)
		}
		blockIDs = append(blockIDs, uint32(bid))
	}
	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[fetchBlockIDs] rows error for %s: %v", hash, err)
	}

	return blockIDs, nil
}

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
