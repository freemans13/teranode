package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// minedChunkSize is the maximum number of hashes per bulk UPDATE.
// Larger chunks = fewer round trips. Simple array append keeps per-row cost constant.
const minedChunkSize = 2000

// SQL strings for mined operations. The `% NumPartitions` modulus is
// substituted from the Go constant so bumping NumPartitions automatically
// updates every partition_key derivation.
var (
	upsertBlocksSQL = fmt.Sprintf(`
		INSERT INTO txs_blocks (hash, partition_key, block_ids, block_heights, subtree_idxs)
		SELECT u.hash, get_byte(u.hash, 1) %% %d, ARRAY[$2::int], ARRAY[$3::int], ARRAY[$4::int]
		FROM UNNEST($1::bytea[]) AS u(hash)
		ON CONFLICT (hash, partition_key) DO UPDATE SET
		    block_ids     = txs_blocks.block_ids     || EXCLUDED.block_ids,
		    block_heights = txs_blocks.block_heights || EXCLUDED.block_heights,
		    subtree_idxs  = txs_blocks.subtree_idxs  || EXCLUDED.subtree_idxs`, NumPartitions)

	unsetMinedReadSQL = fmt.Sprintf(
		`SELECT block_ids, block_heights, subtree_idxs FROM txs_blocks WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d`,
		NumPartitions)

	unsetMinedUpdateBlocksSQL = fmt.Sprintf(`
			UPDATE txs_blocks SET block_ids = $2, block_heights = $3, subtree_idxs = $4
			WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d`, NumPartitions)

	unsetMinedSetUnminedSinceSQL = fmt.Sprintf(`
				UPDATE txs SET unmined_since = $2 WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d`, NumPartitions)

	fetchBlockIDsSQL = fmt.Sprintf(
		`SELECT block_ids FROM txs_blocks WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d`,
		NumPartitions)
)

// SetMinedMulti updates the block ID for multiple transactions that have been mined.
// Normal path: single UPDATE on txs with array append.
// UnsetMined path (reorg): read arrays, remove entry in Go, write back.
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

	var newDAH int64
	var withDAH bool
	if minedBlockInfo.OnLongestChain {
		retention := uint32(0)
		if s.settings != nil {
			retention = s.settings.GetUtxoStoreBlockHeightRetention()
		}
		if retention > 0 {
			newDAH = int64(s.blockHeight.Load() + 1 + retention)
			withDAH = true
		}
	}

	// Combined operation: upsert array appends into txs_blocks, then
	// update the flag/DAH columns on txs. Two sequential statements on
	// one held connection (one round-trip per chunk per statement).
	// upsertBlocksSQL is a package-level var so NumPartitions flows in.

	var updateFlagsSQL string
	switch {
	case minedBlockInfo.OnLongestChain && withDAH:
		updateFlagsSQL = fmt.Sprintf(`UPDATE txs t SET
			locked = false, unmined_since = NULL,
			delete_at_height = CASE
				WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
				WHEN t.delete_at_height IS NOT NULL AND t.delete_at_height < %d THEN %d
				WHEN t.delete_at_height IS NULL
				     AND (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash)
				         = (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash)
				     THEN %d
				ELSE t.delete_at_height END
		WHERE t.hash = ANY($1)`, newDAH, newDAH, newDAH)
	case minedBlockInfo.OnLongestChain:
		updateFlagsSQL = `UPDATE txs SET
			locked = false, unmined_since = NULL
		WHERE hash = ANY($1)`
	default:
		updateFlagsSQL = `UPDATE txs SET locked = false WHERE hash = ANY($1)`
	}
	const fetchSQL = `SELECT hash, block_ids FROM txs_blocks WHERE hash = ANY($1)`

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] acquire connection: %v", err)
	}
	defer conn.Release()

	allHashBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		allHashBytes[i] = h[:]
	}

	for i := 0; i < len(allHashBytes); i += minedChunkSize {
		end := i + minedChunkSize
		if end > len(allHashBytes) {
			end = len(allHashBytes)
		}
		chunk := allHashBytes[i:end]
		if _, err := conn.Exec(ctx, upsertBlocksSQL,
			chunk,
			int32(minedBlockInfo.BlockID),
			int32(minedBlockInfo.BlockHeight),
			int32(minedBlockInfo.SubtreeIdx),
		); err != nil {
			return nil, errors.NewStorageError("[SetMinedMulti] upsert txs_blocks chunk %d-%d: %v", i, end-1, err)
		}
		if _, err := conn.Exec(ctx, updateFlagsSQL, chunk); err != nil {
			return nil, errors.NewStorageError("[SetMinedMulti] update txs chunk %d-%d: %v", i, end-1, err)
		}
	}

	rows, err := conn.Query(ctx, fetchSQL, allHashBytes)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] fetch: %v", err)
	}
	defer rows.Close()

	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))
	for rows.Next() {
		var h []byte
		var bids []int32
		if err := rows.Scan(&h, &bids); err != nil {
			return nil, errors.NewStorageError("[SetMinedMulti] scan: %v", err)
		}
		var ch chainhash.Hash
		copy(ch[:], h)
		result := make([]uint32, len(bids))
		for k, bid := range bids {
			result[k] = uint32(bid)
		}
		resultMap[ch] = result
	}
	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] rows: %v", err)
	}
	return resultMap, nil
}

// unsetMinedMulti handles the reorg path: remove a block_id from the arrays
// stored in txs_blocks. If no block_ids remain after removal, sets the
// txs.unmined_since column on the matching txs row.
func (s *Store) unsetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (map[chainhash.Hash][]uint32, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	currentBlockHeight := int64(s.blockHeight.Load())

	for _, hash := range hashes {
		var blockIDs, blockHeights, subtreeIdxs []int32
		err := s.pool.QueryRow(ctx, unsetMinedReadSQL,
			hash[:],
		).Scan(&blockIDs, &blockHeights, &subtreeIdxs)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No txs_blocks row → nothing to unset for this hash.
				resultMap[*hash] = nil
				continue
			}
			return nil, errors.NewStorageError("[UnsetMined] read arrays for %s: %v", hash, err)
		}

		newBlockIDs := make([]int32, 0, len(blockIDs))
		newBlockHeights := make([]int32, 0, len(blockHeights))
		newSubtreeIdxs := make([]int32, 0, len(subtreeIdxs))
		for i, bid := range blockIDs {
			if bid == int32(blockID) {
				continue
			}
			newBlockIDs = append(newBlockIDs, bid)
			if i < len(blockHeights) {
				newBlockHeights = append(newBlockHeights, blockHeights[i])
			}
			if i < len(subtreeIdxs) {
				newSubtreeIdxs = append(newSubtreeIdxs, subtreeIdxs[i])
			}
		}

		_, err = s.pool.Exec(ctx, unsetMinedUpdateBlocksSQL,
			hash[:], newBlockIDs, newBlockHeights, newSubtreeIdxs,
		)
		if err != nil {
			return nil, errors.NewStorageError("[UnsetMined] UPDATE arrays for %s: %v", hash, err)
		}

		// If no block_ids remain after removal, set txs.unmined_since.
		if len(newBlockIDs) == 0 {
			_, err = s.pool.Exec(ctx, unsetMinedSetUnminedSinceSQL,
				hash[:], currentBlockHeight,
			)
			if err != nil {
				return nil, errors.NewStorageError("[UnsetMined] UPDATE unmined_since for %s: %v", hash, err)
			}
		}

		result := make([]uint32, len(newBlockIDs))
		for i, bid := range newBlockIDs {
			result[i] = uint32(bid)
		}
		resultMap[*hash] = result
	}

	return resultMap, nil
}

// fetchBlockIDs returns the list of block_ids for a transaction from the
// txs_blocks side table. Returns nil if no txs_blocks row exists.
func (s *Store) fetchBlockIDs(ctx context.Context, hash *chainhash.Hash) ([]uint32, error) {
	var blockIDs []int32
	err := s.pool.QueryRow(ctx, fetchBlockIDsSQL,
		hash[:],
	).Scan(&blockIDs)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.NewStorageError("[fetchBlockIDs] query for %s: %v", hash, err)
	}

	result := make([]uint32, len(blockIDs))
	for i, bid := range blockIDs {
		result[i] = uint32(bid)
	}
	return result, nil
}

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
