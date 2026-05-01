package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// minedChunkSize is the maximum number of hashes per bulk UPDATE.
// Larger chunks = fewer round trips. Simple array append keeps per-row cost constant.
const minedChunkSize = 2000

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

	// Single bulk UPDATE against parent `txs`. Postgres prunes the
	// affected partitions itself from `hash = ANY($1)`. Future-multi-shard:
	// bucket hashes by Route(h).Shard and dispatch one UPDATE per shard
	// pool. Today (NumShards=1) one query covers all hashes.
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

	var updateSQL string
	switch {
	case minedBlockInfo.OnLongestChain && withDAH:
		updateSQL = fmt.Sprintf(`UPDATE txs t SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
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
		updateSQL = `UPDATE txs SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
			locked = false, unmined_since = NULL
		WHERE hash = ANY($1)`
	default:
		updateSQL = `UPDATE txs SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
			locked = false
		WHERE hash = ANY($1)`
	}
	const fetchSQL = `SELECT hash, block_ids FROM txs WHERE hash = ANY($1)`

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] acquire connection: %v", err)
	}
	defer conn.Release()

	allHashBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		allHashBytes[i] = h[:]
	}

	// UPDATE in chunks to keep parameter sizes bounded.
	for i := 0; i < len(allHashBytes); i += minedChunkSize {
		end := i + minedChunkSize
		if end > len(allHashBytes) {
			end = len(allHashBytes)
		}
		if _, err := conn.Exec(ctx, updateSQL,
			allHashBytes[i:end],
			[]int32{int32(minedBlockInfo.BlockID)},
			[]int32{int32(minedBlockInfo.BlockHeight)},
			[]int32{int32(minedBlockInfo.SubtreeIdx)},
		); err != nil {
			return nil, errors.NewStorageError("[SetMinedMulti] UPDATE chunk %d-%d: %v", i, end-1, err)
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

// unsetMinedMulti handles the reorg path: remove a block_id from the arrays.
// If no block_ids remain after removal, sets unmined_since to current block height.
func (s *Store) unsetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (map[chainhash.Hash][]uint32, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	currentBlockHeight := int64(s.blockHeight.Load())

	for _, hash := range hashes {
		// Read current arrays.
		var blockIDs, blockHeights, subtreeIdxs []int32
		err := s.pool.QueryRow(ctx,
			`SELECT block_ids, block_heights, subtree_idxs FROM txs WHERE hash = $1`,
			hash[:],
		).Scan(&blockIDs, &blockHeights, &subtreeIdxs)
		if err != nil {
			return nil, errors.NewStorageError("[UnsetMined] read arrays for %s: %v", hash, err)
		}

		// Remove entry at matching index.
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

		// Write back. If no block_ids remain, set unmined_since.
		var unminedSince interface{}
		if len(newBlockIDs) == 0 {
			unminedSince = currentBlockHeight
		}

		_, err = s.pool.Exec(ctx, `
			UPDATE txs SET block_ids = $2, block_heights = $3, subtree_idxs = $4, unmined_since = $5
			WHERE hash = $1`,
			hash[:], newBlockIDs, newBlockHeights, newSubtreeIdxs, unminedSince,
		)
		if err != nil {
			return nil, errors.NewStorageError("[UnsetMined] UPDATE arrays for %s: %v", hash, err)
		}

		// Build result.
		result := make([]uint32, len(newBlockIDs))
		for i, bid := range newBlockIDs {
			result[i] = uint32(bid)
		}
		resultMap[*hash] = result
	}

	return resultMap, nil
}

// fetchBlockIDs returns the list of block_ids for a transaction from the txs array.
func (s *Store) fetchBlockIDs(ctx context.Context, hash *chainhash.Hash) ([]uint32, error) {
	var blockIDs []int32
	err := s.pool.QueryRow(ctx,
		`SELECT block_ids FROM txs WHERE hash = $1`,
		hash[:],
	).Scan(&blockIDs)
	if err != nil {
		return nil, errors.NewStorageError("[fetchBlockIDs] query for %s: %v", hash, err)
	}

	result := make([]uint32, len(blockIDs))
	for i, bid := range blockIDs {
		result[i] = uint32(bid)
	}
	return result, nil
}

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
