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
const minedChunkSize = 500

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

	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	// Pipeline all UPDATE chunks + fetch queries via SendBatch.
	// This eliminates per-chunk round-trip latency.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] acquire connection: %v", err)
	}
	defer conn.Release()

	batch := &pgx.Batch{}

	// Queue UPDATE chunks.
	// Use array_append only if the block_id is NOT already present (idempotent).
	// Also handle delete_at_height for pruning (mirrors SQL store logic).
	var updateSQL string
	if minedBlockInfo.OnLongestChain {
		retention := uint32(0)
		if s.settings != nil {
			retention = s.settings.GetUtxoStoreBlockHeightRetention()
		}
		if retention > 0 {
			newDAH := int64(s.blockHeight.Load() + 1 + retention)
			updateSQL = fmt.Sprintf(`UPDATE txs SET
				block_ids = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_ids
					ELSE COALESCE(block_ids, '{}') || $2::int[] END,
				block_heights = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_heights
					ELSE COALESCE(block_heights, '{}') || $3::int[] END,
				subtree_idxs = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN subtree_idxs
					ELSE COALESCE(subtree_idxs, '{}') || $4::int[] END,
				locked = false, unmined_since = NULL,
				delete_at_height = CASE
					WHEN preserve_until IS NOT NULL THEN delete_at_height
					WHEN delete_at_height IS NOT NULL AND delete_at_height < %d THEN %d
					ELSE delete_at_height END
			WHERE hash = ANY($1)`, newDAH, newDAH)
		} else {
			updateSQL = `UPDATE txs SET
				block_ids = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_ids
					ELSE COALESCE(block_ids, '{}') || $2::int[] END,
				block_heights = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_heights
					ELSE COALESCE(block_heights, '{}') || $3::int[] END,
				subtree_idxs = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN subtree_idxs
					ELSE COALESCE(subtree_idxs, '{}') || $4::int[] END,
				locked = false, unmined_since = NULL
			WHERE hash = ANY($1)`
		}
	} else {
		updateSQL = `UPDATE txs SET
			block_ids = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_ids
				ELSE COALESCE(block_ids, '{}') || $2::int[] END,
			block_heights = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN block_heights
				ELSE COALESCE(block_heights, '{}') || $3::int[] END,
			subtree_idxs = CASE WHEN $2::int[] && COALESCE(block_ids, '{}'::int[]) THEN subtree_idxs
				ELSE COALESCE(subtree_idxs, '{}') || $4::int[] END,
			locked = false
		WHERE hash = ANY($1)`
	}

	numUpdateChunks := 0
	for i := 0; i < len(hashes); i += minedChunkSize {
		end := i + minedChunkSize
		if end > len(hashes) {
			end = len(hashes)
		}
		chunk := hashes[i:end]
		hashBytes := make([][]byte, len(chunk))
		for j, h := range chunk {
			hashBytes[j] = h[:]
		}
		batch.Queue(updateSQL,
			hashBytes,
			[]int32{int32(minedBlockInfo.BlockID)},
			[]int32{int32(minedBlockInfo.BlockHeight)},
			[]int32{int32(minedBlockInfo.SubtreeIdx)},
		)
		numUpdateChunks++
	}

	// Queue fetch query (one query for all hashes).
	allHashBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		allHashBytes[i] = h[:]
	}
	batch.Queue(`SELECT hash, block_ids FROM txs WHERE hash = ANY($1)`, allHashBytes)

	// Send entire batch in one network flush.
	br := conn.SendBatch(ctx, batch)

	// Drain UPDATE results.
	for i := 0; i < numUpdateChunks; i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return nil, errors.NewStorageError("[SetMinedMulti] UPDATE chunk %d: %v", i, err)
		}
	}

	// Read fetch results.
	rows, err := br.Query()
	if err != nil {
		br.Close()
		return nil, errors.NewStorageError("[SetMinedMulti] bulk fetch block_ids: %v", err)
	}
	for rows.Next() {
		var h []byte
		var bids []int32
		if err := rows.Scan(&h, &bids); err != nil {
			rows.Close()
			br.Close()
			return nil, errors.NewStorageError("[SetMinedMulti] scan block_ids: %v", err)
		}
		var ch chainhash.Hash
		copy(ch[:], h)
		result := make([]uint32, len(bids))
		for i, bid := range bids {
			result[i] = uint32(bid)
		}
		resultMap[ch] = result
	}
	rows.Close()
	br.Close()

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
