package postgres

import (
	"context"
	"fmt"
	"sync"
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

	// Bucket hashes by partition. Per-partition we run an UPDATE on `txs_pK`
	// (not the parent), then a SELECT on the same partition to fetch the
	// updated block_ids. Each partition runs in its own goroutine on its own
	// connection so the N partitions work independently.
	buckets := make([][]int, NumPartitions)
	for i, h := range hashes {
		rk := Route(h)
		buckets[rk.Partition] = append(buckets[rk.Partition], i)
	}

	// Build the per-partition UPDATE SQL templates. Behaviour mirrors the
	// pre-refactor logic — only the table name changes.
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
	buildUpdateSQL := func(partition int) string {
		ps := PartitionSuffix(partition)
		if minedBlockInfo.OnLongestChain {
			if withDAH {
				return fmt.Sprintf(`UPDATE txs%s t SET
					block_ids = COALESCE(block_ids, '{}') || $2::int[],
					block_heights = COALESCE(block_heights, '{}') || $3::int[],
					subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
					locked = false, unmined_since = NULL,
					delete_at_height = CASE
						WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
						WHEN t.delete_at_height IS NOT NULL AND t.delete_at_height < %d THEN %d
						WHEN t.delete_at_height IS NULL
						     AND (SELECT count(*) FROM outputs%s o WHERE o.tx_hash = t.hash)
						         = (SELECT count(*) FROM spends%s s WHERE s.prev_tx_hash = t.hash)
						     THEN %d
						ELSE t.delete_at_height END
				WHERE t.hash = ANY($1)`, ps, newDAH, newDAH, ps, ps, newDAH)
			}
			return fmt.Sprintf(`UPDATE txs%s SET
				block_ids = COALESCE(block_ids, '{}') || $2::int[],
				block_heights = COALESCE(block_heights, '{}') || $3::int[],
				subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
				locked = false, unmined_since = NULL
			WHERE hash = ANY($1)`, ps)
		}
		return fmt.Sprintf(`UPDATE txs%s SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
			locked = false
		WHERE hash = ANY($1)`, ps)
	}

	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))
	var resultMu sync.Mutex
	var firstErr error
	var errMu sync.Mutex
	recordErr := func(e error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	for partition := 0; partition < NumPartitions; partition++ {
		idxs := buckets[partition]
		if len(idxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(partition int, idxs []int) {
			defer wg.Done()
			conn, err := s.pool.Acquire(ctx)
			if err != nil {
				recordErr(errors.NewStorageError("[SetMinedMulti] acquire connection: %v", err))
				return
			}
			defer conn.Release()

			updateSQL := buildUpdateSQL(partition)
			fetchSQL := fmt.Sprintf(`SELECT hash, block_ids FROM txs%s WHERE hash = ANY($1)`, PartitionSuffix(partition))

			// UPDATE in chunks, then fetch.
			for i := 0; i < len(idxs); i += minedChunkSize {
				end := i + minedChunkSize
				if end > len(idxs) {
					end = len(idxs)
				}
				chunkIdxs := idxs[i:end]
				hashBytes := make([][]byte, len(chunkIdxs))
				for j, hi := range chunkIdxs {
					hashBytes[j] = hashes[hi][:]
				}
				if _, err := conn.Exec(ctx, updateSQL,
					hashBytes,
					[]int32{int32(minedBlockInfo.BlockID)},
					[]int32{int32(minedBlockInfo.BlockHeight)},
					[]int32{int32(minedBlockInfo.SubtreeIdx)},
				); err != nil {
					recordErr(errors.NewStorageError("[SetMinedMulti] UPDATE partition %d: %v", partition, err))
					return
				}
			}

			allHashBytes := make([][]byte, len(idxs))
			for j, hi := range idxs {
				allHashBytes[j] = hashes[hi][:]
			}
			rows, err := conn.Query(ctx, fetchSQL, allHashBytes)
			if err != nil {
				recordErr(errors.NewStorageError("[SetMinedMulti] fetch partition %d: %v", partition, err))
				return
			}
			defer rows.Close()
			for rows.Next() {
				var h []byte
				var bids []int32
				if err := rows.Scan(&h, &bids); err != nil {
					recordErr(errors.NewStorageError("[SetMinedMulti] scan partition %d: %v", partition, err))
					return
				}
				var ch chainhash.Hash
				copy(ch[:], h)
				result := make([]uint32, len(bids))
				for k, bid := range bids {
					result[k] = uint32(bid)
				}
				resultMu.Lock()
				resultMap[ch] = result
				resultMu.Unlock()
			}
		}(partition, idxs)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
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
