package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// minedChunkSize is the maximum number of hashes per bulk UPDATE.
// Larger chunks = fewer round trips. Simple array append keeps per-row cost constant.
const minedChunkSize = 2000

// SQL strings for mined operations. The `% NumPartitions` modulus is
// substituted from the Go constant so bumping NumPartitions automatically
// updates every partition_key derivation.
//
// Each chunk SQL is a writable CTE: it does the INSERT … ON CONFLICT into
// txs_blocks AND the UPDATE on txs in a single round-trip, halving network
// round-trips per chunk vs running them sequentially. Both data-modifying
// statements in the CTE see the same snapshot — they don't depend on each
// other's effects, so single-snapshot semantics are fine.
//
// The UPDATE on txs joins through UNNEST so partition_key is derived per row
// from the hash, which keeps partition pruning intact (postgres routes the
// UPDATE to the right child partition).
var (
	// minedChunkOnLongestWithDAHSQL — OnLongestChain && retention>0.
	//
	// DAH semantics: we only forward-bump an already-set DAH. We do NOT set
	// DAH from NULL here — the count(*) outputs/spends subqueries that the
	// previous implementation used to detect "already fully spent" transactions
	// at mining time were the dominant per-chunk cost (chunkSize=2000 → 4,000
	// sub-scans per chunk).
	//
	// The canonical DAH-on-mined-then-fully-spent path is the spend path's
	// dah_upd CTE in spend.go (both spendValidationSQL and bulkSpendSQL set
	// DAH when the last output of a tx is spent). The "spent before mining"
	// edge case is extremely rare in production and is also covered by the
	// Spend path on any subsequent spend; if no further spend happens, the
	// pruner's preserve_until-driven path or an explicit setDAH call handles it.
	minedChunkOnLongestWithDAHSQL = fmt.Sprintf(`
		WITH upserted AS (
		    INSERT INTO txs_blocks (hash, partition_key, block_ids, block_heights, subtree_idxs)
		    SELECT u.hash, get_byte(u.hash, 1) %% %d, ARRAY[$2::int], ARRAY[$3::int], ARRAY[$4::int]
		    FROM UNNEST($1::bytea[]) AS u(hash)
		    ON CONFLICT (hash, partition_key) DO UPDATE SET
		        block_ids     = txs_blocks.block_ids     || EXCLUDED.block_ids,
		        block_heights = txs_blocks.block_heights || EXCLUDED.block_heights,
		        subtree_idxs  = txs_blocks.subtree_idxs  || EXCLUDED.subtree_idxs
		)
		UPDATE txs t SET
		    locked = false,
		    unmined_since = NULL,
		    delete_at_height = CASE
		        WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
		        WHEN t.delete_at_height IS NOT NULL AND t.delete_at_height < $5 THEN $5
		        ELSE t.delete_at_height
		    END
		FROM UNNEST($1::bytea[]) AS u(hash)
		WHERE t.hash = u.hash AND t.partition_key = get_byte(u.hash, 1) %% %d
		`, NumPartitions, NumPartitions)

	// minedChunkOnLongestSQL — OnLongestChain but retention disabled.
	minedChunkOnLongestSQL = fmt.Sprintf(`
		WITH upserted AS (
		    INSERT INTO txs_blocks (hash, partition_key, block_ids, block_heights, subtree_idxs)
		    SELECT u.hash, get_byte(u.hash, 1) %% %d, ARRAY[$2::int], ARRAY[$3::int], ARRAY[$4::int]
		    FROM UNNEST($1::bytea[]) AS u(hash)
		    ON CONFLICT (hash, partition_key) DO UPDATE SET
		        block_ids     = txs_blocks.block_ids     || EXCLUDED.block_ids,
		        block_heights = txs_blocks.block_heights || EXCLUDED.block_heights,
		        subtree_idxs  = txs_blocks.subtree_idxs  || EXCLUDED.subtree_idxs
		)
		UPDATE txs t SET locked = false, unmined_since = NULL
		FROM UNNEST($1::bytea[]) AS u(hash)
		WHERE t.hash = u.hash AND t.partition_key = get_byte(u.hash, 1) %% %d
		`, NumPartitions, NumPartitions)

	// minedChunkNotOnLongestSQL — competing/orphan block; only flip locked.
	minedChunkNotOnLongestSQL = fmt.Sprintf(`
		WITH upserted AS (
		    INSERT INTO txs_blocks (hash, partition_key, block_ids, block_heights, subtree_idxs)
		    SELECT u.hash, get_byte(u.hash, 1) %% %d, ARRAY[$2::int], ARRAY[$3::int], ARRAY[$4::int]
		    FROM UNNEST($1::bytea[]) AS u(hash)
		    ON CONFLICT (hash, partition_key) DO UPDATE SET
		        block_ids     = txs_blocks.block_ids     || EXCLUDED.block_ids,
		        block_heights = txs_blocks.block_heights || EXCLUDED.block_heights,
		        subtree_idxs  = txs_blocks.subtree_idxs  || EXCLUDED.subtree_idxs
		)
		UPDATE txs t SET locked = false
		FROM UNNEST($1::bytea[]) AS u(hash)
		WHERE t.hash = u.hash AND t.partition_key = get_byte(u.hash, 1) %% %d
		`, NumPartitions, NumPartitions)

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
// Normal path: hashes are bucketed by partition_key (byte 1 % NumPartitions)
// and one goroutine per non-empty partition runs the writable-CTE upsert.
// Partition affinity ensures each worker writes only to one txs/txs_blocks
// child partition, removing lwlock contention on shared-buffer pages.
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

	// Pick the chunk SQL once. Each variant is a single writable CTE that
	// does the txs_blocks upsert + txs UPDATE in a single round-trip.
	var chunkSQL string
	switch {
	case minedBlockInfo.OnLongestChain && withDAH:
		chunkSQL = minedChunkOnLongestWithDAHSQL
	case minedBlockInfo.OnLongestChain:
		chunkSQL = minedChunkOnLongestSQL
	default:
		chunkSQL = minedChunkNotOnLongestSQL
	}

	// Bucket hashes by partition_key (byte 1 % NumPartitions) so each worker
	// writes only to one child partition (txs_p0X / txs_blocks_p0X). This
	// eliminates lwlock contention on shared-buffer pages — backends never
	// touch the same heap or index pages. Natural parallelism = NumPartitions.
	allHashBytes := make([][]byte, len(hashes))
	buckets := make([][][]byte, NumPartitions)
	for i, h := range hashes {
		allHashBytes[i] = h[:]
		p := int(h[1]) % NumPartitions
		buckets[p] = append(buckets[p], h[:])
	}

	g, gctx := errgroup.WithContext(ctx)
	blockID := int32(minedBlockInfo.BlockID)
	blockHeightArg := int32(minedBlockInfo.BlockHeight)
	subtreeIdx := int32(minedBlockInfo.SubtreeIdx)

	for p := 0; p < NumPartitions; p++ {
		bucket := buckets[p]
		if len(bucket) == 0 {
			continue
		}
		g.Go(func() error {
			conn, err := s.pool.Acquire(gctx)
			if err != nil {
				return errors.NewStorageError("[SetMinedMulti] acquire connection: %v", err)
			}
			defer conn.Release()

			// Sub-chunk if the bucket is bigger than minedChunkSize so a
			// single huge UNNEST array doesn't dominate per-statement cost.
			for i := 0; i < len(bucket); i += minedChunkSize {
				end := i + minedChunkSize
				if end > len(bucket) {
					end = len(bucket)
				}
				chunk := bucket[i:end]
				if withDAH {
					if _, err := conn.Exec(gctx, chunkSQL,
						chunk, blockID, blockHeightArg, subtreeIdx, newDAH,
					); err != nil {
						return errors.NewStorageError("[SetMinedMulti] chunk: %v", err)
					}
				} else {
					if _, err := conn.Exec(gctx, chunkSQL,
						chunk, blockID, blockHeightArg, subtreeIdx,
					); err != nil {
						return errors.NewStorageError("[SetMinedMulti] chunk: %v", err)
					}
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Final read-back on a single connection.
	const fetchSQL = `SELECT hash, block_ids FROM txs_blocks WHERE hash = ANY($1)`
	rows, err := s.pool.Query(ctx, fetchSQL, allHashBytes)
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
