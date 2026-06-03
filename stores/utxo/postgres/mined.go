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
	// Simple array append — no idempotency check. Duplicates only occur on crash
	// recovery (same block re-processed). UnsetMined removes all matching entries
	// so duplicates are harmless.
	var updateSQL string
	if minedBlockInfo.OnLongestChain {
		currentHeight := int64(s.blockHeight.Load())
		// Record mined_at_height for Worker 2 (deferred DAH sweep).
		// DAH is no longer stamped inline here; Worker 2 handles the
		// "fully-spent-then-mined" case by reading mined_at_height.
		updateSQL = fmt.Sprintf(`UPDATE txs SET
				block_ids = COALESCE(block_ids, '{}') || $2::int[],
				block_heights = COALESCE(block_heights, '{}') || $3::int[],
				subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
				locked = false, unmined_since = NULL,
				mined_at_height = %d
			WHERE hash = ANY($1)`, currentHeight)
	} else {
		updateSQL = `UPDATE txs SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
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
		// Remove the entry for blockID from the parallel block_ids / block_heights /
		// subtree_idxs arrays in a SINGLE atomic UPDATE. The previous SELECT-then-
		// UPDATE left a window where a concurrent SetMinedMulti could append a block
		// entry between the read and the write-back, silently dropping it on reorg.
		//
		// block_ids removal is array_remove by value; the parallel arrays must drop
		// the SAME positions, done by unnesting block_ids alongside each parallel
		// array WITH ORDINALITY and re-aggregating the rows whose block_id != blockID.
		// All SET expressions read the row's pre-update arrays, and a single UPDATE
		// statement re-evaluates them against the locked (latest) row version, so a
		// concurrent append is preserved rather than clobbered. RETURNING yields the
		// post-update block_ids for the result map.
		var newBlockIDs []int32
		err := s.pool.QueryRow(ctx, `
			UPDATE txs t SET
				block_ids = array_remove(t.block_ids, $2),
				block_heights = COALESCE((
					SELECT array_agg(e.bh ORDER BY e.ord)
					FROM unnest(t.block_ids, t.block_heights) WITH ORDINALITY AS e(bid, bh, ord)
					WHERE e.bid <> $2
				), '{}'::int[]),
				subtree_idxs = COALESCE((
					SELECT array_agg(e.si ORDER BY e.ord)
					FROM unnest(t.block_ids, t.subtree_idxs) WITH ORDINALITY AS e(bid, si, ord)
					WHERE e.bid <> $2
				), '{}'::int[]),
				unmined_since = CASE
					WHEN COALESCE(array_length(array_remove(t.block_ids, $2), 1), 0) = 0
					THEN $3::bigint ELSE NULL END
			WHERE t.hash = $1
			RETURNING t.block_ids`,
			hash[:], int32(blockID), currentBlockHeight,
		).Scan(&newBlockIDs)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NewStorageError("[UnsetMined] tx not found for %s", hash)
			}
			return nil, errors.NewStorageError("[UnsetMined] update arrays for %s: %v", hash, err)
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
