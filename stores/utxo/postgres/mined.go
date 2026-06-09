package postgres

import (
	"context"
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
		return s.unsetMinedMulti(ctx, hashes, minedBlockInfo.BlockID, minedBlockInfo.BlockHeight)
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
		// Record mined_at_height for Worker 2 (deferred DAH sweep).
		// DAH is no longer stamped inline here; Worker 2 handles the
		// "fully-spent-then-mined" case by reading mined_at_height.
		//
		// mined_at_height is the height of the block this tx is mined into
		// (minedBlockInfo.BlockHeight), bound as $5 — NOT the store's current
		// chain tip. Binding it as a parameter (a) keeps mined_at_height
		// consistent with the block_heights entry appended in $3 even under
		// concurrent SetBlockHeight, and (b) preserves a single prepared-plan
		// cache entry across heights (a literal would re-plan per height).
		updateSQL = `UPDATE txs SET
				block_ids = COALESCE(block_ids, '{}') || $2::int[],
				block_heights = COALESCE(block_heights, '{}') || $3::int[],
				subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
				locked = false, unmined_since = NULL,
				mined_at_height = $5
			WHERE hash = ANY($1)
			RETURNING hash, block_ids`
	} else {
		updateSQL = `UPDATE txs SET
			block_ids = COALESCE(block_ids, '{}') || $2::int[],
			block_heights = COALESCE(block_heights, '{}') || $3::int[],
			subtree_idxs = COALESCE(subtree_idxs, '{}') || $4::int[],
			locked = false
		WHERE hash = ANY($1)
		RETURNING hash, block_ids`
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
		args := []interface{}{
			hashBytes,
			[]int32{int32(minedBlockInfo.BlockID)},
			[]int32{int32(minedBlockInfo.BlockHeight)},
			[]int32{int32(minedBlockInfo.SubtreeIdx)},
		}
		if minedBlockInfo.OnLongestChain {
			// $5: mined_at_height — the block height this tx is mined into.
			args = append(args, int64(minedBlockInfo.BlockHeight))
		}
		batch.Queue(updateSQL, args...)
		numUpdateChunks++
	}

	// Send entire batch in one network flush.
	br := conn.SendBatch(ctx, batch)

	// Drain each UPDATE chunk's RETURNING rows straight into the result map —
	// the post-update block_ids come back on the UPDATE itself, so no separate
	// verification SELECT (a second full = ANY probe pass over all partitions,
	// measured as a top-10 CPU consumer) is needed.
	for i := 0; i < numUpdateChunks; i++ {
		rows, err := br.Query()
		if err != nil {
			br.Close()
			return nil, errors.NewStorageError("[SetMinedMulti] UPDATE chunk %d: %v", i, err)
		}
		for rows.Next() {
			var h []byte
			var bids []int32
			if err := rows.Scan(&h, &bids); err != nil {
				rows.Close()
				br.Close()
				return nil, errors.NewStorageError("[SetMinedMulti] scan block_ids chunk %d: %v", i, err)
			}
			var ch chainhash.Hash
			copy(ch[:], h)
			result := make([]uint32, len(bids))
			for j, bid := range bids {
				result[j] = uint32(bid)
			}
			resultMap[ch] = result
		}
		// A mid-stream failure (network reset, statement timeout) makes rows.Next()
		// stop early with the error parked in rows.Err(); without this check a
		// truncated resultMap would be returned as success.
		if err := rows.Err(); err != nil {
			rows.Close()
			br.Close()
			return nil, errors.NewStorageError("[SetMinedMulti] iterate block_ids chunk %d: %v", i, err)
		}
		rows.Close()
	}
	br.Close()

	// Hard postcondition (interface contract, matches the aerospike store): every
	// input hash MUST appear in the result map and its block_ids MUST contain the
	// block we just mined into. A hash absent from txs (never created, pruned, or
	// a Create/SetMinedMulti race) silently no-ops the UPDATE; surface it as an
	// error rather than returning a partial map that a caller would read as
	// "all mined".
	for _, h := range hashes {
		bids, ok := resultMap[*h]
		if !ok {
			return nil, errors.NewTxNotFoundError("[SetMinedMulti] transaction not found: %s", h)
		}
		found := false
		for _, bid := range bids {
			if bid == minedBlockInfo.BlockID {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.NewStorageError("[SetMinedMulti] block %d not recorded for %s", minedBlockInfo.BlockID, h)
		}
	}

	return resultMap, nil
}

// unsetMinedMulti handles the reorg path: remove a block_id from the arrays.
// If no block_ids remain after removal, sets unmined_since to current block height,
// clears delete_at_height (the tx is no longer mined on the longest chain, so any
// deferred-prune stamp from when it was fully-spent+mined is now invalid), and
// clears locked — mirroring the aerospike setMined(UnsetMined) re-evaluation of DAH
// eligibility. Without the DAH clear a reorged-out tx would be pruned at the stale
// stamp height, destroying still-live UTXOs.
func (s *Store) unsetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32, blockHeight uint32) (map[chainhash.Hash][]uint32, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	currentBlockHeight := int64(s.blockHeight.Load())

	// One transaction for the whole set: a reorg that unsets a block_id from many
	// txs commits all-or-nothing rather than leaving some rows updated on failure.
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[UnsetMined] begin: %v", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

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
		err := pgxTx.QueryRow(ctx, `
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
				-- Set unmined_since only when the tx is fully reorged out (no block_ids
				-- remain). Otherwise PRESERVE the existing value: a tx still mined only
				-- on non-longest-chain forks was legitimately unmined at creation and
				-- must keep that marker so the DAH sweep's "unmined_since IS NOT NULL"
				-- guard continues to protect it. Clobbering to NULL here would mis-classify
				-- it as longest-chain-mined (matches aerospike teranode.lua:637-644 and
				-- sql.go:3268-3308, which only write unmined_since when zero blocks remain).
				unmined_since = CASE
					WHEN COALESCE(array_length(array_remove(t.block_ids, $2), 1), 0) = 0
					THEN $3::bigint ELSE t.unmined_since END,
				-- Clear the deferred-prune stamp when the tx falls off the longest
				-- chain (no block_ids remain). It is no longer "mined and fully spent
				-- on the longest chain", so it must not be pruned at the old stamp.
				delete_at_height = CASE
					WHEN COALESCE(array_length(array_remove(t.block_ids, $2), 1), 0) = 0
					THEN NULL ELSE t.delete_at_height END,
				locked = false
			WHERE t.hash = $1
			RETURNING t.block_ids`,
			hash[:], int32(blockID), currentBlockHeight,
		).Scan(&newBlockIDs)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Per Interface.go:295-303, UnsetMined tolerates missing/empty entries:
				// the call may no-op for transactions that no longer exist. A reorg
				// unsets every tx in the reorged block, some of which may already have
				// been pruned — that must not abort the whole reorg. Record an empty
				// result and move on (matches aerospike set_mined.go:502-506 and
				// sql.go:3162-3176).
				resultMap[*hash] = []uint32{}
				continue
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

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[UnsetMined] commit: %v", err)
	}

	// Rewind the deferred-DAH sweep watermark to the reorged block's height so the
	// new chain's spends in (blockHeight, tip] — tagged at heights the cursor may
	// already have passed — get re-evaluated for DAH eligibility rather than waiting
	// for the slow keyspace backstop. Best-effort: correctness already holds via the
	// inline DAH clear above and Unspend; this only restores timely re-stamping.
	// RewindDAHWatermark only ever moves the watermark backward (guarded), so a
	// spurious call cannot skip heights.
	if blockHeight > 0 {
		if err := s.RewindDAHWatermark(ctx, int64(blockHeight)); err != nil {
			s.logger.Infof("[UnsetMined] rewind DAH watermark to %d failed (continuing): %v", blockHeight, err)
		}
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
