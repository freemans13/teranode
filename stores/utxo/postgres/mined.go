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
// Larger chunks = fewer round trips. Simple array append keeps per-row cost
// constant. NOTE: 4000 was A/B'd (one statement per typical mined batch) and
// REGRESSED the 2-shard balanced rate 95.7K -> 86K — the bigger statement
// holds buffers/locks longer and breaks the create/reclaim interleaving.
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
	if _, err := blockHeightToInt32(minedBlockInfo.BlockHeight); err != nil {
		return nil, err
	}

	if minedBlockInfo.UnsetMined {
		return s.unsetMinedMulti(ctx, hashes, minedBlockInfo.BlockID, minedBlockInfo.BlockHeight)
	}

	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	// Pipeline all UPDATE chunks + fetch queries via SendBatch.
	// This eliminates per-chunk round-trip latency.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] acquire connection", err)
	}
	defer conn.Release()

	batch := &pgx.Batch{}

	// Queue UPDATE chunks.
	// Idempotent block append: a `block_ids @> $2` containment guard skips the
	// append (on all three parallel arrays together, so they stay index-aligned)
	// when this block id is already recorded. Re-processing the same block for the
	// same tx (crash-recovery replay, retry, duplicate call) is then a no-op rather
	// than appending a duplicate (block_id, block_height, subtree_idx) triple that
	// Get/BatchDecorate would surface verbatim. Mirrors the aerospike (blockExists)
	// and sql (ON CONFLICT DO NOTHING) stores, and the create.go conflicting-children
	// @> pattern.
	retention := int32(s.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	var updateSQL string
	if minedBlockInfo.OnLongestChain {
		// Record mined_at_height for the deferred DAH sweep, and stamp DAH INLINE for
		// any tx that is now FULLY SPENT at the mine event (Design-C, stamp site S6).
		// This generalizes the old zero-spendable inline stamp and closes the
		// "spent-before-mined" orphan gap: a tx whose outputs are all spent while it is
		// unmined is excluded by the spends-driven sweep's mined-gate, so without S6 it
		// would only be caught by the O(table) backstop. S6 evaluates fully-spent right
		// here, covering both orderings (mined-then-spent is covered by the sweep;
		// spent-then-mined is covered here).
		//
		// The CASE predicate:
		//   guard: preserve_until IS NULL AND out_count > 0
		//   zero-spendable: spendable_count = 0 (cheap, no join — the old case)
		//   OR fully-spent: EXISTS(spends for this tx) AND
		//       spendable_count = count(spends where output IS spendable)
		// The EXISTS pre-filter is REQUIRED: for freshly-mined txs whose outputs are NOT
		// yet spent (the common case), EXISTS returns false immediately and the costlier
		// count/max subqueries are never evaluated. Only the rare spent-before-mined
		// orphans pay the join cost.
		//
		// Completion-height formula: GREATEST(max(spent_at_height), $5/*minedHeight*/)
		// + 1 + $6/*retention*/ — matches the sweep and backstop so a late mine cannot
		// schedule deletion too early. Cast to int to satisfy the INT4 column.
		//
		// mined_at_height is the height of the block this tx is mined into
		// (minedBlockInfo.BlockHeight), bound as $5 — NOT the store's current
		// chain tip. Binding it as a parameter (a) keeps mined_at_height
		// consistent with the block_heights entry appended in $3 even under
		// concurrent SetBlockHeight, and (b) preserves a single prepared-plan
		// cache entry across heights (a literal would re-plan per height).
		//
		// The UPDATE is wrapped in a CTE so that any fully-spent tx that gets an inline
		// DAH stamp is also upserted into pending_deletes in the same statement. The
		// outer SELECT returns hash, block_ids so the drain loop is unchanged.
		updateSQL = `WITH upd AS (
				UPDATE txs SET
					block_ids = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN block_ids ELSE COALESCE(block_ids, '{}') || $2::int[] END,
					block_heights = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN block_heights ELSE COALESCE(block_heights, '{}') || $3::int[] END,
					subtree_idxs = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN subtree_idxs ELSE COALESCE(subtree_idxs, '{}') || $4::int[] END,
					locked = false, unmined_since = NULL,
					mined_at_height = $5,
					delete_at_height = CASE
						WHEN preserve_until IS NULL AND out_count > 0 AND (
							spendable_count = 0
							OR (
								EXISTS (SELECT 1 FROM spends s WHERE s.prev_tx_hash = txs.hash)
								AND spendable_count = (
									SELECT count(*) FROM spends s
									WHERE s.prev_tx_hash = txs.hash
									  AND CASE WHEN s.prev_output_idx < txs.out_count
									           THEN get_bit(txs.out_spendables, s.prev_output_idx) = 1
									           ELSE false END)
							))
						THEN (GREATEST(
								COALESCE((SELECT max(s.spent_at_height) FROM spends s WHERE s.prev_tx_hash = txs.hash), 0),
								$5) + 1 + $6)::int
						ELSE delete_at_height
					END
				WHERE hash = ANY($1)
				RETURNING hash, block_ids, delete_at_height
			),
			_pd AS (
				INSERT INTO pending_deletes (hash, delete_at_height)
				SELECT hash, delete_at_height FROM upd WHERE delete_at_height IS NOT NULL
				ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
			),
			_pu AS (
				DELETE FROM pending_unmined WHERE hash = ANY($1)
			)
			SELECT hash, block_ids FROM upd`
	} else {
		updateSQL = `WITH _pu AS (
			DELETE FROM pending_unmined WHERE hash = ANY($1)
		)
		UPDATE txs SET
			block_ids = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN block_ids ELSE COALESCE(block_ids, '{}') || $2::int[] END,
			block_heights = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN block_heights ELSE COALESCE(block_heights, '{}') || $3::int[] END,
			subtree_idxs = CASE WHEN COALESCE(block_ids, '{}') @> $2::int[] THEN subtree_idxs ELSE COALESCE(subtree_idxs, '{}') || $4::int[] END,
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
			// $5: mined_at_height — the block height this tx is mined into
			// (INT4 column; heights < 2^31). $6: retention, for the inline
			// zero-spendable DAH stamp (delete_at_height = $5 + 1 + $6).
			args = append(args, int32(minedBlockInfo.BlockHeight), retention)
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
			return nil, errors.NewStorageError("[SetMinedMulti] UPDATE chunk %d", i, err)
		}
		for rows.Next() {
			var h []byte
			var bids []int32
			if err := rows.Scan(&h, &bids); err != nil {
				rows.Close()
				br.Close()
				return nil, errors.NewStorageError("[SetMinedMulti] scan block_ids chunk %d", i, err)
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
			return nil, errors.NewStorageError("[SetMinedMulti] iterate block_ids chunk %d", i, err)
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

	// unmined_since is INT4 (heights < 2^31). Use blockHeight + 1, matching the
	// aerospike Lua (unmined_since = currentBlockHeight) and the sql store
	// (sql.go: s.blockHeight.Load() + 1) so a reorged-out tx is classified
	// identically across backends — a bare blockHeight.Load() made it one block low
	// and thus prune-eligible one block earlier than the gold standard. Floor at 1:
	// a stored unmined_since of 0 is indistinguishable at the meta layer from
	// "mined" — NULL (mined) and 0 both map to UnminedSince==0 in getInternal — so
	// the value must never be 0 (the +1 already guarantees this for tip >= 0, but
	// keep the explicit floor as a guard).
	currentBlockHeight := int32(s.blockHeight.Load()) + 1
	if currentBlockHeight < 1 {
		currentBlockHeight = 1
	}

	// One transaction for the whole set: a reorg that unsets a block_id from many
	// txs commits all-or-nothing rather than leaving some rows updated on failure.
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[UnsetMined] begin", err)
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
					THEN $3::int ELSE t.unmined_since END,
				-- Clear the deferred-prune stamp UNCONDITIONALLY on any unset-mined.
				-- Removing a block changes the tx's chain membership, so any existing
				-- delete_at_height is no longer trustworthy. Clearing it always (not
				-- only when the LAST block is removed) closes the partial-reorg window
				-- where one block is removed but others — themselves later reorged out
				-- in a separate call — leave a stale stamp the pruner would act on. The
				-- DAH sweep re-stamps the tx for free once it is genuinely eligible
				-- again; the cost is at most one extra sweep pass.
				delete_at_height = NULL,
				-- Clear mined_at_height when the tx is fully reorged out (no block_ids
				-- remain) so the DAH sweep's mine-arm stops re-enumerating it as a
				-- phantom candidate on every pass over the old mining height. Preserve
				-- it on a partial reorg (still mined on a remaining block).
				mined_at_height = CASE
					WHEN COALESCE(array_length(array_remove(t.block_ids, $2), 1), 0) = 0
					THEN NULL ELSE t.mined_at_height END,
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
			return nil, errors.NewStorageError("[UnsetMined] update arrays for %s", hash, err)
		}

		// C5: remove the hash from the pending_deletes side-table in the same pgxTx so
		// the clear is atomic with the DAH null above. Harmless no-op if the hash was
		// never stamped (e.g. reorged before the sweep ran).
		if _, err := pgxTx.Exec(ctx,
			`DELETE FROM pending_deletes WHERE hash = $1`, hash[:]); err != nil {
			return nil, errors.NewStorageError("[UnsetMined] failed to delete pending_deletes for %s (C5)", hash, err)
		}

		// U1: when fully reorged out (block_ids is now empty), insert into pending_unmined.
		// pending_unmined is ALWAYS-ON (no flag). We derive whether the tx is now fully
		// unmined from the post-update block_ids array returned by RETURNING above.
		// unmined_since is currentBlockHeight (= s.blockHeight.Load()+1), matching the
		// CASE expression in the UPDATE above — COPY-FROM-RETURNING semantics without
		// re-reading the row. Harmless ON CONFLICT DO UPDATE if the hash was already
		// in pending_unmined from a prior reorg (idempotent).
		if len(newBlockIDs) == 0 {
			if _, err := pgxTx.Exec(ctx,
				`INSERT INTO pending_unmined (hash, unmined_since)
				 VALUES ($1, $2)
				 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
				hash[:], currentBlockHeight); err != nil {
				return nil, errors.NewStorageError("[UnsetMined] failed to insert pending_unmined for %s (U1)", hash, err)
			}
		}

		// Build result.
		result := make([]uint32, len(newBlockIDs))
		for i, bid := range newBlockIDs {
			result[i] = uint32(bid)
		}
		resultMap[*hash] = result
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[UnsetMined] commit", err)
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

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
