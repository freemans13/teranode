package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// PreserveTransactions marks transactions to be preserved from deletion.
// Sets preserve_until to the given height and clears delete_at_height.
//
// PRUNE-ELIGIBILITY GATE: only transactions that already carry a delete_at_height
// stamp (eligible now), OR are already being preserved (preserve_until set, so a
// still-needed preservation can be renewed each cycle even though the first
// preservation cleared the DAH), are preserved. A tx with neither is not fully
// spent, so it is not at risk of pruning and there is nothing to protect —
// preserving it would be pointless work, and it is exactly the not-fully-spent
// input that the expiry path could otherwise turn into a bad deletion stamp.
//
// Postgres note: delete_at_height is set by the BACKGROUND sweep (dah_sweep.go),
// which can lag behind a tx becoming fully spent. So a freshly-fully-spent parent
// may have no DAH yet when a pruner cycle runs and be skipped here — that is benign:
// the sweep stamps it shortly after, and because the stamp is a FUTURE height
// (completion+1+retention) the pruner will not act for `retention` blocks, during
// which the next pruner cycle re-admits and re-preserves it. ProcessExpiredPreservations
// is the setter-side safety net for the case where a preserved (eligible) tx is later
// un-spent by a reorg.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	totalAffected := int64(0)

	for i := 0; i < len(txIDs); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txIDs) {
			end = len(txIDs)
		}

		chunk := make([][]byte, end-i)
		for j, txID := range txIDs[i:end] {
			id := txID
			chunk[j] = id[:]
		}

		inClause, args := buildINClauseLocal(chunk, 2)
		// Only preserve prune-eligible txs (see the gate rationale above): those that
		// already carry a delete_at_height stamp, or are already preserved.
		q := fmt.Sprintf(`UPDATE txs SET preserve_until = $1, delete_at_height = NULL
			WHERE hash IN %s
			  AND (delete_at_height IS NOT NULL OR preserve_until IS NOT NULL)`, inClause)

		allArgs := append([]interface{}{int32(preserveUntilHeight)}, args...) // preserve_until is INT4
		result, err := s.pool.Exec(ctx, q, allArgs...)
		if err != nil {
			return errors.NewStorageError("[PreserveTransactions] failed to preserve chunk", err)
		}

		totalAffected += result.RowsAffected()
	}

	s.logger.Debugf("[PreserveTransactions] preserved %d out of %d transactions", totalAffected, len(txIDs))

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has
// expired. For each tx with preserve_until <= currentHeight it clears preserve_until,
// and sets delete_at_height ONLY when the tx is genuinely safe to drop.
//
// Upholding the invariant "delete_at_height set ⟹ safe to delete": preservation can
// be requested for any parent of an old unmined tx, including a parent that is not
// fully spent (e.g. one output spent by a now-resolved mempool child, another output
// still live). Stamping such a parent for deletion would let the pruner — which
// deletes purely on the stamp (see the DESIGN CONTRACT in
// pruner_provider.deleteTombstonedPartition) — remove a tx that still has live UTXOs.
//
// The eligibility CASE mirrors the sweep's conditions (the canonical DAH setter):
//   - conflicting txs get a DAH (they need not be mined); else
//   - on the longest chain (unmined_since IS NULL) AND mined (has block_ids) AND fully
//     spent (every spendable output has a spend row).
//
// An ineligible tx just has preserve_until cleared, delete_at_height left NULL; the
// sweep re-stamps it if/when it actually becomes eligible. This runs once per pruner
// cycle over only the small set of expiring preservations (gated by px_preserve_until),
// so the per-row fully-spent subquery is off the hot delete path.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	// Retention 0 disables pruning: never stamp a DAH (mirrors the sweep's early
	// return); just clear the expired preservations.
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		if _, err := s.pool.Exec(ctx,
			`UPDATE txs SET preserve_until = NULL WHERE preserve_until IS NOT NULL AND preserve_until <= $1`,
			int32(currentHeight)); err != nil {
			return errors.NewStorageError("failed to process expired preservations", err)
		}

		return nil
	}

	// Widen to int64 before adding so the sum cannot wrap in uint32 arithmetic,
	// then narrow to int32 for the INT4 delete_at_height column.
	deleteAtHeight := int32(int64(currentHeight) + int64(retention))

	result, err := s.pool.Exec(ctx, `
		UPDATE txs SET
			delete_at_height = CASE
				WHEN conflicting THEN $1
				WHEN unmined_since IS NULL
				     AND block_ids IS NOT NULL AND array_length(block_ids, 1) IS NOT NULL
				     AND out_count > 0
				     AND spendable_count = (
				         SELECT count(*) FROM spends s
				         WHERE s.prev_tx_hash = txs.hash
				           AND CASE WHEN s.prev_output_idx < txs.out_count
				                    THEN get_bit(txs.out_spendables, s.prev_output_idx) = 1
				                    ELSE false END
				     )
				THEN $1::int
				ELSE NULL
			END,
			preserve_until = NULL
		WHERE preserve_until IS NOT NULL AND preserve_until <= $2
	`, deleteAtHeight, int32(currentHeight))
	if err != nil {
		return errors.NewStorageError("failed to process expired preservations", err)
	}

	rowsAffected := result.RowsAffected()
	s.logger.Infof("[ProcessExpiredPreservations] processed %d expired preservations at height %d", rowsAffected, currentHeight)

	return nil
}
