package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// Returns an error if any UTXO is already spent or frozen.
// Atomically guards the freeze on the txs array subscript; spend-state is
// confirmed via a LEFT JOIN against the spends table before the UPDATE.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Atomic freeze on txs array: flip out_frozens[vout+1] false→true only if
		// the slot exists, is not already frozen, and has no matching spends row.
		// Performing the guard inside the UPDATE's WHERE closes the check-then-write
		// race where two concurrent freezes both pass the check and both "succeed".
		//
		// Freezing is PER-OUTPUT (gold standard: aerospike freezes only the target
		// slot). We deliberately do NOT set the transaction-level `frozen` column
		// here: that column is the whole-tx freeze gate (set only at create via
		// WithFrozen) and the spend-validation CTE checks it as `tx_frozen`, so
		// setting it would block every other output of a multi-output tx. The
		// per-output `out_frozens[slot]` flag is the sole gate for an individually
		// frozen output.
		tag, err := s.pool.Exec(ctx, `
			UPDATE txs
			SET out_frozens[$2::int + 1] = true
			WHERE hash = $1
			  AND array_length(utxo_hashes, 1) >= $2::int + 1
			  AND NOT COALESCE(out_frozens[$2::int + 1], false)
			  AND NOT EXISTS (
				SELECT 1 FROM spends sp
				WHERE sp.prev_tx_hash = $1 AND sp.prev_output_idx = $2
			  )
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		if tag.RowsAffected() == 0 {
			// Nothing was frozen — diagnose why so we return the correct typed error.
			if reason := s.freezeRejectReason(ctx, spend); reason != nil {
				return reason
			}
			// Output exists, unspent and unfrozen at diagnosis time but the guarded
			// UPDATE matched nothing — a concurrent freeze won the race. Treat as
			// already frozen.
			return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
		}
	}

	return nil
}

// freezeRejectReason returns the typed error explaining why a guarded freeze
// UPDATE affected no rows (spent or not found), or nil if the output is present,
// unspent and unfrozen — meaning a concurrent freeze won the race.
// v4: reads from txs arrays instead of the outputs table.
func (s *Store) freezeRejectReason(ctx context.Context, spend *utxo.Spend) error {
	var (
		outputFrozen bool
		spendingData []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT
		    COALESCE(CASE WHEN array_length(t.out_frozens, 1) >= $2::int + 1 THEN t.out_frozens[$2::int + 1] END, false),
		    sp.spending_data
		FROM txs t
		LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
		WHERE t.hash = $1 AND array_length(t.utxo_hashes, 1) >= $2::int + 1
		ORDER BY t.bucket DESC LIMIT 1
	`, spend.TxID[:], spend.Vout).Scan(&outputFrozen, &spendingData)
	if err != nil {
		return errors.NewStorageError("[FreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
	}

	if spendingData != nil {
		sd, parseErr := spendpkg.NewSpendingDataFromBytes(spendingData)
		if parseErr != nil {
			return errors.NewProcessingError("failed to create spending data from bytes", parseErr)
		}
		return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, sd)
	}

	if outputFrozen {
		return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO is not frozen.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Verify output is frozen (read from txs array).
		var outputFrozen bool
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(
			    CASE WHEN array_length(t.out_frozens, 1) >= $2::int + 1 THEN t.out_frozens[$2::int + 1] END,
			    false)
			FROM txs t WHERE t.hash = $1 AND array_length(t.utxo_hashes, 1) >= $2::int + 1
			ORDER BY t.bucket DESC LIMIT 1
		`, spend.TxID[:], spend.Vout).Scan(&outputFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
		}

		if !outputFrozen {
			return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
		}

		// Unfreeze the array element and recompute txs.frozen.
		// txs.frozen is set to true iff any out_frozens element is true.
		// We use array_positions(out_frozens, true) to check whether any remain.
		_, err = s.pool.Exec(ctx, `
			UPDATE txs
			SET out_frozens[$2::int + 1] = false,
			    frozen = (
			        SELECT COALESCE(
			            cardinality(array_positions(
			                CASE WHEN array_length(out_frozens, 1) IS NOT NULL
			                     THEN out_frozens[:$2::int] || ARRAY[false] ||
			                          COALESCE(out_frozens[$2::int + 2:], ARRAY[]::boolean[])
			                     ELSE ARRAY[]::boolean[] END,
			                true)) > 0,
			            false)
			    )
			WHERE hash = $1
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			// Fallback: set the element and recompute frozen separately. Both steps
			// run in ONE transaction so a concurrent Get/Spend never observes the torn
			// state where out_frozens[idx]=false but txs.frozen still reflects the old
			// value (which would wrongly reject a spend on the tx_frozen gate).
			pgxTx, beginErr := s.pool.Begin(ctx)
			if beginErr != nil {
				return errors.NewStorageError("[UnFreezeUTXOs] begin fallback tx for %s:%d", spend.TxID, spend.Vout, beginErr)
			}
			if _, err2 := pgxTx.Exec(ctx, `
				UPDATE txs SET out_frozens[$2::int + 1] = false WHERE hash = $1
			`, spend.TxID[:], spend.Vout); err2 != nil {
				_ = pgxTx.Rollback(ctx)
				return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze txs array for %s:%d", spend.TxID, spend.Vout, err2)
			}
			// Recompute txs.frozen from the updated array.
			if _, err2 := pgxTx.Exec(ctx, `
				UPDATE txs
				SET frozen = COALESCE(cardinality(array_positions(out_frozens, true)) > 0, false)
				WHERE hash = $1
			`, spend.TxID[:]); err2 != nil {
				_ = pgxTx.Rollback(ctx)
				return errors.NewStorageError("[UnFreezeUTXOs] failed to recompute txs.frozen for %s", spend.TxID, err2)
			}
			if commitErr := pgxTx.Commit(ctx); commitErr != nil {
				return errors.NewStorageError("[UnFreezeUTXOs] commit fallback tx for %s", spend.TxID, commitErr)
			}
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	// Verify source UTXO is frozen (read from txs array).
	var outputFrozen bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(
		    CASE WHEN array_length(t.out_frozens, 1) >= $2::int + 1 THEN t.out_frozens[$2::int + 1] END,
		    false)
		FROM txs t WHERE t.hash = $1 AND array_length(t.utxo_hashes, 1) >= $2::int + 1
		ORDER BY t.bucket DESC LIMIT 1
	`, utxoSpend.TxID[:], utxoSpend.Vout).Scan(&outputFrozen)
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] output lookup failed for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err)
	}

	if !outputFrozen {
		return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", utxoSpend.TxID, utxoSpend.Vout)
	}

	// Use configurable setting if provided, otherwise fall back to constant.
	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}
	spendableIn := s.GetBlockHeight() + reassignBlocks

	// Update txs arrays: new utxo_hash, clear frozen, set spendable_in for this slot.
	si := int32(spendableIn)
	_, err = s.pool.Exec(ctx, `
		UPDATE txs
		SET utxo_hashes[$2::int + 1]               = $3,
		    out_frozens[$2::int + 1]               = false,
		    spendable_ins[$2::int + 1]             = $4,
		    frozen = COALESCE(
		        cardinality(array_positions(
		            CASE WHEN array_length(out_frozens, 1) IS NOT NULL
		                 THEN out_frozens[:$2::int] || ARRAY[false] ||
		                      COALESCE(out_frozens[$2::int + 2:], ARRAY[]::boolean[])
		                 ELSE ARRAY[]::boolean[] END,
		            true)) > 0,
		        false)
		WHERE hash = $1
	`, utxoSpend.TxID[:], utxoSpend.Vout, newUtxo.UTXOHash[:], si)
	if err != nil {
		// Fallback: update each field then recompute frozen. Both steps run in ONE
		// transaction so a concurrent Get/Spend never observes the torn state where
		// out_frozens[idx]=false but txs.frozen still reflects the old value.
		pgxTx, beginErr := s.pool.Begin(ctx)
		if beginErr != nil {
			return errors.NewStorageError("[ReAssignUTXO] begin fallback tx for %s:%d", utxoSpend.TxID, utxoSpend.Vout, beginErr)
		}
		if _, err2 := pgxTx.Exec(ctx, `
			UPDATE txs
			SET utxo_hashes[$2::int + 1]   = $3,
			    out_frozens[$2::int + 1]   = false,
			    spendable_ins[$2::int + 1] = $4
			WHERE hash = $1
		`, utxoSpend.TxID[:], utxoSpend.Vout, newUtxo.UTXOHash[:], si); err2 != nil {
			_ = pgxTx.Rollback(ctx)
			return errors.NewStorageError("[ReAssignUTXO] failed to update txs arrays for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err2)
		}
		if _, err2 := pgxTx.Exec(ctx, `
			UPDATE txs
			SET frozen = COALESCE(cardinality(array_positions(out_frozens, true)) > 0, false)
			WHERE hash = $1
		`, utxoSpend.TxID[:]); err2 != nil {
			_ = pgxTx.Rollback(ctx)
			return errors.NewStorageError("[ReAssignUTXO] failed to recompute txs.frozen for %s", utxoSpend.TxID, err2)
		}
		if commitErr := pgxTx.Commit(ctx); commitErr != nil {
			return errors.NewStorageError("[ReAssignUTXO] commit fallback tx for %s", utxoSpend.TxID, commitErr)
		}
	}

	return nil
}
