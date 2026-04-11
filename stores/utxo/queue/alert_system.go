package queue

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// Returns an error if any UTXO is already spent or frozen.
// v7: frozen_outputs array on txs replaces outputs.frozen column.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Verify output exists and is not already spent or frozen.
		var (
			outputFrozen bool
			spendingData []byte
		)

		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(t.frozen_outputs[$2+1], false), sp.spending_data
			FROM txs t
			LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
			WHERE t.hash = $1 AND t.utxo_hashes[$2+1] IS NOT NULL
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

		// Freeze the output in the array and set txs.frozen = true.
		_, err = s.pool.Exec(ctx, `
			UPDATE txs SET frozen_outputs[$2+1] = true, frozen = true
			WHERE hash = $1
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze output %s:%d", spend.TxID, spend.Vout, err)
		}
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO is not frozen.
// v7: reads/writes frozen_outputs array on txs.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Verify output is frozen.
		var outputFrozen bool
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(t.frozen_outputs[$2+1], false)
			FROM txs t WHERE t.hash = $1
		`, spend.TxID[:], spend.Vout).Scan(&outputFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
		}

		if !outputFrozen {
			return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
		}

		// Unfreeze the output in the array.
		_, err = s.pool.Exec(ctx, `
			UPDATE txs SET frozen_outputs[$2+1] = false
			WHERE hash = $1
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		// Only clear txs.frozen if no other frozen outputs remain.
		var remainingFrozen int
		err = s.pool.QueryRow(ctx, `
			SELECT COALESCE((SELECT COUNT(*) FROM UNNEST(frozen_outputs) AS f(v) WHERE f.v = true), 0)
			FROM txs WHERE hash = $1
		`, spend.TxID[:]).Scan(&remainingFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to count frozen outputs for %s", spend.TxID, err)
		}

		if remainingFrozen == 0 {
			_, err = s.pool.Exec(ctx, `
				UPDATE txs SET frozen = false WHERE hash = $1
			`, spend.TxID[:])
			if err != nil {
				return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze txs for %s", spend.TxID, err)
			}
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
// v7: updates utxo_hashes, frozen_outputs, spendable_in_arr arrays on txs.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	// Verify source UTXO is frozen.
	var outputFrozen bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(t.frozen_outputs[$2+1], false)
		FROM txs t WHERE t.hash = $1
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

	// Reassign: update utxo_hash, unfreeze, set spendable_in on txs arrays.
	_, err = s.pool.Exec(ctx, `
		UPDATE txs
		SET utxo_hashes[$2+1] = $3,
		    frozen_outputs[$2+1] = false,
		    spendable_in_arr[$2+1] = $4
		WHERE hash = $1
	`, utxoSpend.TxID[:], utxoSpend.Vout, newUtxo.UTXOHash[:], int32(spendableIn))
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err)
	}

	return nil
}
