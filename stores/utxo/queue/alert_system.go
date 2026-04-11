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
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		idx := int(spend.Vout) + 1 // PG arrays are 1-based

		// Verify output exists and is not already spent or frozen.
		var outputFrozen bool
		var spendingDataByte []byte

		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(frozen_outputs[$2], false), spending_data[$2]
			FROM utxos WHERE hash = $1`,
			spend.TxID[:], idx,
		).Scan(&outputFrozen, &spendingDataByte)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
		}

		if spendingDataByte != nil {
			sd, parseErr := spendpkg.NewSpendingDataFromBytes(spendingDataByte)
			if parseErr != nil {
				return errors.NewProcessingError("failed to create spending data from bytes", parseErr)
			}
			return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, sd)
		}

		if outputFrozen {
			return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
		}

		// Freeze the output and set tx-level frozen.
		_, err = s.pool.Exec(ctx, `
			UPDATE utxos SET frozen_outputs[$2] = true, frozen = true
			WHERE hash = $1`,
			spend.TxID[:], idx,
		)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze output %s:%d", spend.TxID, spend.Vout, err)
		}
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO is not frozen.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		idx := int(spend.Vout) + 1 // PG arrays are 1-based

		// Verify output is frozen.
		var outputFrozen bool
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(frozen_outputs[$2], false) FROM utxos WHERE hash = $1`,
			spend.TxID[:], idx,
		).Scan(&outputFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
		}

		if !outputFrozen {
			return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
		}

		// Unfreeze the output.
		_, err = s.pool.Exec(ctx, `
			UPDATE utxos SET frozen_outputs[$2] = false
			WHERE hash = $1`,
			spend.TxID[:], idx,
		)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		// Only clear utxos.frozen if no other frozen outputs remain.
		var remainingFrozen int
		err = s.pool.QueryRow(ctx, `
			SELECT COALESCE(
				(SELECT COUNT(*) FROM unnest(frozen_outputs) AS fo(val) WHERE fo.val = true),
				0
			) FROM utxos WHERE hash = $1`,
			spend.TxID[:],
		).Scan(&remainingFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to count frozen outputs for %s", spend.TxID, err)
		}

		if remainingFrozen == 0 {
			_, err = s.pool.Exec(ctx, `
				UPDATE utxos SET frozen = false WHERE hash = $1`,
				spend.TxID[:],
			)
			if err != nil {
				return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze utxos for %s", spend.TxID, err)
			}
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	idx := int(utxoSpend.Vout) + 1 // PG arrays are 1-based

	// Verify source UTXO is frozen.
	var outputFrozen bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(frozen_outputs[$2], false) FROM utxos WHERE hash = $1`,
		utxoSpend.TxID[:], idx,
	).Scan(&outputFrozen)
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

	// Reassign: update utxo_hash, unfreeze, set spendable_in.
	_, err = s.pool.Exec(ctx, `
		UPDATE utxos
		SET utxo_hashes[$2] = $3, frozen_outputs[$2] = false, spendable_in[$2] = $4
		WHERE hash = $1`,
		utxoSpend.TxID[:], idx, newUtxo.UTXOHash[:], int32(spendableIn),
	)
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err)
	}

	return nil
}
