package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// SQL strings for alert_system operations. The `% NumPartitions` modulus is
// substituted from the Go constant so bumping NumPartitions automatically
// updates every partition_key derivation.
var (
	alertFreezeOutputLookupSQL = fmt.Sprintf(`
		SELECT o.frozen, sp.spending_data
		FROM outputs o
		LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.partition_key = o.partition_key AND sp.prev_output_idx = o.idx
		WHERE o.tx_hash = $1 AND o.partition_key = get_byte($1, 1) %% %d AND o.idx = $2
	`, NumPartitions)

	alertFreezeOutputSQL = fmt.Sprintf(`
		UPDATE outputs SET frozen = true
		WHERE tx_hash = $1 AND partition_key = get_byte($1, 1) %% %d AND idx = $2
	`, NumPartitions)

	alertFreezeTxSQL = fmt.Sprintf(`
		UPDATE txs SET frozen = true WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d
	`, NumPartitions)

	alertUnfreezeOutputLookupSQL = fmt.Sprintf(`
		SELECT o.frozen FROM outputs o WHERE o.tx_hash = $1 AND o.partition_key = get_byte($1, 1) %% %d AND o.idx = $2
	`, NumPartitions)

	alertUnfreezeOutputSQL = fmt.Sprintf(`
		UPDATE outputs SET frozen = false
		WHERE tx_hash = $1 AND partition_key = get_byte($1, 1) %% %d AND idx = $2 AND frozen = true
	`, NumPartitions)

	alertCountFrozenOutputsSQL = fmt.Sprintf(`
		SELECT COUNT(*) FROM outputs WHERE tx_hash = $1 AND partition_key = get_byte($1, 1) %% %d AND frozen = true
	`, NumPartitions)

	alertUnfreezeTxSQL = fmt.Sprintf(`
		UPDATE txs SET frozen = false WHERE hash = $1 AND partition_key = get_byte($1, 1) %% %d
	`, NumPartitions)

	alertReassignOutputLookupSQL = fmt.Sprintf(`
		SELECT o.frozen FROM outputs o WHERE o.tx_hash = $1 AND o.partition_key = get_byte($1, 1) %% %d AND o.idx = $2
	`, NumPartitions)

	alertReassignUpdateSQL = fmt.Sprintf(`
		UPDATE outputs
		SET utxo_hash = $1, frozen = false, spendable_in = $2
		WHERE tx_hash = $3 AND partition_key = get_byte($3, 1) %% %d AND idx = $4 AND frozen = true
	`, NumPartitions)
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// Returns an error if any UTXO is already spent or frozen.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Verify output exists and is not already spent or frozen.
		var (
			outputFrozen bool
			spendingData []byte
		)

		err := s.pool.QueryRow(ctx, alertFreezeOutputLookupSQL, spend.TxID[:], spend.Vout).Scan(&outputFrozen, &spendingData)
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

		// Freeze the output.
		_, err = s.pool.Exec(ctx, alertFreezeOutputSQL, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		// Set frozen on txs.
		_, err = s.pool.Exec(ctx, alertFreezeTxSQL, spend.TxID[:])
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze txs for %s", spend.TxID, err)
		}
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO is not frozen.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Verify output is frozen.
		var outputFrozen bool
		err := s.pool.QueryRow(ctx, alertUnfreezeOutputLookupSQL, spend.TxID[:], spend.Vout).Scan(&outputFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
		}

		if !outputFrozen {
			return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
		}

		// Unfreeze the output.
		_, err = s.pool.Exec(ctx, alertUnfreezeOutputSQL, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		// Only clear txs.frozen if no other frozen outputs remain.
		var remainingFrozen int
		err = s.pool.QueryRow(ctx, alertCountFrozenOutputsSQL, spend.TxID[:]).Scan(&remainingFrozen)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to count frozen outputs for %s", spend.TxID, err)
		}

		if remainingFrozen == 0 {
			_, err = s.pool.Exec(ctx, alertUnfreezeTxSQL, spend.TxID[:])
			if err != nil {
				return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze txs for %s", spend.TxID, err)
			}
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	// Verify source UTXO is frozen.
	var outputFrozen bool
	err := s.pool.QueryRow(ctx, alertReassignOutputLookupSQL, utxoSpend.TxID[:], utxoSpend.Vout).Scan(&outputFrozen)
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
	_, err = s.pool.Exec(ctx, alertReassignUpdateSQL,
		newUtxo.UTXOHash[:], int32(spendableIn), utxoSpend.TxID[:], utxoSpend.Vout)
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err)
	}

	return nil
}
