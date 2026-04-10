package queue

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// spendValidationSQL is the CTE used to validate a spend attempt and insert
// into the append-only spends table in a single round-trip.
//
// Parameters: $1=prev_tx_hash, $2=prev_output_idx, $3=spending_data,
// $4=expected_utxo_hash, $5=blockHeight, $6=ignoreLocked, $7=ignoreConflicting
const spendValidationSQL = `
WITH validation AS (
    SELECT o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
           o.coinbase_spending_height,
           ts.locked AS tx_locked, ts.conflicting AS tx_conflicting,
           ts.frozen AS tx_frozen,
           sp.spending_data AS existing_spend
    FROM outputs o
    JOIN tx_state ts ON ts.tx_hash = o.tx_hash
    LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
    WHERE o.tx_hash = $1 AND o.idx = $2
)
INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data)
SELECT $1, $2, $3
FROM validation v
WHERE v.existing_spend IS NULL
  AND v.utxo_hash = $4
  AND NOT v.output_frozen AND NOT v.tx_frozen
  AND ($6 OR NOT v.tx_locked)
  AND ($7 OR NOT v.tx_conflicting)
  AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
  AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
RETURNING 1
`

// spendDiagnosticSQL re-queries the validation CTE when the INSERT returned
// 0 rows, so we can determine the exact reason the spend failed.
const spendDiagnosticSQL = `
SELECT o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
       o.coinbase_spending_height,
       ts.locked AS tx_locked, ts.conflicting AS tx_conflicting,
       ts.frozen AS tx_frozen,
       sp.spending_data AS existing_spend
FROM outputs o
JOIN tx_state ts ON ts.tx_hash = o.tx_hash
LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
WHERE o.tx_hash = $1 AND o.idx = $2
`

// Spend marks UTXOs consumed by the given transaction as spent.
// It uses a validation CTE + INSERT for each input and falls back to
// a diagnostic query when the INSERT returns 0 rows.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("No spends provided", nil)
	}

	spentSpends := make([]*utxo.Spend, 0, len(spends))

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		spendingDataBytes := spend.SpendingData.Bytes()

		// Try the atomic INSERT with validation CTE.
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			spend.TxID[:],        // $1 prev_tx_hash
			spend.Vout,           // $2 prev_output_idx
			spendingDataBytes,    // $3 spending_data
			spend.UTXOHash[:],    // $4 expected_utxo_hash
			int64(blockHeight),   // $5 blockHeight
			useIgnoreLocked,      // $6 ignoreLocked
			useIgnoreConflicting, // $7 ignoreConflicting
		).Scan(&inserted)

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			// Unexpected DB error
			spends[idx].Err = errors.NewStorageError("[Spend] query failed for %s:%d", spend.TxID, spend.Vout, err)
			continue
		}

		if err == nil {
			// INSERT succeeded -- spend was recorded.
			spentSpends = append(spentSpends, spend)
			continue
		}

		// INSERT returned 0 rows (pgx.ErrNoRows from RETURNING).
		// Run diagnostic query to determine the reason.
		diagErr := s.diagnoseSpendFailure(ctx, spend, spendingDataBytes, blockHeight, useIgnoreLocked, useIgnoreConflicting)
		if diagErr == nil {
			// Idempotent: same spending data already recorded, treat as success.
			spentSpends = append(spentSpends, spend)
			continue
		}

		spends[idx].Err = diagErr

		var errSpent *errors.UtxoSpentErrData
		if errors.AsData(diagErr, &errSpent) {
			spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
		}
	}

	// If not all spends succeeded, rollback the successful ones and return errors.
	if len(spends) != len(spentSpends) {
		// Rollback successful spends for genuine validation failures
		if needsSpendRollback(spends) {
			if unspendErr := s.Unspend(context.Background(), spentSpends); unspendErr != nil {
				s.logger.Errorf("error in queue unspend (rollback): %v", unspendErr)
			}
		}

		var spendErrors error
		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}

		return spends, errors.NewUtxoError("error in queue spend - errors", spendErrors)
	}

	return spends, nil
}

// diagnoseSpendFailure queries the output + tx_state + spends to determine
// why a spend INSERT failed. Returns the appropriate typed error.
func (s *Store) diagnoseSpendFailure(ctx context.Context, spend *utxo.Spend, spendingDataBytes []byte,
	blockHeight uint32, ignoreLocked, ignoreConflicting bool) error {

	var (
		utxoHashBytes          []byte
		outputFrozen           bool
		spendableIn            *int32
		coinbaseSpendingHeight int64
		txLocked               bool
		txConflicting          bool
		txFrozen               bool
		existingSpendBytes     []byte
	)

	err := s.pool.QueryRow(ctx, spendDiagnosticSQL,
		spend.TxID[:], // $1
		spend.Vout,    // $2
	).Scan(&utxoHashBytes, &outputFrozen, &spendableIn,
		&coinbaseSpendingHeight, &txLocked, &txConflicting, &txFrozen, &existingSpendBytes)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
		}
		return errors.NewStorageError("[Spend] diagnostic query failed for %s:%d", spend.TxID, spend.Vout, err)
	}

	// Check existing spend (double-spend or idempotent)
	if existingSpendBytes != nil {
		if bytes.Equal(existingSpendBytes, spendingDataBytes) {
			// Idempotent: same spending data already recorded.
			return nil
		}
		// Different spender: double-spend error.
		existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(existingSpendBytes)
		if parseErr != nil {
			return errors.NewProcessingError("failed to parse existing spending data", parseErr)
		}
		return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
	}

	// Check frozen
	if outputFrozen || txFrozen {
		return errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
	}

	// Check locked (when not ignored)
	if txLocked && !ignoreLocked {
		return errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
	}

	// Check conflicting (when not ignored)
	if txConflicting && !ignoreConflicting {
		return errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
	}

	// Check UTXO hash mismatch
	if !bytes.Equal(utxoHashBytes, spend.UTXOHash[:]) {
		return errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	// Check coinbase maturity
	if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > int64(blockHeight) {
		return errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d",
			spend.TxID, spend.Vout, coinbaseSpendingHeight, blockHeight)
	}

	// Check spendable_in
	if spendableIn != nil && *spendableIn > 0 && blockHeight < uint32(*spendableIn) {
		return errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *spendableIn)
	}

	// If we get here, the reason is unknown (possible race condition).
	return errors.NewStorageError("[Spend] unknown failure for %s:%d", spend.TxID, spend.Vout)
}

// needsSpendRollback returns true if any spend failed due to a validation error
// that indicates the transaction is genuinely invalid.
func needsSpendRollback(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend.Err == nil {
			continue
		}
		if errors.Is(spend.Err, errors.ErrSpent) ||
			errors.Is(spend.Err, errors.ErrTxConflicting) ||
			errors.Is(spend.Err, errors.ErrFrozen) ||
			errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}
	return false
}

// Unspend reverses a previous spend operation by deleting from the spends table.
// TODO: implement fully in Task 8.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	for _, spend := range spends {
		if spend == nil {
			continue
		}
		_, err := s.pool.Exec(ctx,
			`DELETE FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = $2`,
			spend.TxID[:], spend.Vout,
		)
		if err != nil {
			return errors.NewStorageError("[Unspend] failed for %s:%d", spend.TxID, spend.Vout, err)
		}
	}

	// If flagAsLocked is requested, lock the parent transactions.
	if len(flagAsLocked) > 0 && flagAsLocked[0] {
		uniqueHashes := make(map[chainhash.Hash]struct{}, len(spends))
		for _, spend := range spends {
			if spend != nil && spend.TxID != nil {
				uniqueHashes[*spend.TxID] = struct{}{}
			}
		}
		hashes := make([]chainhash.Hash, 0, len(uniqueHashes))
		for h := range uniqueHashes {
			hashes = append(hashes, h)
		}
		if len(hashes) > 0 {
			if err := s.SetLocked(ctx, hashes, true); err != nil {
				return errors.NewStorageError("[Unspend] failed to lock parent txs", err)
			}
		}
	}

	return nil
}
