package queue

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// ---------------------------------------------------------------------------
// Batch types
// ---------------------------------------------------------------------------

// batchSpendItem represents a single spend queued into the batcher.
type batchSpendItem struct {
	spend             *utxo.Spend
	blockHeight       uint32
	errCh             chan error
	ignoreConflicting bool
	ignoreLocked      bool
}

// ---------------------------------------------------------------------------
// Direct-mode SQL (used when batcher is not active)
// ---------------------------------------------------------------------------

// spendValidationSQL is the CTE used to validate a spend attempt and insert
// into the append-only spends table in a single round-trip.
const spendValidationSQL = `
WITH validation AS (
    SELECT o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
           o.coinbase_spending_height,
           t.locked AS tx_locked, t.conflicting AS tx_conflicting,
           t.frozen AS tx_frozen,
           sp.spending_data AS existing_spend
    FROM outputs o
    JOIN txs t ON t.hash = o.tx_hash
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
       t.locked AS tx_locked, t.conflicting AS tx_conflicting,
       t.frozen AS tx_frozen,
       sp.spending_data AS existing_spend
FROM outputs o
JOIN txs t ON t.hash = o.tx_hash
LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
WHERE o.tx_hash = $1 AND o.idx = $2
`

// ---------------------------------------------------------------------------
// Spend — public API
// ---------------------------------------------------------------------------

// Spend marks UTXOs consumed by the given transaction as spent.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if prometheusDirectSpend != nil {
		prometheusDirectSpend.Inc()
	}

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

	if s.spendBatcher != nil {
		return s.spendBatched(ctx, tx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting)
	}

	return s.spendDirect(ctx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting)
}

// ---------------------------------------------------------------------------
// spendBatched — enqueue each input into the batcher
// ---------------------------------------------------------------------------

func (s *Store) spendBatched(ctx context.Context, tx *bt.Tx, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	// Enqueue each spend into the batcher and wait for results.
	errChs := make([]chan error, len(spends))
	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}
		errCh := make(chan error, 1)
		errChs[idx] = errCh
		s.spendBatcher.Put(&batchSpendItem{
			spend:             spend,
			blockHeight:       blockHeight,
			errCh:             errCh,
			ignoreConflicting: ignoreConflicting,
			ignoreLocked:      ignoreLocked,
		})
	}

	// Wait for all results.
	for idx, spend := range spends {
		var batchErr error
		select {
		case batchErr = <-errChs[idx]:
		case <-ctx.Done():
			spends[idx].Err = errors.NewContextCanceledError("[Spend] context cancelled for %s:%d", spend.TxID, spend.Vout)
			continue
		}

		if batchErr != nil {
			spends[idx].Err = batchErr

			var errSpent *errors.UtxoSpentErrData
			if errors.AsData(batchErr, &errSpent) {
				spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
				if prometheusDirectConflicts != nil {
					prometheusDirectConflicts.Inc()
				}
			}
			continue
		}
		spentSpends = append(spentSpends, spend)
	}

	if len(spends) != len(spentSpends) {
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

// ---------------------------------------------------------------------------
// spendDirect — per-input validation CTE (no batcher)
// ---------------------------------------------------------------------------

func (s *Store) spendDirect(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		spendingDataBytes := spend.SpendingData.Bytes()
		inputStart := time.Now()

		// Try the atomic INSERT with validation CTE.
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			spend.TxID[:],      // $1 prev_tx_hash
			spend.Vout,         // $2 prev_output_idx
			spendingDataBytes,  // $3 spending_data
			spend.UTXOHash[:],  // $4 expected_utxo_hash
			int64(blockHeight), // $5 blockHeight
			ignoreLocked,       // $6 ignoreLocked
			ignoreConflicting,  // $7 ignoreConflicting
		).Scan(&inserted)

		if prometheusDirectSpendDuration != nil {
			prometheusDirectSpendDuration.Observe(time.Since(inputStart).Seconds())
		}

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			spends[idx].Err = errors.NewStorageError("[Spend] query failed for %s:%d", spend.TxID, spend.Vout, err)
			continue
		}

		if err == nil {
			spentSpends = append(spentSpends, spend)
			continue
		}

		// INSERT returned 0 rows — run diagnostic query.
		diagErr := s.diagnoseSpendFailure(ctx, spend, spendingDataBytes, blockHeight, ignoreLocked, ignoreConflicting)
		if diagErr == nil {
			spentSpends = append(spentSpends, spend)
			continue
		}

		spends[idx].Err = diagErr

		var errSpent *errors.UtxoSpentErrData
		if errors.AsData(diagErr, &errSpent) {
			spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
			if prometheusDirectConflicts != nil {
				prometheusDirectConflicts.Inc()
			}
		}
	}

	if len(spends) != len(spentSpends) {
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

// ---------------------------------------------------------------------------
// sendSpendBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

// spendSelectResult holds the result of a bulk SELECT for a single spend item.
type spendSelectResult struct {
	batchIdx               int
	utxoHash               []byte
	outputFrozen           bool
	spendableIn            *int32
	coinbaseSpendingHeight int64
	txLocked               bool
	txConflicting          bool
	txFrozen               bool
	existingSpendBytes     []byte
}

func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		retryable := s.trySendSpendBatch(batch)
		if !retryable {
			return
		}
		s.logger.Warnf("[Spend] deadlock detected (attempt %d/%d), retrying batch of %d items", attempt+1, maxRetries, len(batch))
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	for _, item := range batch {
		item.errCh <- errors.NewStorageError("[Spend] deadlock persisted after %d retries", maxRetries)
	}
}

func (s *Store) trySendSpendBatch(batch []*batchSpendItem) (retryable bool) {
	ctx := context.Background()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to acquire connection", err)
		}
		return false
	}
	defer conn.Release()

	// Pipeline N validation CTEs via SendBatch — each CTE does validate+insert
	// in a single query, all sent in one network flush. No transaction needed.
	pgxBatch := &pgx.Batch{}
	for _, item := range batch {
		pgxBatch.Queue(spendValidationSQL,
			item.spend.TxID[:],
			item.spend.Vout,
			item.spend.SpendingData.Bytes(),
			item.spend.UTXOHash[:],
			int64(item.blockHeight),
			item.ignoreLocked,
			item.ignoreConflicting,
		)
	}

	br := conn.SendBatch(ctx, pgxBatch)

	for _, item := range batch {
		var inserted int
		err := br.QueryRow().Scan(&inserted)

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			item.errCh <- errors.NewStorageError("[Spend] pipeline query failed for %s:%d: %v", item.spend.TxID, item.spend.Vout, err)
			continue
		}

		if err == nil {
			// Validation CTE succeeded — spend inserted.
			item.errCh <- nil
			continue
		}

		// INSERT returned 0 rows — run diagnostic to determine exact failure reason.
		diagErr := s.diagnoseSpendFailure(ctx, item.spend, item.spend.SpendingData.Bytes(),
			item.blockHeight, item.ignoreLocked, item.ignoreConflicting)
		if diagErr == nil {
			item.errCh <- nil
		} else {
			item.errCh <- diagErr
		}
	}

	br.Close()
	return false
}

// isDeadlock checks if an error is a PostgreSQL deadlock (40P01).
func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "40P01") || strings.Contains(err.Error(), "deadlock")
}

// diagnoseSpendFailure queries the output + txs + spends to determine
// why a spend INSERT failed.
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

	// Check existing spend (double-spend or idempotent).
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

	// Check frozen.
	if outputFrozen || txFrozen {
		return errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
	}

	// Check locked (when not ignored).
	if txLocked && !ignoreLocked {
		return errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
	}

	// Check conflicting (when not ignored).
	if txConflicting && !ignoreConflicting {
		return errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
	}

	// Check UTXO hash mismatch.
	if !bytes.Equal(utxoHashBytes, spend.UTXOHash[:]) {
		return errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	// Check coinbase maturity.
	if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > int64(blockHeight) {
		return errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d",
			spend.TxID, spend.Vout, coinbaseSpendingHeight, blockHeight)
	}

	// Check spendable_in.
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
