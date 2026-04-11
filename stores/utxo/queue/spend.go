package queue

import (
	"context"
	"encoding/hex"
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
// spendDirect — per-input spend_utxo() function call (no batcher)
// ---------------------------------------------------------------------------

func (s *Store) spendDirect(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		spendingDataBytes := spend.SpendingData.Bytes()
		inputStart := time.Now()

		// Call the spend_utxo() stored function.
		var result string
		err := s.pool.QueryRow(ctx, `SELECT spend_utxo($1,$2,$3,$4,$5,$6,$7)`,
			spend.TxID[:],      // $1 prev_tx_hash
			spend.Vout,         // $2 output_idx (0-based, function adds 1)
			spendingDataBytes,  // $3 spending_data
			spend.UTXOHash[:],  // $4 expected_utxo_hash
			int64(blockHeight), // $5 block_height
			ignoreLocked,       // $6 ignore_locked
			ignoreConflicting,  // $7 ignore_conflicting
		).Scan(&result)

		if prometheusDirectSpendDuration != nil {
			prometheusDirectSpendDuration.Observe(time.Since(inputStart).Seconds())
		}

		if err != nil {
			spends[idx].Err = errors.NewStorageError("[Spend] spend_utxo call failed for %s:%d", spend.TxID, spend.Vout, err)
			continue
		}

		spendErr := parseSpendResult(result, spend)
		if spendErr != nil {
			spends[idx].Err = spendErr
			var errSpent *errors.UtxoSpentErrData
			if errors.AsData(spendErr, &errSpent) {
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
// sendSpendBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	ctx := context.Background()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to acquire connection", err)
		}
		return
	}
	defer conn.Release()

	// Pipeline N spend_utxo() function calls via SendBatch.
	pgxBatch := &pgx.Batch{}
	for _, item := range batch {
		spendingDataBytes := item.spend.SpendingData.Bytes()
		pgxBatch.Queue(`SELECT spend_utxo($1,$2,$3,$4,$5,$6,$7)`,
			item.spend.TxID[:],
			item.spend.Vout,
			spendingDataBytes,
			item.spend.UTXOHash[:],
			int64(item.blockHeight),
			item.ignoreLocked,
			item.ignoreConflicting,
		)
	}

	br := conn.SendBatch(ctx, pgxBatch)

	// Read results for each queued call.
	for i, item := range batch {
		var result string
		err := br.QueryRow().Scan(&result)
		if err != nil {
			batch[i].errCh <- errors.NewStorageError("[Spend] spend_utxo batch call failed for %s:%d", item.spend.TxID, item.spend.Vout, err)
			continue
		}

		spendErr := parseSpendResult(result, item.spend)
		batch[i].errCh <- spendErr
	}

	br.Close()
}

// ---------------------------------------------------------------------------
// parseSpendResult maps the TEXT return from spend_utxo() to error types.
// ---------------------------------------------------------------------------

func parseSpendResult(result string, spend *utxo.Spend) error {
	switch {
	case result == "OK":
		return nil
	case result == "TX_NOT_FOUND":
		return errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
	case result == "LOCKED":
		return errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
	case result == "CONFLICTING":
		return errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
	case result == "TX_FROZEN" || result == "OUTPUT_FROZEN":
		return errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
	case result == "HASH_MISMATCH":
		return errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	case result == "COINBASE_IMMATURE":
		return errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d", spend.TxID, spend.Vout)
	case result == "NOT_SPENDABLE":
		return errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable yet", spend.TxID, spend.Vout)
	case strings.HasPrefix(result, "SPENT:"):
		// Parse the existing spending data from hex.
		hexStr := strings.TrimPrefix(result, "SPENT:")
		existingBytes, decodeErr := hex.DecodeString(hexStr)
		if decodeErr != nil {
			return errors.NewProcessingError("failed to decode existing spending data hex", decodeErr)
		}
		existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(existingBytes)
		if parseErr != nil {
			return errors.NewProcessingError("failed to parse existing spending data", parseErr)
		}
		return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
	default:
		return errors.NewStorageError("[Spend] unknown spend_utxo result: %s for %s:%d", result, spend.TxID, spend.Vout)
	}
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

// Unspend reverses a previous spend operation by clearing the spending_data
// array element and decrementing spent_count.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	for _, spend := range spends {
		if spend == nil {
			continue
		}
		// Clear the spending_data element (1-based index).
		_, err := s.pool.Exec(ctx,
			`UPDATE utxos SET spending_data[$2] = NULL, spent_count = GREATEST(spent_count - 1, 0) WHERE hash = $1`,
			spend.TxID[:], int(spend.Vout)+1,
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
