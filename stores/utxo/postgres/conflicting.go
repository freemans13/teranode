package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// SQL strings for conflicting operations. HASH partitioning prunes
// per-partition automatically from the hash-keyed WHERE clause.
const conflictingSetTxSQL = `
UPDATE txs SET
  conflicting = $2,
  delete_at_height = COALESCE(delete_at_height, $3)
WHERE hash = $1`

const conflictingClearTxSQL = `
UPDATE txs SET
  conflicting = $2,
  delete_at_height = $3
WHERE hash = $1`

const conflictingFindSpendingSQL = `
SELECT sp.spending_data
FROM spends sp
WHERE sp.prev_tx_hash = $1`

// GetCounterConflicting delegates to the store-agnostic implementation which
// walks inputs to find counter-conflicting transactions.
func (s *Store) GetCounterConflicting(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, hash)
}

// GetConflictingChildren delegates to the store-agnostic implementation which
// recursively walks spending children and conflicting_children records.
func (s *Store) GetConflictingChildren(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, hash)
}

// SetConflicting marks transactions as conflicting or not conflicting.
// It updates txs.conflicting and txs.delete_at_height, appends to the
// conflicting_children array on parent txs, and returns affected parent spends
// and spending child tx hashes.
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if len(txHashes) == 0 {
		return nil, nil, nil
	}

	// Compute delete_at_height when setting conflicting=true.
	var deleteAtHeight *int64
	if s.settings.GetUtxoStoreBlockHeightRetention() > 0 && setValue {
		v := int64(s.blockHeight.Load() + 1 + s.settings.GetUtxoStoreBlockHeightRetention())
		deleteAtHeight = &v
	}

	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))

	for _, conflictingTxHash := range txHashes {
		txHash := conflictingTxHash

		// Get the full tx so we can identify parents and outputs.
		txMeta, err := s.Get(ctx, &txHash)
		if err != nil {
			return nil, nil, err
		}

		// Update txs: set conflicting flag + delete_at_height.
		if setValue {
			// When setting conflicting=true: set DAH only if not already set.
			_, err = s.pool.Exec(ctx, conflictingSetTxSQL,
				txHash[:], setValue, deleteAtHeight,
			)
		} else {
			// When clearing conflicting: clear DAH.
			_, err = s.pool.Exec(ctx, conflictingClearTxSQL,
				txHash[:], setValue, deleteAtHeight,
			)
		}
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to set conflicting flag for %s", txHash, err)
		}

		// Append this tx as a conflicting child of each parent (array on txs).
		if txMeta.Tx != nil {
			seen := make(map[chainhash.Hash]struct{}, len(txMeta.Tx.Inputs))
			for _, input := range txMeta.Tx.Inputs {
				parentHash := *input.PreviousTxIDChainHash()
				if _, ok := seen[parentHash]; ok {
					continue
				}
				seen[parentHash] = struct{}{}

				_, insertErr := s.pool.Exec(ctx, `
					UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
					WHERE hash = $1`,
					parentHash[:], [][]byte{txHash[:]},
				)
				if insertErr != nil {
					return nil, nil, errors.NewStorageError("failed to update conflicting_children for %s", txHash, insertErr)
				}
			}
		}

		// Build affected parent spends (inputs of the conflicting tx).
		if txMeta.Tx != nil {
			for i, input := range txMeta.Tx.Inputs {
				utxoHash, hashErr := util.UTXOHashFromInput(input)
				if hashErr != nil {
					return nil, nil, hashErr
				}

				affectedParentSpends = append(affectedParentSpends, &utxo.Spend{
					TxID:         input.PreviousTxIDChainHash(),
					Vout:         input.PreviousTxOutIndex,
					UTXOHash:     utxoHash,
					SpendingData: spendpkg.NewSpendingData(&txHash, i),
				})
			}
		}

		// Find spending child transactions by querying the spends table directly.
		if txMeta.Tx != nil {
			rows, queryErr := s.pool.Query(ctx, conflictingFindSpendingSQL,
				txHash[:],
			)
			if queryErr != nil {
				return nil, nil, queryErr
			}

			for rows.Next() {
				var spendingDataBytes []byte
				if scanErr := rows.Scan(&spendingDataBytes); scanErr != nil {
					rows.Close()
					return nil, nil, scanErr
				}
				if len(spendingDataBytes) >= 32 {
					sd, parseErr := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
					if parseErr != nil {
						rows.Close()
						return nil, nil, parseErr
					}
					if sd.TxID != nil {
						spendingTxHashes = append(spendingTxHashes, *sd.TxID)
					}
				}
			}
			rows.Close()
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, nil, rowsErr
			}
		}
	}

	return affectedParentSpends, spendingTxHashes, nil
}

// batchUnlockItem represents a single SetLocked(false) request.
type batchUnlockItem struct {
	hash chainhash.Hash
	done chan error
}

// runUnlockBatch is the Unlock batcher callback. Acquires its own
// pgxpool connection for the duration of the batch and runs a single
// bulk UPDATE on the `txs` parent table; postgres prunes per-partition
// itself from the hash-keyed WHERE clause.
func (s *Store) runUnlockBatch(batch []*batchUnlockItem) {
	ctx := context.Background()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		e := errors.NewStorageError("[Unlock] failed to acquire conn: %v", err)
		for _, item := range batch {
			item.done <- e
		}
		return
	}
	defer conn.Release()

	hashBytes := make([][]byte, len(batch))
	for i, item := range batch {
		hashBytes[i] = item.hash[:]
	}
	const q = `UPDATE txs SET locked = false WHERE hash = ANY($1)`
	if _, err := conn.Exec(ctx, q, hashBytes); err != nil {
		e := errors.NewStorageError("[Unlock] bulk update: %v", err)
		for _, item := range batch {
			item.done <- e
		}
		return
	}
	for _, item := range batch {
		item.done <- nil
	}
}

// SetLocked marks transactions as locked or unlocked.
// When locking (setValue=true), delete_at_height is cleared so the tx is not pruned.
// When unlocking (setValue=false), delete_at_height is left as-is.
// Single-hash unlock calls are batched when unlockBatcher is active.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	// Single-hash unlock → route through the unlock batcher.
	if s.workersStarted() && !setValue && len(txHashes) == 1 {
		done := make(chan error, 1)
		s.unlockBatcher.Put(&batchUnlockItem{hash: txHashes[0], done: done})
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for i := 0; i < len(txHashes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txHashes) {
			end = len(txHashes)
		}
		chunk := txHashes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hashBytes := make([][]byte, len(chunk))
		for j := range chunk {
			hashBytes[j] = chunk[j][:]
		}

		if setValue {
			inClause, args := buildINClauseLocal(hashBytes, 1)
			q := fmt.Sprintf(`UPDATE txs SET locked = true, delete_at_height = NULL WHERE hash IN %s`, inClause)
			if _, err := s.pool.Exec(ctx, q, args...); err != nil {
				return errors.NewStorageError("failed to set locked flag", err)
			}
		} else {
			inClause, args := buildINClauseLocal(hashBytes, 1)
			q := fmt.Sprintf(`UPDATE txs SET locked = false WHERE hash IN %s`, inClause)
			if _, err := s.pool.Exec(ctx, q, args...); err != nil {
				return errors.NewStorageError("failed to clear locked flag", err)
			}
		}
	}

	return nil
}

// MarkTransactionsOnLongestChain updates unmined_since for transactions.
// onLongestChain=true: clears unmined_since (transaction is mined on main chain).
// onLongestChain=false: sets unmined_since to current block height (transaction is unmined).
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	currentBlockHeight := s.GetBlockHeight()

	attempted := len(txHashes)
	totalUpdated := 0
	allErrors := make([]error, 0, 10)
	errorCount := 0

	allHashBytes := make([][]byte, len(txHashes))
	for i := range txHashes {
		allHashBytes[i] = txHashes[i][:]
	}

	for i := 0; i < len(allHashBytes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(allHashBytes) {
			end = len(allHashBytes)
		}
		chunk := allHashBytes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var q string
		var args []interface{}

		if onLongestChain {
			inClause, inArgs := buildINClauseLocal(chunk, 1)
			q = fmt.Sprintf(`UPDATE txs SET unmined_since = NULL WHERE hash IN %s`, inClause)
			args = inArgs
		} else {
			inClause, inArgs := buildINClauseLocal(chunk, 2)
			q = fmt.Sprintf(`UPDATE txs SET unmined_since = $1 WHERE hash IN %s`, inClause)
			args = append([]interface{}{int64(currentBlockHeight)}, inArgs...)
		}

		result, err := s.pool.Exec(ctx, q, args...)
		if err != nil {
			errorCount += len(chunk)
			if len(allErrors) < 10 {
				s.logger.Errorf("[MarkTransactionsOnLongestChain] chunk %d-%d error: %v", i, end-1, err)
				allErrors = append(allErrors, errors.NewStorageError("failed to mark chunk %d-%d: %v", i, end-1, err))
			}
			continue
		}

		totalUpdated += int(result.RowsAffected())
	}

	if errorCount > 0 {
		s.logger.Errorf("[MarkTransactionsOnLongestChain] completed with errors: attempted=%d, succeeded=%d, failed=%d",
			attempted, totalUpdated, errorCount)
		return errors.Join(allErrors...)
	}

	if totalUpdated < attempted {
		missing := attempted - totalUpdated
		s.logger.Errorf("[MarkTransactionsOnLongestChain] FATAL: %d/%d transactions not found in txs", missing, attempted)
	}

	return nil
}
