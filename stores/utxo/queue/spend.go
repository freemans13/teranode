package queue

import (
	"bytes"
	"context"
	"fmt"
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

	pgxTx, err := conn.Begin(ctx)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to begin transaction", err)
		}
		return false
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Phase 1: Bulk SELECT — fetch all output states in one query.
	var sb strings.Builder
	sb.WriteString(`
		SELECT v.batch_idx, o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
		       o.coinbase_spending_height, t.locked, t.conflicting, t.frozen AS tx_frozen,
		       sp.spending_data AS existing_spend
		FROM (VALUES `)
	args := make([]interface{}, 0, len(batch)*3)
	paramIdx := 1
	for i, item := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("($%d::bytea,$%d::int,$%d::int)", paramIdx, paramIdx+1, paramIdx+2))
		args = append(args, item.spend.TxID[:], item.spend.Vout, i)
		paramIdx += 3
	}
	sb.WriteString(`) AS v(hash, idx, batch_idx)
		JOIN outputs o ON o.tx_hash = v.hash AND o.idx = v.idx
		JOIN txs t ON t.hash = v.hash
		LEFT JOIN spends sp ON sp.prev_tx_hash = v.hash AND sp.prev_output_idx = v.idx`)

	rows, err := pgxTx.Query(ctx, sb.String(), args...)
	if err != nil {
		if isDeadlock(err) {
			return true
		}
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed: bulk SELECT outputs", err)
		}
		return false
	}

	resultMap := make(map[int]*spendSelectResult, len(batch))
	for rows.Next() {
		r := &spendSelectResult{}
		if err := rows.Scan(&r.batchIdx, &r.utxoHash, &r.outputFrozen, &r.spendableIn,
			&r.coinbaseSpendingHeight, &r.txLocked, &r.txConflicting, &r.txFrozen, &r.existingSpendBytes); err != nil {
			rows.Close()
			if isDeadlock(err) {
				return true
			}
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] failed: scanning bulk SELECT results", err)
			}
			return false
		}
		resultMap[r.batchIdx] = r
	}
	rows.Close()

	// Phase 2: Validate each item in Go and build the bulk INSERT set.
	validationErrors := make(map[int]error, len(batch))
	type insertItem struct {
		batchIdx      int
		prevTxHash    []byte
		prevOutputIdx uint32
		spendingData  []byte
	}
	var toInsert []insertItem

	for i, item := range batch {
		spend := item.spend
		r, found := resultMap[i]
		if !found {
			validationErrors[i] = errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
			continue
		}

		if r.outputFrozen || r.txFrozen {
			validationErrors[i] = errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.txLocked && !item.ignoreLocked {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.txConflicting && !item.ignoreConflicting {
			validationErrors[i] = errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.spendableIn != nil && *r.spendableIn > 0 && item.blockHeight < uint32(*r.spendableIn) {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *r.spendableIn)
			continue
		}

		// Check if already spent.
		if len(r.existingSpendBytes) > 0 {
			spendingDataBytes := spend.SpendingData.Bytes()
			if !bytes.Equal(r.existingSpendBytes, spendingDataBytes) {
				existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(r.existingSpendBytes)
				if parseErr != nil {
					validationErrors[i] = errors.NewProcessingError("failed to parse existing spending data", parseErr)
					continue
				}
				validationErrors[i] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
				continue
			}
			// Idempotent re-spend: same spending data — treat as success without INSERT.
			continue
		}

		if !bytes.Equal(r.utxoHash, spend.UTXOHash[:]) {
			validationErrors[i] = errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.coinbaseSpendingHeight > 0 && r.coinbaseSpendingHeight > int64(item.blockHeight) {
			validationErrors[i] = errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d",
				spend.TxID, spend.Vout, r.coinbaseSpendingHeight, item.blockHeight)
			continue
		}

		toInsert = append(toInsert, insertItem{
			batchIdx:      i,
			prevTxHash:    spend.TxID[:],
			prevOutputIdx: spend.Vout,
			spendingData:  spend.SpendingData.Bytes(),
		})
	}

	// Phase 3: Deduplicate toInsert entries targeting the same (prevTxHash, prevOutputIdx).
	type utxoKey struct {
		hash string
		vout uint32
	}
	seenKeys := make(map[utxoKey]int, len(toInsert))
	var dedupedInsert []insertItem
	for _, u := range toInsert {
		key := utxoKey{string(u.prevTxHash), u.prevOutputIdx}
		if _, seen := seenKeys[key]; !seen {
			seenKeys[key] = u.batchIdx
			dedupedInsert = append(dedupedInsert, u)
		}
	}

	// Bulk INSERT with ON CONFLICT DO NOTHING + RETURNING for optimistic locking.
	insertedSet := make(map[int]bool)
	if len(dedupedInsert) > 0 {
		var ib strings.Builder
		ib.WriteString(`
			WITH to_insert AS (
				SELECT * FROM (VALUES `)
		insertArgs := make([]interface{}, 0, len(dedupedInsert)*4)
		pidx := 1
		for j, u := range dedupedInsert {
			if j > 0 {
				ib.WriteByte(',')
			}
			ib.WriteString(fmt.Sprintf("($%d::bytea, $%d::bigint, $%d::bytea, $%d::int)", pidx, pidx+1, pidx+2, pidx+3))
			insertArgs = append(insertArgs, u.prevTxHash, u.prevOutputIdx, u.spendingData, u.batchIdx)
			pidx += 4
		}
		ib.WriteString(`) AS v(prev_tx_hash, prev_output_idx, spending_data, batch_idx)
			),
			inserted AS (
				INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data)
				SELECT prev_tx_hash, prev_output_idx, spending_data FROM to_insert
				ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
				RETURNING prev_tx_hash, prev_output_idx
			)
			SELECT ti.batch_idx
			FROM inserted i
			JOIN to_insert ti ON ti.prev_tx_hash = i.prev_tx_hash AND ti.prev_output_idx = i.prev_output_idx`)

		iRows, err := pgxTx.Query(ctx, ib.String(), insertArgs...)
		if err != nil {
			if isDeadlock(err) {
				return true
			}
			for i, item := range batch {
				if valErr, ok := validationErrors[i]; ok {
					item.errCh <- valErr
				} else {
					item.errCh <- errors.NewStorageError("[Spend] failed: bulk INSERT spends", err)
				}
			}
			return false
		}

		for iRows.Next() {
			var bIdx int
			if err := iRows.Scan(&bIdx); err != nil {
				iRows.Close()
				if isDeadlock(err) {
					return true
				}
				for _, item := range batch {
					item.errCh <- errors.NewStorageError("[Spend] failed: scanning bulk INSERT results", err)
				}
				return false
			}
			insertedSet[bIdx] = true
		}
		iRows.Close()

		// Check for items not inserted (concurrent spend between SELECT and INSERT).
		for _, u := range dedupedInsert {
			if !insertedSet[u.batchIdx] {
				spend := batch[u.batchIdx].spend
				validationErrors[u.batchIdx] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
			}
		}
		// Mark duplicate batch entries as successful.
		for _, u := range toInsert {
			key := utxoKey{string(u.prevTxHash), u.prevOutputIdx}
			if firstIdx, ok := seenKeys[key]; ok && firstIdx != u.batchIdx {
				if insertedSet[firstIdx] {
					insertedSet[u.batchIdx] = true
				}
			}
		}
	}

	// Commit.
	if err := pgxTx.Commit(ctx); err != nil {
		if isDeadlock(err) {
			return true
		}
		for i, item := range batch {
			if valErr, ok := validationErrors[i]; ok {
				item.errCh <- valErr
			} else {
				item.errCh <- errors.NewStorageError("[Spend] failed to commit transaction", err)
			}
		}
		return false
	}

	// Signal results.
	for i, item := range batch {
		if valErr, ok := validationErrors[i]; ok {
			item.errCh <- valErr
		} else {
			item.errCh <- nil // success
		}
	}
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
