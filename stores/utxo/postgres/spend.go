package postgres

import (
	"bytes"
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
// SQL — targets parent tables; postgres prunes partitions itself given the
// hash-keyed WHERE clause.
//
// $8 = newDAH (blockHeight+1+retention). Pass 0 to no-op the DAH branch.
// ---------------------------------------------------------------------------

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
),
inserted AS (
    -- ON CONFLICT silently skips a duplicate (prev_tx_hash, prev_output_idx).
    -- Combined with the validation CTE's existing_spend IS NULL predicate,
    -- this is defense-in-depth against any race that bypasses within-shard
    -- worker serialization.
    INSERT INTO spends (prev_tx_hash, partition_key, prev_output_idx, spending_data)
    SELECT $1, get_byte($1, 1) % 8, $2, $3
    FROM validation v
    WHERE v.existing_spend IS NULL
      AND v.utxo_hash = $4
      AND NOT v.output_frozen AND NOT v.tx_frozen
      AND ($6 OR NOT v.tx_locked)
      AND ($7 OR NOT v.tx_conflicting)
      AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
      AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
    ON CONFLICT (prev_tx_hash, prev_output_idx, partition_key) DO NOTHING
    RETURNING prev_tx_hash
),
dah_upd AS (
    UPDATE txs t SET delete_at_height = $8
    FROM inserted i
    WHERE $8 > 0
      AND t.hash = i.prev_tx_hash
      AND t.preserve_until IS NULL
      AND t.unmined_since IS NULL
      AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) > 0
      AND (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash) + 1
          = (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash)
      AND (t.delete_at_height IS NULL OR t.delete_at_height < $8)
    RETURNING 1
)
SELECT 1 FROM inserted
`

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

	if s.workersStarted() {
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
		item := &batchSpendItem{
			spend:             spend,
			blockHeight:       blockHeight,
			errCh:             errCh,
			ignoreConflicting: ignoreConflicting,
			ignoreLocked:      ignoreLocked,
		}
		rk := Route(spend.TxID)
		s.spendSlots[rk.Shard].input <- item
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
				s.logger.Errorf("error in postgres unspend (rollback): %v", unspendErr)
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
		return spends, errors.NewUtxoError("error in postgres spend - errors", spendErrors)
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

		// Atomic INSERT with validation CTE on parent tables — postgres
		// prunes the partition itself from `o.tx_hash = $1` and friends.
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			spend.TxID[:],      // $1 prev_tx_hash
			spend.Vout,         // $2 prev_output_idx
			spendingDataBytes,  // $3 spending_data
			spend.UTXOHash[:],  // $4 expected_utxo_hash
			int64(blockHeight), // $5 blockHeight
			ignoreLocked,       // $6 ignoreLocked
			ignoreConflicting,  // $7 ignoreConflicting
			s.newDAHOrZero(),   // $8 newDAH for dah_upd CTE
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
				s.logger.Errorf("error in postgres unspend (rollback): %v", unspendErr)
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
		return spends, errors.NewUtxoError("error in postgres spend - errors", spendErrors)
	}

	return spends, nil
}

// ---------------------------------------------------------------------------
// runSpendBatch — per-shard Spend worker callback. The worker holds a
// pgxpool connection for life and dispatches bulk Spend queries on the
// parent tables; postgres prunes per-partition itself from the
// hash-keyed JOIN/WHERE clauses. All items in `batch` were routed by
// prev_tx_hash to this shard, so the planner can reuse the cached plan.
// ---------------------------------------------------------------------------
func (s *Store) runSpendBatch(conn *pgxpool.Conn, batch []*batchSpendItem) {
	ctx := context.Background()
	newDAH := s.newDAHOrZero()

	// Single-item fast path: direct validation CTE.
	if len(batch) == 1 {
		item := batch[0]
		var inserted int
		err := conn.QueryRow(ctx, spendValidationSQL,
			item.spend.TxID[:], item.spend.Vout,
			item.spend.SpendingData.Bytes(), item.spend.UTXOHash[:],
			int64(item.blockHeight), item.ignoreLocked, item.ignoreConflicting,
			newDAH,
		).Scan(&inserted)
		if err == nil {
			item.errCh <- nil
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			item.errCh <- errors.NewStorageError("[Spend] query failed for %s:%d: %v", item.spend.TxID, item.spend.Vout, err)
			return
		}
		diagErr := s.diagnoseSpendFailureOnConn(ctx, conn, item.spend, item.spend.SpendingData.Bytes(),
			item.blockHeight, item.ignoreLocked, item.ignoreConflicting)
		if diagErr == nil {
			item.errCh <- nil
		} else {
			item.errCh <- diagErr
		}
		return
	}

	n := len(batch)
	prevTxHashes := make([][]byte, n)
	prevIdxs := make([]int64, n)
	spendingDatas := make([][]byte, n)
	utxoHashes := make([][]byte, n)
	blockHeights := make([]int64, n)
	ignLockeds := make([]bool, n)
	ignConflictings := make([]bool, n)
	batchIdxs := make([]int32, n)
	for i, item := range batch {
		prevTxHashes[i] = item.spend.TxID[:]
		prevIdxs[i] = int64(item.spend.Vout)
		spendingDatas[i] = item.spend.SpendingData.Bytes()
		utxoHashes[i] = item.spend.UTXOHash[:]
		blockHeights[i] = int64(item.blockHeight)
		ignLockeds[i] = item.ignoreLocked
		ignConflictings[i] = item.ignoreConflicting
		batchIdxs[i] = int32(i)
	}

	rows, err := conn.Query(ctx, bulkSpendSQL,
		prevTxHashes, prevIdxs, spendingDatas, utxoHashes,
		blockHeights, ignLockeds, ignConflictings, batchIdxs,
		newDAH,
	)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] bulk query failed: %v", err)
		}
		return
	}

	type bulkResult struct {
		inserted       bool
		found          bool
		existingSpend  []byte
		outputFrozen   bool
		txFrozen       bool
		txLocked       bool
		txConflicting  bool
		utxoHashMatch  bool
		coinbaseBlock  bool
		spendableBlock bool
	}
	resultMap := make(map[int]*bulkResult, n)
	for rows.Next() {
		var bIdx int32
		r := &bulkResult{}
		if err := rows.Scan(&bIdx, &r.inserted, &r.existingSpend,
			&r.outputFrozen, &r.txFrozen, &r.txLocked, &r.txConflicting,
			&r.utxoHashMatch, &r.coinbaseBlock, &r.spendableBlock); err != nil {
			rows.Close()
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] scan: %v", err)
			}
			return
		}
		r.found = true
		resultMap[int(bIdx)] = r
	}
	rows.Close()

	for i, item := range batch {
		spend := item.spend
		r, found := resultMap[i]
		if !found {
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
			continue
		}
		if r.inserted {
			item.errCh <- nil
			continue
		}
		if len(r.existingSpend) > 0 {
			spendingDataBytes := spend.SpendingData.Bytes()
			if bytes.Equal(r.existingSpend, spendingDataBytes) {
				item.errCh <- nil
				continue
			}
			existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(r.existingSpend)
			if parseErr != nil {
				item.errCh <- errors.NewProcessingError("failed to parse existing spending data", parseErr)
				continue
			}
			item.errCh <- errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
			continue
		}
		if r.outputFrozen || r.txFrozen {
			item.errCh <- errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
		} else if r.txLocked && !item.ignoreLocked {
			item.errCh <- errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
		} else if r.txConflicting && !item.ignoreConflicting {
			item.errCh <- errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
		} else if !r.utxoHashMatch {
			item.errCh <- errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
		} else if r.coinbaseBlock {
			item.errCh <- errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready for %s:%d", spend.TxID, spend.Vout)
		} else if r.spendableBlock {
			item.errCh <- errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable yet", spend.TxID, spend.Vout)
		} else {
			item.errCh <- errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
		}
	}
}

// bulkSpendSQL — bulk validation+insert against parent tables. The
// FROM/JOIN/INSERT/UPDATE references hit `outputs`, `txs`, and `spends`
// directly; postgres prunes the relevant partitions itself from the
// hash-keyed predicates.
//
// $9 = newDAH (blockHeight+1+retention). Pass 0 to no-op the DAH UPDATE.
const bulkSpendSQL = `
WITH items AS (
    SELECT unnest($1::bytea[])   AS prev_tx_hash,
           unnest($2::bigint[])  AS prev_idx,
           unnest($3::bytea[])   AS spending_data,
           unnest($4::bytea[])   AS expected_utxo_hash,
           unnest($5::bigint[])  AS block_height,
           unnest($6::boolean[]) AS ign_locked,
           unnest($7::boolean[]) AS ign_conflicting,
           unnest($8::int[])     AS batch_idx
),
validated AS (
    SELECT i.batch_idx, i.prev_tx_hash, i.prev_idx, i.spending_data,
           i.expected_utxo_hash, i.block_height, i.ign_locked, i.ign_conflicting,
           o.utxo_hash, o.frozen AS out_frozen, o.spendable_in,
           o.coinbase_spending_height,
           t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen,
           sp.spending_data AS existing_spend
    FROM items i
    JOIN outputs o ON o.tx_hash = i.prev_tx_hash AND o.idx = i.prev_idx
    JOIN txs t ON t.hash = i.prev_tx_hash
    LEFT JOIN spends sp ON sp.prev_tx_hash = i.prev_tx_hash AND sp.prev_output_idx = i.prev_idx
),
to_insert AS (
    SELECT prev_tx_hash, prev_idx, spending_data, batch_idx
    FROM validated
    WHERE existing_spend IS NULL
      AND utxo_hash = expected_utxo_hash
      AND NOT out_frozen AND NOT tx_frozen
      AND (ign_locked OR NOT tx_locked)
      AND (ign_conflicting OR NOT tx_conflicting)
      AND NOT (coinbase_spending_height > 0 AND coinbase_spending_height > block_height)
      AND NOT (COALESCE(spendable_in, 0) > 0 AND block_height < COALESCE(spendable_in, 0))
),
inserted AS (
    -- See note in spendValidationSQL — same defense-in-depth rationale.
    INSERT INTO spends (prev_tx_hash, partition_key, prev_output_idx, spending_data)
    SELECT prev_tx_hash, get_byte(prev_tx_hash, 1) % 8, prev_idx, spending_data FROM to_insert
    ON CONFLICT (prev_tx_hash, prev_output_idx, partition_key) DO NOTHING
    RETURNING prev_tx_hash, prev_output_idx
),
parents AS (
    SELECT prev_tx_hash AS tx_hash, count(*) AS spent_in_batch
    FROM inserted GROUP BY prev_tx_hash
),
dah_upd AS (
    UPDATE txs t SET delete_at_height = $9
    FROM parents p
    WHERE $9 > 0
      AND t.hash = p.tx_hash
      AND t.preserve_until IS NULL
      AND t.unmined_since IS NULL
      AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) > 0
      AND (SELECT count(*) FROM spends s WHERE s.prev_tx_hash = t.hash) + p.spent_in_batch
          = (SELECT count(*) FROM outputs o WHERE o.tx_hash = t.hash)
      AND (t.delete_at_height IS NULL OR t.delete_at_height < $9)
    RETURNING 1
)
SELECT v.batch_idx,
       (i.prev_tx_hash IS NOT NULL) AS inserted,
       v.existing_spend,
       v.out_frozen, v.tx_frozen, v.tx_locked, v.tx_conflicting,
       (v.utxo_hash = v.expected_utxo_hash) AS utxo_match,
       (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > v.block_height) AS coinbase_block,
       (COALESCE(v.spendable_in, 0) > 0 AND v.block_height < COALESCE(v.spendable_in, 0)) AS spendable_block
FROM validated v
LEFT JOIN inserted i ON i.prev_tx_hash = v.prev_tx_hash AND i.prev_output_idx = v.prev_idx
ORDER BY v.batch_idx
`

// diagnoseSpendFailure queries the output + txs + spends to determine
// why a spend INSERT failed. Pool-acquiring path used by spendDirect when
// no worker is available.
func (s *Store) diagnoseSpendFailure(ctx context.Context, spend *utxo.Spend, spendingDataBytes []byte,
	blockHeight uint32, ignoreLocked, ignoreConflicting bool) error {

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return errors.NewStorageError("[Spend] failed to acquire connection for diagnostic", err)
	}
	defer conn.Release()
	return s.diagnoseSpendFailureOnConn(ctx, conn, spend, spendingDataBytes,
		blockHeight, ignoreLocked, ignoreConflicting)
}

// diagnoseSpendFailureOnConn is the worker variant of diagnoseSpendFailure
// that uses a held connection rather than s.pool.QueryRow.
func (s *Store) diagnoseSpendFailureOnConn(ctx context.Context, conn *pgxpool.Conn,
	spend *utxo.Spend, spendingDataBytes []byte, blockHeight uint32, ignoreLocked, ignoreConflicting bool) error {

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

	row := conn.QueryRow(ctx, spendDiagnosticSQL, spend.TxID[:], spend.Vout)
	err := row.Scan(&utxoHashBytes, &outputFrozen, &spendableIn,
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

// newDAHOrZero returns the delete_at_height to assign to a tx that becomes
// fully spent by the current batch, or 0 when retention is disabled. The
// spend-side SQL CTEs use a `$N > 0` guard to no-op when this is zero.
func (s *Store) newDAHOrZero() int64 {
	if s.settings == nil {
		return 0
	}
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		return 0
	}
	return int64(s.blockHeight.Load() + 1 + retention)
}
