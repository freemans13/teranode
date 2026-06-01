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

// spendValidationSQL is the CTE used to validate a spend attempt and insert into
// the append-only spends table. spent_at_height is recorded for deferred DAH
// computation by Worker 2. Inline DAH stamping (dah_upd) has been removed.
//
// v3: the LEFT JOIN to spends in the validation CTE has been dropped.
// ON CONFLICT DO NOTHING enforces the "spend-once" uniqueness atomically;
// the caller detects "already spent" by seeing 0 RETURNING rows and then
// invoking diagnoseSpendFailure (the rare path).
const spendValidationSQL = `
WITH validation AS (
    SELECT o.utxo_hash, o.frozen AS output_frozen, o.spendable_in,
           o.coinbase_spending_height,
           t.locked AS tx_locked, t.conflicting AS tx_conflicting,
           t.frozen AS tx_frozen
    FROM outputs o
    JOIN txs t ON t.hash = o.tx_hash
    WHERE o.tx_hash = $1 AND o.idx = $2
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT $1, $2, $3, $5
    FROM validation v
    WHERE v.utxo_hash = $4
      AND NOT v.output_frozen AND NOT v.tx_frozen
      AND ($6 OR NOT v.tx_locked)
      AND ($7 OR NOT v.tx_conflicting)
      AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
      AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash
)
SELECT 1 FROM inserted
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
	useIgnoreUTXOHash := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreUTXOHash

	// Trusted-connect fast path: record spends by outpoint only, no validation
	// CTE and no expected-UTXO-hash. Only used for checkpoint-anchored legacy IBD
	// blocks (caller passes IgnoreUTXOHash), where the chain is canonical and the
	// tx is not extended. Leaves the validated hot path entirely untouched.
	if useIgnoreUTXOHash {
		spends, err := utxo.GetSpendsWithoutUTXOHash(tx)
		if err != nil {
			return nil, err
		}
		if len(spends) == 0 {
			return nil, errors.NewProcessingError("No spends provided", nil)
		}
		return s.spendTrusted(ctx, spends, blockHeight)
	}

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

// spendTrusted records spends by outpoint with a single append-only bulk INSERT,
// skipping the expected-UTXO-hash / frozen / locked / conflicting / coinbase
// validation entirely. It is the trusted-connect path for checkpoint-anchored
// legacy IBD: PoW + checkpoint linkage already establish the spent outputs as
// canonical, so the validation is redundant and the tx need not be extended.
// ON CONFLICT DO NOTHING keeps it idempotent across re-processing. spent_at_height
// is recorded for Worker 2's deferred DAH sweep, exactly as the validated path.
func (s *Store) spendTrusted(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) ([]*utxo.Spend, error) {
	prevTxHashes := make([][]byte, len(spends))
	prevIdxs := make([]int64, len(spends))
	spendingDatas := make([][]byte, len(spends))

	for i, sp := range spends {
		if sp == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}
		prevTxHashes[i] = sp.TxID[:]
		prevIdxs[i] = int64(sp.Vout)
		spendingDatas[i] = sp.SpendingData.Bytes()
	}

	if _, err := s.pool.Exec(ctx, trustedSpendSQL,
		prevTxHashes, prevIdxs, spendingDatas, int64(blockHeight),
	); err != nil {
		return spends, errors.NewStorageError("[Spend] trusted spend insert failed", err)
	}

	return spends, nil
}

// trustedSpendSQL is the append-only insert used by spendTrusted. Mirrors the
// INSERT in bulkSpendSQL but with no validation CTE — every supplied outpoint is
// recorded (or silently skipped if already present).
const trustedSpendSQL = `
INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
SELECT unnest($1::bytea[]), unnest($2::bigint[]), unnest($3::bytea[]), $4
ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING`

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

		// Try the atomic INSERT with validation CTE.
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			spend.TxID[:],      // $1 prev_tx_hash
			spend.Vout,         // $2 prev_output_idx
			spendingDataBytes,  // $3 spending_data
			spend.UTXOHash[:],  // $4 expected_utxo_hash
			int64(blockHeight), // $5 blockHeight (also written to spent_at_height)
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
// sendSpendBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	s.trySendSpendBatch(batch)
}

func (s *Store) trySendSpendBatch(batch []*batchSpendItem) (retryable bool) {
	ctx := context.Background()

	// Single-item fast path: use the direct validation CTE instead of bulk UNNEST.
	if len(batch) == 1 {
		item := batch[0]
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			item.spend.TxID[:], item.spend.Vout,
			item.spend.SpendingData.Bytes(), item.spend.UTXOHash[:],
			int64(item.blockHeight), item.ignoreLocked, item.ignoreConflicting,
		).Scan(&inserted)
		if err == nil {
			item.errCh <- nil
			return false
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			item.errCh <- errors.NewStorageError("[Spend] query failed for %s:%d: %v", item.spend.TxID, item.spend.Vout, err)
			return false
		}
		diagErr := s.diagnoseSpendFailure(ctx, item.spend, item.spend.SpendingData.Bytes(),
			item.blockHeight, item.ignoreLocked, item.ignoreConflicting)
		if diagErr == nil {
			item.errCh <- nil
		} else {
			item.errCh <- diagErr
		}
		return false
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to acquire connection", err)
		}
		return false
	}
	defer conn.Release()

	// Single bulk query: UNNEST parallel arrays → 3-table JOIN validation →
	// INSERT valid spends → return per-item results with diagnostic flags.
	// One parse/plan/execute for the entire batch (§6.1 multi-row INSERT).
	prevTxHashes := make([][]byte, len(batch))
	prevIdxs := make([]int64, len(batch))
	spendingDatas := make([][]byte, len(batch))
	utxoHashes := make([][]byte, len(batch))
	blockHeights := make([]int64, len(batch))
	ignLockeds := make([]bool, len(batch))
	ignConflictings := make([]bool, len(batch))
	batchIdxs := make([]int32, len(batch))

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
	)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] bulk query failed: %v", err)
		}
		return false
	}

	type bulkResult struct {
		inserted       bool
		found          bool
		outputFrozen   bool
		txFrozen       bool
		txLocked       bool
		txConflicting  bool
		utxoHashMatch  bool
		coinbaseBlock  bool
		spendableBlock bool
	}
	resultMap := make(map[int]*bulkResult, len(batch))
	for rows.Next() {
		var bIdx int32
		r := &bulkResult{}
		if err := rows.Scan(&bIdx, &r.inserted,
			&r.outputFrozen, &r.txFrozen, &r.txLocked, &r.txConflicting,
			&r.utxoHashMatch, &r.coinbaseBlock, &r.spendableBlock); err != nil {
			rows.Close()
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] scan", err)
			}
			return false
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
		// Not inserted — classify by visible validation columns first; if none of
		// them rejected the spend, the only remaining reason is "row already
		// existed in spends" (ON CONFLICT DO NOTHING fired). Fall through to
		// diagnoseSpendFailure to fetch the existing spending_data for the
		// idempotent-vs-double-spend distinction. This is the rare path.
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
			// All validation passed → must be an already-spent row. Look up the
			// existing spending_data to differentiate idempotent retry vs
			// double-spend with a different spender.
			diagErr := s.diagnoseSpendFailure(ctx, item.spend, item.spend.SpendingData.Bytes(),
				item.blockHeight, item.ignoreLocked, item.ignoreConflicting)
			if diagErr == nil {
				item.errCh <- nil // idempotent retry — same spender
			} else {
				item.errCh <- diagErr
			}
		}
	}

	return false
}

// bulkSpendSQL processes an entire batch in ONE query: UNNEST → validate → INSERT → results.
// spent_at_height is recorded per row for deferred DAH computation by Worker 2.
//
// v3: the LEFT JOIN to spends has been dropped from the validation CTE. The
// ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING on the INSERT already
// gives us "already-spent" semantics atomically — duplicate inserts are silently
// skipped, and the RETURNING set tells us which inputs successfully inserted.
// The rare "spent but with different spending_data" case (double-spend with a
// different spender) is classified after the fact by diagnoseSpendFailure,
// which still queries spends. This drops 1 JOIN per spend on the hot path.
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
           t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM items i
    JOIN outputs o ON o.tx_hash = i.prev_tx_hash AND o.idx = i.prev_idx
    JOIN txs t ON t.hash = i.prev_tx_hash
),
to_insert AS (
    SELECT prev_tx_hash, prev_idx, spending_data, block_height, batch_idx
    FROM validated
    WHERE utxo_hash = expected_utxo_hash
      AND NOT out_frozen AND NOT tx_frozen
      AND (ign_locked OR NOT tx_locked)
      AND (ign_conflicting OR NOT tx_conflicting)
      AND NOT (coinbase_spending_height > 0 AND coinbase_spending_height > block_height)
      AND NOT (COALESCE(spendable_in, 0) > 0 AND block_height < COALESCE(spendable_in, 0))
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT prev_tx_hash, prev_idx, spending_data, block_height FROM to_insert
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash, prev_output_idx
)
SELECT v.batch_idx,
       (i.prev_tx_hash IS NOT NULL) AS inserted,
       v.out_frozen, v.tx_frozen, v.tx_locked, v.tx_conflicting,
       (v.utxo_hash = v.expected_utxo_hash) AS utxo_match,
       (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > v.block_height) AS coinbase_block,
       (COALESCE(v.spendable_in, 0) > 0 AND v.block_height < COALESCE(v.spendable_in, 0)) AS spendable_block
FROM validated v
LEFT JOIN inserted i ON i.prev_tx_hash = v.prev_tx_hash AND i.prev_output_idx = v.prev_idx
ORDER BY v.batch_idx
`

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

	parentSet := make(map[chainhash.Hash]struct{}, len(spends))

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
		if spend.TxID != nil {
			parentSet[*spend.TxID] = struct{}{}
		}
	}

	// After deleting spend rows the affected parents are no longer fully spent,
	// so any deferred-DAH stamp they carry is now invalid. Clear it directly
	// here (targeted, O(unspent)) rather than relying on the Worker 2 sweep,
	// which now only enumerates bounded height ranges. This is the reorg-clear.
	if len(parentSet) > 0 {
		parentHashes := make([][]byte, 0, len(parentSet))
		for h := range parentSet {
			hb := h
			parentHashes = append(parentHashes, hb[:])
		}
		if _, err := s.pool.Exec(ctx,
			`UPDATE txs SET delete_at_height = NULL WHERE hash = ANY($1)`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to clear delete_at_height", err)
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
