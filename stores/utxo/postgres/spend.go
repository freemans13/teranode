package postgres

import (
	"bytes"
	"context"
	"math"
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

// voutToInt32 guards the uint32 → INT4 narrowing at the point a vout enters a
// SQL binding: spends.prev_output_idx is an INT4 column, and a protocol-valid
// transaction can never carry 2^31 outputs, so a vout above math.MaxInt32 is
// corrupt input and must be rejected with a typed error rather than failing
// deep inside pgx parameter encoding.
func voutToInt32(vout uint32) (int32, error) {
	if vout > math.MaxInt32 {
		return 0, errors.NewProcessingError("vout %d exceeds int32 range for INT prev_output_idx", vout)
	}
	return int32(vout), nil
}

// blockHeightToInt32 guards the uint32 → INT4 narrowing for block heights, mirroring
// voutToInt32. Heights land in INT4 columns (spent_at_height, mined_at_height,
// unmined_since, block_heights, delete_at_height); a height above MaxInt32 is far
// beyond any real chain (millennia out) but must be rejected at the write entry
// points rather than silently wrapping negative inside pgx encoding. Validating once
// at the entry makes the downstream int32(height) casts provably safe.
func blockHeightToInt32(height uint32) (int32, error) {
	if height > math.MaxInt32 {
		return 0, errors.NewProcessingError("blockHeight %d exceeds int32 range for INT height columns", height)
	}
	return int32(height), nil
}

// ---------------------------------------------------------------------------
// Direct-mode SQL (used when batcher is not active)
// ---------------------------------------------------------------------------

// spendValidationSQL is the CTE used to validate a spend attempt and insert into
// the append-only spends table. spent_at_height is recorded for deferred DAH
// computation by Worker 2.
//
// Packed form: per-output access is O(1) byte arithmetic — utxo_hash is a
// 32-byte substr at offset $2*32 (substr is 1-based), frozen/spendable flags
// are get_bit() bitmap probes, coinbase_spending_height is a scalar. The WHERE
// clause requires $2 < t.out_count, so a missing tx OR an OOB index both
// produce 0 RETURNING rows — diagnosed by diagnoseSpendFailure. get_bit is
// only reached when $2 < out_count (bitmap is sized to out_count bits rounded
// up), so it can never raise an out-of-range error.
const spendValidationSQL = `
WITH validation AS (
    SELECT
        substr(t.utxo_hashes, $2::int * 32 + 1, 32) AS utxo_hash,
        (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) AS output_frozen,
        -- An output is only spendable if its out_spendables bit is set. A non-spendable
        -- output (e.g. OP_RETURN) still has a utxo_hash stored, so the hash match alone
        -- is not enough to authorise a spend — without this gate a caller presenting the
        -- correct hash of an OP_RETURN output could insert a spend row. Matches the
        -- aerospike (UTXO_NOT_FOUND) and sql (no output row) stores.
        (t.out_spendables IS NOT NULL AND get_bit(t.out_spendables, $2::int) = 1) AS output_spendable,
        t.coinbase_spending_height AS coinbase_spending_height,
        CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END AS spendable_in,
        t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM txs t
    WHERE t.hash = $1 AND $2::int < t.out_count
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT $1, $2, $3, $5 FROM validation v
    WHERE v.utxo_hash = $4 AND v.output_spendable AND NOT v.output_frozen AND NOT v.tx_frozen
      AND ($6 OR NOT v.tx_locked) AND ($7 OR NOT v.tx_conflicting)
      -- coinbase_spending_height is the FIRST height at which the coinbase output is
      -- spendable (inclusive): a spend at exactly that height is allowed, so the
      -- immaturity test is strictly greater-than. Do not change to >=.
      AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
      AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash
)
SELECT 1 FROM inserted
`

// spendDiagnosticSQL re-queries when the validation INSERT returned 0 rows so
// we can determine the exact reason the spend failed.
//
// Packed form: reads the flat per-output columns on txs. Returns NOT FOUND
// (0 rows) when the tx is missing OR the index is OOB — both are TxNotFound.
const spendDiagnosticSQL = `
SELECT
    substr(t.utxo_hashes, $2::int * 32 + 1, 32) AS utxo_hash,
    (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) AS output_frozen,
    (t.out_spendables IS NOT NULL AND get_bit(t.out_spendables, $2::int) = 1) AS output_spendable,
    CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END AS spendable_in,
    t.coinbase_spending_height AS coinbase_spending_height,
    t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen,
    sp.spending_data AS existing_spend
FROM txs t
LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
WHERE t.hash = $1 AND $2::int < t.out_count
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
	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return nil, err
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

	// prev_output_idx is INT4 — reject out-of-range vouts before any spend is
	// attempted, so a corrupt input can never partially apply a batch.
	for _, spend := range spends {
		if spend == nil {
			continue
		}
		if _, err := voutToInt32(spend.Vout); err != nil {
			return nil, err
		}
	}

	if s.spendBatcher != nil {
		return s.spendBatched(ctx, tx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting)
	}

	return s.spendDirect(ctx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting)
}

// ---------------------------------------------------------------------------
// spendBatched — enqueue each input into the batcher
// ---------------------------------------------------------------------------

// spendBatched enqueues each input into the spend batcher and waits for the
// per-input result. The batcher runs the DB write on context.Background() (see
// sendSpendBatch), so cancelling the request ctx only stops US WAITING — a spend the
// batcher has already committed stays committed. That is intentional and matches the
// sql and aerospike stores: a committed spend is rolled back ONLY for a genuine
// validation failure (see needsSpendRollback), never for cancellation or timeout,
// because a spend is idempotent for the same spender — a retry re-issues the same
// spending_data and the ON CONFLICT DO NOTHING in spendValidationSQL makes it a
// no-op. Rolling back on cancellation would diverge from the other stores and break
// that idempotent-retry contract.
//
// The wait is bounded by SpendWaitTimeout (parity with sql/aerospike) so a stalled
// batcher cannot block a caller whose ctx carries no deadline.
func (s *Store) spendBatched(ctx context.Context, tx *bt.Tx, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	// Bound the wait so a stalled batcher cannot hang a deadline-less caller. This
	// bounds only OUR wait; the batch still completes on context.Background().
	spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
	if spendTimeout <= 0 {
		spendTimeout = 30 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, spendTimeout)
	defer cancel()

	// Enqueue each spend into the batcher and wait for results. PutCtx (not Put) links
	// the request span onto the batch span for tracing, matching sql/aerospike.
	errChs := make([]chan error, len(spends))
	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}
		errCh := make(chan error, 1)
		errChs[idx] = errCh
		s.spendBatcher.PutCtx(ctx, &batchSpendItem{
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
		case <-waitCtx.Done():
			// Distinguish our timeout from an upstream cancellation, but in BOTH cases
			// fall through without rolling back any already-committed spend (see doc).
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				spends[idx].Err = errors.NewServiceUnavailableError("[Spend] batch operation timed out after %s for %s:%d", spendTimeout, spend.TxID, spend.Vout)
			} else {
				spends[idx].Err = errors.NewContextCanceledError("[Spend] context cancelled for %s:%d", spend.TxID, spend.Vout)
			}
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
		// Roll back already-committed spends ONLY for genuine validation failures.
		// Cancellation/timeout errors deliberately fall through without a rollback —
		// the committed spend is idempotent on retry (see the spendBatched doc). This
		// matches the sql and aerospike stores.
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
			int32(spend.Vout),  // $2 prev_output_idx (INT4; range-checked in Spend)
			spendingDataBytes,  // $3 spending_data
			spend.UTXOHash[:],  // $4 expected_utxo_hash
			int32(blockHeight), // $5 blockHeight (also written to INT4 spent_at_height)
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
	s.batchStats.spendItems.Add(int64(len(batch)))
	s.batchStats.spendBatches.Add(1)
	// Reject later same-outpoint spend claims with different spending data in
	// memory before the batch reaches the DB, exactly as the aerospike
	// (sendSpendBatchLua) and sql stores do. Two concurrent spends of the same
	// UTXO that coalesce into one batch otherwise both map to the single
	// ON CONFLICT-inserted row in bulkSpendSQL's result LEFT JOIN and both
	// return success — a double-spend. Filtering here gives first-seen
	// exactly-one-error semantics and parity with the aerospike store; it is a
	// no-op for batches of distinct outpoints (the hot path) and preserves
	// idempotent retries (identical spending data is kept).
	batch = utxo.FilterConflictingDuplicateSpendClaims(batch,
		func(item *batchSpendItem) *utxo.Spend {
			if item == nil {
				return nil
			}
			return item.spend
		},
		func(item *batchSpendItem, err error) {
			item.errCh <- err
		},
	)
	if len(batch) == 0 {
		return
	}

	s.trySendSpendBatch(batch)
}

func (s *Store) trySendSpendBatch(batch []*batchSpendItem) (retryable bool) {
	ctx := context.Background()

	// Single-item fast path: use the direct validation CTE instead of bulk UNNEST.
	if len(batch) == 1 {
		item := batch[0]
		var inserted int
		err := s.pool.QueryRow(ctx, spendValidationSQL,
			item.spend.TxID[:], int32(item.spend.Vout),
			item.spend.SpendingData.Bytes(), item.spend.UTXOHash[:],
			int32(item.blockHeight), item.ignoreLocked, item.ignoreConflicting,
		).Scan(&inserted)
		if err == nil {
			item.errCh <- nil
			return false
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			item.errCh <- errors.NewStorageError("[Spend] query failed for %s:%d", item.spend.TxID, item.spend.Vout, err)
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
	prevIdxs := make([]int32, len(batch)) // prev_output_idx is INT4 (vout range-checked in Spend)
	spendingDatas := make([][]byte, len(batch))
	utxoHashes := make([][]byte, len(batch))
	blockHeights := make([]int32, len(batch)) // spent_at_height is INT4
	ignLockeds := make([]bool, len(batch))
	ignConflictings := make([]bool, len(batch))
	batchIdxs := make([]int32, len(batch))

	for i, item := range batch {
		prevTxHashes[i] = item.spend.TxID[:]
		prevIdxs[i] = int32(item.spend.Vout)
		spendingDatas[i] = item.spend.SpendingData.Bytes()
		utxoHashes[i] = item.spend.UTXOHash[:]
		blockHeights[i] = int32(item.blockHeight)
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
			item.errCh <- errors.NewStorageError("[Spend] bulk query failed", err)
		}
		return false
	}

	type bulkResult struct {
		inserted        bool
		found           bool
		outputFrozen    bool
		txFrozen        bool
		txLocked        bool
		txConflicting   bool
		utxoHashMatch   bool
		coinbaseBlock   bool
		spendableBlock  bool
		slotExists      bool // true when the array subscript is in-bounds
		outputSpendable bool // false for non-spendable outputs (e.g. OP_RETURN)
	}
	resultMap := make(map[int]*bulkResult, len(batch))
	for rows.Next() {
		var bIdx int32
		r := &bulkResult{}
		if err := rows.Scan(&bIdx, &r.inserted,
			&r.outputFrozen, &r.txFrozen, &r.txLocked, &r.txConflicting,
			&r.utxoHashMatch, &r.coinbaseBlock, &r.spendableBlock, &r.slotExists, &r.outputSpendable); err != nil {
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

	// A mid-stream error (statement_timeout, server reset, network drop) makes
	// rows.Next() stop early with the error parked in rows.Err(). Without this
	// check the partially-filled resultMap would dispatch TxNotFound to every
	// not-yet-received item — silently turning a transient DB failure into a false
	// "output not found" success. Surface it as a storage error to all items
	// instead (the caller decides whether to retry), matching the scan-error path.
	if err := rows.Err(); err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] bulk rows iteration", err)
		}
		return false
	}

	for i, item := range batch {
		spend := item.spend
		r, found := resultMap[i]

		if !found {
			// Tx hash not found in txs table.
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
			continue
		}
		if !r.slotExists {
			// Tx found but output index is OOB — treat as not found (no such output).
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found (index OOB)", spend.TxID, spend.Vout)
			continue
		}
		if !r.outputSpendable {
			// Output exists but is not a spendable UTXO (e.g. OP_RETURN). Treat as
			// not found, matching the aerospike/sql stores.
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found (not a spendable UTXO)", spend.TxID, spend.Vout)
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
// Packed form: per-output access is O(1) byte arithmetic on the txs row. A tx
// with a missing hash produces no row in validated (JOIN miss → no result row
// for that batch_idx). An OOB index produces a validated row but utxo_hash IS
// NULL (slot_exists=false). The CASE guards on prev_idx < out_count are
// load-bearing: get_bit() ERRORS on an out-of-range bit index (unlike an array
// subscript, which returns NULL), and substr() past the end returns an EMPTY
// bytea (not NULL) which would corrupt the slot_exists classification. The
// result dispatch handles these cases:
//   - no row in resultMap → TxNotFound (tx missing)
//   - row exists, slot_exists=false → TxNotFound (OOB index)
//   - row exists, !utxo_match → UtxoHashMismatch (wrong UTXO hash)
const bulkSpendSQL = `
WITH items AS (
    SELECT unnest($1::bytea[])   AS prev_tx_hash,
           unnest($2::int[])     AS prev_idx,
           unnest($3::bytea[])   AS spending_data,
           unnest($4::bytea[])   AS expected_utxo_hash,
           unnest($5::int[])     AS block_height,
           unnest($6::boolean[]) AS ign_locked,
           unnest($7::boolean[]) AS ign_conflicting,
           unnest($8::int[])     AS batch_idx
),
validated AS (
    SELECT i.batch_idx, i.prev_tx_hash, i.prev_idx, i.spending_data, i.expected_utxo_hash,
           i.block_height, i.ign_locked, i.ign_conflicting,
           CASE WHEN i.prev_idx::int < t.out_count THEN substr(t.utxo_hashes, i.prev_idx::int * 32 + 1, 32) END AS utxo_hash,
           CASE WHEN i.prev_idx::int < t.out_count AND t.out_frozens IS NOT NULL THEN get_bit(t.out_frozens, i.prev_idx::int) = 1 ELSE false END AS out_frozen,
           -- Spendable bit: a non-spendable output (OP_RETURN) carries a utxo_hash but
           -- must not be spendable. See spendValidationSQL for the rationale.
           CASE WHEN i.prev_idx::int < t.out_count AND t.out_spendables IS NOT NULL THEN get_bit(t.out_spendables, i.prev_idx::int) = 1 ELSE false END AS out_spendable,
           CASE WHEN array_length(t.spendable_ins,1) >= i.prev_idx::int+1 THEN t.spendable_ins[i.prev_idx::int+1] END AS spendable_in,
           t.coinbase_spending_height AS coinbase_spending_height,
           t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM items i JOIN txs t ON t.hash = i.prev_tx_hash
),
to_insert AS (
    SELECT prev_tx_hash, prev_idx, spending_data, block_height, batch_idx FROM validated
    WHERE utxo_hash = expected_utxo_hash AND out_spendable AND NOT out_frozen AND NOT tx_frozen
      AND (ign_locked OR NOT tx_locked) AND (ign_conflicting OR NOT tx_conflicting)
      -- coinbase_spending_height is the FIRST spendable height (inclusive): a spend
      -- at exactly that height is valid, so immaturity is strictly >. Do not use >=.
      AND NOT (coinbase_spending_height > 0 AND coinbase_spending_height > block_height)
      AND NOT (COALESCE(spendable_in,0) > 0 AND block_height < COALESCE(spendable_in,0))
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT prev_tx_hash, prev_idx, spending_data, block_height FROM to_insert
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash, prev_output_idx
)
SELECT v.batch_idx, (i.prev_tx_hash IS NOT NULL) AS inserted,
       v.out_frozen, v.tx_frozen, v.tx_locked, v.tx_conflicting,
       (v.utxo_hash IS NOT NULL AND v.utxo_hash = v.expected_utxo_hash) AS utxo_match,
       (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > v.block_height) AS coinbase_block,
       (COALESCE(v.spendable_in,0) > 0 AND v.block_height < COALESCE(v.spendable_in,0)) AS spendable_block,
       (v.utxo_hash IS NOT NULL) AS slot_exists,
       v.out_spendable AS output_spendable
FROM validated v LEFT JOIN inserted i ON i.prev_tx_hash = v.prev_tx_hash AND i.prev_output_idx = v.prev_idx
ORDER BY v.batch_idx
`

// diagnoseSpendFailure queries the output + txs + spends to determine
// why a spend INSERT failed.
func (s *Store) diagnoseSpendFailure(ctx context.Context, spend *utxo.Spend, spendingDataBytes []byte,
	blockHeight uint32, ignoreLocked, ignoreConflicting bool) error {

	var (
		utxoHashBytes          []byte
		outputFrozen           bool
		outputSpendable        bool
		spendableIn            *int32
		coinbaseSpendingHeight int64
		txLocked               bool
		txConflicting          bool
		txFrozen               bool
		existingSpendBytes     []byte
	)

	err := s.pool.QueryRow(ctx, spendDiagnosticSQL,
		spend.TxID[:],     // $1
		int32(spend.Vout), // $2 (INT4; range-checked at the Spend entry point)
	).Scan(&utxoHashBytes, &outputFrozen, &outputSpendable, &spendableIn,
		&coinbaseSpendingHeight, &txLocked, &txConflicting, &txFrozen, &existingSpendBytes)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
		}
		return errors.NewStorageError("[Spend] diagnostic query failed for %s:%d", spend.TxID, spend.Vout, err)
	}

	// Check existing spend (double-spend or idempotent). Honour a recorded spend
	// regardless of the spendable bit so an already-spent UTXO is still reported.
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

	// Non-spendable output (e.g. OP_RETURN): exists with a utxo_hash but is not a
	// UTXO, so it can never be spent. Report not-found, matching aerospike/sql.
	if !outputSpendable {
		return errors.NewTxNotFoundError("output %s:%d not found (not a spendable UTXO)", spend.TxID, spend.Vout)
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

	// We reach here only when the INSERT recorded 0 rows yet the diagnostic read
	// finds a spendable, unfrozen, unlocked, non-conflicting, hash-matching, mature
	// output with NO existing spend row. The only way to produce that is the
	// ON CONFLICT having fired against a row that a concurrent Unspend then deleted
	// before this read (separate pool connection, separate snapshot). The UTXO is
	// therefore back to unspent and the caller's retry will re-insert cleanly — so
	// this is an idempotent no-op, NOT a hard error. Returning a StorageError here
	// (the old behaviour) spuriously failed a valid retry and, in a batch, left
	// already-committed sibling inputs un-rolled-back (StorageError is not in
	// needsSpendRollback). Treat it as success.
	return nil
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

// Unspend reverses a previous spend by deleting the owning spend rows and clearing
// the now-invalid deferred-prune stamp on the affected parents — all in ONE
// transaction so a reorg never observes a torn state (some outputs unspent while the
// parent still carries a stale delete_at_height the pruner could act on).
//
// Ownership (matches the aerospike and sql stores): a spend row is removed only when
// the caller's SpendingData token equals the stored spender. A non-owning caller — a
// stale reorg record whose output has since been re-spent by a different tx — is a
// no-op for that row (it must NOT wipe the live spender), but its parent still takes
// part in the DAH housekeeping below. SpendingData is therefore mandatory: a nil
// token is a hard error rather than an unconditional delete.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	// SpendingData is the ownership token — reject up front rather than risk
	// deleting a spend we do not own. Vout must also fit the INT4
	// prev_output_idx column (separate entry point from Spend).
	for _, spend := range spends {
		if spend == nil {
			continue
		}
		if spend.SpendingData == nil {
			return errors.NewProcessingError("[Unspend] SpendingData (ownership token) required for %s:%d", spend.TxID, spend.Vout)
		}
		if _, err := voutToInt32(spend.Vout); err != nil {
			return err
		}
	}

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[Unspend] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	parentSet := make(map[chainhash.Hash]struct{}, len(spends))

	for _, spend := range spends {
		if spend == nil {
			continue
		}
		// Ownership-checked delete: the spending_data predicate ensures only this
		// caller's spend row is removed. A mismatching (non-owning) caller deletes
		// 0 rows — intentional — but still drives the parent's DAH housekeeping.
		if _, err := pgxTx.Exec(ctx,
			`DELETE FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = $2 AND spending_data = $3`,
			spend.TxID[:], int32(spend.Vout), spend.SpendingData.Bytes(),
		); err != nil {
			return errors.NewStorageError("[Unspend] failed for %s:%d", spend.TxID, spend.Vout, err)
		}
		if spend.TxID != nil {
			parentSet[*spend.TxID] = struct{}{}
		}
	}

	// Affected parents are no longer fully spent, so any deferred-DAH stamp they
	// carry is now invalid. Clear it in the SAME transaction as the spend deletion
	// (the reorg-clear) so the pruner can never see a deleted-spend / stale-DAH
	// torn state. Housekeeping runs for every affected parent, including the
	// non-owning no-op deletes above.
	if len(parentSet) > 0 {
		parentHashes := make([][]byte, 0, len(parentSet))
		for h := range parentSet {
			hb := h
			parentHashes = append(parentHashes, hb[:])
		}
		if _, err := pgxTx.Exec(ctx,
			`UPDATE txs SET delete_at_height = NULL WHERE hash = ANY($1)`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to clear delete_at_height", err)
		}

		// C6: remove the parent hashes from the pending_deletes side-table in the same
		// pgxTx so the clear is atomic with the DAH null above. A revived (unspent)
		// parent is no longer a prune candidate; leaving it in the list would let the
		// pruner wrongly delete it after the retention period. DELETE WHERE hash = ANY($1)
		// is a harmless no-op for hashes not in the table.
		if _, err := pgxTx.Exec(ctx,
			`DELETE FROM pending_deletes WHERE hash = ANY($1)`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to delete pending_deletes (C6)", err)
		}

		// If requested, lock the parents within the same transaction. SetLocked(true)
		// semantics also clear DAH (already cleared above); keeping it in-tx preserves
		// all-or-nothing with the spend reversal.
		if len(flagAsLocked) > 0 && flagAsLocked[0] {
			if _, err := pgxTx.Exec(ctx,
				`UPDATE txs SET locked = true, delete_at_height = NULL WHERE hash = ANY($1)`, parentHashes,
			); err != nil {
				return errors.NewStorageError("[Unspend] failed to lock parent txs", err)
			}
		}
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[Unspend] commit", err)
	}

	return nil
}
