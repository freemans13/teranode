package postgres

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// dahReconcileCursorDDL is the per-partition rotating cursor for the bounded
// reconciliation backstop. One row per hash partition holds the hash of the last
// tx audited in that partition, so each reconcile pass resumes from there and
// rotates through the whole partition over many passes WITHOUT ever scanning all
// history in a single run. Created unconditionally in createSchemaInternal.
const dahReconcileCursorDDL = `
CREATE TABLE IF NOT EXISTS dah_reconcile_cursor (
    partition   INT   PRIMARY KEY,
    last_hash   BYTEA
);`

// reconcileSpentProgressPartition runs one BOUNDED reconciliation pass over a
// single hash partition. It is the audit backstop for the MAINTAINED Setter-C
// spent_progress counter: the forward-only fold (dah_sweep_batch) can drift the
// counter (arithmetic bug, lost update, or a reorg RewindDAHWatermark re-fold that
// double-counts still-present spends) and, unlike the old v10 sweep that
// re-derived full-spentness every pass, it can never self-correct. This pass
// restores that self-healing property, bounded so it never costs O(all history).
//
// Bounding: it selects at most `slice` txs from the partition, resuming from a
// per-partition rotating cursor (dah_reconcile_cursor.last_hash) ordered by hash,
// so successive passes rotate through the whole partition a slice at a time. When
// a pass returns fewer than `slice` rows it has reached the partition's end and
// the cursor wraps to the start. Per-tx recompute is O(that tx's spends), so the
// whole pass is O(slice + spends of those slice txs) — never the full chain.
//
// For each tx in the slice it recomputes the TRUE values directly from the spends
// table using the SAME spendable predicate the fold uses
// (prev_output_idx < out_count AND get_bit(out_spendables, prev_output_idx)=1):
//
//   - true_progress   = count of that tx's spendable-output spends
//   - true_last_spend = max(spent_at_height) over those spends
//
// Where the stored counter differs it CORRECTS it, and — mirroring the fold's
// stamp — if the correction drives spent_progress = spendable_count on a mined,
// not-yet-stamped, unpreserved, mined (unmined_since IS NULL) tx it also stamps
// delete_at_height = GREATEST(true_last_spend, mined_at_height)+1+retention and
// upserts it into pending_deletes (catching a previously-missed completion). It
// respects the same mined/preserve/unmined gates as the fold, so an unmined
// fully-spent tx is corrected but left unstamped (the mine path owns that stamp).
//
// It also UN-STAMPS in the reverse direction (defense-in-depth): a tx carrying a
// delete_at_height while it is NOT actually fully spent (true_progress <
// spendable_count) has that premature/stale stamp CLEARED and is removed from
// pending_deletes — otherwise the pruner (which deletes purely on the stamp) would
// delete a tx with a live UTXO. Legitimate not-fully-spent stamps are preserved:
// conflicting (double-spend loser) and preserved (preserve_until) txs are excluded,
// as is the zero-spendable case (spendable_count = 0).
//
// It is authoritative for the reorg-rewind range: because it RECOMPUTES from the
// spends table rather than adding a delta, a doubled counter is set back to the
// true count in one pass regardless of how the drift arose.
//
// safeTip bounds spends to spent_at_height <= safeTip, matching the fold's safe
// window (no spend still mid-commit is counted). retention is the same block-height
// retention the fold uses. Returns the number of rows whose stored values were
// corrected (drift found). The pass runs in one autocommit statement per
// partition; it is a background/maintenance path (never on the hot path).
func (s *Store) reconcileSpentProgressPartition(ctx context.Context, partition int, safeTip int64, retention int32, slice int) (int64, error) {
	if slice <= 0 {
		slice = 1000
	}

	suffix := fmt.Sprintf("%02d", partition)

	// Resume from the per-partition cursor (NULL/absent → start of partition).
	var cursor []byte
	_ = s.maint().QueryRow(ctx,
		`SELECT last_hash FROM dah_reconcile_cursor WHERE partition = $1`, partition).Scan(&cursor)

	// One statement:
	//  1. `slice_txs` picks up to `slice` candidate txs from THIS partition,
	//     hash > cursor (rotating), ordered by hash (stable, index-friendly on PK).
	//  2. `truth` recomputes true_progress + true_last_spend from that partition's
	//     spends for exactly those candidate hashes (O(their spends)).
	//  3. `upd` corrects only the DRIFTED rows (stored != true), and stamps inline
	//     when the correction completes a mined, unstamped, unpreserved tx.
	//  4. `ins` mirrors freshly-stamped completions into pending_deletes.
	//  5. the final SELECT returns (corrected_count, max_hash_seen) so Go can both
	//     report drift and advance/wrap the cursor.
	//
	// The spendable predicate mirrors the fold exactly. max_h is NULL when a tx has
	// no spendable spends → true_last_spend stays NULL and true_progress = 0.
	query := fmt.Sprintf(`
		WITH slice_txs AS (
			SELECT hash, spendable_count, mined_at_height, delete_at_height,
			       spent_progress, last_spend_height, preserve_until, unmined_since,
			       out_count, out_spendables, conflicting
			  FROM txs_p%[1]s
			 WHERE ($1::bytea IS NULL OR hash > $1::bytea)
			 ORDER BY hash
			 LIMIT $2
		),
		truth AS (
			SELECT c.hash,
			       COALESCE(count(s.*) FILTER (
			           WHERE s.prev_output_idx < c.out_count
			             AND get_bit(c.out_spendables, s.prev_output_idx) = 1
			       ), 0) AS true_progress,
			       max(s.spent_at_height) FILTER (
			           WHERE s.prev_output_idx < c.out_count
			             AND get_bit(c.out_spendables, s.prev_output_idx) = 1
			       ) AS true_last_spend
			  FROM slice_txs c
			  LEFT JOIN spends_p%[1]s s
			    ON s.prev_tx_hash = c.hash
			   AND s.spent_at_height <= $3
			 GROUP BY c.hash
		),
		drift AS (
			SELECT c.hash, c.spendable_count, c.mined_at_height, c.delete_at_height,
			       c.preserve_until, c.unmined_since, c.conflicting,
			       t.true_progress, t.true_last_spend
			  FROM slice_txs c
			  JOIN truth t ON t.hash = c.hash
			 WHERE c.spent_progress IS DISTINCT FROM t.true_progress
			    OR c.last_spend_height IS DISTINCT FROM t.true_last_spend
			    -- Stamped-but-not-fully-spent: a premature/stale delete_at_height left
			    -- behind by residual drift (a stamp taken from a transiently-inflated
			    -- counter that was later corrected down). The counter may already be
			    -- correct, so this is NOT caught by the drift predicates above. Exclude
			    -- the legitimate not-fully-spent stamps: conflicting losers and preserved
			    -- txs. Zero-spendable (spendable_count = 0) is excluded by the > 0 guard.
			    OR (c.delete_at_height IS NOT NULL
			        AND t.true_progress < c.spendable_count
			        AND c.spendable_count > 0
			        AND NOT c.conflicting
			        AND c.preserve_until IS NULL
			        AND c.unmined_since IS NULL)
		),
		upd AS (
			UPDATE txs_p%[1]s t
			   SET spent_progress    = d.true_progress,
			       last_spend_height = d.true_last_spend,
			       delete_at_height  = CASE
			           WHEN d.true_progress = d.spendable_count
			                AND d.spendable_count > 0
			                AND d.mined_at_height IS NOT NULL
			                AND d.delete_at_height IS NULL
			                AND d.preserve_until IS NULL
			                AND d.unmined_since IS NULL
			           THEN (GREATEST(COALESCE(d.true_last_spend, 0), d.mined_at_height) + 1 + $4)::int
			           -- Un-stamp a premature/stale stamp on a genuinely not-fully-spent,
			           -- non-conflicting, unpreserved, mined tx (defense-in-depth). Leaving
			           -- it would let the pruner delete a tx with a live UTXO.
			           WHEN d.delete_at_height IS NOT NULL
			                AND d.true_progress < d.spendable_count
			                AND d.spendable_count > 0
			                AND NOT d.conflicting
			                AND d.preserve_until IS NULL
			                AND d.unmined_since IS NULL
			           THEN NULL
			           ELSE d.delete_at_height
			       END
			  FROM drift d
			 WHERE t.hash = d.hash
			RETURNING t.hash, t.delete_at_height, t.spent_progress, t.spendable_count
		),
		ins AS (
			INSERT INTO pending_deletes_p%[1]s (hash, delete_at_height)
			SELECT hash, delete_at_height FROM upd
			 WHERE delete_at_height IS NOT NULL AND spent_progress = spendable_count
			ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
		),
		del AS (
			DELETE FROM pending_deletes_p%[1]s pd
			 USING upd u
			 WHERE pd.hash = u.hash AND u.delete_at_height IS NULL
		)
		SELECT (SELECT count(*) FROM upd),
		       (SELECT min(hash) FROM upd),
		       (SELECT max(hash) FROM slice_txs),
		       (SELECT count(*) FROM slice_txs)
	`, suffix)

	var corrected int64
	var sampleCorrectedHash []byte
	var maxHash []byte
	var seen int64
	if err := s.maint().QueryRow(ctx, query, cursor, slice, safeTip, retention).
		Scan(&corrected, &sampleCorrectedHash, &maxHash, &seen); err != nil {
		return 0, errors.NewStorageError("[dahReconcile] partition %d audit pass", partition, err)
	}

	// Advance the cursor. If fewer than `slice` rows were seen the partition is
	// exhausted → wrap to the start (NULL) so the next pass re-scans from the top.
	var next []byte
	if seen >= int64(slice) && maxHash != nil {
		next = maxHash
	} // else next stays nil → wrap

	if _, err := s.maint().Exec(ctx,
		`INSERT INTO dah_reconcile_cursor (partition, last_hash) VALUES ($1, $2)
		 ON CONFLICT (partition) DO UPDATE SET last_hash = EXCLUDED.last_hash`,
		partition, next,
	); err != nil {
		return corrected, errors.NewStorageError("[dahReconcile] partition %d advance cursor", partition, err)
	}

	if corrected > 0 {
		// Log the drift with a small sample (one actually-corrected hash) so a persistent
		// counter bug is visible in ops without dumping every row.
		sample := ""
		if sampleCorrectedHash != nil {
			sample = hex.EncodeToString(sampleCorrectedHash)
		}
		s.logger.Warnf("[dahReconcile] partition %d corrected %d drifted spent_progress row(s) (slice=%d, sample hash≈%s)",
			partition, corrected, slice, sample)
		prometheusDAHReconcileCorrected.Add(float64(corrected))
	}

	return corrected, nil
}

// reconcileAllPartitionsOnce runs one bounded reconciliation pass across every
// partition (rotating cursor per partition) and returns the total rows corrected.
// It is the maintenance-loop entry point; per-partition errors are logged and
// skipped (best-effort backstop, never fatal).
func (s *Store) reconcileAllPartitionsOnce(ctx context.Context, safeTip int64, retention int32, slice int) int64 {
	var total int64

	for p := 0; p < numPartitions; p++ {
		if ctx.Err() != nil {
			return total
		}

		n, err := s.reconcileSpentProgressPartition(ctx, p, safeTip, retention, slice)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Infof("[dahReconcile] partition %d pass error (retry next tick): %v", p, err)
			}

			continue
		}

		total += n
	}

	return total
}

// runDAHReconcile is the background reconciliation loop. It is intentionally SLOW
// and OFF the hot path: one bounded slice per partition per tick, on a long idle
// interval, so it rotates through all history over time at negligible cost while
// restoring the self-healing the maintained counter otherwise lacks. Driven off
// the same pruner service lifecycle as the sweep cursor.
func (s *postgresPrunerService) runDAHReconcile(ctx context.Context) {
	cfg := s.store.settings.UtxoStore

	interval := time.Duration(cfg.PostgresDAHReconcileIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 60 * time.Second
	}

	slice := cfg.PostgresDAHReconcileSlice
	if slice <= 0 {
		slice = 1000
	}

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	s.store.logger.Infof("[dahReconcile] backstop active (slice=%d interval=%s)", slice, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			safeTip := s.store.dahSafeTip(lag)
			if safeTip <= 0 {
				continue
			}

			corrected := s.store.reconcileAllPartitionsOnce(ctx, safeTip, retention, slice)
			if corrected > 0 {
				s.store.logger.Warnf("[dahReconcile] pass corrected %d drifted spent_progress row(s)", corrected)
			}
		}
	}
}
