package postgres

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// dahSweepWorkers bounds the per-partition sweep fan-out. Matches the prune
// delete fan-out and keeps the concurrent-transaction memory envelope modest
// (each partition sweep materialises at most `limit` candidate hashes).
const dahSweepWorkers = 8

// dahSweepMaxCandidatesPerCall caps the candidates one partition pass
// enumerates+stamps, regardless of the caller's batch budget (Worker 2 passes
// 50K, Prune passes 100K — both AS the per-partition LIMIT). The cap matters
// because the per-candidate stamp cost degrades superlinearly with batch size
// (measured: ~58µs/candidate at ~1.7K-candidate calls vs ~130µs at ~19K — giant
// bytea[] parameters pay parse/bind/plan and lose index-probe locality), and a
// single multi-second call stalls the sweep cursor, which lets the NEXT batch
// grow even bigger — a metastable collapse observed as 56K↔89K TPS oscillation
// (CV 13%) on the sustained-prune bench. Bounded calls keep per-call latency
// a few hundred ms; the existing truncation + adaptive height-step machinery
// handles "more candidates than one pass" for multi-height windows. The cap
// must stay ABOVE the per-partition single-height candidate count (sweepDAHUpTo
// advances past a truncated single-height window, orphaning the excess to the
// keyspace backstop — a pre-existing property of the design), so it is derived
// from the partition count: ~40K single-height candidates total (160K TPS at
// 250ms height ticks) spread across numPartitions.
const dahSweepMaxCandidatesTotal = 40000

var dahSweepMaxCandidatesPerCall = dahSweepMaxCandidatesTotal / numPartitions

// sweepDAHRange evaluates every parent with spend activity (spent_at_height) or
// mine activity (mined_at_height) in (fromH, toH] and bidirectionally sets
// delete_at_height: stamp completion_height+1+retention when (mined AND fully
// spent AND not preserved/unmined); clear to NULL otherwise. completion_height =
// GREATEST(max spent_at_height of its spends, mined_at_height). BRIN indexes make
// candidate enumeration touch only the recent heap ranges (both arms are bounded
// by (fromH, toH]). limit bounds candidates. The target DAH is computed once as
// new_dah in the state CTE and the UPDATE only writes when it actually changes
// (IS DISTINCT FROM), avoiding re-stamping the same value every sweep (MVCC churn).
// Reorg clears are handled directly by Unspend, so no unbounded enumeration of
// already-stamped txs is needed here.
//
// The query is pinned to index access via SET LOCAL enable_seqscan = off, scoped
// to a dedicated transaction: the BRIN selectivity estimate is unreliable (after
// ANALYZE the planner can value the height-range bitmap scan at full-table rows
// and flip the mine-activity arm — and the candidates join — to a Seq Scan of
// txs). That is catastrophic when the range sits above the highest mined height
// (matches zero rows yet scans the whole table). candidates is MATERIALIZED so
// the bounded (<= limit) set drives a PK nested loop over txs rather than a
// hash join that could re-introduce a txs seq scan.
func (s *Store) sweepDAHRange(ctx context.Context, fromH, toH int64, limit int) (int, bool, error) {
	retention := int64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, false, nil
	}

	// Fan the range sweep across partitions IN PARALLEL — the analog of the
	// partition-local cascade delete. The three tables hash-partition on the SAME
	// tx hash with the SAME modulus, so a candidate hash in txs_pNN has all its
	// spends in spends_pNN and outputs in outputs_pNN: each partition sweep is
	// self-contained, touches only its own leaves, and never contends with another
	// partition's sweep. This is what lets stamping (delete_at_height) keep pace
	// with a high concurrent create rate — under load the single-relation sweep
	// could not enumerate+stamp fully-spent+mined candidates fast enough, so the
	// table grew unbounded; 1/numPartitions-sized partition scans running
	// concurrently restore stamping throughput. Each partition is limited
	// independently, so aggregate per-call capacity scales with numPartitions.
	// Cap the per-partition batch: bounded call latency beats fewer round trips
	// (see dahSweepMaxCandidatesPerCall). Truncation against the CAPPED limit
	// preserves the no-skip invariant: a partition that filled the cap signals
	// the caller to shrink/retry exactly as it did with the raw limit.
	perPartLimit := limit
	if perPartLimit > dahSweepMaxCandidatesPerCall {
		perPartLimit = dahSweepMaxCandidatesPerCall
	}

	var total atomic.Int64
	var truncated atomic.Bool
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(dahSweepWorkers)
	for p := 0; p < numPartitions; p++ {
		p := p
		g.Go(func() error {
			stamped, candidates, err := s.sweepDAHRangePartition(gctx, p, fromH, toH, perPartLimit, retention)
			total.Add(int64(stamped))
			// A partition that enumerated `perPartLimit` candidates may have MORE in
			// this height window than it processed → the caller must not advance the
			// watermark past it; signal truncation so it shrinks the window.
			if candidates >= perPartLimit {
				truncated.Store(true)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return int(total.Load()), truncated.Load(), err
	}

	return int(total.Load()), truncated.Load(), nil
}

// sweepDAHRangePartition runs the bidirectional DAH stamp/clear over the
// candidates of ONE partition's aligned leaves (txs_pNN / spends_pNN), in two
// steps inside one transaction: enumerate candidate hashes (BRIN range scans),
// then aggregate + stamp via a bytea[] parameter so the planner always knows
// the exact candidate cardinality. The per-tx spendable output count is read
// from the txs array column out_spendables (no outputs table on this path).
func (s *Store) sweepDAHRangePartition(ctx context.Context, partIdx int, fromH, toH int64, limit int, retention int64) (stamped int, candidates int, err error) {
	txsLeaf := fmt.Sprintf("txs_p%02d", partIdx)
	spendsLeaf := fmt.Sprintf("spends_p%02d", partIdx)

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] begin %s: %v", txsLeaf, err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	if _, err := pgxTx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] set local enable_seqscan: %v", err)
	}
	// Force a fresh custom plan per sweep. pgx caches a prepared-statement plan
	// per connection; for the candidate-enumeration query the planner can lock in a
	// bad GENERIC plan when it is first prepared against cold/skewed stats (early
	// in a run, before autoanalyze has sampled the fast-growing partitions) and
	// then reuse it for the whole connection's life — which manifested as the sweep
	// non-deterministically stamping ~0 under load (same config, opposite outcome
	// run-to-run). Re-planning each call against current row estimates makes the
	// sweep reliably enumerate the live height ranges. It also lets the stamp
	// query below plan against the ACTUAL candidate array (exact cardinality), so
	// no enable_nestloop=off hack is needed to avoid the old CTE misestimate.
	if _, err := pgxTx.Exec(ctx, `SET LOCAL plan_cache_mode = force_custom_plan`); err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] set local plan_cache_mode: %v", err)
	}

	// Step 1: enumerate candidates only (BRIN height-range scans, bounded by
	// limit) and pull the hashes back to the client. Splitting enumeration from
	// stamping is a deliberate CPU fix: the previous single-statement form hid the
	// candidate cardinality inside a CTE the planner misestimated (~200 vs tens of
	// thousands), and the enable_nestloop=off guard it needed forced HASH joins
	// that re-scanned the ENTIRE spends partition on every call — measured via
	// pg_stat_statements as ~30% of total server CPU under sustained load
	// (8 partitions x ~130ms x ~2 calls/s). With the candidates passed back in as
	// a bytea[] parameter and a custom plan, the planner sees the exact array
	// cardinality and probes the spends/txs indexes per candidate instead.
	rows, err := pgxTx.Query(ctx, fmt.Sprintf(`
        SELECT hash FROM (
            SELECT DISTINCT prev_tx_hash AS hash FROM %[1]s
            WHERE spent_at_height > $1 AND spent_at_height <= $2
            UNION
            SELECT hash FROM %[2]s
            WHERE mined_at_height > $1 AND mined_at_height <= $2
        ) c
        LIMIT $3`, spendsLeaf, txsLeaf), fromH, toH, limit)
	if err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] enumerate (%d,%d] %s: %v", fromH, toH, txsLeaf, err)
	}
	candidateHashes := make([][]byte, 0, 1024)
	for rows.Next() {
		var h []byte
		if scanErr := rows.Scan(&h); scanErr != nil {
			rows.Close()
			return 0, 0, errors.NewStorageError("[dahSweep] scan candidate %s: %v", txsLeaf, scanErr)
		}
		candidateHashes = append(candidateHashes, h)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, errors.NewStorageError("[dahSweep] enumerate rows (%d,%d] %s: %v", fromH, toH, txsLeaf, err)
	}
	rows.Close()
	candidates = len(candidateHashes)

	if candidates == 0 {
		if err := pgxTx.Commit(ctx); err != nil {
			return 0, 0, errors.NewStorageError("[dahSweep] commit empty range (%d,%d] %s: %v", fromH, toH, txsLeaf, err)
		}
		return 0, 0, nil
	}

	// Step 2: aggregate + bidirectionally stamp ONLY the candidate hashes.
	// spent_count counts only spends of SPENDABLE outputs, so it is comparable to
	// the spendable-output count computed inline in state. Counting all spend rows
	// (including any recorded against a non-spendable output index) could
	// spuriously satisfy the fully-spent test and stamp DAH while a spendable
	// output is still unspent.
	tag, err := pgxTx.Exec(ctx, fmt.Sprintf(`
WITH spend_agg AS (
    SELECT s.prev_tx_hash AS hash,
           count(*) FILTER (WHERE t.out_spendables[s.prev_output_idx::int + 1] = true) AS spent_count,
           max(s.spent_at_height) AS max_spent
    FROM %[1]s s
    JOIN %[2]s t ON t.hash = s.prev_tx_hash
    WHERE s.prev_tx_hash = ANY($1::bytea[])
    GROUP BY s.prev_tx_hash
),
state AS (
    SELECT t.hash,
           CASE
               WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
               WHEN t.unmined_since IS NOT NULL THEN t.delete_at_height
               WHEN t.block_ids IS NULL OR array_length(t.block_ids, 1) IS NULL THEN t.delete_at_height
               WHEN COALESCE(sa.spent_count, 0) = COALESCE(cardinality(array_positions(t.out_spendables, true)), 0)
                    AND COALESCE(cardinality(t.out_spendables), 0) > 0
                    AND GREATEST(COALESCE(sa.max_spent, 0), COALESCE(t.mined_at_height, 0)) <= $2
                   THEN GREATEST(COALESCE(sa.max_spent, 0), COALESCE(t.mined_at_height, 0)) + 1 + $3
               ELSE NULL
           END AS new_dah
    FROM %[2]s t
    LEFT JOIN spend_agg sa ON sa.hash = t.hash
    WHERE t.hash = ANY($1::bytea[])
)
UPDATE %[2]s t SET delete_at_height = st.new_dah
FROM state st
WHERE t.hash = st.hash
  AND t.delete_at_height IS DISTINCT FROM st.new_dah`,
		spendsLeaf, txsLeaf), candidateHashes, toH, retention)
	if err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] stamp (%d,%d] %s: %v", fromH, toH, txsLeaf, err)
	}
	stamped = int(tag.RowsAffected())

	if err := pgxTx.Commit(ctx); err != nil {
		return 0, 0, errors.NewStorageError("[dahSweep] commit range (%d,%d] %s: %v", fromH, toH, txsLeaf, err)
	}

	return stamped, candidates, nil
}

// dahSweep height-step bounds for the ADAPTIVE walk in sweepDAHUpTo.
//
// The watermark must never advance past a candidate that was not stamped, or the
// candidate is orphaned to the slow keyspace backstop (this is what collapsed
// reclaim to ~0 under load — stamping, not deleting, was the wall). The naive fix
// (a tiny fixed step) is correct but catastrophic when the watermark is far behind
// safeTip over a mostly-EMPTY range — e.g. a SetBlockHeight jump to 1e6 makes a
// 4-height step iterate ~250k times. So the walk is ADAPTIVE: start with a large
// step to skip empty/sparse ranges in a few passes, and shrink (only) when a pass
// reports truncation (a partition hit `limit` candidates), which guarantees no
// candidate is skipped. It grows back after a clean pass. Semantically identical
// to one big sweep — the fully-spent test aggregates each tx's own spends
// regardless of which window enumerated it, and re-evaluation is idempotent
// (IS DISTINCT FROM).
const (
	dahSweepMaxHeightStep = 4096
	dahSweepMinHeightStep = 1
)

// sweepDAHUpTo sweeps from the persisted watermark up to toH and advances the
// watermark, in ADAPTIVELY-sized height windows so no single range sweep truncates
// at LIMIT and silently skips candidates, while empty/sparse ranges are skipped in
// O(log) passes rather than one-per-height. It runs to completion (toH); use
// sweepDAHStep for a bounded slice.
func (s *Store) sweepDAHUpTo(ctx context.Context, toH int64, limit int) (int, error) {
	return s.sweepDAH(ctx, toH, limit, 0)
}

// sweepDAHStep is the bounded variant: it advances the watermark by at most
// maxPasses windows and returns. Callers that must stay responsive (Prune
// interleaves stamping slices with delete slices in its caller's loop) use this
// so a large stamping backlog never turns one call into a multi-second monolith
// that starves deletion — the alternating 15s-sweep/10s-delete cycle is exactly
// what let the table grow unboundedly under sustained load. For small datasets
// (unit tests) one 4096-height window covers the whole range, so a single step
// behaves identically to a full sweep.
func (s *Store) sweepDAHStep(ctx context.Context, toH int64, limit, maxPasses int) (int, error) {
	return s.sweepDAH(ctx, toH, limit, maxPasses)
}

func (s *Store) sweepDAH(ctx context.Context, toH int64, limit, maxPasses int) (int, error) {
	var from int64
	if err := s.pool.QueryRow(ctx, `SELECT last_swept_height FROM dah_watermark WHERE id = 1`).Scan(&from); err != nil {
		return 0, errors.NewStorageError("[dahSweep] read watermark: %v", err)
	}

	if toH <= from {
		return 0, nil
	}

	total := 0
	passes := 0
	step := int64(dahSweepMaxHeightStep)
	for from < toH {
		if maxPasses > 0 && passes >= maxPasses {
			break
		}
		passes++
		stepTo := from + step
		if stepTo > toH {
			stepTo = toH
		}

		n, truncated, err := s.sweepDAHRange(ctx, from, stepTo, limit)
		if err != nil {
			return total, err
		}

		// A multi-height window that truncated held more candidates than one pass
		// covered — shrink and retry the SAME window WITHOUT advancing the watermark,
		// so nothing is skipped. A single-height window cannot shrink further; it is
		// advanced (one height with >limit-per-partition candidates is not a real
		// workload, and the keyspace backstop is the last-resort safety net).
		if truncated && (stepTo-from) > 1 {
			step /= 4
			if step < dahSweepMinHeightStep {
				step = dahSweepMinHeightStep
			}
			continue
		}

		total += n

		// Advance the watermark only as far as this window actually swept, so a
		// crash or cancellation never leaves a gap above the watermark. The
		// `last_swept_height < $1` guard makes the advance forward-only: if a
		// concurrent sweeper (Prune + the cursor both call this) already advanced
		// the watermark past stepTo, a lagging caller must not regress it and force
		// a redundant re-sweep. Re-sweeping is idempotent, so a no-op here is safe.
		if _, err := s.pool.Exec(ctx, `UPDATE dah_watermark SET last_swept_height = $1 WHERE id = 1 AND last_swept_height < $1`, stepTo); err != nil {
			return total, errors.NewStorageError("[dahSweep] advance watermark: %v", err)
		}
		from = stepTo

		// Grow back toward the max after a clean (non-truncated) pass so a long
		// empty stretch ahead is skipped quickly.
		if !truncated && step < dahSweepMaxHeightStep {
			step *= 2
			if step > dahSweepMaxHeightStep {
				step = dahSweepMaxHeightStep
			}
		}

		if ctx.Err() != nil {
			return total, ctx.Err()
		}
	}

	return total, nil
}

// dahSafeTip returns the highest height Worker 2 may scan: the persisted block
// height minus a safety lag, so no spend tagged in range can still be mid-commit.
func (s *Store) dahSafeTip(lag int64) int64 {
	h := int64(s.blockHeight.Load())
	if h <= lag {
		return 0
	}

	return h - lag
}

// runDAHCursor is the Worker 2 background loop: it wakes on a ticker, sweeps
// sweepDAHUpTo(dahSafeTip(lag)) in a tight inner loop until fewer than batch
// rows are touched, then sleeps until the next tick. The loop exits cleanly
// when ctx is cancelled.
func (s *postgresPrunerService) runDAHCursor(ctx context.Context) {
	cfg := s.store.settings.UtxoStore
	batch := cfg.PostgresDAHSweepBatchSize
	if batch <= 0 {
		batch = 50000
	}
	interval := time.Duration(cfg.PostgresDAHSweepIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	backstopTicker := time.NewTicker(backstopInterval)
	defer backstopTicker.Stop()
	var backstopByte int // rotates 0x00..0xff

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				n, err := s.store.sweepDAHUpTo(ctx, s.store.dahSafeTip(lag), batch)
				if err != nil {
					s.logger.Infof("[dahCursor] sweep error (retry next tick): %v", err)
					break
				}
				if n < batch {
					break
				}
				if ctx.Err() != nil {
					return
				}
			}
		case <-backstopTicker.C:
			b := backstopByte
			backstopByte = (backstopByte + 1) & 0xff
			n, err := s.store.backstopReconcile(ctx, b, b, batch)
			if err != nil {
				s.logger.Infof("[dahBackstop] slice 0x%02x error (best-effort, continuing): %v", b, err)
				continue
			}
			if n > 0 {
				s.logger.Infof("[dahBackstop] slice 0x%02x recovered %d missed tx(s)", b, n)
			}
		}
	}
}

// backstopInterval is the period between keyspace-slice ticks in the backstop
// reconciliation loop embedded in runDAHCursor (G5 guarantee-of-last-resort).
// One byte-slice is processed per tick; the full 256-byte keyspace is covered
// every 256 ticks (≈4.3 hours at the default 60s/tick).
const backstopInterval = 60 * time.Second

// backstopReconcile stamps DAH for mined, fully-spent, un-stamped txs whose hash
// leading byte is in [loByte, hiByte]. Keyspace-sliced so each run is bounded;
// the cursor rotates slices over time. Guarantee-of-last-resort (G5) against any
// enumeration gap in the height-range sweep; normally finds nothing. Stamp-only.
//
// The slice is expressed as a bytea range on the hash primary key
// (hash >= lo AND hash < hi) so the candidate scan rides the hash btree and
// touches only ~1/256 of the table per single-byte slice, rather than a full
// partition scan. txids are always exactly 32 bytes and uniformly distributed,
// so [b,0..0] <= hash < [b+1,0..0] selects exactly first-byte == b. For the
// top slice (hiByte == 0xff) the upper bound is a 33-byte sentinel
// [0xff×32, 0x00] which is lexicographically greater than any 32-byte txid,
// so the all-0xff hash is included.
func (s *Store) backstopReconcile(ctx context.Context, loByte, hiByte int, limit int) (int, error) {
	retention := int64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, nil
	}

	lo := make([]byte, 32)
	lo[0] = byte(loByte)

	var hi []byte
	if hiByte < 0xff {
		hi = make([]byte, 32)
		hi[0] = byte(hiByte + 1)
	} else {
		hi = append(bytes.Repeat([]byte{0xff}, 32), 0x00)
	}

	// Same set-based aggregation as sweepDAHRange: take a bounded hash-range
	// slice of stamp-candidates (cheap row predicates + LIMIT, riding the hash
	// btree), aggregate spends over just that slice and compute spendable count
	// from the txs.out_spendables array, then stamp the ones that are fully spent.
	tag, err := s.pool.Exec(ctx, `
		WITH slice AS (
			SELECT t.hash, t.mined_at_height, t.out_spendables,
			       COALESCE(cardinality(t.out_spendables), 0) AS total_out,
			       COALESCE(cardinality(array_positions(t.out_spendables, true)), 0) AS spendable_out
			FROM txs t
			WHERE t.hash >= $1 AND t.hash < $2
			  AND t.delete_at_height IS NULL
			  AND t.preserve_until IS NULL
			  AND t.unmined_since IS NULL
			  AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) IS NOT NULL
			LIMIT $3
		),
		spend_agg AS (
			-- Count only spends of SPENDABLE outputs (see sweepDAHRangePartition),
			-- so spent_count is comparable to slice.spendable_out below.
			SELECT s.prev_tx_hash AS hash,
			       count(*) FILTER (WHERE sl.out_spendables[s.prev_output_idx::int + 1] = true) AS spent_count,
			       max(s.spent_at_height) AS max_spent
			FROM spends s JOIN slice sl ON sl.hash = s.prev_tx_hash
			GROUP BY s.prev_tx_hash
		),
		eligible AS (
			SELECT sl.hash,
			       GREATEST(COALESCE(sa.max_spent, 0), COALESCE(sl.mined_at_height, 0)) AS completion_height
			FROM slice sl
			LEFT JOIN spend_agg sa ON sa.hash = sl.hash
			WHERE sl.total_out > 0
			  AND sl.spendable_out = COALESCE(sa.spent_count, 0)
		)
		UPDATE txs t SET delete_at_height = e.completion_height + 1 + $4
		FROM eligible e WHERE t.hash = e.hash`,
		lo, hi, limit, retention)
	if err != nil {
		return 0, errors.NewStorageError("[dahBackstop] [%d,%d]: %v", loByte, hiByte, err)
	}
	return int(tag.RowsAffected()), nil
}

// RewindDAHWatermark moves the sweep watermark BACK to forkHeight so that a reorg
// causes (forkHeight, tip] to be re-swept: the new chain's spends are tagged at
// heights the watermark may already have passed, and must be re-evaluated. The
// guard last_swept_height > forkHeight ensures the watermark is never advanced
// forward by this call (it only ever rewinds).
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE dah_watermark SET last_swept_height = $1 WHERE id = 1 AND last_swept_height > $1`,
		forkHeight,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark to %d: %v", forkHeight, err)
	}

	return nil
}
