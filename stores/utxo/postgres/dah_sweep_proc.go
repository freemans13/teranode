package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
)

// dahSweepProcVersion is bumped whenever dahSweepProcDDL changes. bootstrap
// recreates the procedure whenever the version stored in dah_sweep_control
// differs (in EITHER direction, so a binary rollback reinstalls the older body).
//
// Bumped 9→10: row-targeted batching (LIMIT batch_rows on the stamped set,
// loop-until-short-pass, single watermark advance) replaces the v_step height-window loop.
//
// Bumped 10→11: fold-forward consumer. v10's fatal flaw was that every CALL
// re-aggregated each candidate parent's ENTIRE spend history (the spend_agg CTE
// joined spends_pNN over the whole (watermark, safe_tip] range) — O(lifetime
// spends), which grows unbounded with the chain and cannot keep up with IBD,
// freezing the watermark. v11 folds only NEW spends, once, forward-only: per
// bounded height band it aggregates ONLY that band's spends, increments each
// parent's spent_progress counter, and stamps delete_at_height when the counter
// reaches spendable_count. Cost per band is O(new spends in band), independent of
// chain size, so the watermark always advances.
//
// Bumped 11→12: merge the per-band FOLD and STAMP into a SINGLE UPDATE ...
// RETURNING. v11 wrote the hot txs table TWICE per band — once to fold
// (spent_progress += n, last_spend_height) and again to stamp delete_at_height —
// and scanned the band twice. For txs that complete on the folded spend this
// doubled txs dead-tuple churn on the hottest table (~15% measured throughput
// regression). v12 stamps INLINE from the NEW spent_progress in the same UPDATE,
// so each folded tx is written once and the band is scanned once. Semantics are
// identical: same rows stamped, same delete_at_height values, same pending_deletes
// upserts.
const dahSweepProcVersion = 12

// dahSweepControlDDL is the kill-switch / tunables / proc-version / per-CALL
// outcome table. Created unconditionally in createSchemaInternal (plain DDL);
// the procedure itself is bootstrapped separately and only in proc mode.
const dahSweepControlDDL = `
CREATE TABLE IF NOT EXISTS dah_sweep_control (
    id                   INT  PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled              BOOL        NOT NULL DEFAULT TRUE,
    proc_version         INT         NOT NULL DEFAULT 0,
    batch_rows           INT         NOT NULL DEFAULT 5000,
    max_windows_per_call INT         NOT NULL DEFAULT 32,
    band_heights         INT         NOT NULL DEFAULT 5000,
    last_called_at       TIMESTAMPTZ,
    last_rows_stamped    BIGINT      NOT NULL DEFAULT 0,
    last_candidates_seen BIGINT      NOT NULL DEFAULT 0,
    last_watermark       BIGINT      NOT NULL DEFAULT 0,
    total_rows_stamped   BIGINT      NOT NULL DEFAULT 0,
    last_caught_up       BOOL        NOT NULL DEFAULT FALSE,
    last_backlog         BIGINT      NOT NULL DEFAULT 0,
    last_hit_budget      BOOL        NOT NULL DEFAULT FALSE,
    last_lock_contended  BOOL        NOT NULL DEFAULT FALSE,
    last_error           TEXT
);
-- band_heights was added in proc v11 (fold-forward). Idempotent ALTER so existing
-- DBs get the column without a manual migration.
ALTER TABLE dah_sweep_control ADD COLUMN IF NOT EXISTS band_heights INT NOT NULL DEFAULT 5000;`

// dahSweepProcDDL returns the server-side self-committing DAH stamp procedure DDL.
// It reproduces the exact DAH semantics of the in-process Go sweep
// (sweepDAHRangePartition) but commits per fully-drained height window, needs no
// statement_timeout, and is driven by a thin adaptive ticker via CALL. See
// docs/superpowers/dah-stored-proc-proposal.md for the full design.
//
// The procedure always upserts each stamped row's (hash, delete_at_height) into the
// per-leaf pending_deletes_pNN table inside the same EXECUTE block, keeping the
// side-table — the only pruner path — in sync without extra round-trips.
func dahSweepProcDDL() string {
	return dahSweepProcDDLWithPendingDeletes
}

// dahSweepProcDDLWithPendingDeletes is the fold-forward (v12) DAH sweep procedure.
//
// v11 replaced v10's full-range re-aggregation with a forward-only fold. v12
// merges v11's two per-band txs writes (fold + stamp) into one UPDATE ... RETURNING
// so the hot txs table is written once per folded tx and the band is scanned once.
// Per bounded height band (v_band heights, from dah_sweep_control.band_heights) it:
//
//  1. FOLDS the band's NEW spends ONLY — aggregates spends_pNN over
//     (v_from, v_to] grouped by prev_tx_hash, counting SPENDABLE-output spends,
//     and increments each parent's spent_progress by that count and advances
//     last_spend_height. This reads only the band's rows (O(new spends in band)),
//     NEVER a parent's whole spend history — the key change that lets the sweep
//     keep up with IBD.
//  2. STAMPS, in the SAME UPDATE, the parents whose fold just completed by
//     computing delete_at_height inline from the NEW progress (t.spent_progress +
//     d.n = spendable_count > 0), mined_at_height IS NOT NULL, delete_at_height IS
//     NULL, preserve_until IS NULL, unmined_since IS NULL. delete_at_height =
//     GREATEST(new last_spend_height, mined_at_height) + 1 + retention. The freshly
//     stamped rows (RETURNING delete_at_height IS NOT NULL AND spent_progress =
//     spendable_count) are upserted into pending_deletes_pNN in the same statement.
//     A parent stamped in a PRIOR band that is folded again keeps its old
//     delete_at_height (delete_at_height IS NULL guard in the CASE) and now has
//     spent_progress > spendable_count, so it is excluded from the pending_deletes
//     feed — no double-insert.
//  3. ADVANCES dah_part_watermark to v_to and COMMITs — fold + stamp + advance are
//     ONE transaction per band, so a torn commit never double-folds.
//
// The loop runs at most v_max_bands (dah_sweep_control.max_windows_per_call) bands
// per CALL, so per-CALL work is bounded; the background driver re-fires while the
// backlog is positive.
//
// Division of labour: a tx spent-before-mined has its spent_progress folded here
// but is NOT stamped (mined gate) — the mine path (SetMinedMulti) stamps it on
// mine, evaluating fully-spent directly from spends. The sweep only stamps the
// mined-then-spent completion. A late reorg-driven re-fold is handled by
// RewindDAHWatermark, which also resets spent_progress/last_spend_height for the
// re-swept range (Task 8).
const dahSweepProcDDLWithPendingDeletes = `CREATE OR REPLACE PROCEDURE dah_sweep_batch(
    p_partition  INT,
    p_safe_tip   BIGINT,
    p_retention  INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_leaf_suffix    TEXT := lpad(p_partition::text, 2, '0');
    v_from           BIGINT;
    v_to             BIGINT;
    v_band           INT;
    v_max_bands      INT;
    v_enabled        BOOLEAN;
    v_n              BIGINT;
    v_total_stamped  BIGINT := 0;
    v_bands_done     INT := 0;
BEGIN
    SELECT enabled, band_heights, max_windows_per_call
      INTO v_enabled, v_band, v_max_bands
      FROM dah_sweep_control WHERE id = 1;

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    IF v_band IS NULL OR v_band <= 0 THEN
        v_band := 5000;
    END IF;

    IF v_max_bands IS NULL OR v_max_bands <= 0 THEN
        v_max_bands := 8;
    END IF;

    IF current_setting('server_version_num')::int < 110000 THEN
        RAISE EXCEPTION 'dah_sweep_batch requires PostgreSQL 11+';
    END IF;

    SELECT last_swept_height INTO v_from
      FROM dah_part_watermark WHERE partition = p_partition;

    IF v_from IS NULL OR v_from >= p_safe_tip THEN
        COMMIT;
        RETURN;
    END IF;

    -- Fold forward one bounded band at a time. Each iteration processes ONLY the
    -- band's new spends (O(new spends in band)) and commits atomically, so the
    -- watermark always advances and per-CALL work is capped at v_max_bands bands.
    WHILE v_from < p_safe_tip AND v_bands_done < v_max_bands LOOP
        SELECT enabled INTO v_enabled FROM dah_sweep_control WHERE id = 1;
        EXIT WHEN NOT v_enabled;

        IF NOT pg_try_advisory_xact_lock(20240684 + p_partition) THEN
            EXIT;
        END IF;

        v_to := LEAST(v_from + v_band, p_safe_tip);

        SET LOCAL lock_timeout = '5s';

        -- (1+2) FOLD + STAMP in ONE UPDATE: read ONLY this band's spends, increment
        -- spent_progress and advance last_spend_height on each parent, and — in the
        -- SAME write — stamp delete_at_height inline for parents whose NEW progress
        -- (t.spent_progress + d.n) just reached spendable_count. This writes the hot
        -- txs table once per folded tx and scans the band once. Spendable-output test
        -- mirrors the v10 semantics (prev_output_idx < out_count AND get_bit=1).
        --
        -- The stamp uses the NEW last_spend_height inline
        -- (GREATEST(COALESCE(t.last_spend_height,0), d.max_h)) because t.last_spend_height
        -- in the UPDATE expression evaluates against the PRE-update row. The
        -- delete_at_height IS NULL guard keeps a prior-band-stamped row's stamp; if
        -- such a row is folded again its progress goes ABOVE spendable_count, so it is
        -- excluded from the pending_deletes feed (= spendable_count) — no double-insert.
        EXECUTE format($q$
            WITH band_agg AS (
                SELECT s.prev_tx_hash AS hash,
                       count(*) FILTER (
                           WHERE CASE WHEN s.prev_output_idx < t.out_count
                                      THEN get_bit(t.out_spendables, s.prev_output_idx) = 1
                                      ELSE false END
                       ) AS n,
                       max(s.spent_at_height) AS max_h
                FROM spends_p%1$s s
                JOIN txs_p%1$s t ON t.hash = s.prev_tx_hash
                WHERE s.spent_at_height > $1 AND s.spent_at_height <= $2
                GROUP BY s.prev_tx_hash
            ),
            upd AS (
                UPDATE txs_p%1$s t
                   SET spent_progress    = t.spent_progress + d.n,
                       last_spend_height = GREATEST(COALESCE(t.last_spend_height, 0), d.max_h),
                       delete_at_height  = CASE
                           WHEN t.spent_progress + d.n = t.spendable_count
                                AND t.spendable_count > 0
                                AND t.mined_at_height IS NOT NULL
                                AND t.delete_at_height IS NULL
                                AND t.preserve_until IS NULL
                                AND t.unmined_since IS NULL
                           THEN (GREATEST(GREATEST(COALESCE(t.last_spend_height, 0), d.max_h),
                                          t.mined_at_height) + 1 + $3)::int
                           ELSE t.delete_at_height
                       END
                  FROM band_agg d
                 WHERE t.hash = d.hash
                   AND d.n > 0
                RETURNING t.hash, t.delete_at_height, t.spent_progress, t.spendable_count
            ),
            ins AS (
                INSERT INTO pending_deletes_p%1$s (hash, delete_at_height)
                SELECT hash, delete_at_height FROM upd
                 WHERE delete_at_height IS NOT NULL AND spent_progress = spendable_count
                ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
            )
            SELECT count(*) FROM upd
             WHERE delete_at_height IS NOT NULL AND spent_progress = spendable_count
        $q$, v_leaf_suffix)
        USING v_from, v_to, p_retention
        INTO v_n;

        -- (3) ADVANCE the watermark to the band end (forward-only guard), then
        -- COMMIT: fold + stamp + advance are one transaction for this band.
        UPDATE dah_part_watermark
           SET last_swept_height = v_to
         WHERE partition = p_partition AND last_swept_height < v_to;

        v_total_stamped := v_total_stamped + v_n;
        COMMIT;

        v_from := v_to;
        v_bands_done := v_bands_done + 1;
    END LOOP;

    UPDATE dah_sweep_control
       SET last_called_at = now(),
           total_rows_stamped = total_rows_stamped + v_total_stamped,
           last_rows_stamped = v_total_stamped
     WHERE id = 1;
    COMMIT;
END;
$$;`

// bootstrapDAHSweepProc seeds the control-row knobs from settings and installs
// dah_sweep_batch() when the stored proc_version differs from dahSweepProcVersion.
// The procedure is the ONLY DAH sweep mechanism (there is no in-process fallback),
// so it is mandatory: a missing CREATE privilege (SQLSTATE 42501) or a postgres
// older than 11 (COMMIT-inside-PROCEDURE) is returned as an error and fails store
// startup, surfacing the deployment problem instead of silently not pruning.
func (s *Store) bootstrapDAHSweepProc(ctx context.Context) (err error) {
	// PG version guard: COMMIT inside a PROCEDURE requires PostgreSQL 11+.
	var verNum int
	if err = s.pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&verNum); err != nil {
		return errors.NewStorageError("[dahSweep] read server_version_num", err)
	}

	if verNum < 110000 {
		return errors.NewStorageError("[dahSweep] postgres server_version_num=%d < 110000; the DAH sweep procedure requires PostgreSQL 11+ (COMMIT inside a procedure)", verNum)
	}

	// Seed tunable knobs from settings into the control row so ops tuning flows
	// through settings and the proc reads a single source of truth.
	batchRows := s.settings.UtxoStore.PostgresDAHSweepBatchRows
	if batchRows <= 0 {
		batchRows = 5000
	}

	maxWin := s.settings.UtxoStore.PostgresDAHSweepMaxWindowsPerCall
	if maxWin <= 0 {
		maxWin = 32
	}

	// band_heights bounds the per-band fold width (proc v11). Seeded from settings
	// so ops tuning flows through the same single source of truth as the other knobs.
	bandHeights := s.settings.UtxoStore.PostgresDAHSweepBandHeights
	if bandHeights <= 0 {
		bandHeights = 5000
	}

	if _, err = s.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET batch_rows = $1, max_windows_per_call = $2, band_heights = $3 WHERE id = 1`,
		batchRows, maxWin, bandHeights,
	); err != nil {
		if isInsufficientPrivilege(err) {
			return errors.NewStorageError("[dahSweep] app role lacks privilege to write dah_sweep_control (42501); grant it so the DAH sweep can run")
		}

		return errors.NewStorageError("[dahSweep] seed control knobs", err)
	}

	// Recreate the procedure only when the stored version differs (either direction).
	var storedVersion int
	if err = s.pool.QueryRow(ctx, `SELECT proc_version FROM dah_sweep_control WHERE id = 1`).Scan(&storedVersion); err != nil {
		return errors.NewStorageError("[dahSweep] read proc_version", err)
	}

	if storedVersion == dahSweepProcVersion {
		return nil
	}

	// Drop the legacy single-CALL (BIGINT, INT) signature from the v1 era. The v2
	// procedure has a different signature (INT, BIGINT, INT), so CREATE OR REPLACE
	// would otherwise leave the old overload behind, unused. Best-effort.
	_, _ = s.pool.Exec(ctx, `DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT)`)

	if _, err = s.pool.Exec(ctx, dahSweepProcDDL()); err != nil {
		if isInsufficientPrivilege(err) {
			return errors.NewStorageError("[dahSweep] app role lacks CREATE privilege for the dah_sweep_batch procedure (42501); grant CREATE so the DAH sweep can run")
		}

		return errors.NewStorageError("[dahSweep] create procedure", err)
	}

	if _, err = s.pool.Exec(ctx, `UPDATE dah_sweep_control SET proc_version = $1 WHERE id = 1`, dahSweepProcVersion); err != nil {
		return errors.NewStorageError("[dahSweep] record proc_version", err)
	}

	s.logger.Infof("[dahSweep] installed dah_sweep_batch procedure version %d", dahSweepProcVersion)

	return nil
}

// isInsufficientPrivilege reports whether err is a postgres insufficient_privilege
// (SQLSTATE 42501). Checked on the raw driver error before any teranode wrapping.
func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == usql.PgErrInsufficientPriv
}

// runDAHCursorProc drives the server-side dah_sweep_batch() procedure. Each tick
// it fans one CALL per partition across the 8 partitions IN PARALLEL (recovering
// the parallelism a single sequential-8-partition CALL lost on a cold-cache disk)
// via sweepAllPartitionsOnce; each CALL drains row-bounded batches (up to batch_rows
// stamps per pass), committing per pass. Cadence is driven by the real watermark lag
// (min across partitions): backlog>0 → drain hard (call again immediately), else idle.
//
// The spent-before-mined orphan gap (previously covered by the O(table) keyspace
// backstop) is now closed by the mine-time stamp (S6) in SetMinedMulti, so no
// backstop timer is needed here.
//
// A startup smoke CALL verifies COMMIT-inside-CALL is not suppressed by pgx
// middleware (2D000); there is no Go fallback, so it just logs the root cause.
func (s *postgresPrunerService) runDAHCursorProc(ctx context.Context) {
	cfg := s.store.settings.UtxoStore

	interval := time.Duration(cfg.PostgresDAHSweepIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}

	idleInterval := time.Duration(cfg.PostgresDAHSweepIdleIntervalMillis) * time.Millisecond
	if idleInterval <= 0 {
		idleInterval = 5 * time.Second
	}

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	// Smoke CALL (partition 0, negative safe_tip → immediate no-op): verify
	// COMMIT-inside-CALL works. A wrapping transaction from pgx middleware raises
	// 2D000. No Go fallback exists; log the root cause loudly.
	smokeCtx, smokeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, smokeErr := s.store.maint().Exec(smokeCtx, `CALL dah_sweep_batch($1, $2, $3)`, 0, int64(-1), int32(0))
	smokeCancel()

	if ctx.Err() != nil {
		return
	}

	if smokeErr != nil {
		var pgErr *pgconn.PgError
		if errors.As(smokeErr, &pgErr) && pgErr.Code == "2D000" {
			s.store.logger.Errorf("[dahCursor] COMMIT-in-CALL blocked by middleware (2D000): the DAH sweep cannot commit; CALL must run in autocommit (no wrapping transaction/tracer)")
		} else {
			s.store.logger.Errorf("[dahCursor] proc smoke CALL failed: %v", smokeErr)
		}
	}

	sweepConcurrency := cfg.PostgresDAHSweepConcurrency
	if sweepConcurrency <= 0 {
		sweepConcurrency = 1
	}

	s.logger.Infof("[dahCursor] proc driver active (%d partitions, concurrency=%d; interval=%s idle=%s)", numPartitions, sweepConcurrency, interval, idleInterval)

	sweepTimer := time.NewTimer(0) // fire the first sweep immediately
	defer sweepTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-sweepTimer.C:
			safeTip := s.store.dahSafeTip(lag)
			if safeTip <= 0 {
				sweepTimer.Reset(idleInterval)
				continue
			}

			start := time.Now()
			stamped := s.store.sweepAllPartitionsOnce(ctx, safeTip, retention)
			elapsed := time.Since(start)

			if ctx.Err() != nil {
				return
			}

			// Cadence from the real watermark lag (min across partitions), not a
			// per-CALL flag: backlog>0 → drain hard immediately, else idle poll.
			backlog := s.store.dahWatermarkBacklog(ctx, safeTip)
			prometheusDAHSweepCallDuration.Observe(elapsed.Seconds())
			prometheusDAHSweepRowsStamped.Add(float64(stamped))
			prometheusDAHSweepWatermarkLag.Set(float64(backlog))

			next := idleInterval
			if backlog > 0 {
				next = 0
			}

			if stamped > 0 || backlog > 0 {
				s.logger.Infof("[dahCursor] proc stamped=%d backlog=%d next=%s elapsed=%s",
					stamped, backlog, next.Truncate(time.Millisecond), elapsed.Truncate(time.Millisecond))
			}

			sweepTimer.Reset(next)
		}
	}
}

// sweepAllPartitionsOnce fires one dah_sweep_batch() CALL per partition IN
// PARALLEL and returns the rows stamped this pass (delta of the control row's
// cumulative counter). Each CALL drains row-bounded batches (up to batch_rows
// stamps per pass), committing per pass, looping until a pass stamps fewer than
// batch_rows; a cancelled CALL loses at most one uncommitted pass and resumes
// from the per-partition watermark next pass — the watermark only advances when
// the range fully drains. Per-CALL timeout is a generous backstop
// (PostgresDAHSweepCallTimeoutSeconds, default 120s — NOT a tight per-pass
// limit). Per-partition errors are logged, not fatal. Shared by the background
// cursor and Prune.
func (s *Store) sweepAllPartitionsOnce(ctx context.Context, safeTip int64, retention int32) int64 {
	callTimeout := time.Duration(s.settings.UtxoStore.PostgresDAHSweepCallTimeoutSeconds) * time.Second
	if callTimeout <= 0 {
		callTimeout = 120 * time.Second
	}

	var before int64
	_ = s.maint().QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&before)

	// Bound how many partitions sweep at once. Each CALL scans cold partition pages
	// from disk; firing all 8 at once thrashes a single contended/cold disk and is
	// SLOWER in aggregate than fewer sweepers (measured). A buffered channel acts as
	// a semaphore; concurrency=1 → fully sequential, concurrency>=numPartitions → the
	// original all-parallel behaviour.
	concurrency := s.settings.UtxoStore.PostgresDAHSweepConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	if concurrency > numPartitions {
		concurrency = numPartitions
	}

	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup

	for p := 0; p < numPartitions; p++ {
		p := p

		wg.Add(1)

		go func() {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			// Stop launching new CALLs once the parent context is done.
			if ctx.Err() != nil {
				return
			}

			cctx, cancel := context.WithTimeout(ctx, callTimeout)
			defer cancel()

			if _, err := s.maint().Exec(cctx, `CALL dah_sweep_batch($1, $2, $3)`, p, safeTip, retention); err != nil {
				if ctx.Err() == nil {
					s.logger.Infof("[dahCursor] partition %d CALL error (retry next tick): %v", p, err)
					prometheusDAHSweepErrors.Inc()
				}
			}
		}()
	}

	wg.Wait()

	var after int64
	_ = s.maint().QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&after)

	if after >= before {
		return after - before
	}

	return 0
}

// dahWatermarkBacklog returns safeTip minus the lowest per-partition watermark —
// the number of heights still unswept on the furthest-behind partition (0 when all
// partitions have reached the safe tip).
func (s *Store) dahWatermarkBacklog(ctx context.Context, safeTip int64) int64 {
	var minWM int64
	if err := s.maint().QueryRow(ctx, `SELECT COALESCE(MIN(last_swept_height), 0) FROM dah_part_watermark`).Scan(&minWM); err != nil {
		return 0
	}

	if safeTip > minWM {
		return safeTip - minWM
	}

	return 0
}
