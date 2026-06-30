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
const dahSweepProcVersion = 10

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
)`

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

// dahSweepProcDDLWithPendingDeletes is the variant of the DAH sweep procedure
// that also upserts each newly-stamped row into the per-leaf pending_deletes_pNN
// table inside the same EXECUTE block. The upd CTE returns (hash, delete_at_height)
// instead of just 1 so the ins CTE can populate the side-table, and the outer
// SELECT count(*) FROM upd still counts stamped rows for the watermark logic.
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
    v_max_rows       INT;
    v_enabled        BOOLEAN;
    v_n              BIGINT;
    v_total_stamped  BIGINT := 0;
    v_drained        BOOLEAN := false;
BEGIN
    SELECT enabled, batch_rows INTO v_enabled, v_max_rows
      FROM dah_sweep_control WHERE id = 1;

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    IF v_max_rows IS NULL OR v_max_rows <= 0 THEN
        v_max_rows := 5000;
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

    -- Row-targeted batches: each pass stamps up to v_max_rows fully-spent-unstamped
    -- parents over the WHOLE (v_from, p_safe_tip] range. Stamped rows drop out via the
    -- delete_at_height IS NULL filter, so the qualifying set strictly shrinks each pass.
    -- The watermark stays at v_from during the loop and advances once at the end.
    LOOP
        SELECT enabled INTO v_enabled FROM dah_sweep_control WHERE id = 1;
        EXIT WHEN NOT v_enabled;

        IF NOT pg_try_advisory_xact_lock(20240684 + p_partition) THEN
            EXIT;
        END IF;

        SET LOCAL enable_seqscan = off;
        SET LOCAL lock_timeout = '5s';

        EXECUTE format($q$
            WITH candidates AS MATERIALIZED (
                SELECT DISTINCT prev_tx_hash AS hash FROM spends_p%1$s
                 WHERE spent_at_height > $1 AND spent_at_height <= $2
            ),
            eligible AS MATERIALIZED (
                SELECT t.hash, t.out_count, t.out_spendables, t.spendable_count, t.mined_at_height
                  FROM txs_p%1$s t
                  JOIN candidates c ON c.hash = t.hash
                 WHERE t.delete_at_height IS NULL
                   AND t.preserve_until IS NULL
                   AND t.unmined_since IS NULL
                   AND t.block_ids IS NOT NULL AND array_length(t.block_ids, 1) IS NOT NULL
            ),
            spend_agg AS (
                SELECT s.prev_tx_hash AS hash,
                       count(*) FILTER (WHERE CASE WHEN s.prev_output_idx < e.out_count THEN get_bit(e.out_spendables, s.prev_output_idx) = 1 ELSE false END) AS spent_count,
                       max(s.spent_at_height) AS max_spent
                FROM spends_p%1$s s
                JOIN eligible e ON e.hash = s.prev_tx_hash
                GROUP BY s.prev_tx_hash
            ),
            state AS (
                SELECT e.hash,
                       CASE
                           WHEN COALESCE(sa.spent_count, 0) = e.spendable_count
                                AND e.out_count > 0
                                AND GREATEST(COALESCE(sa.max_spent, 0), COALESCE(e.mined_at_height, 0)) <= $2
                               THEN (GREATEST(COALESCE(sa.max_spent, 0), COALESCE(e.mined_at_height, 0)) + 1 + $3)::int
                           ELSE NULL
                       END AS new_dah
                FROM eligible e
                LEFT JOIN spend_agg sa ON sa.hash = e.hash
            ),
            to_stamp AS MATERIALIZED (
                SELECT hash, new_dah FROM state WHERE new_dah IS NOT NULL LIMIT $4
            ),
            upd AS (
                UPDATE txs_p%1$s t
                   SET delete_at_height = ts.new_dah
                  FROM to_stamp ts
                 WHERE t.hash = ts.hash
                   AND t.delete_at_height IS DISTINCT FROM ts.new_dah
                RETURNING t.hash, ts.new_dah AS delete_at_height
            ),
            ins AS (
                INSERT INTO pending_deletes_p%1$s (hash, delete_at_height)
                SELECT hash, delete_at_height FROM upd WHERE delete_at_height IS NOT NULL
                ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
            )
            SELECT count(*) FROM upd
        $q$, v_leaf_suffix)
        USING v_from, p_safe_tip, p_retention, v_max_rows
        INTO v_n;

        v_total_stamped := v_total_stamped + v_n;
        COMMIT;

        IF v_n < v_max_rows THEN
            v_drained := true;
            EXIT;
        END IF;
    END LOOP;

    -- Advance the watermark only when the range was genuinely drained (not on
    -- advisory-lock-miss or kill-switch exit). An empty range still sets v_drained
    -- (v_n=0 < v_max_rows) so a caught-up partition still marks itself swept.
    IF v_drained THEN
        UPDATE dah_part_watermark
           SET last_swept_height = p_safe_tip
         WHERE partition = p_partition AND last_swept_height < p_safe_tip;
    END IF;

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

	if _, err = s.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET batch_rows = $1, max_windows_per_call = $2 WHERE id = 1`,
		batchRows, maxWin,
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
// via sweepAllPartitionsOnce; each CALL drains time-adaptive windows committing
// per window. Cadence is driven by the real watermark lag (min across partitions):
// backlog>0 → drain hard (call again immediately), else idle.
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
// cumulative counter). Each CALL drains up to max_windows_per_call time-adaptive
// windows, committing per window, under a generous per-CALL timeout
// (PostgresDAHSweepCallTimeoutSeconds, default 120s — NOT a tight interval
// multiple): a cancelled CALL loses at most one uncommitted window and resumes
// from the per-partition watermark next pass. Per-partition errors are logged, not
// fatal. Shared by the background cursor and Prune.
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
