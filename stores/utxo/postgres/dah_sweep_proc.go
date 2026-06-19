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
const dahSweepProcVersion = 6

// dahSweepControlDDL is the kill-switch / tunables / proc-version / per-CALL
// outcome table. Created unconditionally in createSchemaWithPool (plain DDL);
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

// dahSweepProcDDL is the server-side self-committing DAH stamp procedure. It
// reproduces the exact DAH semantics of the in-process Go sweep
// (sweepDAHRangePartition) but commits per fully-drained height window, needs no
// statement_timeout, and is driven by a thin adaptive ticker via CALL. See
// docs/superpowers/dah-stored-proc-proposal.md for the full design.
const dahSweepProcDDL = `CREATE OR REPLACE PROCEDURE dah_sweep_batch(
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
    v_step           BIGINT;
    v_max_win        INT;
    v_per_part_cap   INT;
    v_enabled        BOOLEAN;
    v_n              BIGINT;
    v_total_stamped  BIGINT := 0;
    v_windows        INT := 0;
    v_t0             timestamptz;
    v_ms             double precision;
BEGIN
    -- One partition per CALL; the Go cursor fans 8 CALLs across the partitions in
    -- PARALLEL (recovering the parallelism the old Go errgroup sweep had, which a
    -- single sequential-8-partition CALL lost on a cold-cache disk).
    SELECT enabled, batch_rows, max_windows_per_call
      INTO v_enabled, v_per_part_cap, v_max_win
      FROM dah_sweep_control WHERE id = 1;

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    IF current_setting('server_version_num')::int < 110000 THEN
        RAISE EXCEPTION 'dah_sweep_batch requires PostgreSQL 11+';
    END IF;

    -- Per-partition advisory lock so the 8 parallel CALLs (and Prune) never
    -- contend across partitions; within a partition it serialises sweepers.
    -- Transaction-level → auto-released on COMMIT/ROLLBACK (no pooled-conn leak).
    IF NOT pg_try_advisory_xact_lock(20240684 + p_partition) THEN
        RETURN;
    END IF;

    SELECT last_swept_height INTO v_from
      FROM dah_part_watermark WHERE partition = p_partition;

    IF v_from IS NULL OR v_from >= p_safe_tip THEN
        COMMIT;
        RETURN;
    END IF;

    -- v_step is TIME-ADAPTIVE and is the SOLE governor of per-window cost: a slow
    -- (dense) window halves it for the NEXT window, a fast (sparse) one doubles it.
    -- There is no candidate cap and no shrink-retry — every window enumerates its
    -- COMPLETE distinct-parent set in ONE pass, stamps, and advances the watermark, so
    -- spends pages are never re-read for the same window. Seed small so the very first
    -- window after a cold start / reorg surge is bounded; it doubles to 4096 within a
    -- few fast windows on sparse history.
    v_step := 256;

    WHILE v_from < p_safe_tip LOOP
        EXIT WHEN v_max_win > 0 AND v_windows >= v_max_win;

        SELECT enabled INTO v_enabled FROM dah_sweep_control WHERE id = 1;
        EXIT WHEN NOT v_enabled;

        IF NOT pg_try_advisory_xact_lock(20240684 + p_partition) THEN
            EXIT;
        END IF;

        SET LOCAL enable_seqscan = off;
        SET LOCAL lock_timeout = '5s';

        v_to := LEAST(v_from + v_step, p_safe_tip);
        v_t0 := clock_timestamp();

        -- SINGLE PASS for THIS partition. Enumerate candidate parent hashes whose
        -- outputs were spent in (v_from,v_to] purely from the APPEND-ORDERED spends side
        -- ("spends enumerate, txs decides"). The composite btree (spent_at_height,
        -- prev_tx_hash) serves this as an INDEX-ONLY range scan — it reads only the
        -- window's index leaves, no heap, so the cost is proportional to the window's
        -- spend rows and independent of accumulated chain size (measured on betfair:
        -- 22,728 buffers/lossy-BRIN → ~520 index-only, 25s cold → ~0.1s). enable_seqscan
        -- stays off so it never regresses to a seq scan. No LIMIT: candidates is the
        -- COMPLETE distinct-parent set of the window, so there is nothing to truncate and
        -- no shrink-retry is needed. Aggregate each candidate's FULL spend history (no
        -- height bound on the JOIN → correct fully-spent counting), bidirectionally stamp
        -- delete_at_height with the exact DAH formula, and COUNT the stamped rows from the
        -- SAME pass: the data-modifying upd CTE runs once to completion and the outer
        -- SELECT counts its RETURNING, so spends is scanned ONCE per window. Exact DAH
        -- semantics unchanged (preserve_until/unmined_since/block_ids guards, spendable
        -- get_bit, GREATEST+1+retention, IS DISTINCT FROM).
        EXECUTE format($q$
            WITH candidates AS MATERIALIZED (
                SELECT DISTINCT prev_tx_hash AS hash FROM spends_p%1$s
                 WHERE spent_at_height > $1 AND spent_at_height <= $2
            ),
            spend_agg AS (
                SELECT s.prev_tx_hash AS hash,
                       count(*) FILTER (WHERE CASE WHEN s.prev_output_idx < t.out_count THEN get_bit(t.out_spendables, s.prev_output_idx) = 1 ELSE false END) AS spent_count,
                       max(s.spent_at_height) AS max_spent
                FROM spends_p%1$s s
                JOIN txs_p%1$s    t ON t.hash = s.prev_tx_hash
                WHERE s.prev_tx_hash IN (SELECT hash FROM candidates)
                GROUP BY s.prev_tx_hash
            ),
            state AS (
                SELECT t.hash,
                       CASE
                           WHEN t.preserve_until IS NOT NULL THEN t.delete_at_height
                           WHEN t.unmined_since IS NOT NULL THEN t.delete_at_height
                           WHEN t.block_ids IS NULL OR array_length(t.block_ids, 1) IS NULL THEN t.delete_at_height
                           WHEN COALESCE(sa.spent_count, 0) = t.spendable_count
                                AND t.out_count > 0
                                AND GREATEST(COALESCE(sa.max_spent, 0), COALESCE(t.mined_at_height, 0)) <= $2
                               THEN (GREATEST(COALESCE(sa.max_spent, 0), COALESCE(t.mined_at_height, 0)) + 1 + $3)::int
                           ELSE NULL
                       END AS new_dah
                FROM txs_p%1$s t
                LEFT JOIN spend_agg sa ON sa.hash = t.hash
                WHERE t.hash IN (SELECT hash FROM candidates)
            ),
            upd AS (
                UPDATE txs_p%1$s t
                   SET delete_at_height = st.new_dah
                  FROM state st
                 WHERE t.hash = st.hash
                   AND t.delete_at_height IS DISTINCT FROM st.new_dah
                RETURNING 1
            )
            SELECT count(*) FROM upd
        $q$, v_leaf_suffix)
        USING v_from, v_to, p_retention
        INTO v_n;

        v_total_stamped := v_total_stamped + v_n;

        -- Window fully drained in one pass (no cap to clip it): advance unconditionally.
        UPDATE dah_part_watermark
           SET last_swept_height = v_to
         WHERE partition = p_partition AND last_swept_height < v_to;
        COMMIT;

        v_windows := v_windows + 1;
        v_from    := v_to;

        -- TIME-ADAPTIVE step is the sole density regulator: a dense (slow) window
        -- shrinks the NEXT window; a sparse (fast) one grows it.
        v_ms := extract(epoch FROM clock_timestamp() - v_t0) * 1000;
        IF v_ms > 2000 THEN
            v_step := GREATEST(v_step / 2, 1);
        ELSIF v_ms < 500 THEN
            v_step := LEAST(v_step * 2, 4096);
        END IF;
    END LOOP;

    -- Best-effort observability (additive across the 8 partitions).
    UPDATE dah_sweep_control
       SET last_called_at = now(),
           total_rows_stamped = total_rows_stamped + v_total_stamped
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
		return errors.NewStorageError("[dahSweep] read server_version_num: %v", err)
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

		return errors.NewStorageError("[dahSweep] seed control knobs: %v", err)
	}

	// Recreate the procedure only when the stored version differs (either direction).
	var storedVersion int
	if err = s.pool.QueryRow(ctx, `SELECT proc_version FROM dah_sweep_control WHERE id = 1`).Scan(&storedVersion); err != nil {
		return errors.NewStorageError("[dahSweep] read proc_version: %v", err)
	}

	if storedVersion == dahSweepProcVersion {
		return nil
	}

	// Drop the legacy single-CALL (BIGINT, INT) signature from the v1 era. The v2
	// procedure has a different signature (INT, BIGINT, INT), so CREATE OR REPLACE
	// would otherwise leave the old overload behind, unused. Best-effort.
	_, _ = s.pool.Exec(ctx, `DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT)`)

	if _, err = s.pool.Exec(ctx, dahSweepProcDDL); err != nil {
		if isInsufficientPrivilege(err) {
			return errors.NewStorageError("[dahSweep] app role lacks CREATE privilege for the dah_sweep_batch procedure (42501); grant CREATE so the DAH sweep can run")
		}

		return errors.NewStorageError("[dahSweep] create procedure: %v", err)
	}

	if _, err = s.pool.Exec(ctx, `UPDATE dah_sweep_control SET proc_version = $1 WHERE id = 1`, dahSweepProcVersion); err != nil {
		return errors.NewStorageError("[dahSweep] record proc_version: %v", err)
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
// backlog>0 → drain hard (call again immediately), else idle. The keyspace
// backstop runs as a safety net.
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

	batch := cfg.PostgresDAHSweepBatchSize
	if batch <= 0 {
		batch = 50000
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	// Smoke CALL (partition 0, negative safe_tip → immediate no-op): verify
	// COMMIT-inside-CALL works. A wrapping transaction from pgx middleware raises
	// 2D000. No Go fallback exists; log the root cause loudly.
	smokeCtx, smokeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, smokeErr := s.store.pool.Exec(smokeCtx, `CALL dah_sweep_batch($1, $2, $3)`, 0, int64(-1), int32(0))
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

	backstopTicker := time.NewTicker(backstopInterval)
	defer backstopTicker.Stop()

	var backstopByte int

	for {
		select {
		case <-ctx.Done():
			return

		case <-backstopTicker.C:
			b := backstopByte
			backstopByte = (backstopByte + 1) & 0xff

			if n, err := s.store.backstopReconcile(ctx, b, b, batch); err != nil {
				s.logger.Infof("[dahBackstop] slice 0x%02x error (best-effort, continuing): %v", b, err)
			} else if n > 0 {
				s.logger.Infof("[dahBackstop] slice 0x%02x recovered %d missed tx(s)", b, n)
			}

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
	_ = s.pool.QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&before)

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

			if _, err := s.pool.Exec(cctx, `CALL dah_sweep_batch($1, $2, $3)`, p, safeTip, retention); err != nil {
				if ctx.Err() == nil {
					s.logger.Infof("[dahCursor] partition %d CALL error (retry next tick): %v", p, err)
					prometheusDAHSweepErrors.Inc()
				}
			}
		}()
	}

	wg.Wait()

	var after int64
	_ = s.pool.QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&after)

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
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(MIN(last_swept_height), 0) FROM dah_part_watermark`).Scan(&minWM); err != nil {
		return 0
	}

	if safeTip > minWM {
		return safeTip - minWM
	}

	return 0
}
