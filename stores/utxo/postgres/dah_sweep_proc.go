package postgres

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
)

// dahSweepProcVersion is bumped whenever dahSweepProcDDL changes. bootstrap
// recreates the procedure whenever the version stored in dah_sweep_control
// differs (in EITHER direction, so a binary rollback reinstalls the older body).
const dahSweepProcVersion = 1

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
    p_safe_tip   BIGINT,
    p_retention  INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from           BIGINT;
    v_to             BIGINT;
    v_step           BIGINT;
    v_max_win        INT;
    v_per_part_cap   INT;
    v_enabled        BOOLEAN;
    v_part           INT;
    v_n              BIGINT;
    v_candidates     BIGINT;
    v_total_stamped  BIGINT := 0;
    v_total_cands    BIGINT := 0;
    v_windows        INT := 0;
    v_got_lock       BOOLEAN;
    v_truncated      BOOLEAN;
    v_lock_contended BOOLEAN := FALSE;  -- set if a window yields to another sweeper
BEGIN
    -- Read control knobs once per CALL. Cheap single-row indexed read.
    SELECT enabled, batch_rows, max_windows_per_call
      INTO v_enabled, v_per_part_cap, v_max_win
      FROM dah_sweep_control
     WHERE id = 1;

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    -- Postgres version guard: COMMIT inside PROCEDURE requires PG11+.
    -- bootstrapDAHSweepProc checks this in Go before calling CREATE PROCEDURE,
    -- so this ASSERT is a belt-and-suspenders defence.
    -- (current_setting returns e.g. '160004' for PG16.4)
    IF current_setting('server_version_num')::int < 110000 THEN
        RAISE EXCEPTION 'dah_sweep_batch requires PostgreSQL 11+';
    END IF;

    -- Transaction-level advisory lock acquired at the start of each window's
    -- transaction (i.e. this first one, and re-acquired after each COMMIT).
    -- Transaction-level locks are released automatically on COMMIT or ROLLBACK,
    -- eliminating the session-lock leak risk on pooled connections. Two concurrent
    -- sweepers are safe: the loser returns immediately; the forward-only watermark
    -- and IS DISTINCT FROM make concurrent sweepers idempotent.
    SELECT pg_try_advisory_xact_lock(20240684::bigint) INTO v_got_lock;
    IF NOT v_got_lock THEN
        RETURN;  -- another caller is sweeping; this CALL is a no-op
    END IF;

    -- Read watermark.
    SELECT last_swept_height INTO v_from
      FROM dah_watermark
     WHERE id = 1;

    IF v_from IS NULL OR v_from >= p_safe_tip THEN
        COMMIT;  -- releases the xact advisory lock
        RETURN;
    END IF;

    v_step := 4096;  -- matches dahSweepMaxHeightStep; fixed size, no adaptive grow

    -- Height window loop.
    WHILE v_from < p_safe_tip LOOP
        EXIT WHEN v_max_win > 0 AND v_windows >= v_max_win;

        -- Re-read kill switch at the top of each window's fresh transaction.
        -- (Each COMMIT ends the prior transaction; we are now in a new one.)
        SELECT enabled INTO v_enabled FROM dah_sweep_control WHERE id = 1;
        EXIT WHEN NOT v_enabled;

        -- Re-acquire the transaction-level advisory lock in the new transaction.
        SELECT pg_try_advisory_xact_lock(20240684::bigint) INTO v_got_lock;
        IF NOT v_got_lock THEN
            v_lock_contended := TRUE;
            EXIT;  -- another caller started between our COMMITs; yield gracefully
        END IF;

        -- Disable seq scan so BRIN range index is used on mined/spent height cols.
        -- Re-issued each window because SET LOCAL resets on each COMMIT.
        -- force_custom_plan is NOT set because EXECUTE format() re-plans every
        -- invocation (no plpgsql plan cache for dynamic SQL), so generic-plan
        -- lock-in cannot occur. enable_seqscan=off is still required because
        -- BRIN misestimation can otherwise pick a seq scan even in a fresh plan.
        SET LOCAL enable_seqscan = off;
        SET LOCAL lock_timeout = '5s';  -- fast-fail on lock contention, do not block writers

        v_to        := LEAST(v_from + v_step, p_safe_tip);
        v_truncated := FALSE;

        FOR v_part IN 0..7 LOOP
            -- Step 1: enumerate candidate hashes for this partition and window.
            -- MATERIALIZED + LIMIT ensures the planner applies the row cap before
            -- the downstream JOIN, reproducing the cardinality control that the
            -- Go two-step (enumerate+round-trip+stamp via bytea[]) achieved.
            -- We capture candidate count separately to detect truncation.
            --
            -- IMPORTANT: spend_agg aggregates over the FULL spends partition for
            -- the candidate hashes (no height bound on the JOIN), so fully-spent
            -- counting is correct across all historical spends, not just in-window
            -- spends. This reproduces sweepDAHRangePartition's behaviour exactly.
            EXECUTE format($q$
                WITH candidates AS MATERIALIZED (
                    SELECT DISTINCT hash FROM (
                        SELECT prev_tx_hash AS hash
                          FROM spends_p%1$s
                         WHERE spent_at_height > $1
                           AND spent_at_height <= $2
                        UNION
                        SELECT hash
                          FROM txs_p%1$s
                         WHERE mined_at_height > $1
                           AND mined_at_height <= $2
                    ) src
                    LIMIT $3
                ),
                spend_agg AS (
                    -- Aggregate full spend history for candidate hashes.
                    -- No height bound here: fully-spent means ALL spends ever,
                    -- not just spends within this window.
                    SELECT s.prev_tx_hash AS hash,
                           count(*) FILTER (
                               WHERE CASE
                                   WHEN s.prev_output_idx < t.out_count
                                   THEN get_bit(t.out_spendables, s.prev_output_idx) = 1
                                   ELSE false
                               END
                           )                       AS spent_count,
                           max(s.spent_at_height)  AS max_spent
                      FROM spends_p%1$s s
                      JOIN txs_p%1$s    t ON t.hash = s.prev_tx_hash
                     WHERE s.prev_tx_hash IN (SELECT hash FROM candidates)
                     GROUP BY s.prev_tx_hash
                ),
                state AS (
                    SELECT t.hash,
                           CASE
                               WHEN t.preserve_until IS NOT NULL
                                   THEN t.delete_at_height
                               WHEN t.unmined_since IS NOT NULL
                                   THEN t.delete_at_height
                               WHEN t.block_ids IS NULL
                                 OR array_length(t.block_ids, 1) IS NULL
                                   THEN t.delete_at_height
                               WHEN COALESCE(sa.spent_count, 0) = t.spendable_count
                                AND t.out_count > 0
                                AND GREATEST(
                                        COALESCE(sa.max_spent, 0),
                                        COALESCE(t.mined_at_height, 0)
                                    ) <= $2
                                   THEN (
                                       GREATEST(
                                           COALESCE(sa.max_spent, 0),
                                           COALESCE(t.mined_at_height, 0)
                                       ) + 1 + $4
                                   )::int
                               ELSE NULL
                           END AS new_dah
                      FROM txs_p%1$s t
                      LEFT JOIN spend_agg sa ON sa.hash = t.hash
                     WHERE t.hash IN (SELECT hash FROM candidates)
                )
                UPDATE txs_p%1$s t
                   SET delete_at_height = st.new_dah
                  FROM state st
                 WHERE t.hash = st.hash
                   AND t.delete_at_height IS DISTINCT FROM st.new_dah;

                -- Candidate count is read separately after the UPDATE via
                -- a second EXECUTE into v_candidates (see below). ROW_COUNT
                -- here only gives stamped rows (where IS DISTINCT FROM fired),
                -- which is the correct metric for observability.
            $q$, lpad(v_part::text, 2, '0'))
            USING v_from, v_to, v_per_part_cap, p_retention;

            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_total_stamped := v_total_stamped + v_n;

            -- Detect truncation: count how many distinct candidate hashes the
            -- enumeration found. If it equals the cap, the window was truncated.
            EXECUTE format($q$
                SELECT count(*) FROM (
                    SELECT DISTINCT hash FROM (
                        SELECT prev_tx_hash AS hash
                          FROM spends_p%1$s
                         WHERE spent_at_height > $1
                           AND spent_at_height <= $2
                        UNION
                        SELECT hash
                          FROM txs_p%1$s
                         WHERE mined_at_height > $1
                           AND mined_at_height <= $2
                    ) src
                    LIMIT $3
                ) cands
            $q$, lpad(v_part::text, 2, '0'))
            USING v_from, v_to, v_per_part_cap
            INTO v_candidates;

            v_total_cands := v_total_cands + v_candidates;
            IF v_candidates >= v_per_part_cap THEN
                v_truncated := TRUE;
            END IF;
        END LOOP;  -- partition loop

        IF v_truncated THEN
            -- Window was not fully drained. Commit the stamps already done
            -- (they are correct and idempotent under IS DISTINCT FROM) but
            -- DO NOT advance the watermark. Shrink the step and re-run the
            -- same (v_from, v_from+v_step] window on the next iteration.
            -- This reproduces dah_sweep.go lines 407-427 exactly.
            -- Candidates beyond the cap remain below the watermark's current
            -- position and will be picked up on the retry with a smaller step.
            v_step := GREATEST(v_step / 2, 1);
            COMMIT;
            -- Re-acquire xact lock for next iteration.
            -- (We do not advance v_from, so the same range is retried.)
        ELSE
            -- Window fully drained: advance the watermark and commit atomically.
            -- The watermark advance and the final stamps are in the SAME transaction.
            -- A crash before COMMIT leaves watermark at the prior position;
            -- the proc resumes from there on the next CALL.
            -- A crash after COMMIT has advanced the watermark; the next CALL
            -- starts from the new position. No height is ever skipped;
            -- no fully-spent tx below the watermark is left un-stamped.
            UPDATE dah_watermark
               SET last_swept_height = v_to
             WHERE id = 1
               AND last_swept_height < v_to;

            COMMIT;

            v_windows := v_windows + 1;
            v_from    := v_to;
            -- On a clean window, grow step back toward maximum.
            v_step    := LEAST(v_step * 2, 4096);
        END IF;

    END LOOP;  -- height window loop

    -- Write observability stats AND the outcome flags the Go ticker reads to
    -- choose its next cadence (see section 4.7). This is a best-effort write;
    -- not a correctness path. Runs in a fresh transaction after the loop.
    --   caught_up  : drained all the way to safe_tip — nothing left below the tip.
    --   hit_budget : stopped at max_windows_per_call with work remaining (MORE to do).
    --   backlog    : heights still unswept below safe_tip at return.
    --   contended  : a window yielded because another sweeper held the lock.
    -- caught_up and hit_budget are mutually exclusive; if the loop ended only
    -- because the kill switch flipped or the lock was contended, neither is set.
    UPDATE dah_sweep_control
       SET last_called_at       = now(),
           last_rows_stamped    = v_total_stamped,
           last_candidates_seen = v_total_cands,
           last_watermark       = v_from,
           total_rows_stamped   = total_rows_stamped + v_total_stamped,
           last_backlog         = GREATEST(p_safe_tip - v_from, 0),
           last_caught_up       = (v_from >= p_safe_tip),
           last_hit_budget      = (v_max_win > 0 AND v_windows >= v_max_win AND v_from < p_safe_tip),
           last_lock_contended  = v_lock_contended
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

// runDAHCursorProc drives the server-side dah_sweep_batch() procedure with an
// adaptive ticker: it CALLs the proc (which does all enumeration, stamping and
// per-window COMMITs server-side), reads the outcome from dah_sweep_control, and
// chooses how soon to call again — drain hard when behind (next=0), idle when
// caught up, back off under advisory-lock contention. The keyspace backstop runs
// as a safety net, exactly as in the Go sweep.
//
// A startup smoke CALL verifies COMMIT-inside-CALL is not suppressed by pgx
// middleware (a wrapping transaction raises SQLSTATE 2D000); if it is, or the
// smoke CALL otherwise fails, the driver falls back to the in-process Go sweep.
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

	contendedBackoff := time.Duration(cfg.PostgresDAHSweepContendedBackoffMillis) * time.Millisecond
	if contendedBackoff <= 0 {
		contendedBackoff = interval * 5
	}

	callTimeoutMult := cfg.PostgresDAHSweepCallTimeoutMultiple
	if callTimeoutMult <= 0 {
		callTimeoutMult = 50
	}

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	batch := cfg.PostgresDAHSweepBatchSize
	if batch <= 0 {
		batch = 50000
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // retention is a small positive height delta

	// Smoke CALL: verify COMMIT-inside-CALL is active. A negative safe_tip makes
	// the proc return immediately (no-op). A wrapping transaction injected by pgx
	// middleware would raise 2D000 (invalid transaction termination) on the proc's
	// first COMMIT. There is no Go fallback; log it loudly once so the per-tick
	// CALL errors that follow have an obvious root cause (the keyspace backstop
	// loop below still stamps via plain UPDATEs in the meantime).
	smokeCtx, smokeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, smokeErr := s.store.pool.Exec(smokeCtx, `CALL dah_sweep_batch($1, $2)`, int64(-1), int32(0))
	smokeCancel()

	if ctx.Err() != nil {
		return
	}

	if smokeErr != nil {
		var pgErr *pgconn.PgError
		if errors.As(smokeErr, &pgErr) && pgErr.Code == "2D000" {
			s.store.logger.Errorf("[dahCursor] COMMIT-in-CALL blocked by middleware (2D000): the DAH sweep procedure cannot commit; the pgx connection must run CALL in autocommit (no wrapping transaction/tracer)")
		} else {
			s.store.logger.Errorf("[dahCursor] proc smoke CALL failed: %v", smokeErr)
		}
	}

	s.logger.Infof("[dahCursor] proc driver active (interval=%s idle=%s)", interval, idleInterval)

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
			next := interval // default cadence; overridden by the proc's outcome below

			safeTip := s.store.dahSafeTip(lag)
			if safeTip <= 0 {
				sweepTimer.Reset(idleInterval)
				continue
			}

			// Bounded context: a disk-wedged CALL cannot hold the pooled connection
			// indefinitely. A cancelled CALL loses at most one uncommitted window;
			// all prior windows are committed and the next fire resumes from the
			// watermark.
			callCtx, callCancel := context.WithTimeout(ctx, time.Duration(callTimeoutMult)*interval)
			start := time.Now()
			_, err := s.store.pool.Exec(callCtx, `CALL dah_sweep_batch($1, $2)`, safeTip, retention)
			callCancel()

			elapsed := time.Since(start)

			if ctx.Err() != nil {
				return
			}

			if err != nil {
				s.logger.Infof("[dahCursor] CALL error (retry): %v elapsed=%s", err, elapsed.Truncate(time.Millisecond))
				prometheusDAHSweepErrors.Inc()
				sweepTimer.Reset(interval)

				continue
			}

			// Read the outcome the proc recorded and choose the next cadence from it.
			var (
				stamped, cands, watermark, backlog int64
				caughtUp, hitBudget, contended     bool
			)

			if err2 := s.store.pool.QueryRow(ctx,
				`SELECT last_rows_stamped, last_candidates_seen, last_watermark,
				        last_backlog, last_caught_up, last_hit_budget, last_lock_contended
				   FROM dah_sweep_control WHERE id = 1`,
			).Scan(&stamped, &cands, &watermark, &backlog, &caughtUp, &hitBudget, &contended); err2 == nil {
				prometheusDAHSweepCallDuration.Observe(elapsed.Seconds())
				prometheusDAHSweepRowsStamped.Add(float64(stamped))
				prometheusDAHSweepWatermarkLag.Set(float64(backlog))

				switch {
				case hitBudget:
					next = 0 // more remains — call again immediately, drain hard
				case contended:
					next = contendedBackoff // another sweeper is draining — yield
				case caughtUp:
					next = idleInterval // nothing below the tip — cheap idle poll
				default:
					next = interval
				}

				if stamped > 0 || backlog > 0 {
					s.logger.Infof(
						"[dahCursor] mode=proc stamped=%d candidates=%d watermark=%d backlog=%d caughtUp=%t hitBudget=%t next=%s elapsed=%s",
						stamped, cands, watermark, backlog, caughtUp, hitBudget,
						next.Truncate(time.Millisecond), elapsed.Truncate(time.Millisecond),
					)
				}
			}

			sweepTimer.Reset(next)
		}
	}
}
