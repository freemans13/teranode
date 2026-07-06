package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v7 turbo UTXO tables in the connected PostgreSQL
// database: 2 LOGGED hash-partitioned tables (txs, spends). The outputs
// table has been folded into txs as PACKED per-output columns (utxo_hashes
// flat bytea, out_spendables/out_frozens bitmaps, out_count/spendable_count
// scalars, coinbase_spending_height scalar, spendable_ins array), eliminating
// the per-tx outputs INSERT, the random-hash PK index, and the cascade-DELETE.
// raw_tx lives on the txs row (immutable BYTEA blob).
//
// Schema design tradeoffs (vs. the standard stores/utxo/sql store):
//
//   - No foreign keys. Child rows (spends) reference txs by BYTEA hash rather
//     than a surrogate id, and pruning explicitly cascades via
//     DELETE FROM spends → txs inside one txn (see pruner_provider).
//
//   - "Is this output spent?" lives in the spends table as a row, not as a
//     nullable spending_data column. Spends become pure INSERTs (no MVCC
//     bloat on txs output arrays).
//
//   - block_ids / block_heights / subtree_idxs / conflicting_children are
//     arrays on txs, and all per-output UTXO fields are PACKED columns on txs
//     (flat bytea + bitmaps + scalar counts). A single row lookup returns
//     everything needed for spend validation — zero extra table JOINs.
func (s *Store) createSchema(ctx context.Context) error {
	if err := createSchemaInternal(ctx, s.pool, s.logger); err != nil {
		return err
	}

	// The server-side DAH sweep procedure is the only sweep mechanism (there is no
	// in-process fallback), so bootstrapping it is mandatory: a failure (missing
	// CREATE privilege, or postgres < 11) fails store startup and surfaces the
	// deployment problem rather than silently never pruning.
	if err := s.bootstrapDAHSweepProc(ctx); err != nil {
		return err
	}

	return nil
}

// partitionSpec defines a partitioned table, the fillfactor for its children,
// and the autovacuum storage params for its children.
//
// autovacuum MUST be set on the leaf partitions: autovacuum operates on the
// leaf relations, and storage params set on a partitioned PARENT are never
// inherited by autovacuum on the leaves. (A parent-level ALTER TABLE txs SET
// (autovacuum_*) silently does nothing for vacuum scheduling — it only affects
// partitions created later via the parent's defaults, not the leaf's vacuum.)
type partitionSpec struct {
	name       string
	fillfactor int
	autovacuum string // SET-clause body applied per leaf partition
}

// numPartitions is the number of hash partitions per table. Burst benchmarks
// favoured a single partition (no fan-out overhead). Under SUSTAINED high-churn
// load that inverts: the txs `locked`-flag UPDATE produces ~1 dead tuple per tx,
// and a single autovacuum worker on one large partition cannot reclaim at the
// dead-tuple generation rate (measured: txs_dead grows unbounded to 24M+ at
// 60K TPS). Splitting into N partitions lets up to autovacuum_max_workers leaves
// be vacuumed concurrently, each a fraction of the size, so aggregate vacuum
// throughput scales with the churn. 8 keeps vacuum ahead with the default 3
// workers while bounding fan-out cost on reads.
const numPartitions = 8

// createSchemaWithPoolFlag executes all DDL statements using the provided pool.
// The usePendingDeletes parameter is retained for backward compatibility with
// existing test call sites but is now ignored: the pending_deletes side-table
// is the only pruner path, so it is always created and the px_delete_at_height
// BRIN index on txs is always backfilled into pending_deletes and dropped.
func createSchemaWithPoolFlag(ctx context.Context, pool *pgxpool.Pool, _ bool) error {
	return createSchemaInternal(ctx, pool, ulogger.TestLogger{})
}

// createSchemaInternal executes all DDL statements using the provided pool. It
// always creates the pending_deletes side-table (8 leaves + per-leaf btree on
// delete_at_height) and unconditionally backfills+drops the legacy
// px_delete_at_height BRIN index on txs — pending_deletes is the only pruner path.
func createSchemaInternal(ctx context.Context, pool *pgxpool.Pool, logger ulogger.Logger) error {
	ddlStatements := []string{
		txsDDL,
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed\nDDL: %s", ddl, err)
		}
	}

	// Create numPartitions hash partitions for each table with appropriate
	// fillfactor and per-leaf autovacuum tuning.
	//
	// txs is the only high-churn table: the validator hot path UPDATEs the
	// `locked` flag per tx (Create(WithLocked)+SetLocked), producing one dead
	// tuple per tx. At scale_factor=0.05 on a multi-million-row partition that
	// lets ~5% (≈1.7M) dead tuples accumulate before vacuum fires — observed as
	// a 0.3M–1.9M dead-tuple oscillation that breaks HOT-chain reuse under load.
	// 0.01 fires at ~1% (≈340K), and cost_delay=0 keeps the sweep from being
	// throttled mid-run. spends is insert-only (no UPDATE churn), so the
	// insert_scale_factor path (keeping the visibility map fresh for the
	// deferred-DAH sweep's index-only counts) is what matters there — left at the
	// gentler 0.05 so insert-triggered vacuums don't steal hot-path I/O.
	const commonAV = "autovacuum_vacuum_insert_scale_factor = 0.02, "
	tables := []partitionSpec{
		// txs is the only UPDATE-churned table: SetLocked(false), the SetMinedMulti
		// block_ids append, AND the DAH sweep's delete_at_height stamp each rewrite
		// the row — up to ~3 updates/tx. fillfactor=50 leaves half the page free so
		// those rewrites stay HOT (no new index entries, reclaimed cheaply by
		// HOT-prune on the next page touch) instead of accumulating index-bloating
		// dead tuples that only a full vacuum can reclaim. This bounds dead-tuple
		// density at the SOURCE, so a gentle autovacuum config (which the DAH sweep
		// needs — an aggressive one starves the sweep) is enough. cost_limit=8000
		// (4x default) + cost_delay=0 keep what vacuum is still needed un-throttled;
		// freeze_max_age is raised so no anti-wraparound vacuum stalls reclaim mid-run.
		{"txs", 50, "autovacuum_vacuum_scale_factor = 0.01, " + commonAV +
			"autovacuum_vacuum_cost_limit = 8000, " +
			"autovacuum_freeze_max_age = 500000000, " +
			"autovacuum_vacuum_cost_delay = 0, " +
			"autovacuum_analyze_scale_factor = 0.005, " +
			"autovacuum_vacuum_insert_threshold = 1000"},
		{"spends", 100, "autovacuum_vacuum_scale_factor = 0.05, " + commonAV +
			"autovacuum_vacuum_cost_limit = 2000, " +
			"autovacuum_vacuum_cost_delay = 2, " +
			"autovacuum_analyze_scale_factor = 0.05"},
	}
	for _, spec := range tables {
		for i := 0; i < numPartitions; i++ {
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s_p%02d PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = %d)",
				spec.name, i, spec.name, numPartitions, i, spec.fillfactor,
			)
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("partition creation failed for %s_p%02d", spec.name, i, err)
			}

			// Idempotent: also back-fills partitions created before these settings
			// existed. Set on the leaf (autovacuum ignores parent-level params).
			av := fmt.Sprintf("ALTER TABLE %s_p%02d SET (%s)", spec.name, i, spec.autovacuum)
			if _, err := pool.Exec(ctx, av); err != nil {
				return errors.NewStorageError("autovacuum tuning failed for %s_p%02d", spec.name, i, err)
			}
		}
	}

	// Height indexes on spends. The DAH sweep enumerates candidate parents purely from
	// the spends side by height window ("spends enumerate, txs decides").
	//
	// BRIN(spent_at_height): near-free on insert; kept as a planner fallback and used by
	// the backstop. But spent_at_height is only LOOSELY correlated with heap order
	// (concurrent block validation interleaves heights), so its bitmap goes LOSSY — a
	// 1024-height window rechecks ~1.9M rows / ~28k heap pages to keep ~280k (measured),
	// which does not scale and pins the cold disk during the sweep.
	//
	// Composite btree (spent_at_height, prev_tx_hash): makes the candidate enumeration an
	// INDEX-ONLY range scan (the CTE needs only prev_tx_hash for a height range) — it
	// reads only the window's index leaves, no heap (measured on betfair: 22,728 buffers
	// → ~520, zero heap fetches). The cursor's per-window cost becomes proportional to
	// that window's spend rows, NOT to accumulated chain size, so it scales and stays at
	// the warm edge. spends is INSERT-ONLY so a btree does NOT break HOT (HOT is an
	// UPDATE concern); spent_at_height is MONOTONIC (block height) so inserts land on the
	// right-most leaf (cheap, hot-page append) and deduplicate_items collapses the long
	// runs of identical heights into posting lists (index ~0.77× the random-hash PK
	// btree already paid per spend). Measured spend-INSERT cost: ~+18%; create/mine path:
	// 0% (it does not write spends).
	//
	// There is deliberately NO index on txs.mined_at_height (uncorrelated; a btree there
	// would break HOT on the hot mine UPDATE). The only txs that become fully-spent AT
	// mine time without a spend in a swept window (zero-spendable) are stamped inline in
	// SetMinedMulti; the rarer spent-while-unmined case is caught by the backstop.
	for i := 0; i < numPartitions; i++ {
		// spends_spent_at_height_brin removed: it only ever served the O(table) DAH
		// backstop, which was deleted in 3e3f7f4e9. Bench-confirmed neutral on create/
		// spend TPS (V1), so the BRIN is pure dead weight — one fewer index per spend
		// INSERT. The composite (spent_at_height, prev_tx_hash) btree stays: it gives the
		// DAH-sweep candidate enumeration an index-only scan and a height-only btree was
		// bench-confirmed to cause bimodal sweep collapse (V3, CV 38%).
		idxStmts := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS spends_p%02d_h_hash_btree ON spends_p%02d USING btree (spent_at_height, prev_tx_hash) WITH (fillfactor = 90, deduplicate_items = on)`, i, i),
		}
		for _, ddl := range idxStmts {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("height index creation failed", err)
			}
		}
	}

	// Drop legacy txs.mined_at_height indexes if a prior build created them. The sweep
	// no longer scans txs by mined_at_height (see above), so these are dead weight: the
	// BRIN was useless (uncorrelated) and the btree hurt the mine-path HOT ratio.
	for i := 0; i < numPartitions; i++ {
		for _, ddl := range []string{
			fmt.Sprintf(`DROP INDEX IF EXISTS txs_p%02d_mined_at_height_btree`, i),
			fmt.Sprintf(`DROP INDEX IF EXISTS txs_p%02d_mined_at_height_brin`, i),
			// Dead since the DAH backstop was removed (3e3f7f4e9); drop on existing DBs.
			fmt.Sprintf(`DROP INDEX IF EXISTS spends_p%02d_spent_at_height_brin`, i),
		} {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("legacy mined_at_height index drop failed", err)
			}
		}
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dah_watermark (
			id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			last_swept_height BIGINT NOT NULL DEFAULT 0
		)`); err != nil {
		return errors.NewStorageError("dah_watermark creation failed", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_watermark (id, last_swept_height) VALUES (1, 0) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_watermark seed failed", err)
	}

	// Per-partition DAH sweep watermark: one row per hash partition so the 8
	// dah_sweep_batch() CALLs the cursor fans out in PARALLEL each track their own
	// progress independently. Seeded from the legacy single-row dah_watermark so an
	// existing deployment continues from where it had swept (no re-sweep from 0;
	// re-sweeping would be idempotent but wasteful).
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS dah_part_watermark (
			partition INT PRIMARY KEY CHECK (partition BETWEEN 0 AND %d),
			last_swept_height BIGINT NOT NULL DEFAULT 0
		)`, numPartitions-1)); err != nil {
		return errors.NewStorageError("dah_part_watermark creation failed", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dah_part_watermark (partition, last_swept_height)
		SELECT g, COALESCE((SELECT last_swept_height FROM dah_watermark WHERE id = 1), 0)
		FROM generate_series(0, %d) g
		ON CONFLICT (partition) DO NOTHING`, numPartitions-1)); err != nil {
		return errors.NewStorageError("dah_part_watermark seed failed", err)
	}

	// dah_sweep_control: kill switch, tunable knobs, proc version, and the
	// per-CALL outcome the proc-mode adaptive ticker reads (see dah_sweep_proc.go).
	// Plain DDL, always created; the procedure itself is bootstrapped separately
	// (bootstrapped separately in Store.createSchema).
	if _, err := pool.Exec(ctx, dahSweepControlDDL); err != nil {
		return errors.NewStorageError("dah_sweep_control creation failed", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_sweep_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_sweep_control seed failed", err)
	}

	// dah_reconcile_cursor: per-partition rotating cursor for the bounded
	// spent_progress reconciliation backstop (Task 8). See dah_reconcile.go.
	if _, err := pool.Exec(ctx, dahReconcileCursorDDL); err != nil {
		return errors.NewStorageError("dah_reconcile_cursor creation failed", err)
	}

	// conflict_intents: write-ahead log for crash-safe ProcessConflicting /
	// ReverseProcessConflicting (see #861). One row per in-flight conflict-
	// resolution operation, recorded BEFORE its first state mutation and removed
	// once its terminal step commits; rows that survive a restart drive replay.
	// Intentionally NOT tied to txs — intents reference tx hashes, not row ids,
	// and must outlive any individual transaction record.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS conflict_intents (
			intent_id     BYTEA PRIMARY KEY,
			kind          TEXT NOT NULL,
			block_height  BIGINT NOT NULL,
			block_hash    BYTEA NOT NULL,
			tx_hashes     BYTEA NOT NULL,
			started_at    BIGINT NOT NULL
		)`); err != nil {
		return errors.NewStorageError("conflict_intents creation failed", err)
	}

	// pending_deletes side-table: ALWAYS-ON (no flag). The pruner populates this
	// table with (hash, delete_at_height) rows and reads from it directly rather
	// than scanning txs via a BRIN index. Each hash partition gets its own leaf
	// and a btree index on delete_at_height for efficient height-range scans by
	// the pruner.
	//
	// This block is intentionally placed BEFORE the BRIN drop/backfill block
	// below so that pending_deletes exists when the backfill INSERT runs.
	if _, err := pool.Exec(ctx, pendingDeletesDDL); err != nil {
		return errors.NewStorageError("pending_deletes creation failed", err)
	}
	for i := 0; i < numPartitions; i++ {
		leafDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS pending_deletes_p%02d PARTITION OF pending_deletes FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		)
		if _, err := pool.Exec(ctx, leafDDL); err != nil {
			return errors.NewStorageError("pending_deletes_p%02d creation failed", i, err)
		}
		idxDDL := fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS px_pd_dah_p%02d ON pending_deletes_p%02d USING btree (delete_at_height)",
			i, i,
		)
		if _, err := pool.Exec(ctx, idxDDL); err != nil {
			return errors.NewStorageError("pending_deletes_p%02d index creation failed", i, err)
		}
	}

	// pending_unmined side-table: ALWAYS-ON (no flag). Stores (hash, unmined_since)
	// rows for efficient old-unmined-tx queries by the iterator and pruner.
	// Each hash partition gets its own leaf and a btree index on unmined_since for
	// height-range scans. The backfill is guarded by a marker index so it runs only
	// once (on first startup with this schema version) rather than on every restart.
	if _, err := pool.Exec(ctx, pendingUnminedDDL); err != nil {
		return errors.NewStorageError("pending_unmined creation failed", err)
	}
	for i := 0; i < numPartitions; i++ {
		leafDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS pending_unmined_p%02d PARTITION OF pending_unmined FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		)
		if _, err := pool.Exec(ctx, leafDDL); err != nil {
			return errors.NewStorageError("pending_unmined_p%02d creation failed", i, err)
		}
		idxDDL := fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS px_pu_since_p%02d ON pending_unmined_p%02d USING btree (unmined_since)",
			i, i,
		)
		if _, err := pool.Exec(ctx, idxDDL); err != nil {
			return errors.NewStorageError("pending_unmined_p%02d index creation failed", i, err)
		}
	}
	// Retire the legacy one-shot backfill marker index unconditionally, on every
	// startup (cheap no-op once already dropped) — see pendingUnminedBackfillDDL.
	if _, err := pool.Exec(ctx, pendingUnminedBackfillDropLegacyMarkerDDL); err != nil {
		return errors.NewStorageError("legacy pending_unmined backfill marker drop failed", err)
	}
	// Clean-shutdown-gated reconciliation backfill: skips the txs seq scan when
	// the previous shutdown was clean (see resolvePendingUnminedBackfill and
	// pendingUnminedBackfillDDL for details).
	if err := resolvePendingUnminedBackfill(ctx, pool, logger); err != nil {
		return err
	}

	// Partial indexes on txs for iterator/pruner queries. The base indexes are
	// always applied. The legacy px_delete_at_height BRIN on txs is no longer
	// used: pending_deletes is the only pruner path, so the BRIN is
	// unconditionally backfilled into pending_deletes (for any orphaned rows on
	// an existing DB) and dropped. The pending_deletes table is created above so
	// the backfill INSERT can run while the BRIN still exists (if present) for a
	// fast index-assisted scan.
	if _, err := pool.Exec(ctx, txsIndexesDDLBase); err != nil {
		return errors.NewStorageError("txs base index creation failed", err)
	}
	if _, err := pool.Exec(ctx, txsDAHBrinBackfillAndDropDDL); err != nil {
		return errors.NewStorageError("px_delete_at_height backfill+drop failed", err)
	}

	// Setter-C counter columns: add to existing DBs idempotently. Both columns
	// are deliberately UNINDEXED — indexing either would disqualify HOT updates
	// on txs (measured: HOT ratio collapses from 83%+ to 33% when a btree covers
	// a column touched by the sweep UPDATE). The migration is a no-op if the
	// columns already exist (ALTER TABLE ... ADD COLUMN IF NOT EXISTS).
	if _, err := pool.Exec(ctx, txsSetterCMigrationDDL); err != nil {
		return errors.NewStorageError("setter-c counter column migration failed", err)
	}

	// Playbook §4: LZ4 compression on raw_tx (faster than default pglz).
	_, _ = pool.Exec(ctx, `ALTER TABLE txs ALTER COLUMN raw_tx SET COMPRESSION lz4`)

	// NOTE: autovacuum tuning lives on the leaf partitions (see partitionSpec
	// above), not here. A parent-level ALTER TABLE txs SET (autovacuum_*) is a
	// no-op for vacuum scheduling — autovacuum reads the leaf's reloptions — so
	// the previous parent-level block was removed to avoid a false sense of
	// safety. The effective per-partition values are the ones in the loop.

	return nil
}

// ---------------------------------------------------------------------------
// Table DDL — 2 LOGGED hash-partitioned tables (txs + spends)
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + raw_tx + block_ids (arrays) +
// conflicting_children (array) + PACKED per-output UTXO columns. LOGGED — UTXO
// set is durable state.
//
// Packed per-output encoding ("array packing", replaces the previous 5 parallel
// per-output array columns — a hot-path CPU fix: bulk create no longer pays
// per-row array_agg re-aggregation, and per-output access is O(1) byte
// arithmetic instead of per-element array CASE/array_length evaluation):
//
//	utxo_hashes              — flat concatenation, 32 bytes per output; output i
//	                           lives at byte offset i*32 (substr(.., i*32+1, 32),
//	                           substr is 1-based). NULL when no outputs.
//	out_count                — number of outputs (slot_exists test: idx < out_count).
//	spendable_count          — count of spendable outputs; the deferred-DAH
//	                           "fully spent" comparand.
//	out_spendables           — bitmap, bit i = output i is spendable. Encoding
//	                           matches PostgreSQL get_bit(): bit n lives in byte
//	                           n/8 at position n%8 counting from the LEAST
//	                           significant bit (Go: buf[i/8] |= 1 << (i%8)).
//	                           NULL when no outputs.
//	out_frozens              — same bitmap encoding; NULL means "no output frozen"
//	                           (the common case — freeze materialises it on demand,
//	                           sized to (out_count+7)/8 bytes).
//	coinbase_spending_height — scalar coinbase maturity height (0 = non-coinbase).
//	                           The old per-output array was redundant: every output
//	                           of a coinbase tx gets the SAME maturity height.
//	spendable_ins            — per-output spendable_in height, kept as an INT[]
//	                           (set only by rare ReAssignUTXO; NULL common case;
//	                           NULL element = no restriction). 0-based output
//	                           index i → Postgres 1-based subscript [i+1].
//
// MIGRATION NOTE: this packed layout replaces the v7 array columns
// (utxo_hashes BYTEA[], out_spendables BOOLEAN[], out_frozens BOOLEAN[],
// coinbase_spending_heights BIGINT[], spendable_ins kept as-is). No live
// migration DDL is provided: bench databases are recreated from scratch and CI
// creates fresh schemas. A pre-existing database with the array layout must be
// dropped and re-synced.
const txsDDL = `
CREATE TABLE IF NOT EXISTS txs (
    hash                      BYTEA PRIMARY KEY,
    version                   BIGINT NOT NULL,
    lock_time                 BIGINT NOT NULL,
    fee                       BIGINT NOT NULL,
    size_in_bytes             BIGINT NOT NULL,
    coinbase                  BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx                    BYTEA,
    locked                    BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting               BOOLEAN NOT NULL DEFAULT FALSE,
    frozen                    BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since             INT,
    delete_at_height          INT,
    preserve_until            INT,
    block_ids                 INT[],
    block_heights             INT[],
    subtree_idxs              INT[],
    conflicting_children      BYTEA[],
    inserted_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mined_at_height           INT,
    utxo_hashes               BYTEA,
    out_count                 INT NOT NULL DEFAULT 0,
    spendable_count           INT NOT NULL DEFAULT 0,
    out_spendables            BYTEA,
    out_frozens               BYTEA,
    coinbase_spending_height  INT NOT NULL DEFAULT 0,
    spendable_ins             INT[],
    spent_progress            INT NOT NULL DEFAULT 0,
    last_spend_height         INT
) PARTITION BY HASH (hash);`

// Indexes on txs.
//
// unmined_since is BRIN, not btree, and that choice is HOT-path-critical:
// SetMinedMulti modifies unmined_since (-> NULL) on every mined tx, and an
// UPDATE that modifies any column covered by a non-summarizing (btree) index
// disqualifies the row from a HOT update — forcing a full row copy plus new
// entries in EVERY index on the table, including the 32-byte random-hash PK.
// Measured on the sustained-prune bench: with a partial btree here the txs HOT
// ratio was 33.7%; BRIN is a summarizing AM (PG16+), so the mined update stays
// HOT (83%+ measured). Its consumers (unmined iterators, old-unmined queries)
// are background paths that tolerate bitmap-scan rechecks.
//
// delete_at_height no longer has an index on txs: the BRIN was retired by
// txsDAHBrinBackfillAndDropDDL. The pruner now reads the pending_deletes
// btree side-table exclusively; delete_at_height on txs is a stamp-only column
// used by the DAH sweep worker and read back only on restart recovery.
// txsIndexesDDLBase contains the txs partial indexes that are always created.
const txsIndexesDDLBase = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs USING brin (unmined_since) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS px_preserve_until ON txs (preserve_until) WHERE preserve_until IS NOT NULL;`

// txsDAHBrinBackfillAndDropDDL is the one-time migration that retires the legacy
// px_delete_at_height BRIN index on txs now that pending_deletes is the only
// pruner path. It runs unconditionally on every startup.
//
// The gated DO block runs only when the BRIN index px_delete_at_height still
// exists (i.e. the DB was previously running the old inline-delete path). In
// that case it backfills pending_deletes from any txs rows that already carry a
// non-NULL delete_at_height (orphaned rows the side-table pruner would otherwise
// never see), then drops the BRIN. The INSERT runs BEFORE the DROP so it can use
// the BRIN for a fast index-assisted scan rather than a full sequential scan.
//
// ON CONFLICT (hash) DO NOTHING makes the INSERT idempotent: rows already in
// pending_deletes (e.g. from a partial earlier run) are skipped cleanly.
//
// On all subsequent startups the BRIN is already gone so the IF EXISTS guard is
// false — neither the INSERT nor the DROP executes, avoiding a seq-scan.
const txsDAHBrinBackfillAndDropDDL = `
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height') THEN
    INSERT INTO pending_deletes (hash, delete_at_height)
      SELECT hash, delete_at_height FROM txs WHERE delete_at_height IS NOT NULL
      ON CONFLICT (hash) DO NOTHING;
    DROP INDEX px_delete_at_height;
  END IF;
END $$;`

// txsSetterCMigrationDDL adds the two Setter-C counter columns to existing txs
// tables on DB startup. Both use ADD COLUMN IF NOT EXISTS so the statement is
// safe to re-run on every startup.
//
// spent_progress INT NOT NULL DEFAULT 0 — count of distinct spendable outputs
//
//	the background sweep consumer has folded for this tx. The DAH stamp fires
//	when spent_progress = spendable_count (all outputs fully spent). Initialised
//	to 0, not spendable_count: the consumer folds forward-only from scratch;
//	a live-DB backfill is a deferred decision handled by a separate migration.
//
// last_spend_height INT (nullable) — running max spent_at_height across all
//
//	spendable outputs folded so far. NULL until the first spendable spend is
//	folded. Used by the sweep to stamp delete_at_height = last_spend_height.
//
// NEITHER column is indexed. An UPDATE touching any btree-indexed column on txs
// disqualifies the row from a HOT update (forcing a full row copy + index
// maintenance on the 32-byte random-hash PK), collapsing the HOT ratio from
// 83%+ to ~34% (measured). delete_at_height is the canonical unindexed precedent.
const txsSetterCMigrationDDL = `
ALTER TABLE txs ADD COLUMN IF NOT EXISTS spent_progress    INT NOT NULL DEFAULT 0;
ALTER TABLE txs ADD COLUMN IF NOT EXISTS last_spend_height INT;`

// pendingDeletesDDL creates the pending_deletes partitioned parent table. Each
// hash partition leaf is created separately in createSchemaWithPoolFlag.
// The table stores (hash, delete_at_height) rows for the pruner to consume;
// the stamp path populates it and the pruner clears it as rows are deleted.
const pendingDeletesDDL = `
CREATE TABLE IF NOT EXISTS pending_deletes (
    hash             BYTEA NOT NULL,
    delete_at_height INT   NOT NULL,
    PRIMARY KEY (hash)
) PARTITION BY HASH (hash);`

// pendingUnminedDDL creates the pending_unmined partitioned parent table. Each
// hash partition leaf is created separately in createSchemaWithPoolFlag.
// The table stores (hash, unmined_since) rows for the iterator and pruner to
// consume; the projection invariant maintains it in line with txs mutations.
// ALWAYS-ON: no feature flag guards this table — it is created unconditionally.
const pendingUnminedDDL = `
CREATE TABLE IF NOT EXISTS pending_unmined (
    hash           BYTEA NOT NULL,
    unmined_since  INT   NOT NULL,
    PRIMARY KEY (hash)
) PARTITION BY HASH (hash);`

// pendingUnminedBackfillDropLegacyMarkerDDL drops the retired one-shot backfill
// marker index. It runs unconditionally on every startup (a cheap no-op once
// the index is already gone) — unlike pendingUnminedBackfillDDL below, this
// statement carries no seq-scan cost, so it does not need the clean-shutdown
// gate.
const pendingUnminedBackfillDropLegacyMarkerDDL = `
DROP INDEX IF EXISTS px_pu_backfill_marker;`

// pendingUnminedBackfillDDL is the idempotent reconciliation backfill that
// repairs pending_unmined from txs. The create hot path no longer writes
// pending_unmined synchronously — rows are projected by the in-process
// write-behind projector (see pending_unmined_projector.go) — so an UNCLEAN
// stop (crash) can lose the projector's not-yet-flushed buffer. This
// INSERT..SELECT repairs any such gap by copying every non-conflicting unmined
// tx from txs (one seq scan). ON CONFLICT (hash) DO NOTHING makes re-runs free
// for rows already present.
//
// It is now gated by the store_clean_shutdown marker (see
// resolvePendingUnminedBackfill): a graceful Store.Stop()/Close() already
// performs a final drain of the write-behind buffer via
// stopPendingUnminedProjector(), which makes pending_unmined complete and
// correct on its own — so after a CLEAN shutdown this seq scan is redundant
// and is skipped. On a production mainnet database (hundreds of millions of
// txs rows) that seq scan costs minutes; skipping it on the common clean-
// restart path removes that cost entirely. It still runs whenever the marker
// says the previous shutdown was unclean, is missing, or fails to read — the
// fail-safe direction that preserves today's behaviour for crash recovery.
const pendingUnminedBackfillDDL = `
INSERT INTO pending_unmined (hash, unmined_since)
  SELECT hash, unmined_since FROM txs
  WHERE unmined_since IS NOT NULL AND conflicting = false
  ON CONFLICT (hash) DO NOTHING;`

// storeCleanShutdownDDL creates the single-row marker table that records
// whether the store's previous run shut down cleanly. Additive/idempotent:
// CREATE TABLE IF NOT EXISTS plus a seeding INSERT that is a no-op once the row
// exists, so it is safe to run unconditionally on every startup — including
// against a pre-existing database that predates this table, which naturally
// seeds clean=false and so falls into the fail-safe backfill branch on its
// first startup with this schema version.
//
// id=1 is the only row ever used; there is exactly one store per database.
const storeCleanShutdownDDL = `
CREATE TABLE IF NOT EXISTS store_clean_shutdown (
    id INT PRIMARY KEY,
    clean BOOLEAN NOT NULL,
    stamped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO store_clean_shutdown (id, clean, stamped_at) VALUES (1, false, now()) ON CONFLICT (id) DO NOTHING;`

// resolvePendingUnminedBackfill gates the pending_unmined reconciliation
// backfill (pendingUnminedBackfillDDL) on the store_clean_shutdown marker.
//
// It first ensures the marker table exists (idempotent, safe every startup),
// then reads the id=1 row's clean value:
//   - clean == true:  the previous shutdown already drained the write-behind
//     projector (Store.Stop/Close → stopPendingUnminedProjector), so
//     pending_unmined is already complete. The backfill seq scan is skipped.
//   - clean == false, the row is missing, or the read errors for any reason:
//     the fail-safe direction — run the full backfill, exactly as before this
//     gate existed.
//
// In both branches the marker is then stamped clean=false: the store is now
// running, so any crash from this point forward must be treated as unclean on
// the next startup.
func resolvePendingUnminedBackfill(ctx context.Context, pool *pgxpool.Pool, logger ulogger.Logger) error {
	if _, err := pool.Exec(ctx, storeCleanShutdownDDL); err != nil {
		return errors.NewStorageError("store_clean_shutdown marker table creation failed", err)
	}

	var clean bool

	queryErr := pool.QueryRow(ctx, `SELECT clean FROM store_clean_shutdown WHERE id = 1`).Scan(&clean)

	if queryErr == nil && clean {
		logger.Infof("[pendingUnminedBackfill] skipped: previous shutdown was clean, pending_unmined is already reconciled")
	} else {
		if queryErr != nil {
			logger.Infof("[pendingUnminedBackfill] running full backfill: store_clean_shutdown marker missing or unreadable (%v)", queryErr)
		} else {
			logger.Infof("[pendingUnminedBackfill] running full backfill: previous shutdown was unclean")
		}

		if _, err := pool.Exec(ctx, pendingUnminedBackfillDDL); err != nil {
			return errors.NewStorageError("pending_unmined backfill failed", err)
		}
	}

	if _, err := pool.Exec(ctx, `UPDATE store_clean_shutdown SET clean = false, stamped_at = now() WHERE id = 1`); err != nil {
		return errors.NewStorageError("store_clean_shutdown marker update failed", err)
	}

	return nil
}

// spends: append-only spend records. LOGGED. A row here is the canonical
// "this output was spent by that tx" marker; Unspend deletes the row, and
// the pruner removes all rows for a parent_tx before removing the parent.
//
// prev_output_idx and spent_at_height are INT4, not BIGINT: a vout is a
// uint32 that can never reach 2^31 in a protocol-valid tx (guarded at the Go
// boundary — see voutToInt32), and block heights stay far below 2^31. The
// narrowing shrinks every heap row by 8 bytes and every UNIQUE-index entry by
// 4 — fewer bytes to copy/compare on each insert, probe, and reclaim delete
// in this hottest of tables. The same reasoning narrows the txs height
// columns above (unmined_since/delete_at_height/preserve_until/
// mined_at_height/coinbase_spending_height).
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA NOT NULL,
    prev_output_idx INT   NOT NULL,
    spending_data   BYTEA NOT NULL,
    spent_at_height INT,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`
