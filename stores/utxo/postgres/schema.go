package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
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
	if err := createSchemaWithPoolFlag(ctx, s.pool, s.settings.UtxoStore.PostgresUsePendingDeletesTable); err != nil {
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

// createSchemaWithPool executes all DDL statements using the provided pool
// with the pending_deletes feature disabled (legacy default).
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	return createSchemaWithPoolFlag(ctx, pool, false)
}

// createSchemaWithPoolFlag executes all DDL statements using the provided pool.
// When usePendingDeletes is true the pending_deletes partitioned side-table (8
// leaves) is created and the px_delete_at_height BRIN index on txs is dropped
// (the pruner reads from the side-table instead). When false the BRIN index is
// created and no pending_deletes tables are created.
func createSchemaWithPoolFlag(ctx context.Context, pool *pgxpool.Pool, usePendingDeletes bool) error {
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
		idxStmts := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS spends_p%02d_spent_at_height_brin ON spends_p%02d USING brin (spent_at_height) WITH (pages_per_range = 32, autosummarize = on)`, i, i),
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

	// pending_deletes side-table: when enabled, the pruner populates this
	// table with (hash, delete_at_height) rows and reads from it directly
	// rather than scanning txs via the BRIN index. Each hash partition gets
	// its own leaf and a btree index on delete_at_height for efficient
	// height-range scans by the pruner.
	//
	// This block is intentionally placed BEFORE the BRIN drop/backfill block
	// below so that pending_deletes exists when the backfill INSERT runs.
	if usePendingDeletes {
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
	}

	// Partial indexes on txs for iterator/pruner queries. The base indexes are
	// always applied. The BRIN on delete_at_height is conditional: when the
	// pending_deletes side-table is enabled the pruner reads from that table
	// instead, so the BRIN is unnecessary overhead — drop it (with a one-time
	// backfill first). When disabled (default), the pruner scans txs directly
	// and the BRIN is required.
	//
	// The pending_deletes table is created above (flag-ON path) so the backfill
	// INSERT can run while the BRIN still exists for a fast index-assisted scan.
	if _, err := pool.Exec(ctx, txsIndexesDDLBase); err != nil {
		return errors.NewStorageError("txs base index creation failed", err)
	}
	if usePendingDeletes {
		if _, err := pool.Exec(ctx, txsDAHBrinBackfillAndDropDDL); err != nil {
			return errors.NewStorageError("px_delete_at_height backfill+drop failed", err)
		}
	} else {
		if _, err := pool.Exec(ctx, txsDAHBrinDDL); err != nil {
			return errors.NewStorageError("px_delete_at_height creation failed", err)
		}
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
    spendable_ins             INT[]
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
// delete_at_height is BRIN for the same HOT reason: the DAH sweep stamps it on
// (almost) every tx once, and a btree here makes every stamp a non-HOT row
// rewrite (measured: stamp cost 70ms -> 132ms per 5K-candidate sweep call with
// the btree, the sweep saturating ~57-78K stamps/s and falling behind an ~88K
// create rate). With BRIN the stamp stays HOT. The pruner's doomed-row scan
// (delete_at_height <= H LIMIT N) tolerates BRIN's bitmap rechecks while the
// table is bounded; both index choices were A/B'd under sustained load and
// BRIN-everywhere is the best-known configuration (88K vs 65-71K medians).
// txsIndexesDDLBase contains the txs partial indexes that are always created
// regardless of the pending_deletes flag.
const txsIndexesDDLBase = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs USING brin (unmined_since) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS px_preserve_until ON txs (preserve_until) WHERE preserve_until IS NOT NULL;`

// txsDAHBrinDDL creates the BRIN index on txs.delete_at_height used by the
// pruner when the pending_deletes side-table is NOT in use. See the comment on
// delete_at_height in txsDDL for the HOT-chain rationale.
const txsDAHBrinDDL = `CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs USING brin (delete_at_height) WITH (pages_per_range = 32, autosummarize = on);`

// txsDAHBrinDropDDL drops the BRIN index when the pending_deletes side-table
// IS in use. The pruner reads from pending_deletes directly, so the BRIN index
// on txs is unnecessary overhead.
const txsDAHBrinDropDDL = `DROP INDEX IF EXISTS px_delete_at_height;`

// txsDAHBrinBackfillAndDropDDL is the one-time migration used when the
// pending_deletes flag is turned ON against an existing database.
//
// The gated DO block runs only when the BRIN index px_delete_at_height still
// exists (i.e. the DB was previously running with flag OFF). In that case it
// backfills pending_deletes from any txs rows that already carry a non-NULL
// delete_at_height (orphaned rows the side-table pruner would otherwise never
// see), then drops the BRIN. The INSERT runs BEFORE the DROP so it can use the
// BRIN for a fast index-assisted scan rather than a full sequential scan.
//
// ON CONFLICT (hash) DO NOTHING makes the INSERT idempotent: rows already in
// pending_deletes (e.g. from a partial earlier run) are skipped cleanly.
//
// On all subsequent flag-ON startups the BRIN is already gone so the IF EXISTS
// guard is false — neither the INSERT nor the DROP executes, avoiding a seq-scan.
const txsDAHBrinBackfillAndDropDDL = `
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height') THEN
    INSERT INTO pending_deletes (hash, delete_at_height)
      SELECT hash, delete_at_height FROM txs WHERE delete_at_height IS NOT NULL
      ON CONFLICT (hash) DO NOTHING;
    DROP INDEX px_delete_at_height;
  END IF;
END $$;`

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
