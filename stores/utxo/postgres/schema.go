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
	if err := createSchemaWithPool(ctx, s.pool); err != nil {
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

// createSchemaWithPool executes all DDL statements using the provided pool.
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	ddlStatements := []string{
		txsDDL,
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, ddl)
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
				return errors.NewStorageError("partition creation failed for %s_p%02d: %v", spec.name, i, err)
			}

			// Idempotent: also back-fills partitions created before these settings
			// existed. Set on the leaf (autovacuum ignores parent-level params).
			av := fmt.Sprintf("ALTER TABLE %s_p%02d SET (%s)", spec.name, i, spec.autovacuum)
			if _, err := pool.Exec(ctx, av); err != nil {
				return errors.NewStorageError("autovacuum tuning failed for %s_p%02d: %v", spec.name, i, err)
			}
		}
	}

	// Height-column indexes. spends.spent_at_height is append-only and inserted in
	// increasing height order → BRIN is near-free on insert (summary per heap range,
	// no per-row entries) and selective for recent-height scans, because the column
	// is physically correlated with heap order.
	//
	// txs.mined_at_height is NOT correlated: a tx is inserted unmined and its
	// mined_at_height is set by a later UPDATE, so equal heights are scattered across
	// the whole partition. A BRIN there is useless — every page range overlaps every
	// target height, so a height-range scan degenerates to a full-partition heap scan
	// (measured: ~100k lossy heap blocks, 3.3s, for a SINGLE height). The DAH sweep's
	// candidate enumeration scans by mined_at_height every window, so it needs a real
	// ordered index. A partial btree (mined_at_height IS NOT NULL → excludes the
	// unmined rows, which are never DAH candidates) makes that a 26-buffer index scan
	// (measured: 28ms for the same window, 119x). Keep the BRIN too: it stays
	// near-free and serves coarse scans without hurting the planner's btree choice.
	for i := 0; i < numPartitions; i++ {
		idxStmts := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS spends_p%02d_spent_at_height_brin ON spends_p%02d USING brin (spent_at_height) WITH (pages_per_range = 32, autosummarize = on)`, i, i),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS txs_p%02d_mined_at_height_brin ON txs_p%02d USING brin (mined_at_height) WITH (pages_per_range = 32, autosummarize = on)`, i, i),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS txs_p%02d_mined_at_height_btree ON txs_p%02d USING btree (mined_at_height) WHERE mined_at_height IS NOT NULL`, i, i),
		}
		for _, ddl := range idxStmts {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("height index creation failed: %v", err)
			}
		}
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dah_watermark (
			id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			last_swept_height BIGINT NOT NULL DEFAULT 0
		)`); err != nil {
		return errors.NewStorageError("dah_watermark creation failed: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_watermark (id, last_swept_height) VALUES (1, 0) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_watermark seed failed: %v", err)
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
		return errors.NewStorageError("dah_part_watermark creation failed: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dah_part_watermark (partition, last_swept_height)
		SELECT g, COALESCE((SELECT last_swept_height FROM dah_watermark WHERE id = 1), 0)
		FROM generate_series(0, %d) g
		ON CONFLICT (partition) DO NOTHING`, numPartitions-1)); err != nil {
		return errors.NewStorageError("dah_part_watermark seed failed: %v", err)
	}

	// dah_sweep_control: kill switch, tunable knobs, proc version, and the
	// per-CALL outcome the proc-mode adaptive ticker reads (see dah_sweep_proc.go).
	// Plain DDL, always created; the procedure itself is bootstrapped separately
	// (bootstrapped separately in Store.createSchema).
	if _, err := pool.Exec(ctx, dahSweepControlDDL); err != nil {
		return errors.NewStorageError("dah_sweep_control creation failed: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_sweep_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_sweep_control seed failed: %v", err)
	}

	// Partial indexes on txs for iterator/pruner queries.
	if _, err := pool.Exec(ctx, txsIndexesDDL); err != nil {
		return errors.NewStorageError("index creation failed: %v", err)
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
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs USING brin (unmined_since) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs USING brin (delete_at_height) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS px_preserve_until ON txs (preserve_until) WHERE preserve_until IS NOT NULL;`

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
