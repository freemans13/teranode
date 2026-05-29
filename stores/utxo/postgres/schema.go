package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v5 turbo UTXO tables in the connected PostgreSQL
// database: 3 LOGGED hash-partitioned tables (txs, outputs, spends).
//
// Schema design tradeoffs (vs. the standard stores/utxo/sql store):
//
//   - No foreign keys. Child rows (outputs, spends) reference txs by BYTEA
//     hash rather than a surrogate id, and pruning explicitly cascades via
//     DELETE FROM spends → outputs → txs inside one txn (see pruner_provider).
//     Skips the FK check/maintenance cost on the write path but removes the
//     DB-level safety net — any bug that deletes txs without the matching
//     spends/outputs leaves orphans.
//
//   - "Is this output spent?" lives in the spends table as a row, not as a
//     nullable spending_data column on outputs. Spends become pure INSERTs
//     (no MVCC bloat on outputs) at the cost of needing a LEFT JOIN spends
//     on every spend-validation query.
//
//   - block_ids / block_heights / subtree_idxs / conflicting_children are
//     INT[] / BYTEA[] arrays on txs instead of separate tables. Reads are
//     one column on one row; appends (new block_id on mine) are cheap via
//     `|| $N::int[]`. A full reorg that touches millions of rows still
//     has to rewrite whole arrays — heavy-reorg cost > separate-table
//     variant that just INSERTs/DELETEs rows.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table and the fillfactor for its children.
type partitionSpec struct {
	name       string
	fillfactor int
}

// numPartitions is the number of hash partitions per table. Local benchmarking
// shows a single partition outperforms a higher partition count — the partition
// machinery is kept so we can raise this if future profiling shows contention
// at the PK level on larger deployments.
const numPartitions = 1

// createSchemaWithPool executes all DDL statements using the provided pool.
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	ddlStatements := []string{
		txsDDL,
		outputsDDL,
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, ddl)
		}
	}

	// Create numPartitions hash partitions for each table with appropriate fillfactor.
	tables := []partitionSpec{
		{"txs", 70},      // HOT updates — reserve 30% for in-place updates
		{"outputs", 100}, // immutable
		{"spends", 100},  // append-only
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

			// Aggressive autovacuum for these high-churn tables (idempotent, so
			// it also back-fills partitions created before this setting existed).
			// insert_scale_factor is the important one: spends/outputs are
			// append-only, so delete/update-driven vacuum never fires from
			// inserts — but the visibility map must stay fresh or the deferred-DAH
			// sweep's index-only count subqueries fall back to heap fetches.
			// Insert-triggered autovacuum keeps the VM current. Cost limit/delay
			// are sized to keep pace under legacy-sync write volume without
			// starving the 2GB-budget instance.
			av := fmt.Sprintf(
				"ALTER TABLE %s_p%02d SET ("+
					"autovacuum_vacuum_scale_factor = 0.05, "+
					"autovacuum_vacuum_insert_scale_factor = 0.02, "+
					"autovacuum_vacuum_cost_limit = 2000, "+
					"autovacuum_vacuum_cost_delay = 2, "+
					"autovacuum_analyze_scale_factor = 0.05)",
				spec.name, i,
			)
			if _, err := pool.Exec(ctx, av); err != nil {
				return errors.NewStorageError("autovacuum tuning failed for %s_p%02d: %v", spec.name, i, err)
			}
		}
	}

	// BRIN indexes on the monotonic height columns. spends is append-only and
	// inserted in increasing height order → BRIN is near-free on insert (summary
	// per heap range, no per-row entries) and selective for recent-height scans.
	for i := 0; i < numPartitions; i++ {
		brinStmts := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS spends_p%02d_spent_at_height_brin ON spends_p%02d USING brin (spent_at_height) WITH (pages_per_range = 32, autosummarize = on)`, i, i),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS txs_p%02d_mined_at_height_brin ON txs_p%02d USING brin (mined_at_height) WITH (pages_per_range = 32, autosummarize = on)`, i, i),
		}
		for _, ddl := range brinStmts {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("brin index creation failed: %v", err)
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

	// Partial indexes on txs for iterator/pruner queries.
	if _, err := pool.Exec(ctx, txsIndexesDDL); err != nil {
		return errors.NewStorageError("index creation failed: %v", err)
	}

	// Playbook §4: LZ4 compression on raw_tx (faster than default pglz).
	_, _ = pool.Exec(ctx, `ALTER TABLE txs ALTER COLUMN raw_tx SET COMPRESSION lz4`)

	// Playbook §9: aggressive autovacuum on hot-update table.
	_, _ = pool.Exec(ctx, `ALTER TABLE txs SET (
		autovacuum_vacuum_scale_factor = 0.01,
		autovacuum_analyze_scale_factor = 0.005,
		autovacuum_vacuum_cost_delay = 2,
		autovacuum_vacuum_insert_threshold = 1000
	)`)

	return nil
}

// ---------------------------------------------------------------------------
// Table DDL — 3 LOGGED hash-partitioned tables
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + inputs (raw_tx) + block_ids
// (arrays) + conflicting_children (array). LOGGED — UTXO set is durable state.
const txsDDL = `
CREATE TABLE IF NOT EXISTS txs (
    hash                 BYTEA PRIMARY KEY,
    version              BIGINT NOT NULL,
    lock_time            BIGINT NOT NULL,
    fee                  BIGINT NOT NULL,
    size_in_bytes        BIGINT NOT NULL,
    coinbase             BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx               BYTEA,
    locked               BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting          BOOLEAN NOT NULL DEFAULT FALSE,
    frozen               BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since        BIGINT,
    delete_at_height     BIGINT,
    preserve_until       BIGINT,
    block_ids            INT[],
    block_heights        INT[],
    subtree_idxs         INT[],
    conflicting_children BYTEA[],
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mined_at_height      BIGINT
) PARTITION BY HASH (hash);`

// Partial indexes on txs.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// outputs: immutable transaction outputs. LOGGED. Spent-ness is tracked by
// row-presence in the spends table rather than a nullable column here, so
// these rows never have to be updated after Create.
const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                 BYTEA   NOT NULL,
    idx                     BIGINT  NOT NULL,
    locking_script          BYTEA   NOT NULL,
    satoshis                BIGINT  NOT NULL,
    utxo_hash               BYTEA   NOT NULL,
    coinbase_spending_height BIGINT NOT NULL DEFAULT 0,
    frozen                  BOOLEAN DEFAULT FALSE,
    spendable_in            INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH (tx_hash);`

// spends: append-only spend records. LOGGED. A row here is the canonical
// "this output was spent by that tx" marker; Unspend deletes the row, and
// the pruner removes all rows for a parent_tx before removing the parent.
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    spent_at_height BIGINT,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`
