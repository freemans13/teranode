package queue

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v5 turbo UTXO tables in the connected PostgreSQL
// database. 3 UNLOGGED tables (txs, outputs, spends) with 16 partitions each.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table and the fillfactor for its children.
type partitionSpec struct {
	name       string
	fillfactor int
}

// numPartitions is the number of hash partitions per table.
const numPartitions = 16

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

	// Create 16 hash partitions for each table with appropriate fillfactor.
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
		}
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
// Table DDL — 3 UNLOGGED tables
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + inputs (raw_tx) + block_ids
// (arrays) + conflicting_children (array). UNLOGGED for performance.
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
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (hash);`

// Partial indexes on txs.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// outputs: immutable transaction outputs. UNLOGGED for performance.
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

// spends: append-only spend records. UNLOGGED for performance.
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`

// createStagingTablesSQL is executed per-connection to ensure temp staging
// tables exist for the COPY-based create batcher.
const createStagingTablesSQL = `
CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs EXCLUDING ALL) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs EXCLUDING ALL) ON COMMIT DELETE ROWS;
`
