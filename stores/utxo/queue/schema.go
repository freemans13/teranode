package queue

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v7 UTXO tables in the connected PostgreSQL
// database. 2 UNLOGGED tables (txs, spends) with 16 partitions each.
// Outputs are embedded in txs as parallel arrays.
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
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, ddl)
		}
	}

	// Create 16 hash partitions for each table with appropriate fillfactor.
	tables := []partitionSpec{
		{"txs", 70},     // HOT updates — reserve 30% for in-place updates
		{"spends", 100}, // append-only
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
// Table DDL — 2 UNLOGGED tables (v7: outputs embedded in txs as arrays)
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + inputs (raw_tx) + block_ids
// (arrays) + conflicting_children (array) + output arrays. UNLOGGED for performance.
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
    -- output arrays (v7 — replaces outputs table)
    utxo_hashes          BYTEA[],
    locking_scripts      BYTEA[],
    satoshis_arr         BIGINT[],
    coinbase_heights     BIGINT[],
    frozen_outputs       BOOLEAN[],
    spendable_in_arr     INT[]
) PARTITION BY HASH (hash);`

// Partial indexes on txs.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

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
`
