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
//
// Partitioning: PARTITION BY HASH on the natural key (hash for txs,
// tx_hash for outputs, prev_tx_hash for spends). PostgreSQL handles
// partition routing internally, so no client-side partition_key column
// is required. PRIMARY KEY / UNIQUE constraints work at the parent
// because HASH partitioning makes the natural key the partition key.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table and its child fillfactor.
type partitionSpec struct {
	name       string
	fillfactor int
}

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

	tables := []partitionSpec{
		{name: "txs", fillfactor: 70},      // HOT updates — reserve 30% for in-place updates
		{name: "outputs", fillfactor: 100}, // immutable
		{name: "spends", fillfactor: 100},  // append-only
	}
	for _, spec := range tables {
		for i := 0; i < NumPartitions; i++ {
			child := spec.name + PartitionSuffix(i)
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = %d)",
				child, spec.name, NumPartitions, i, spec.fillfactor,
			)
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("partition creation failed for %s: %v", child, err)
			}
		}
	}

	// Partial indexes on txs for iterator/pruner queries. Created on the
	// parent — PostgreSQL propagates them to each existing partition.
	if _, err := pool.Exec(ctx, txsIndexesDDL); err != nil {
		return errors.NewStorageError("index creation failed: %v", err)
	}

	// LZ4 compression on raw_tx (faster than default pglz).
	_, _ = pool.Exec(ctx, `ALTER TABLE txs ALTER COLUMN raw_tx SET COMPRESSION lz4`)

	// Aggressive autovacuum on hot-update table.
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

// txs: consolidated transaction metadata + state + raw_tx + block arrays +
// conflicting_children. LOGGED. Wide-row design: every metadata Get is one
// PK lookup, one heap fetch — no JOINs.
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

// Partial indexes on txs — defined on the parent so PG propagates them to
// each child partition.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// outputs: immutable transaction outputs. LOGGED. Spent-ness lives in the
// spends table as row-presence, not a nullable column here.
const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                  BYTEA   NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    coinbase_spending_height BIGINT  NOT NULL DEFAULT 0,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH (tx_hash);`

// spends: append-only spend records. LOGGED.
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`

// createStagingTablesSQL is executed per-connection to ensure temp staging
// tables exist for the COPY-based create batcher (rare-path fallback for
// items with MinedBlockInfos / Conflicting).
const createStagingTablesSQL = `
CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs EXCLUDING ALL) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs EXCLUDING ALL) ON COMMIT DELETE ROWS;
`
