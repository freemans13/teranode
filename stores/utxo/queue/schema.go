package queue

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v4 append-only snapshot tables in the connected
// PostgreSQL database. There are no queue tables, stored procedures, pg_cron
// jobs, or batch_notifications -- every operation writes directly.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table and the fillfactor for its children.
type partitionSpec struct {
	name       string
	fillfactor int
}

// createSchemaWithPool executes all DDL statements using the provided pool.
// It is separated from createSchema so tests can call it directly.
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	ddlStatements := []string{
		transactionsDDL,
		txStateDDL,
		txStateIndexesDDL,
		inputsDDL,
		outputsDDL,
		spendsDDL,
		blockIDsDDL,
		conflictingChildrenDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, ddl)
		}
	}

	// Create 64 hash partitions for each table with appropriate fillfactor.
	// PostgreSQL does not allow storage parameters on partitioned parent tables,
	// so fillfactor is set on each child partition.
	tables := []partitionSpec{
		{"transactions", 100},
		{"tx_state", 50},
		{"inputs", 100},
		{"outputs", 100},
		{"spends", 100},
		{"block_ids", 100},
		{"conflicting_children", 100},
	}
	for _, spec := range tables {
		for i := 0; i < 64; i++ {
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s_p%02d PARTITION OF %s FOR VALUES WITH (MODULUS 64, REMAINDER %d) WITH (fillfactor = %d)",
				spec.name, i, spec.name, i, spec.fillfactor,
			)
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("partition creation failed for %s_p%02d: %v", spec.name, i, err)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Table DDL -- append-only snapshot tables (no queue tables)
//
// Note: fillfactor is specified on child partitions, not the parent table,
// because PostgreSQL does not support storage parameters on partitioned tables.
// ---------------------------------------------------------------------------

// transactions: immutable transaction data. PARTITION BY HASH 64-way.
// fillfactor=100 (immutable, no HOT updates expected).
const transactionsDDL = `
CREATE TABLE IF NOT EXISTS transactions (
    hash             BYTEA       NOT NULL,
    version          BIGINT      NOT NULL,
    lock_time        BIGINT      NOT NULL,
    fee              BIGINT      NOT NULL,
    size_in_bytes    BIGINT      NOT NULL,
    coinbase         BOOLEAN     NOT NULL DEFAULT FALSE,
    inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hash)
) PARTITION BY HASH (hash);`

// tx_state: mutable per-transaction state, separated from immutable data.
// PARTITION BY HASH 64-way. fillfactor=50 (frequent HOT updates).
const txStateDDL = `
CREATE TABLE IF NOT EXISTS tx_state (
    tx_hash          BYTEA   NOT NULL,
    locked           BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting      BOOLEAN NOT NULL DEFAULT FALSE,
    frozen           BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since    BIGINT,
    delete_at_height BIGINT,
    preserve_until   BIGINT,
    PRIMARY KEY (tx_hash)
) PARTITION BY HASH (tx_hash);`

// Partial indexes on tx_state for efficient queries.
const txStateIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON tx_state (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON tx_state (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// inputs: immutable transaction inputs. PARTITION BY HASH 64-way.
// fillfactor=100 (immutable).
const inputsDDL = `
CREATE TABLE IF NOT EXISTS inputs (
    tx_hash                     BYTEA  NOT NULL,
    idx                         BIGINT NOT NULL,
    previous_transaction_hash   BYTEA  NOT NULL,
    previous_tx_idx             BIGINT NOT NULL,
    previous_tx_satoshis        BIGINT NOT NULL,
    previous_tx_script          BYTEA,
    unlocking_script            BYTEA  NOT NULL,
    sequence_number             BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH (tx_hash);`

// outputs: immutable transaction outputs. NO spending_data column.
// PARTITION BY HASH 64-way. fillfactor=100 (immutable).
const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                 BYTEA   NOT NULL,
    idx                     BIGINT  NOT NULL,
    locking_script          BYTEA   NOT NULL,
    satoshis                BIGINT  NOT NULL,
    frozen                  BOOLEAN DEFAULT FALSE,
    spendable_in            INT,
    utxo_hash               BYTEA   NOT NULL,
    coinbase_spending_height BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tx_hash, idx)
) PARTITION BY HASH (tx_hash);`

// spends: append-only spend records. UNIQUE on (prev_tx_hash, prev_output_idx).
// PARTITION BY HASH 64-way. fillfactor=100 (append-only).
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`

// block_ids: which blocks a transaction appears in. PARTITION BY HASH 64-way.
// fillfactor=100 (append-only).
const blockIDsDDL = `
CREATE TABLE IF NOT EXISTS block_ids (
    tx_hash      BYTEA  NOT NULL,
    block_id     BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    subtree_idx  BIGINT NOT NULL,
    PRIMARY KEY (tx_hash, block_id)
) PARTITION BY HASH (tx_hash);`

// conflicting_children: parent-child conflict relationships.
// PARTITION BY HASH 64-way. fillfactor=100 (append-only).
const conflictingChildrenDDL = `
CREATE TABLE IF NOT EXISTS conflicting_children (
    tx_hash       BYTEA NOT NULL,
    child_tx_hash BYTEA NOT NULL,
    PRIMARY KEY (tx_hash, child_tx_hash)
) PARTITION BY HASH (tx_hash);`
