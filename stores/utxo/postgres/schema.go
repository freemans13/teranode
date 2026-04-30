package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v5 turbo UTXO tables in the connected PostgreSQL
// database: 3 LOGGED list-partitioned tables (txs, outputs, spends).
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
//     INT[] / BYTEA[] arrays on txs instead of separate tables.
//
// Partitioning: LIST on the expression `(get_byte(<key>, 1) % NumPartitions)`
// so the Go client can compute the partition trivially from byte 1 of the
// hash without replicating PostgreSQL's hash function. Byte 0 is reserved
// for future shard routing (see routing.go). PKs / unique constraints live
// on each child partition rather than the parent — this lets the hot path
// address `<table>_pK` directly with `ON CONFLICT (<key>) DO NOTHING`,
// skipping the parent's planning and lock fanout.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table, its child fillfactor, and the
// per-child unique-key DDL fragment.
type partitionSpec struct {
	name       string
	fillfactor int
	// uniqueCols is the column list for the per-child PRIMARY KEY (or UNIQUE
	// for spends). Keeping the unique constraint on each child lets direct
	// per-partition INSERTs use ON CONFLICT (<cols>) DO NOTHING.
	uniqueCols string
	// usePrimaryKey: if true the per-child constraint is a PRIMARY KEY,
	// otherwise a UNIQUE constraint (used for spends which has no surrogate
	// PK column).
	usePrimaryKey bool
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
		{name: "txs", fillfactor: 70, uniqueCols: "hash", usePrimaryKey: true},
		{name: "outputs", fillfactor: 100, uniqueCols: "tx_hash, idx", usePrimaryKey: true},
		{name: "spends", fillfactor: 100, uniqueCols: "prev_tx_hash, prev_output_idx", usePrimaryKey: false},
	}
	for _, spec := range tables {
		for i := 0; i < NumPartitions; i++ {
			child := spec.name + PartitionSuffix(i)
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES IN (%d) WITH (fillfactor = %d)",
				child, spec.name, i, spec.fillfactor,
			)
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("partition creation failed for %s: %v", child, err)
			}

			// Per-child unique constraint so direct INSERTs into the partition
			// can ON CONFLICT (<cols>) DO NOTHING. We add this *after* the
			// partition exists so it's local to the child relfilenode.
			constraintName := child + "_uk"
			var constraintDDL string
			if spec.usePrimaryKey {
				constraintDDL = fmt.Sprintf(
					"ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
					child, constraintName, spec.uniqueCols,
				)
			} else {
				constraintDDL = fmt.Sprintf(
					"ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
					child, constraintName, spec.uniqueCols,
				)
			}
			// IF NOT EXISTS isn't supported on ADD CONSTRAINT — swallow the
			// duplicate-name error so re-running the schema is idempotent.
			if _, err := pool.Exec(ctx, constraintDDL); err != nil {
				// 42P07 = duplicate_object; 42710 = duplicate_object on constraint
				// Don't fail if the constraint already exists.
				if !isDuplicateObject(err) {
					return errors.NewStorageError("constraint creation failed for %s: %v", constraintName, err)
				}
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

// isDuplicateObject returns true if err is the postgres "duplicate object"
// SQLSTATE — used to make ADD CONSTRAINT idempotent.
func isDuplicateObject(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "42P07") || strings.Contains(msg, "42710") || strings.Contains(msg, "already exists")
}

// ---------------------------------------------------------------------------
// Table DDL — 3 LOGGED list-partitioned tables
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + inputs (raw_tx) + block_ids
// (arrays) + conflicting_children (array). LOGGED — UTXO set is durable state.
const txsDDL = `
CREATE TABLE IF NOT EXISTS txs (
    hash                 BYTEA NOT NULL,
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
) PARTITION BY LIST ((get_byte(hash, 1) % 8));`

// Partial indexes on txs — defined on the parent so PG propagates them to
// each child partition.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// outputs: immutable transaction outputs. LOGGED. Spent-ness is tracked by
// row-presence in the spends table rather than a nullable column here.
const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                 BYTEA   NOT NULL,
    idx                     BIGINT  NOT NULL,
    locking_script          BYTEA   NOT NULL,
    satoshis                BIGINT  NOT NULL,
    utxo_hash               BYTEA   NOT NULL,
    coinbase_spending_height BIGINT NOT NULL DEFAULT 0,
    frozen                  BOOLEAN DEFAULT FALSE,
    spendable_in            INT
) PARTITION BY LIST ((get_byte(tx_hash, 1) % 8));`

// spends: append-only spend records. LOGGED. A row here is the canonical
// "this output was spent by that tx" marker.
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL
) PARTITION BY LIST ((get_byte(prev_tx_hash, 1) % 8));`

// createStagingTablesSQL is executed per-connection to ensure temp staging
// tables exist for the COPY-based create batcher (rare-path fallback for
// items with MinedBlockInfos / Conflicting).
const createStagingTablesSQL = `
CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs EXCLUDING ALL) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs EXCLUDING ALL) ON COMMIT DELETE ROWS;
`
