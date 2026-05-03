package postgres

import (
	"context"
	"fmt"

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
//   - block_ids / block_heights / subtree_idxs are INT[] arrays on the
//     txs_blocks side table; conflicting_children is a BYTEA[] array on
//     the txs_conflicts side table. Side-table split keeps the hot txs
//     row narrow — these arrays are only populated during reorgs/conflict
//     resolution and during mining updates respectively.
//
// Partitioning: LIST on a plain SMALLINT `partition_key` column whose value
// is computed at INSERT time as `get_byte(<key>, 1) % NumPartitions`. We
// can't use a GENERATED ALWAYS STORED expression as a partition key
// (PostgreSQL rejects it with SQLSTATE 42P17), so the value is supplied
// explicitly by every INSERT. Byte 0 is reserved for future shard routing
// (see routing.go). PRIMARY KEY / UNIQUE constraints live on the parent —
// they include `partition_key` so they're valid partition-key supersets,
// and PostgreSQL propagates them to every child automatically.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// partitionSpec defines a partitioned table and its child fillfactor.
// Per-child PK/UNIQUE creation is no longer needed: the parent-level
// constraints already include partition_key, so they propagate.
type partitionSpec struct {
	name       string
	fillfactor int
}

// createSchemaWithPool executes all DDL statements using the provided pool.
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	ddlStatements := []string{
		txsDDL,
		txsRawDDL,
		txsBlocksDDL,
		txsConflictsDDL,
		outputsDDL,
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, ddl)
		}
	}

	tables := []partitionSpec{
		{name: "txs", fillfactor: 70},
		{name: "txs_raw", fillfactor: 100},
		{name: "txs_blocks", fillfactor: 70},
		{name: "txs_conflicts", fillfactor: 100},
		{name: "outputs", fillfactor: 100},
		{name: "spends", fillfactor: 100},
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
		}
	}

	// Partial indexes on txs for iterator/pruner queries. Created on the
	// parent — PostgreSQL propagates them to each existing partition.
	if _, err := pool.Exec(ctx, txsIndexesDDL); err != nil {
		return errors.NewStorageError("index creation failed: %v", err)
	}

	// LZ4 compression on raw_tx (faster than default pglz).
	_, _ = pool.Exec(ctx, `ALTER TABLE txs_raw ALTER COLUMN raw_tx SET COMPRESSION lz4`)

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
// Table DDL — 3 LOGGED list-partitioned tables
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state. LOGGED — UTXO set is
// durable state. raw_tx lives in the txs_raw side table;
// block_ids/heights/subtree_idxs live in the txs_blocks side table;
// conflicting_children lives in the txs_conflicts side table. The hot
// row is just metadata + flags + delete_at_height/preserve_until.
const txsDDL = `
CREATE TABLE IF NOT EXISTS txs (
    hash             BYTEA NOT NULL,
    partition_key    SMALLINT NOT NULL,
    version          BIGINT NOT NULL,
    lock_time        BIGINT NOT NULL,
    fee              BIGINT NOT NULL,
    size_in_bytes    BIGINT NOT NULL,
    coinbase         BOOLEAN NOT NULL DEFAULT FALSE,
    locked           BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting      BOOLEAN NOT NULL DEFAULT FALSE,
    frozen           BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since    BIGINT,
    delete_at_height BIGINT,
    preserve_until   BIGINT,
    inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hash, partition_key)
) PARTITION BY LIST (partition_key);`

// txs_raw: side table holding the serialized raw transaction bytes. Split
// out of txs to keep the hot row narrow — raw_tx is the largest column
// (200B–500B per typical tx, larger for batched/coinbase) and is only read
// when the caller asks for the Tx body. Partition-aligned 1:1 with txs by
// (hash, partition_key); LEFT JOIN on Get-with-body reads only the
// matching child partition.
const txsRawDDL = `
CREATE TABLE IF NOT EXISTS txs_raw (
    hash          BYTEA NOT NULL,
    partition_key SMALLINT NOT NULL,
    raw_tx        BYTEA,
    PRIMARY KEY (hash, partition_key)
) PARTITION BY LIST (partition_key);`

// txs_blocks: side table holding mining-chain arrays. Split out of txs
// because these arrays grow over time (reorgs, multiple-block confirmations)
// and aren't read on the validator hot path. Keeping them off `txs` skips
// array-rewrite WAL on every mining update touching unrelated columns.
// Partition-aligned 1:1 with txs by (hash, partition_key). Fillfactor 70
// because rows can grow as arrays append on mining updates.
const txsBlocksDDL = `
CREATE TABLE IF NOT EXISTS txs_blocks (
    hash          BYTEA NOT NULL,
    partition_key SMALLINT NOT NULL,
    block_ids     INT[],
    block_heights INT[],
    subtree_idxs  INT[],
    PRIMARY KEY (hash, partition_key)
) PARTITION BY LIST (partition_key);`

// txs_conflicts: side table holding the conflicting_children BYTEA[]
// array. Split out of txs because conflicting_children is set only during
// reorgs / conflict resolution — empty for 99.9% of txs but still costs
// row width on the hot path. Partition-aligned 1:1 with txs by
// (hash, partition_key); fillfactor 100 because rows are typically
// inserted once and never updated outside conflict resolution windows.
const txsConflictsDDL = `
CREATE TABLE IF NOT EXISTS txs_conflicts (
    hash                 BYTEA NOT NULL,
    partition_key        SMALLINT NOT NULL,
    conflicting_children BYTEA[],
    PRIMARY KEY (hash, partition_key)
) PARTITION BY LIST (partition_key);`

// Partial indexes on txs — defined on the parent so PG propagates them to
// each child partition.
const txsIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON txs (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// outputs: immutable transaction outputs. LOGGED. Spent-ness is tracked by
// row-presence in the spends table rather than a nullable column here.
const outputsDDL = `
CREATE TABLE IF NOT EXISTS outputs (
    tx_hash                  BYTEA   NOT NULL,
    partition_key            SMALLINT NOT NULL,
    idx                      BIGINT  NOT NULL,
    locking_script           BYTEA   NOT NULL,
    satoshis                 BIGINT  NOT NULL,
    utxo_hash                BYTEA   NOT NULL,
    coinbase_spending_height BIGINT NOT NULL DEFAULT 0,
    frozen                   BOOLEAN DEFAULT FALSE,
    spendable_in             INT,
    PRIMARY KEY (tx_hash, idx, partition_key)
) PARTITION BY LIST (partition_key);`

// spends: append-only spend records. LOGGED. A row here is the canonical
// "this output was spent by that tx" marker.
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA  NOT NULL,
    partition_key   SMALLINT NOT NULL,
    prev_output_idx BIGINT NOT NULL,
    spending_data   BYTEA  NOT NULL,
    UNIQUE (prev_tx_hash, prev_output_idx, partition_key)
) PARTITION BY LIST (partition_key);`

// createStagingTablesSQL is executed per-connection to ensure temp staging
// tables exist for the COPY-based create batcher (rare-path fallback for
// items with MinedBlockInfos / Conflicting).
const createStagingTablesSQL = `
CREATE TEMP TABLE IF NOT EXISTS staging_txs (LIKE txs EXCLUDING ALL) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS staging_outputs (LIKE outputs EXCLUDING ALL) ON COMMIT DELETE ROWS;
`
