package queue

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v6 single-table UTXO schema in the connected
// PostgreSQL database. One UNLOGGED table (utxos) with 16 hash partitions.
func (s *Store) createSchema(ctx context.Context) error {
	return createSchemaWithPool(ctx, s.pool)
}

// numPartitions is the number of hash partitions for the utxos table.
const numPartitions = 16

// createSchemaWithPool executes all DDL statements using the provided pool.
func createSchemaWithPool(ctx context.Context, pool *pgxpool.Pool) error {
	// Create the main utxos table.
	if _, err := pool.Exec(ctx, utxosDDL); err != nil {
		return errors.NewStorageError("schema creation failed: %v\nDDL: %s", err, utxosDDL)
	}

	// Create the spend_utxo stored function.
	if _, err := pool.Exec(ctx, spendUtxoFuncDDL); err != nil {
		return errors.NewStorageError("spend_utxo function creation failed: %v", err)
	}

	// Create 16 hash partitions with fillfactor=70 (HOT updates).
	for i := 0; i < numPartitions; i++ {
		ddl := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS utxos_p%02d PARTITION OF utxos FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = 70)",
			i, numPartitions, i,
		)
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("partition creation failed for utxos_p%02d: %v", i, err)
		}
	}

	// Partial indexes on utxos for iterator/pruner queries.
	if _, err := pool.Exec(ctx, utxosIndexesDDL); err != nil {
		return errors.NewStorageError("index creation failed: %v", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Table DDL — single UNLOGGED table: utxos
// ---------------------------------------------------------------------------

// utxos: one row per transaction. All outputs stored as parallel arrays.
// UNLOGGED for performance.
const utxosDDL = `
CREATE TABLE IF NOT EXISTS utxos (
    hash                 BYTEA PRIMARY KEY,
    version              BIGINT NOT NULL,
    lock_time            BIGINT NOT NULL,
    fee                  BIGINT NOT NULL,
    size_in_bytes        BIGINT NOT NULL,
    coinbase             BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx               BYTEA,
    -- output arrays (parallel, indexed by output position, 1-based in PG)
    utxo_hashes          BYTEA[],
    locking_scripts      BYTEA[],
    satoshis             BIGINT[],
    spending_data        BYTEA[],
    coinbase_heights     BIGINT[],
    frozen_outputs       BOOLEAN[],
    spendable_in         INT[],
    spent_count          INT NOT NULL DEFAULT 0,
    -- tx-level state
    locked               BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting          BOOLEAN NOT NULL DEFAULT FALSE,
    frozen               BOOLEAN NOT NULL DEFAULT FALSE,
    unmined_since        BIGINT,
    delete_at_height     BIGINT,
    preserve_until       BIGINT,
    -- block associations
    block_ids            INT[],
    block_heights        INT[],
    subtree_idxs         INT[],
    -- conflict tracking
    conflicting_children BYTEA[],
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (hash);`

// Partial indexes on utxos.
const utxosIndexesDDL = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON utxos (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_delete_at_height ON utxos (delete_at_height) WHERE delete_at_height IS NOT NULL;`

// spendUtxoFuncDDL creates the spend_utxo stored function that validates and
// applies spends atomically server-side. Returns a TEXT status code.
const spendUtxoFuncDDL = `
CREATE OR REPLACE FUNCTION spend_utxo(
    p_tx_hash BYTEA, p_output_idx INT, p_spending_data BYTEA,
    p_utxo_hash BYTEA, p_block_height BIGINT,
    p_ignore_locked BOOLEAN, p_ignore_conflicting BOOLEAN
) RETURNS TEXT AS $$
DECLARE
    r utxos%ROWTYPE;
    idx INT := p_output_idx + 1;
BEGIN
    SELECT * INTO r FROM utxos WHERE hash = p_tx_hash FOR UPDATE;
    IF NOT FOUND THEN RETURN 'TX_NOT_FOUND'; END IF;
    IF r.locked AND NOT p_ignore_locked THEN RETURN 'LOCKED'; END IF;
    IF r.conflicting AND NOT p_ignore_conflicting THEN RETURN 'CONFLICTING'; END IF;
    IF r.frozen THEN RETURN 'TX_FROZEN'; END IF;
    IF r.frozen_outputs[idx] THEN RETURN 'OUTPUT_FROZEN'; END IF;
    IF r.spending_data[idx] IS NOT NULL THEN
        IF r.spending_data[idx] = p_spending_data THEN RETURN 'OK'; END IF;
        RETURN 'SPENT:' || encode(r.spending_data[idx], 'hex');
    END IF;
    IF r.utxo_hashes[idx] != p_utxo_hash THEN RETURN 'HASH_MISMATCH'; END IF;
    IF r.coinbase_heights[idx] > 0 AND r.coinbase_heights[idx] > p_block_height THEN RETURN 'COINBASE_IMMATURE'; END IF;
    IF COALESCE(r.spendable_in[idx], 0) > 0 AND p_block_height < r.spendable_in[idx] THEN RETURN 'NOT_SPENDABLE'; END IF;

    r.spending_data[idx] := p_spending_data;
    r.spent_count := r.spent_count + 1;
    UPDATE utxos SET spending_data = r.spending_data, spent_count = r.spent_count WHERE hash = p_tx_hash;
    RETURN 'OK';
END;
$$ LANGUAGE plpgsql;`
