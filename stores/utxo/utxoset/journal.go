package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// UndoPartitionBlocks is the width of one journal leaf. Reclaim is DROP TABLE on whole
// leaves, so retention is granular to this.
const UndoPartitionBlocks = 48

// spendJournalSQL deletes the arbiter row AND captures its payload in one statement.
//
// One statement, not merely one transaction. A data-modifying CTE guarantees the delete
// and the journal insert see the same rows and commit together -- there is no ordering,
// no second round trip, and no window in which a coin is gone with nothing recording how
// to put it back. The outer SELECT still returns satoshis and script, so the spend
// remains its own decorate fetch.
//
// Predicates are identical to spendSQL and must stay that way: the full 32-byte txid
// recheck (the ukey is a non-unique 96-bit prefix and can only locate, never authorise),
// the frozen and conflicting flag masks, and the maturity test.
const spendJournalSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[])
        AS t(leaf, ukey, txid, vin)
),
del AS (
    DELETE FROM utxo u USING k
     WHERE u.leaf           = k.leaf
       AND u.ukey           = k.ukey
       AND u.txid           = k.txid
       AND (u.flags & 1)    = 0
       AND (u.flags & 4)    = 0
       AND u.spendable_from <= $5
    RETURNING k.vin, u.satoshis, u.created_height, u.spendable_from, u.flags,
              u.ukey, u.txid, u.script, u.hash_override
),
journal AS (
    INSERT INTO utxo_undo (spent_height, satoshis, created_height, spendable_from,
                           flags, ukey, txid, spending_txid, script, hash_override)
    SELECT $5, d.satoshis, d.created_height, d.spendable_from, d.flags,
           d.ukey, d.txid, $6::bytea, d.script, d.hash_override
      FROM del d
)
SELECT d.vin, d.satoshis, d.script FROM del d`

// ensureUndoPartition creates the journal leaf covering height, if absent.
//
// Called on the spend path, so it must be cheap and idempotent. CREATE TABLE IF NOT
// EXISTS is both, and a leaf covers UndoPartitionBlocks heights so this is a no-op for
// all but one spend in that many.
func (s *Store) ensureUndoPartition(ctx context.Context, height uint32) error {
	leaf := height / UndoPartitionBlocks
	lo := leaf * UndoPartitionBlocks
	hi := lo + UndoPartitionBlocks

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS utxo_undo_%[1]d PARTITION OF utxo_undo
  FOR VALUES FROM (%[2]d) TO (%[3]d)
  WITH (fillfactor = 100,
        autovacuum_vacuum_scale_factor = 0,
        autovacuum_vacuum_threshold    = 50000);
CREATE INDEX IF NOT EXISTS utxo_undo_%[1]d_ukey ON utxo_undo_%[1]d (ukey);`, leaf, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return errors.NewStorageError("[utxoset] create undo partition %d", leaf, err)
	}

	return nil
}

// DropUndoPartitionsBelow reclaims journal leaves entirely below height.
//
// This is the whole reclaim story: DROP TABLE, O(1), no scan, no vacuum, no background
// job that has to keep pace. The failure mode that dominated the old store -- a sweep
// that falls behind and never catches up -- has no analogue here because there is no
// per-row work to fall behind on.
func (s *Store) DropUndoPartitionsBelow(ctx context.Context, height uint32) error {
	rows, err := s.pool.Query(ctx, `
        SELECT c.relname
          FROM pg_class c
          JOIN pg_inherits i ON i.inhrelid = c.oid
          JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = 'utxo_undo'`)
	if err != nil {
		return errors.NewStorageError("[utxoset] list undo partitions", err)
	}

	var names []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset] scan undo partition", err)
		}

		names = append(names, name)
	}

	rows.Close()

	cutoff := height / UndoPartitionBlocks

	for _, name := range names {
		var leaf uint32
		if _, err := fmt.Sscanf(name, "utxo_undo_%d", &leaf); err != nil {
			continue
		}

		if leaf >= cutoff {
			continue
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name)); err != nil {
			return errors.NewStorageError("[utxoset] drop undo partition %s", name, err)
		}
	}

	return nil
}
