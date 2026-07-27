package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// SpendJournalPartitionBlocks is the width of one journal leaf. Reclaim drops whole leaves, so
// retention is granular to this. At the measured frontier (~20,000 spends/block) a leaf
// holds roughly 960,000 rows.
const SpendJournalPartitionBlocks = 48

// DefaultSpendJournalRetentionBlocks is how far back a spend stays undoable.
//
// 1440, not the 288 the design originally proposed. ParentPreservationBlocks is 1440 and
// its own settings documentation says in bold "DO NOT reduce below 1440 as this risks
// invalidating legitimate transaction resubmissions" -- so a 288-block journal would
// have compiled, passed tests, and then silently failed to protect a resubmitted
// transaction ten days later. The measured UTXO growth slope (251/block, not the 802
// assumed) leaves the budget at roughly 60-66% even at this depth, so the correct number
// is affordable.
//
// Steady-state leaf count is retention/SpendJournalPartitionBlocks + 1 = 31 tables. Bounded, and
// reclaimed as the chain advances.
const DefaultSpendJournalRetentionBlocks = 1440

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
    INSERT INTO spend_journal (spent_height, satoshis, created_height, spendable_from,
                           flags, ukey, txid, spending_txid, script, hash_override)
    SELECT $5, d.satoshis, d.created_height, d.spendable_from, d.flags,
           d.ukey, d.txid, $6::bytea, d.script, d.hash_override
      FROM del d
)
SELECT d.vin, d.satoshis, d.script FROM del d`

// ensureSpendJournalPartition creates the spend-journal leaf covering height, if absent.
//
// Called on the spend path, so it must be cheap and idempotent. CREATE TABLE IF NOT
// EXISTS is both, and a leaf covers SpendJournalPartitionBlocks heights so this is a no-op for
// all but one spend in that many.
func (s *Store) ensureSpendJournalPartition(ctx context.Context, height uint32) error {
	leaf := height / SpendJournalPartitionBlocks

	// Only touch the catalog when the leaf actually changes -- once every
	// SpendJournalPartitionBlocks heights, not once per spend.
	//
	// The cache holds leaf+1 so that its zero value means "nothing cached yet" rather
	// than "leaf 0 is already created". Storing the leaf directly made leaf 0 permanently
	// unreachable: a fresh store starts at zero, and the first spend below height 48 would
	// see a hit, skip the DDL, and fail the insert with "no partition found for row" --
	// precisely the initial-sync case.
	if s.journalLeaf.Load() == leaf+1 {
		return nil
	}

	lo := leaf * SpendJournalPartitionBlocks
	hi := lo + SpendJournalPartitionBlocks

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS spend_journal_%[1]d PARTITION OF spend_journal
  FOR VALUES FROM (%[2]d) TO (%[3]d)
  WITH (fillfactor = 100,
        autovacuum_vacuum_scale_factor = 0,
        autovacuum_vacuum_threshold    = 50000);
CREATE INDEX IF NOT EXISTS spend_journal_%[1]d_ukey ON spend_journal_%[1]d (ukey);`, leaf, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return errors.NewStorageError("[utxoset] create spend-journal partition %d", leaf, err)
	}

	// Only record the leaf once the DDL has actually succeeded. Marking it up front would
	// make a transient failure permanent -- every later spend would see a cache hit, skip
	// the retry, and fail on a partition that was never created.
	s.journalLeaf.Store(leaf + 1)

	// Crossing into a new leaf is exactly the moment old ones fall out of retention, so
	// reclaim here rather than from a background job. That is deliberate: a background
	// reclaimer that falls behind is the failure mode that dominated the previous store,
	// and this design has no per-row work to fall behind on -- reclaim is a catalog
	// operation that either happens or does not.
	if height > s.journalRetention {
		if err := s.DropSpendJournalPartitionsBelow(ctx, height-s.journalRetention); err != nil {
			// Not fatal: a spend must not fail because old history could not be
			// discarded. Surfaced so it cannot rot silently.
			s.logger.Warnf("[utxoset] spend journal reclaim below height %d failed: %v",
				height-s.journalRetention, err)
		}
	}

	return nil
}

// DropSpendJournalPartitionsBelow reclaims journal leaves entirely below height.
//
// This is the whole reclaim story: DROP TABLE, O(1), no scan, no vacuum, no background
// job that has to keep pace. The failure mode that dominated the old store -- a sweep
// that falls behind and never catches up -- has no analogue here because there is no
// per-row work to fall behind on.
func (s *Store) DropSpendJournalPartitionsBelow(ctx context.Context, height uint32) error {
	rows, err := s.pool.Query(ctx, `
        SELECT c.relname
          FROM pg_class c
          JOIN pg_inherits i ON i.inhrelid = c.oid
          JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = 'spend_journal'`)
	if err != nil {
		return errors.NewStorageError("[utxoset] list spend-journal partitions", err)
	}

	var names []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset] scan spend-journal partition", err)
		}

		names = append(names, name)
	}

	rows.Close()

	cutoff := height / SpendJournalPartitionBlocks

	for _, name := range names {
		var leaf uint32
		if _, err := fmt.Sscanf(name, "spend_journal_%d", &leaf); err != nil {
			continue
		}

		if leaf >= cutoff {
			continue
		}

		// DETACH CONCURRENTLY first, then drop the now-standalone table. A bare DROP
		// TABLE on an attached partition briefly takes ACCESS EXCLUSIVE on the PARENT,
		// which would stall every concurrent spend; detaching concurrently does not.
		if _, err := s.pool.Exec(ctx,
			fmt.Sprintf(`ALTER TABLE spend_journal DETACH PARTITION %s CONCURRENTLY`, name)); err != nil {
			return errors.NewStorageError("[utxoset] detach spend-journal partition %s", name, err)
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name)); err != nil {
			return errors.NewStorageError("[utxoset] drop spend-journal partition %s", name, err)
		}
	}

	return nil
}
