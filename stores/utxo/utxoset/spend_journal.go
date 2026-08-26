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

// spendJournalSQL deletes the UTXO row AND captures its payload in one statement.
//
// One statement, not merely one transaction. A data-modifying CTE guarantees the delete
// and the journal insert see the same rows and commit together -- there is no ordering,
// no second round trip, and no window in which a coin is gone with nothing recording how
// to put it back. The outer SELECT still returns satoshis and script, so the spend
// remains its own decorate fetch.
//
// This is now the ONLY spend statement. There was a journal-free twin of it, used below
// the checkpoint, whose predicates had to be kept identical by hand; it is deleted rather
// than flagged off, because two copies of a consensus predicate is a defect waiting for
// one of them to be edited. The predicates that authorise the delete are the full 32-byte
// txid recheck (the ukey is a non-unique 96-bit prefix and can only locate, never
// authorise), the frozen and conflicting flag masks, and the maturity test. classifySQL
// deliberately omits the last three, so an excluded row surfaces as frozen or immature
// rather than as spent.
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

	// One writer per process. CREATE TABLE IF NOT EXISTS is NOT concurrency-safe in
	// PostgreSQL: simultaneous attempts race on the pg_type row and raise a unique
	// violation rather than quietly agreeing. With the spend phase running thousands of
	// goroutines, that race is the common case rather than the rare one.
	s.journalDDL.Lock()
	defer s.journalDDL.Unlock()

	// Re-check under the lock: the winner may have created it while we queued.
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

	// Reclaim deliberately does NOT happen here, though crossing into a new leaf is
	// exactly the moment old ones fall out of retention. It used to, and the argument was
	// sound for a catalog operation: dropping a partition is constant time, and a
	// background reclaimer that falls behind is the failure mode that dominated the
	// previous store. What it missed is that DETACH CONCURRENTLY waits for every open
	// transaction on the parent. From a spend, with thousands of goroutines behind it,
	// that stalls the pipeline; and because a spend must not fail over old history that
	// could not be discarded, the only available response to an error was to swallow it.
	// It swallowed a real one for the entire life of the branch.
	//
	// The pruner service drives it instead: see GetPrunerService in pruner.go.

	return nil
}

// journalLeafSQL lists every journal leaf this store owns, in whichever state it is in.
//
// Three states matter, and all three were verified against PostgreSQL 17 rather than
// assumed:
//
//   - ATTACHED. The normal case. relispartition is true and pg_inherits has the row.
//   - ORPHANED. A crash between DETACH and DROP leaves a fully standalone table:
//     relispartition goes FALSE and the pg_inherits row is GONE. A listing that joins
//     pg_inherits can never see it again, so it would leak forever. Found here by name
//     within the journal's own schema, which is the only thing left that identifies it.
//   - DETACH PENDING. A crash DURING a concurrent detach leaves inhdetachpending set.
//     PostgreSQL then refuses any further ATTACH or DETACH on the parent -- "partition
//     already pending detach", hinting at FINALIZE -- so an unhandled one wedges all
//     future reclaim rather than leaking quietly.
//
// Scoped to the schema that 'spend_journal'::regclass resolves to, so this agrees with the
// unqualified DETACH below about which table it means.
const journalLeafSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'spend_journal'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'spend_journal'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^spend_journal_[0-9]+$'`

// DropSpendJournalPartitionsBelow reclaims journal leaves entirely below height, and
// returns how many it reclaimed.
//
// This is the whole reclaim story: DROP TABLE, O(1), no scan, no vacuum, no per-row work
// to fall behind on. It is idempotent, which is what lets the caller treat a crash as
// nothing worse than a repeat: every leaf below the cutoff is reclaimed on every call, in
// whatever state the last attempt left it, so a partial run is simply redone.
//
// It must be the LAST step of a pruner session. Dropping a partition destroys the record
// of which transactions had an output spent in that window, which is precisely the work
// list a later session step reads to decide what is now fully spent.
func (s *Store) DropSpendJournalPartitionsBelow(ctx context.Context, height uint32) (int, error) {
	rows, err := s.pool.Query(ctx, journalLeafSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list spend-journal leaves", err)
	}

	type leafState struct {
		name          string
		attached      bool
		detachPending bool
	}

	var leaves []leafState

	for rows.Next() {
		var l leafState
		if err := rows.Scan(&l.name, &l.attached, &l.detachPending); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset] scan spend-journal leaf", err)
		}

		leaves = append(leaves, l)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list spend-journal leaves", err)
	}

	cutoff := height / SpendJournalPartitionBlocks
	dropped := 0

	for _, l := range leaves {
		var leaf uint32
		if _, err := fmt.Sscanf(l.name, "spend_journal_%d", &leaf); err != nil {
			continue
		}

		if leaf >= cutoff {
			continue
		}

		switch {
		case l.detachPending:
			// FINALIZE is the only way out of this state, and until it runs no other
			// partition of this table can be detached either.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE spend_journal DETACH PARTITION %s FINALIZE`, l.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of spend-journal leaf %s", l.name, err)
			}

		case l.attached:
			// DETACH CONCURRENTLY first, then drop the now-standalone table. A bare DROP
			// TABLE on an attached partition briefly takes ACCESS EXCLUSIVE on the
			// PARENT, which would stall every concurrent spend; detaching concurrently
			// does not. It cannot run inside a transaction block, which is why this is a
			// single-statement Exec on its own and not folded into one.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE spend_journal DETACH PARTITION %s CONCURRENTLY`, l.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach spend-journal leaf %s", l.name, err)
			}

		default:
			// Already standalone: a previous session was interrupted between its DETACH
			// and its DROP. Nothing to detach, just finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, l.name)); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop spend-journal leaf %s", l.name, err)
		}

		dropped++
	}

	return dropped, nil
}
