package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// stampUTXOsSQL writes a block onto every live UTXO of the listed transactions that is still at
// the unconfirmed sentinel. See liveUTXOArgs for the leaf/lo/hi shape; $5 and $6 are the
// parallel height and block-id arrays keyed to the same transaction.
//
// NOTHING RUNS IT YET. It is the write half of the deep stamp of build step 5, kept because
// that stamp reuses it as it stands: the retirement stamp that used to drive it, which took a
// transaction's earliest containment row by an insertion counter as the winner, is deleted
// with the counter, because insertion order is not chain order and it stamped reorg losers
// onto UTXOs. The stamp that replaces it is told the winner by the pruner service.
//
// mined_height = 0 is what makes this idempotent and leaves a block-path UTXO -- written with
// its pair at create, never at the sentinel -- untouched. A UTXO that already carries a pair
// must not be touched a second time: a non-zero pair is final.
//
// The fenced read runs FIRST, in a CTE, and the UPDATE then matches on the exact (leaf, ukey)
// it returns -- the same shape resetUTXOsSQL uses, and for the identical reason. A plain
// `UPDATE ... FROM unnest(...) AS k WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND ...` was
// measured first, on 40,000 seeded UTXOs across all eight leaves with 500 keys: a Hash Join
// against a Seq Scan of every one of the eight UTXO partitions, because the planner can build
// a hash on (leaf, txid) from the whole table more cheaply than it can cost 500 per-key index
// probes, and the ukey range only applies as a post-join filter rather than an index
// condition. The CROSS JOIN LATERAL with an OFFSET 0 fence is what forces an index scan per
// key instead: an UPDATE cannot laterally reference its own target, so the read has to happen
// in a CTE the UPDATE then joins on the row identity it found, not on the search predicate.
//
// EVERY live UTXO of the transaction is stamped, not just one, so the LATERAL carries no
// LIMIT -- unlike utxoFactsSQL, which only ever needs one.
//
// The UPDATE rechecks the FULL TXID and not only the (leaf, ukey) the read found the row by.
// ukey is a 96-bit prefix and NON-UNIQUE by design -- see Pack -- so a stranger's UTXO in the
// same leaf can share it, and it is a UTXO at the SENTINEL that this statement is looking for,
// which is exactly what a colliding row is most likely to be. Stamping it would hand a UTXO the
// facts of a block that does not contain its transaction, and once this window is dropped there
// is nothing left to correct it from. resetUTXOsSQL carries the identical recheck.
const stampUTXOsSQL = `
WITH hit AS (
    SELECT c.leaf, c.ukey, k.txid, k.h, k.b
      FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[], $5::int[], $6::int[])
           AS k(leaf, txid, lo, hi, h, b)
     CROSS JOIN LATERAL (
       SELECT u.leaf, u.ukey
         FROM utxo u
        WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
          AND u.mined_height = 0
       OFFSET 0
     ) AS c
)
UPDATE utxo u SET mined_height = hit.h, block_id = hit.b
  FROM hit
 WHERE u.leaf = hit.leaf AND u.ukey = hit.ukey AND u.txid = hit.txid`

// TxMinedPartitionBlocks is the width of one membership window.
//
// 288, not the journal's 48. A lookup by transaction id with no height probes every live
// window, and six probes at 288 cost about 50 microseconds against 31 at 48 costing 260.
// Nothing needs 48-block drop granularity here.
const TxMinedPartitionBlocks = 288

// ensureTxMinedPartition creates the membership window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the reason
// ensureTxBodyPartition must: the DDL needs its own pool connection.
//
// It REFUSES a window at or below the floor. The floor is the highest window ever dropped
// plus one, and a create below it can only be a block re-offered after its window retired.
// Recreating the window would claim every transaction in that block afresh and double every
// UTXO that is still live. Failing loudly here is the guard.
func (s *Store) ensureTxMinedPartition(ctx context.Context, height uint32) error {
	window := height / TxMinedPartitionBlocks

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	s.minedDDL.Lock()
	defer s.minedDDL.Unlock()

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	floor, err := s.txMinedFloor(ctx)
	if err != nil {
		return err
	}

	if window < floor {
		return errors.NewProcessingError("[utxoset] refusing to recreate dropped membership window %d for height %d (floor %d)", window, height, floor)
	}

	lo := window * TxMinedPartitionBlocks
	hi := lo + TxMinedPartitionBlocks

	// Built standalone and attached, so the block path never takes the parent's strongest
	// lock at a window boundary. See ensureAttachedPartition for the measurement behind it.
	child := fmt.Sprintf("tx_mined_w%d", window)
	if err := s.ensureAttachedPartition(ctx, partitionSpec{
		parent: "tx_mined",
		child:  child,
		key:    "mined_height",
		lo:     lo,
		hi:     hi,
		after:  []string{fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN tx_inpoints SET STORAGE EXTERNAL`, child)},
	}); err != nil {
		return errors.NewStorageError("[utxoset] create tx_mined window %d", window, err)
	}

	s.minedWindow.Store(window + 1)

	return nil
}

// txMinedFloor returns the highest dropped window index plus one; 0 when nothing was dropped.
func (s *Store) txMinedFloor(ctx context.Context) (uint32, error) {
	var floor int32
	if err := s.pool.QueryRow(ctx, `SELECT floor FROM tx_mined_floor WHERE id = 0`).Scan(&floor); err != nil {
		return 0, errors.NewStorageError("[utxoset] read tx_mined floor", err)
	}

	return uint32(floor), nil //nolint:gosec // a window index is never negative
}

// txMinedWindowSQL lists the membership windows in whichever of the three crash states they
// are in; see txBodyWindowSQL for the states and why the join is LEFT.
const txMinedWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_mined'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_mined'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_mined_w[0-9]+$'`

// identityRowsExistSQL is the interim guard's one read: does tx_ident hold any row at all.
const identityRowsExistSQL = `SELECT EXISTS (SELECT 1 FROM tx_ident LIMIT 1)`

// dropTxMinedWindowsBelow drops every containment window whose upper bound is below
// cutoffHeight, oldest first, and advances the floors past each. Returns the count dropped.
//
// This is the INTERIM drop of the containment build, which runs on the old rule -- a window
// goes once its upper bound is journalRetention below the pruner's height -- and it is guarded:
// it refuses to drop anything while tx_ident holds a row. A transaction seen before its block
// keeps its identity row through mining, its UTXOs stay at (0,0), and nothing writes its block
// onto them until the deep stamp of build step 5 exists. Dropping its window before then would
// take the only place its block facts live. Below the checkpoint every create carries its
// block, so tx_ident is empty there and the guard never fires. Where it does fire it is counted
// in utxoset_interim_drop_refused_total and every drop stops; the disk then grows, which is an
// abort criterion of the soak, and the remedy is the stamp, not a relaxed guard.
//
// The refusal is a logged skip and not an error, because the pruner calls this once per block
// and an error every block would drown the log while changing nothing.
//
// Because the guard holds, every window this drops had nothing to stamp, and the drop can
// honestly raise all three floor values together (see the statement below).
func (s *Store) dropTxMinedWindowsBelow(ctx context.Context, cutoffHeight uint32) (int, error) {
	cutoff := cutoffHeight / TxMinedPartitionBlocks

	var identityRows bool
	if err := s.pool.QueryRow(ctx, identityRowsExistSQL).Scan(&identityRows); err != nil {
		return 0, errors.NewStorageError("[utxoset] check tx_ident before dropping windows", err)
	}

	rows, err := s.pool.Query(ctx, txMinedWindowSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	type windowState struct {
		name          string
		window        uint32
		attached      bool
		detachPending bool
	}

	var windows []windowState

	for rows.Next() {
		var w windowState
		if err := rows.Scan(&w.name, &w.attached, &w.detachPending); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset] scan tx_mined window", err)
		}

		if _, err := fmt.Sscanf(w.name, "tx_mined_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	dropped := 0

	for _, w := range windows {
		if w.window >= cutoff {
			continue
		}

		if identityRows {
			interimDropRefused.Inc()
			s.logger.Warnf("[utxoset] refusing to drop tx_mined window %s: tx_ident holds rows and nothing stamps their UTXOs until the deep stamp exists", w.name)

			return dropped, nil
		}

		switch {
		case w.detachPending:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_mined window %s", w.name, err)
			}

		case w.attached:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach tx_mined window %s", w.name, err)
			}

		default:
			// Already standalone after an interrupted session: finish the job.
		}

		// The drop and the floor advance are ONE statement, not two Execs. Both run inside
		// postgres's implicit transaction for a multi-statement Exec, and DROP TABLE is
		// fully transactional, so a crash or connection drop between them cannot happen: it
		// either lands with both effects or neither. Two separate calls would let a crash in
		// between drop the window from the catalog for good -- gone, so it never resurfaces
		// in txMinedWindowSQL's listing to retry -- while the floor stayed pointed below it,
		// and ensureTxMinedPartition would then recreate the very window this loop just
		// destroyed, doubling every UTXO still claimed by a transaction in it. window is a
		// regex-filtered catalog name (^tx_mined_w[0-9]+$), so folding it into the literal
		// with Sprintf carries no injection risk.
		//
		// ALL THREE floor values rise together. floor is a window number; stamp_fence and
		// stamp_complete_floor are heights, and the ordering constraint on the row requires
		// 288 x floor <= stamp_complete_floor <= stamp_fence, so an interim drop that raised
		// floor alone would be refused by the constraint. Raising the other two is honest here
		// because the guard above means every window dropped had nothing to stamp, and it adds
		// no refusal: a write below the raised fence is a write below the dropped floor, which
		// the store refuses already. When the stamp of build step 5 starts on such a database
		// its pass begins at stamp_complete_floor, exactly at the dropped floor. In the full
		// design the drop writes only floor, because its first condition already requires the
		// completion record.
		ddl := fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s;
UPDATE tx_mined_floor
   SET floor                = GREATEST(floor, %[2]d),
       stamp_complete_floor = GREATEST(stamp_complete_floor, %[2]d * %[3]d),
       stamp_fence          = GREATEST(stamp_fence, %[2]d * %[3]d)
 WHERE id = 0;`, w.name, w.window+1, TxMinedPartitionBlocks)

		if _, err := s.pool.Exec(ctx, ddl); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_mined window %s and advance its floor", w.name, err)
		}

		dropped++

		// The ensure cache can be holding the window just destroyed, and would then let a
		// later create at that height skip the floor read entirely. Clear it so the next
		// ensure re-reads the floor and refuses loudly instead of failing on a missing
		// partition. See ensureTxMinedPartition.
		s.minedWindow.Store(0)
	}

	return dropped, nil
}
