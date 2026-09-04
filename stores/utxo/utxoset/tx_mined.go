package utxoset

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// stampRetiringWindowChunk is the page size for reading a retiring window's rows. 20,000 keeps
// one round trip's argument arrays and the chunk's own working set modest, while still being
// far wider than an ordinary block, so a window with millions of transactions is read in a
// bounded number of round trips rather than one per transaction.
const stampRetiringWindowChunk = 20_000

// retiringWindowRowsSQL lists a window's rows for the coin stamp, oldest (txid, seq) first so
// the FIRST row per transaction id -- the one taken below -- is the earliest block that ever
// settled it. Paged by a KEYSET on (txid, seq) rather than OFFSET, so the plan stays an index
// scan at every page rather than degrading as the offset grows: an OFFSET-based page has to
// skip every row before it, which is O(rows read so far) per page over a window that can hold
// tens of millions of rows.
const retiringWindowRowsSQL = `
SELECT txid, mined_height, block_id, seq FROM %[1]s
 WHERE (txid, seq) > ($1::bytea, $2::bigint)
 ORDER BY txid, seq
 LIMIT $3`

// stampCoinsSQL stamps every live (unconfirmed) coin of the listed transactions with the
// block their FIRST surviving tx_mined row named. See liveCoinArgs for the leaf/lo/hi shape;
// $5 and $6 are the parallel height and block-id arrays keyed to the same transaction.
//
// mined_height = 0 is what makes this idempotent and leaves a block-path coin -- stamped with
// its facts at create, never at the sentinel -- untouched. A coin that has already been
// stamped this way, or created directly by the block path, must not be touched a second time:
// nothing here can tell "already correct" from "a later, wrong window" apart except that guard.
//
// The fenced read runs FIRST, in a CTE, and the UPDATE then matches on the exact (leaf, ukey)
// it returns -- the same shape resetCoinsSQL uses, and for the identical reason. A plain
// `UPDATE ... FROM unnest(...) AS k WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND ...` was
// measured first, on 40,000 seeded coins across all eight leaves with 500 keys: a Hash Join
// against a Seq Scan of every one of the eight coin partitions, because the planner can build
// a hash on (leaf, txid) from the whole table more cheaply than it can cost 500 per-key index
// probes, and the ukey range only applies as a post-join filter rather than an index
// condition. The CROSS JOIN LATERAL with an OFFSET 0 fence is what forces an index scan per
// key instead: an UPDATE cannot laterally reference its own target, so the read has to happen
// in a CTE the UPDATE then joins on the row identity it found, not on the search predicate.
//
// EVERY live coin of the transaction is stamped, not just one, so the LATERAL carries no
// LIMIT -- unlike coinFactsSQL, which only ever needs one.
const stampCoinsSQL = `
WITH hit AS (
    SELECT c.leaf, c.ukey, k.h, k.b
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
 WHERE u.leaf = hit.leaf AND u.ukey = hit.ukey`

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
// coin that is still live. Failing loudly here is the guard.
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

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS tx_mined_w%[1]d PARTITION OF tx_mined
  FOR VALUES FROM (%[2]d) TO (%[3]d);
ALTER TABLE tx_mined_w%[1]d ALTER COLUMN tx_inpoints SET STORAGE EXTERNAL;`, window, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
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

// dropTxMinedWindowsBelow drops every membership window whose upper bound is below
// cutoffHeight, oldest first, and advances the floor past each. Returns the count dropped.
//
// This IS identity reclaim in this design: no work list, no probes, no row deletes. The
// coins of transactions in a retiring window are stamped from the window's list first in
// stage 2; in stage 1 every coin was written with its block facts at create.
func (s *Store) dropTxMinedWindowsBelow(ctx context.Context, cutoffHeight uint32) (int, error) {
	cutoff := cutoffHeight / TxMinedPartitionBlocks

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

		// The coin stamp runs BEFORE the detach, and a failure here returns without touching
		// the window at all. A window must never be dropped while any of its transactions'
		// surviving coins still carry the sentinel: once the window is gone, its rows are the
		// only place that block fact lived, and readCoinFacts would then answer with mined
		// height 0 for a parent that really does have a block.
		txCount, coinCount, err := s.stampRetiringWindowCoins(ctx, w.name)
		if err != nil {
			return dropped, err
		}

		s.logger.Infof("[utxoset] stamped %d live coins of %d transactions before dropping tx_mined window %s",
			coinCount, txCount, w.name)

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
		// destroyed, doubling every coin still claimed by a transaction in it. window is a
		// regex-filtered catalog name (^tx_mined_w[0-9]+$), so folding it into the literal
		// with Sprintf carries no injection risk.
		ddl := fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s;
UPDATE tx_mined_floor SET floor = GREATEST(floor, %[2]d) WHERE id = 0;`, w.name, w.window+1)

		if _, err := s.pool.Exec(ctx, ddl); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_mined window %s and advance its floor", w.name, err)
		}

		dropped++
	}

	return dropped, nil
}

// stampRetiringWindowCoins is the lazy coin stamp: it reads window's own rows and stamps the
// FIRST (lowest seq) row's block against every one of its transactions' still-unconfirmed
// coins, before the window is detached. Returns the number of distinct transactions read and
// the number of coin rows the stamp touched.
//
// It reads in pages of stampRetiringWindowChunk rows, oldest (txid, seq) first, rather than one
// query for the whole window: a window can hold a full 288 blocks of mainnet membership, tens
// of millions of rows, and a single unbounded read would hold that whole result set in memory
// and in one round trip.
//
// Keyset pagination on (txid, seq) rather than OFFSET, so the plan is an index scan on every
// page and does not degrade as the window is worked through: an OFFSET-based page N has to skip
// the N-1 pages before it, at O(rows read so far) per page.
//
// A transaction's rows always sort together, because the window's rows are ordered by
// (txid, seq) and a transaction cannot appear in two different windows -- a window is keyed by
// mined_height, which is a property of the transaction's block, not of the transaction. So a
// transaction whose rows straddle a page boundary is still resolved correctly: dedup runs
// across the whole window via lastStampedTxid, not just within one page.
func (s *Store) stampRetiringWindowCoins(ctx context.Context, window string) (txCount, coinCount int, err error) {
	var (
		lastTxid       = []byte{}
		lastSeq  int64 = -1
		// lastStampedTxid is the most recent transaction id this window has already
		// collected a stamp for. Rows for one transaction are contiguous in (txid, seq)
		// order, so equality with the immediately preceding row's txid is enough to skip
		// every row after its first.
		lastStampedTxid []byte
	)

	query := fmt.Sprintf(retiringWindowRowsSQL, window)

	for {
		rows, qerr := s.pool.Query(ctx, query, lastTxid, lastSeq, stampRetiringWindowChunk)
		if qerr != nil {
			return txCount, coinCount, errors.NewStorageError("[utxoset] read retiring window %s", window, qerr)
		}

		var (
			leaves  []int16
			txids   [][]byte
			los     [][16]byte
			his     [][16]byte
			heights []int32
			blockID []int32
			n       int
		)

		for rows.Next() {
			var (
				txid   []byte
				height int32
				block  int32
				seq    int64
			)

			if serr := rows.Scan(&txid, &height, &block, &seq); serr != nil {
				rows.Close()
				return txCount, coinCount, errors.NewStorageError("[utxoset] scan retiring window %s", window, serr)
			}

			n++
			lastTxid = txid
			lastSeq = seq

			// Only the first row seen for a transaction is its earliest stamp.
			if lastStampedTxid != nil && bytes.Equal(txid, lastStampedTxid) {
				continue
			}

			lastStampedTxid = txid
			txCount++

			leaves = append(leaves, LeafFor(txid))
			txids = append(txids, txid)
			los = append(los, Pack(txid, 0))
			his = append(his, Pack(txid, ^uint32(0)))
			heights = append(heights, height)
			blockID = append(blockID, block)
		}

		rerr := rows.Err()

		rows.Close()

		if rerr != nil {
			return txCount, coinCount, errors.NewStorageError("[utxoset] read retiring window %s", window, rerr)
		}

		if len(txids) > 0 {
			tag, uerr := s.pool.Exec(ctx, stampCoinsSQL, leaves, txids, los, his, heights, blockID)
			if uerr != nil {
				return txCount, coinCount, errors.NewStorageError("[utxoset] stamp coins for retiring window %s", window, uerr)
			}

			coinCount += int(tag.RowsAffected())
		}

		if n < stampRetiringWindowChunk {
			break
		}
	}

	return txCount, coinCount, nil
}
