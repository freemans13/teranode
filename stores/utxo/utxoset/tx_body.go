package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/jackc/pgx/v5"
)

// TxBodyPartitionBlocks is the width of one body window. Reclaim drops whole windows, so
// retention is granular to this. It matches the spend journal's width deliberately: the two
// are reclaimed by the same pruner session and there is no reason for them to disagree.
const TxBodyPartitionBlocks = 48

// DefaultTxBodyRetentionBlocks is how long the serialized bytes are kept.
//
// 288, which is global_blockHeightRetention, and it is not a number this store chose. It is
// the horizon at which subtree files are deleted, past which the node physically cannot
// un-mine a block because the un-mine path warns and skips a missing subtree file. Choosing
// 144 instead would not create a new failure class, it would narrow a range that already
// works and make depths 145 to 287 fail quietly inside a band where the rest of the node
// still functions.
const DefaultTxBodyRetentionBlocks = 288

// ensureTxBodyPartition creates the body window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the same reason
// ensureSpendJournalPartition must: the DDL needs its own pool connection, and taking one
// while holding a transaction borrowed from the same pool is a nested acquire. At
// pool_max_conns concurrent writers every connection ends up held by a transaction waiting
// for a connection, and that deadlock has no timeout.
func (s *Store) ensureTxBodyPartition(ctx context.Context, height uint32) error {
	window := height / TxBodyPartitionBlocks

	// Only touch the catalog when the window actually changes. The cache holds window+1 so
	// its zero value means "nothing cached yet" rather than "window 0 is already created",
	// which would make window 0 permanently unreachable on a fresh store.
	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	s.bodyDDL.Lock()
	defer s.bodyDDL.Unlock()

	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	lo := window * TxBodyPartitionBlocks
	hi := lo + TxBodyPartitionBlocks

	// raw_tx is forced out of line on each partition. Keep it for the genuinely large tail,
	// but NOT for the reason the design doc gave: an inline body is not rewritten by an
	// update of another column, because postgres writes only what changed working in from
	// both ends of the row. Do not set toast_tuple_target to force more of it out -- at 128
	// the toaster does not stop at raw_tx, it keeps going and externalises txid too.
	//
	// Built standalone and attached, so the block path never takes the parent's strongest
	// lock at a window boundary. See ensureAttachedPartition.
	child := fmt.Sprintf("tx_body_w%d", window)
	if err := s.ensureAttachedPartition(ctx, partitionSpec{
		parent: "tx_body",
		child:  child,
		key:    "created_height",
		lo:     lo,
		hi:     hi,
		after:  []string{fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN raw_tx SET STORAGE EXTERNAL`, child)},
	}); err != nil {
		return errors.NewStorageError("[utxoset] create tx_body window %d", window, err)
	}

	// Only cache once the DDL has actually succeeded, or a transient failure becomes
	// permanent: every later write would see a hit, skip the retry, and fail on a partition
	// that was never created.
	s.bodyWindow.Store(window + 1)

	return nil
}

// txBodyWindowSQL lists the body windows this store owns, in whichever state they are in.
//
// This used to INNER JOIN pg_inherits, which made it blind to the very state a crash produces.
// The three states, all verified against PostgreSQL rather than assumed, and all of them handled
// twelve lines away in spend_journal.go for the journal's own partitions:
//
//   - ATTACHED. The normal case.
//   - ORPHANED. A crash between DETACH and DROP leaves a fully standalone table: relispartition
//     goes FALSE and the pg_inherits row is GONE. An inner join can never see it again, so its
//     disk is never returned and nothing reports it. Found here by name within tx_body's own
//     schema, which is all that still identifies it.
//   - DETACH PENDING. A crash DURING a concurrent detach leaves inhdetachpending set. PostgreSQL
//     then refuses every further ATTACH and DETACH on the parent, so the detach below fails on
//     every call, dropTxBodyWindowsBelow returns an error, and Prune returns before it reaches
//     the journal loop at all. That is the WHOLE cleanup stopped, not one table leaked, and it
//     would also fail the CREATE TABLE ... PARTITION OF that the create path runs at each
//     48-block rollover.
//
// Resolved through regclass so it finds windows of the SAME table the DROP below will name.
// Matching on a bare name would search every schema in the database, and an unqualified drop
// would then resolve against the search path and fail, aborting the loop before a single real
// window went.
const txBodyWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_body'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_body'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_body_w[0-9]+$'`

// dropTxBodyWindowsBelow discards the serialized transaction bytes that have aged out.
//
// The height passed in is the pruner service's clock, which by default is the last height the
// block persister has archived rather than the chain tip. That matters: the persister is the
// only producer of the permanent archive for a block this node mined, and the store's copy of
// the bytes is its only source. Dropping ahead of it would wedge it permanently, with no
// fallback. If pruner_force_ignore_block_persister_height is ever set, that protection is
// gone and this becomes unsafe.
//
// Before any window goes, the bodies of transactions still waiting to be mined are carried into
// tx_body_carry (carryBodiesAhead), and after the drops the carried bodies that are no longer
// needed are deleted (reclaimCarriedBodies). A carry that fails stops the pass before anything
// is dropped: a dropped body cannot be brought back.
func (s *Store) dropTxBodyWindowsBelow(ctx context.Context, height uint32) (int, error) {
	carried, err := s.carryBodiesAhead(ctx, height, false)
	if err != nil {
		return 0, err
	}

	if height <= s.bodyRetention {
		return 0, nil
	}

	cutoff := (height - s.bodyRetention) / TxBodyPartitionBlocks

	rows, err := s.pool.Query(ctx, txBodyWindowSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_body windows", err)
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
			return 0, errors.NewStorageError("[utxoset] scan tx_body window", err)
		}

		// A name carrying no window number is not one of ours. Parsing it here rather than in
		// the drop loop keeps it in one place, so the ordering below and the cutoff test can
		// never disagree about which window a table is.
		if _, err := fmt.Sscanf(w.name, "tx_body_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_body windows", err)
	}

	// Oldest first, for the reason the journal's own loop sorts: the listing query has no
	// ORDER BY, so the catalog returns whatever order it scanned, and with a backlog that
	// decides which disk comes back first and whether the oldest surviving window is a usable
	// progress measure at all.
	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	if len(windows) > 0 && windows[0].window < cutoff {
		// A drop is due, so carry the waiting set again unless this pass already did: a
		// transaction un-mined by a reorg since the window entered the carry band is waiting
		// again and needs its body.
		if !carried {
			if _, err := s.carryBodiesAhead(ctx, height, true); err != nil {
				return 0, err
			}
		}

		// And carry every body the persister has not archived yet, on every pass that drops,
		// whatever ran before it. See carryUnarchivedBodySQL.
		if _, err := s.carryUnarchivedBodies(ctx, cutoff*TxBodyPartitionBlocks, height); err != nil {
			return 0, err
		}
	}

	dropped := 0

	for _, w := range windows {
		if w.window >= cutoff {
			continue
		}

		switch {
		case w.detachPending:
			// FINALIZE is the only way out of this state, and until it runs no other window of
			// this table can be detached either, and no new one can be created.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_body DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_body window %s", w.name, err)
			}

		case w.attached:
			// Detach without blocking readers, then drop the now-standalone table. A bare drop
			// on an attached partition briefly takes an exclusive lock on the parent, which
			// would stall every concurrent create.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_body DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach tx_body window %s", w.name, err)
			}

		default:
			// Already standalone: a previous session was interrupted between its DETACH and its
			// DROP. Nothing to detach, just finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, w.name)); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_body window %s", w.name, err)
		}

		dropped++
	}

	if err := s.reclaimCarriedBodies(ctx, height); err != nil {
		return dropped, err
	}

	return dropped, nil
}

// carryBodiesAhead runs carryUnminedBodies for every window that drops within one window of
// the tip, once per new window, or now when force is set. It reports whether it ran.
//
// The bound is measured from the TIP, one window ahead of the drop. It is not what keeps the
// body of a transaction mined in a block the persister has not archived: that transaction has
// lost its unmined marker, and this pass sees it while it waits only if the pruner happened to
// run then. The drop pass carries those by their block instead (carryUnarchivedBodySQL).
//
// The band is entered once per 48 blocks, and that is the only time the unmined set is read
// for it; the drop pass reads it a second time (see dropTxBodyWindowsBelow).
func (s *Store) carryBodiesAhead(ctx context.Context, height uint32, force bool) (bool, error) {
	tip := max(s.GetBlockHeight(), height)

	if tip+TxBodyPartitionBlocks <= s.bodyRetention {
		return false, nil
	}

	// Every window below bound drops once the tip is one more window along.
	bound := (tip + TxBodyPartitionBlocks - s.bodyRetention) / TxBodyPartitionBlocks
	if bound == 0 || (!force && s.carryBound.Load() == bound+1) {
		return false, nil
	}

	if _, err := s.carryUnminedBodies(ctx, bound*TxBodyPartitionBlocks); err != nil {
		return false, err
	}

	// Cached only once the copy has committed, so a failure is retried on the next pass.
	s.carryBound.Store(bound + 1)

	return true, nil
}

// carryUnminedBodySQL copies, into tx_body_carry, the body of every transaction still waiting
// to be mined whose body lives below $1, a height.
//
// "Waiting" is the identity row's unmined marker, the predicate copyForwardUnminedSpends uses
// (unminedInpointsSQL), and the scan is driven by the marker's partial index for the same
// reason: it reads the waiting population and nothing else, whatever the size of the windows.
//
// Crash safety. The copy commits on its own and the drop comes later, so a crash between them
// carries the same window again on the next pass. The NOT EXISTS skips a transaction already
// carried before its body is read, and ON CONFLICT is the backstop: the table's key is the
// txid, so a second copy cannot be a second row. Until the window drops a carried body exists
// twice, once in its window and once here, and the reader takes the window's and consults this
// table only when that is gone (readCarriedBodies), so two copies are never two answers.
//
// The body is read through the parent tx_body, so only an attached window is carried from.
// That is enough: a window is detached only by a pass that has already carried it.
const carryUnminedBodySQL = `
INSERT INTO tx_body_carry (txid, raw_tx)
SELECT i.txid, b.raw_tx
  FROM tx_ident i
 CROSS JOIN LATERAL (
   SELECT b.raw_tx
     FROM tx_body b
    WHERE b.created_height = i.created_height
      AND b.txid = i.txid
   OFFSET 0
 ) AS b
 WHERE i.off_chain_since IS NOT NULL
   AND i.created_height < $1::int
   AND b.raw_tx IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM tx_body_carry c WHERE c.txid = i.txid)
ON CONFLICT (txid) DO NOTHING`

// carryUnminedBodies carries the body of every waiting transaction created below bound into
// tx_body_carry, and returns how many it copied. See carryUnminedBodySQL.
func (s *Store) carryUnminedBodies(ctx context.Context, bound uint32) (int64, error) {
	tag, err := s.pool.Exec(ctx, carryUnminedBodySQL, int32(bound)) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] carry bodies of unmined transactions below height %d", bound, err)
	}

	if n := tag.RowsAffected(); n > 0 {
		s.logger.Infof("[utxoset] carried the bodies of %d unmined transactions created below height %d past their window", n, bound)
	}

	return tag.RowsAffected(), nil
}

// carryUnarchivedBodySQL copies, into tx_body_carry, the body of every transaction created below
// $1, a height, that a block above $2 contains. $2 is the pruner's height, the last height the
// block persister has archived, so these are the transactions whose block the persister has
// still to archive, and whose body it will read from this store to do it.
//
// This is the carry's second predicate, beside carryUnminedBodySQL's "still waiting", and it is
// the one that does not depend on when the carry runs. The waiting predicate only holds for a
// transaction carried before it was mined: the tip-ahead pass (carryBodiesAhead) is meant to see
// it while it waits, but that pass runs only when the pruner does, and a pruner whose sessions
// were skipped, or that first runs after the mining, finds a mined transaction with its marker
// already clear. Its window then dropped under it while its block was unarchived, and the
// persister failed that block for good. "Not yet archived" is a fact about the transaction's
// block and the persister alone, so a drop pass that applies it carries every body the
// persister still needs, whatever the pruner did before.
//
// It reads only the windows about to drop ($1 is the drop cutoff), and for each of their rows
// probes tx_mined on its txid-leading primary key for a block above $2, through the fenced
// LATERAL every by-txid probe of tx_mined takes. A drop pass therefore costs one probe per
// transaction in the windows it drops, once per window. NOT EXISTS and ON CONFLICT make a
// repeat after a crash between the carry and the drop copy nothing twice, as for the waiting
// set. reclaimCarriedBodySQL keeps the copy until the block is 288 blocks below the pruner's
// height, so it outlives the persister's archive of that block.
//
// Rows created at or below $3, the highest checkpoint, are not read. Nothing there qualifies: a
// transaction this node mines was first seen at the tip, above the checkpoint, and a block-path
// create carries its own block's height as created_height, which can never be 288 blocks below a
// block above the archived height. Without the bound a sync that writes bodies below the
// checkpoint would pay one containment probe per transaction at every window drop for nothing.
const carryUnarchivedBodySQL = `
INSERT INTO tx_body_carry (txid, raw_tx)
SELECT b.txid, b.raw_tx
  FROM tx_body b
 CROSS JOIN LATERAL (
   SELECT 1
     FROM tx_mined m
    WHERE m.txid = b.txid
      AND m.mined_height > $2::int
    LIMIT 1
   OFFSET 0
 ) AS m
 WHERE b.created_height < $1::int
   AND b.created_height > $3::int
   AND b.raw_tx IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM tx_body_carry c WHERE c.txid = b.txid)
ON CONFLICT (txid) DO NOTHING`

// carryUnarchivedBodies carries the body of every transaction created below bound and contained
// in a block above archived into tx_body_carry, and returns how many it copied. See
// carryUnarchivedBodySQL.
func (s *Store) carryUnarchivedBodies(ctx context.Context, bound, archived uint32) (int64, error) {
	tag, err := s.pool.Exec(ctx, carryUnarchivedBodySQL, int32(bound), int32(archived), //nolint:gosec // heights fit int32
		int32(model.HighestCheckpointHeight(s.checkpoints))) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] carry bodies of transactions created below height %d and mined above the archived height %d", bound, archived, err)
	}

	if n := tag.RowsAffected(); n > 0 {
		s.logger.Infof("[utxoset] carried the bodies of %d transactions created below height %d and mined above the archived height %d past their window", n, bound, archived)
	}

	return tag.RowsAffected(), nil
}

// reclaimCarriedBodySQL deletes the carried bodies nothing needs any more: the transaction is
// not waiting, and no block that contains it is above $1, the pruner's height less the body
// horizon. A carried transaction therefore keeps its body while it waits and for 288 blocks
// after it is mined, and a transaction deleted outright loses it here too.
//
// Both tests read one snapshot, and SetMined records the block and clears the marker in one
// transaction, so a transaction being mined as this runs matches one of them either way.
//
// The same statement records $1 in tx_body_carry_floor, which is what the lookup gates its probe
// of this table on (mayHaveCarriedBody). The latest value is written, not the highest ever: a
// pruner height that went back (a rewind) lowers it, and a lower floor only costs the lookup a
// probe that finds nothing.
const reclaimCarriedBodySQL = `
WITH reclaimed AS (
    DELETE FROM tx_body_carry c
     WHERE NOT EXISTS (
           SELECT 1 FROM tx_ident i
            WHERE i.leaf = (get_byte(c.txid, 0) & 7)::smallint
              AND i.txid = c.txid
              AND i.off_chain_since IS NOT NULL)
       AND NOT EXISTS (
           SELECT 1 FROM tx_mined m
            WHERE m.txid = c.txid
              AND m.mined_height > $1::int)
    RETURNING 1
),
floor AS (
    INSERT INTO tx_body_carry_floor (id, reclaimed_below) VALUES (0, $1::int)
    ON CONFLICT (id) DO UPDATE SET reclaimed_below = EXCLUDED.reclaimed_below
)
SELECT count(*) FROM reclaimed`

// carryFloorSQL reads the floor reclaimCarriedBodySQL last recorded.
const carryFloorSQL = `SELECT reclaimed_below FROM tx_body_carry_floor WHERE id = 0`

// reclaimCarriedBodies applies reclaimCarriedBodySQL at the pruner height.
func (s *Store) reclaimCarriedBodies(ctx context.Context, height uint32) error {
	if height <= s.bodyRetention {
		return nil
	}

	var n int64

	if err := s.pool.QueryRow(ctx, reclaimCarriedBodySQL, int32(height-s.bodyRetention)).Scan(&n); err != nil { //nolint:gosec // a height fits int32
		return errors.NewStorageError("[utxoset] reclaim carried bodies at height %d", height, err)
	}

	if n > 0 {
		s.logger.Infof("[utxoset] dropped %d carried bodies of transactions mined %d or more blocks ago", n, s.bodyRetention)
	}

	return nil
}

// carryFloor returns the floor the last reclaim of tx_body_carry recorded, read again whenever
// the tip has moved since the last read in this process. The reclaim runs in the pruner, which
// may be another process, so the value is read from the database rather than kept from the
// reclaim. A stale value is never higher than the current one unless a rewind lowered it, and
// either way a floor that is too low only costs the lookup a probe that finds nothing.
func (s *Store) carryFloor(ctx context.Context) (uint32, error) {
	tip := s.GetBlockHeight()
	if s.carryFloorTip.Load() == uint64(tip)+1 {
		return s.carryFloorValue.Load(), nil
	}

	var floor int32

	err := s.pool.QueryRow(ctx, carryFloorSQL).Scan(&floor)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		floor = 0
	case err != nil:
		return 0, errors.NewStorageError("[utxoset][lookup] read the carried-body floor", err)
	}

	s.carryFloorValue.Store(uint32(max(floor, 0))) //nolint:gosec // non-negative by max
	s.carryFloorTip.Store(uint64(tip) + 1)

	return s.carryFloorValue.Load(), nil
}
