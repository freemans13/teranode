package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// removeConflictingChildrenSQL takes named transactions off the lists their parents keep of who
// contested their coins.
//
// The grouping is correctness rather than tidiness. PostgreSQL applies at most one update to a
// row per statement, so a parent named by ten removals would otherwise see only one of them
// take effect. The offline rewind genuinely produces that shape, because two deleted children
// can share a parent.
//
// The rebuild splits the list into 32-byte chunks, drops the ones named, and reassembles the
// rest in their original order. Insertion order is load-bearing everywhere this store packs a
// list.
//
// Membership is tested on a 32-byte BOUNDARY, which is the same test the two writers of this
// column use, run in the opposite direction. Without the alignment a plain substring search
// matches a window straddling two neighbouring entries, and as a remover that deletes bytes
// that were never an entry, corrupting both neighbours and leaving a length the reader rejects.
// Writer and remover have to agree on what membership means, or the column means two different
// things.
//
// The guard is where idempotence lives and where a no-op costs nothing. A parent the store does
// not hold fails the join, a parent with no list yields no chunks, and a child that was never
// noted matches nothing. All three write no row, no journal and no vacuum debt.
//
// The rebuild reads the column inside the SET expression rather than through a separate read.
// Under the default isolation an update that meets a concurrently-updated row re-evaluates both
// its condition and its assignment against the new version, so the appender running at
// validator rate cannot lose a note to this statement.
//
// An emptied list becomes NULL, which is what a transaction that never had a contesting child
// already carries, so the two ways of saying "none" stay one.
const removeConflictingChildrenSQL = `
WITH k AS (
    SELECT leaf, parent, array_agg(DISTINCT child) AS drop_list
      FROM unnest($1::smallint[], $2::bytea[], $3::bytea[]) AS t(leaf, parent, child)
     GROUP BY leaf, parent
)
UPDATE tx_ident i
   SET conflicting_children = (
           SELECT string_agg(substring(i.conflicting_children from g * 32 + 1 for 32),
                             ''::bytea ORDER BY g)
             FROM generate_series(0, coalesce(length(i.conflicting_children), 0) / 32 - 1) g
            WHERE NOT (substring(i.conflicting_children from g * 32 + 1 for 32) = ANY (k.drop_list)))
  FROM k
 WHERE i.leaf = k.leaf
   AND i.txid = k.parent
   AND EXISTS (
           SELECT 1
             FROM generate_series(0, coalesce(length(i.conflicting_children), 0) / 32 - 1) g
            WHERE substring(i.conflicting_children from g * 32 + 1 for 32) = ANY (k.drop_list))`

// removeBlockIDsSQL strips named blocks from the transactions that claim them.
//
// The arrays are FLATTENED to one element per transaction and block pair, the same shape the
// create statement uses for its coin arrays, because a SQL array is rectangular and each
// removal carries a ragged list. The grouping folds them back into one list per transaction
// before the update runs, for the same one-update-per-row reason as above.
//
// The unpacking lives in mh_strip rather than being inlined, for the same reason mh_max exists:
// the shift that would silently strip nothing is a trap worth stating once, beside the other
// reader of the same packed form. It also appears twice here, so inlining would mean two copies
// of that trap.
//
// The two extra conditions are what make a replayed rewind free. The first skips a row with
// nothing to strip, covering both an absent membership and the empty residue left behind when a
// transaction loses its last block. The second skips a row that claims none of the blocks being
// removed. Together they mean a second run writes no row, no journal and no vacuum debt.
//
// The mempool marker is deliberately untouched, matching both reference stores. This call does
// not claim to know whether the chain still contains the transaction, only which blocks the
// caller has stopped believing in.
const removeBlockIDsSQL = `
WITH k AS (
    SELECT leaf, txid, array_agg(DISTINCT block_id) AS ids
      FROM unnest($1::smallint[], $2::bytea[], $3::bigint[]) AS t(leaf, txid, block_id)
     GROUP BY leaf, txid
)
UPDATE tx_ident i
   SET membership = mh_strip(i.membership, k.ids)
  FROM k
 WHERE i.leaf = k.leaf
   AND i.txid = k.txid
   AND octet_length(i.membership) > 0
   AND mh_strip(i.membership, k.ids) IS DISTINCT FROM i.membership`

// RemoveFromConflictingChildren takes transactions off their parents' contested-coin lists.
//
// Called only by the offline rewind tool, which runs with the node stopped. A pair naming a
// parent the store does not hold, or a child that was never noted, is a silent no-op rather
// than an error, because a rewind re-run after a crash must not fail on the work it already
// did.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(removals))
	parents := make([][]byte, 0, len(removals))
	children := make([][]byte, 0, len(removals))

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			continue
		}

		leaves = append(leaves, LeafFor(r.ParentHash[:]))
		parents = append(parents, r.ParentHash[:])
		children = append(children, r.ChildHash[:])
	}

	if len(parents) == 0 {
		return nil
	}

	if _, err := s.pool.Exec(ctx, removeConflictingChildrenSQL, leaves, parents, children); err != nil {
		return errors.NewStorageError("[utxoset][RemoveFromConflictingChildren]", err)
	}

	return nil
}

// RemoveBlockIDs makes transactions stop claiming blocks the caller has rewound.
//
// Called only by the offline rewind tool. A transaction the store does not hold, or a block it
// never claimed, is a silent no-op, for the same crash-replay reason.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	// Flattened to one element per transaction and block pair, because a SQL array is
	// rectangular and each removal carries a list of its own length.
	leaves := make([]int16, 0, len(removals))
	txids := make([][]byte, 0, len(removals))
	blockIDs := make([]int64, 0, len(removals))

	for _, r := range removals {
		if r.TxHash == nil {
			continue
		}

		for _, id := range r.BlockIDs {
			leaves = append(leaves, LeafFor(r.TxHash[:]))
			txids = append(txids, r.TxHash[:])
			blockIDs = append(blockIDs, int64(id))
		}
	}

	if len(txids) == 0 {
		return nil
	}

	if _, err := s.pool.Exec(ctx, removeBlockIDsSQL, leaves, txids, blockIDs); err != nil {
		return errors.NewStorageError("[utxoset][RemoveBlockIDs]", err)
	}

	return nil
}
