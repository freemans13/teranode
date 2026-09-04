package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// stampSQL records a block against every transaction that does not already claim it, and
// clears the mempool marker when the block is on the longest chain.
//
// Every key parameter is an ARRAY, so one statement serves one transaction or a whole block.
// This used to queue one UPDATE per transaction down a single connection, which already cost
// only one round trip, so the saving is not round trips. It is PostgreSQL parsing, planning
// and executing one statement instead of thousands. The create path made the same move and
// measured 4x on the batch flush, at the same batch widths a block arrives in.
//
// A hash named twice in one call is stamped once, because an UPDATE never applies to the same
// target row twice within one statement. That matches what the per-transaction loop did, where
// the second attempt found the block already recorded and skipped it.
//
// The append is guarded rather than unconditional, so replaying a block does not record it
// twice. The guard reads the row as it stood before this statement, which is what makes a batch
// mixing already-stamped and never-stamped transactions come out right in both directions.
//
// Membership is tested on a 12-BYTE BOUNDARY. The column is a concatenation of 12-byte triples
// and the reader unpacks it that way. This used to be a plain substring search, which can match
// bytes STRADDLING two neighbouring triples, read that as already-recorded, and silently skip a
// real append, leaving a transaction that never claims a block which actually contains it.
// unstampSQL carries the identical test, and it matters more there.
//
// The marker is cleared only when the caller states the block is on the longest chain. That
// is the same rule the create gate applies, and for the same reason: "mined into some block"
// and "on the main chain" are different facts, and a transaction whose only block later
// loses must stay in the mempool set.
const stampSQL = `
UPDATE tx_ident i
   SET membership = CASE
           WHEN EXISTS (
                SELECT 1
                  FROM generate_series(0, coalesce(length(i.membership), 0) / 12 - 1) g
                 WHERE substring(i.membership from g * 12 + 1 for 12) = $3::bytea)
           THEN i.membership
           ELSE coalesce(i.membership, '\x'::bytea) || $3::bytea
       END,
       off_chain_since = CASE WHEN $4::boolean THEN NULL ELSE i.off_chain_since END
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
 WHERE i.leaf = k.leaf AND i.txid = k.txid`

// unstampSQL removes one block from a transaction's membership and puts it back in the
// mempool set with a FRESH clock. Array-parameterised for the same reason stampSQL is.
//
// The entry to remove is located on a 12-BYTE BOUNDARY, and that is the worse half of the
// alignment rule rather than a symmetry. This used to splice 12 bytes out from wherever a plain
// substring search first matched. At an unaligned offset that destroys the tail of one triple
// and the head of the next, and the result is still a multiple of 12, so the length constraint
// does not catch it and the reader cannot tell it has been handed two invented blocks. A value
// that is not present on a boundary is not an entry at all, so the right answer is to change
// nothing, which is what the coalesce does when no aligned match exists.
//
// The clock is the store's current tip, NOT the transaction's creation height, and that is
// the fact that decides whether these two columns are one concept or two. A transaction
// created at height 100 and un-mined while the tip is 5,000 must wait from 5,000, or the
// preservation pass fires on it immediately. Both reference stores do the same.
const unstampSQL = `
UPDATE tx_ident i
   SET membership = coalesce((
           SELECT overlay(i.membership placing ''::bytea FROM min(g * 12 + 1) FOR 12)
             FROM generate_series(0, coalesce(length(i.membership), 0) / 12 - 1) g
            WHERE substring(i.membership from g * 12 + 1 for 12) = $3::bytea
       ), i.membership),
       off_chain_since = $4::int
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
 WHERE i.leaf = k.leaf AND i.txid = k.txid`

// provePresentSQL is the SECOND statement, and splitting it out is not tidiness.
//
// The interface says every hash asked about must appear in the answer, and an implementation
// that cannot prove it must return an error. A single UPDATE ... RETURNING cannot: it returns
// nothing for a transaction that is already correctly mined, which is indistinguishable from
// the row not existing, so a replayed block would report every transaction in it as missing.
// Asking separately whether the row is there answers the question the contract actually poses.
const provePresentSQL = `
SELECT i.txid, i.membership
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
  JOIN tx_ident i ON i.leaf = k.leaf AND i.txid = k.txid`

// appendMinedSQL records this block against every listed transaction that already has a
// membership row, copying the payload from its earliest row. A transaction with no row at all
// is not appended, so the postcondition still catches an unknown one.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, for the same reason
// minedByTxidSQL does: written as a plain `JOIN tx_mined m ON m.txid = k.txid` the planner is
// free to hash-join the keys against the whole partitioned table, which the measurements
// behind minedByTxidSQL showed as a Seq Scan on every live window at 500 keys. The LATERAL's
// own ORDER BY + LIMIT 1 also picks the earliest row directly, so no outer DISTINCT ON is
// needed.
const appendMinedSQL = `
INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                      size_in_bytes, tx_inpoints, locktime, created_at, flags)
SELECT k.txid, $2, $3, $4, m.created_height, m.size_in_bytes, m.tx_inpoints, m.locktime,
       m.created_at, m.flags
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT created_height, size_in_bytes, tx_inpoints, locktime, created_at, flags
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m
ON CONFLICT (txid, mined_height, block_id) DO NOTHING`

// deleteMinedRowSQL removes one block's membership row for a set of transactions.
//
// This is the UnsetMined counterpart to appendMinedSQL, for a transaction whose mined stamp
// lives only in tx_mined (no identity row for unstampSQL to touch). A reorg below the
// checkpoint cannot reach here, and at the tip stage 2 owns un-mining a coin, so this is a
// best-effort cleanup: absence is tolerated, exactly as the interface allows for UnsetMined.
// mined_height is a literal, not part of the unnest, so partition pruning still confines the
// delete to the one window this block belongs to.
const deleteMinedRowSQL = `
DELETE FROM tx_mined m
 USING unnest($1::bytea[]) AS k(txid)
 WHERE m.txid = k.txid AND m.mined_height = $2 AND m.block_id = $3`

// SetMinedMulti marks transactions as mined in the block described by info.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	// Built once and used by both statements, so the set the stamp acted on and the set the
	// postcondition is checked against cannot disagree.
	leaves := make([]int16, 0, len(hashes))
	txids := make([][]byte, 0, len(hashes))

	for _, h := range hashes {
		if h == nil {
			continue
		}

		leaves = append(leaves, LeafFor(h[:]))
		txids = append(txids, h[:])
	}

	if len(txids) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	entry := packMembership([]utxo.MinedBlockInfo{info})

	stmt := stampSQL
	marker := any(info.OnLongestChain)

	if info.UnsetMined {
		stmt = unstampSQL
		// A fresh clock from the current tip. See unstampSQL.
		marker = int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32
	}

	if _, err := s.pool.Exec(ctx, stmt, leaves, txids, entry, marker); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
	}

	rows, err := s.pool.Query(ctx, provePresentSQL, leaves, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] prove", err)
	}

	out := make(map[chainhash.Hash][]uint32, len(hashes))
	found := make(map[chainhash.Hash]struct{}, len(hashes))

	for rows.Next() {
		var (
			txid       []byte
			membership []byte
		)

		if err := rows.Scan(&txid, &membership); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("[utxoset][SetMinedMulti] scan", err)
		}

		var h chainhash.Hash
		copy(h[:], txid)

		ids, _, _ := unpackMembership(membership)
		out[h] = ids
		found[h] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] rows", err)
	}

	// The residue: every txid provePresentSQL did NOT answer, because it never held an
	// identity row (the block path never writes one) or has since lost it. This is where a
	// block-path transaction -- one the retry path re-stamps, or a sibling block at the same
	// height -- gets answered, from tx_mined instead of tx_ident.
	var residue [][]byte

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		if _, ok := found[h]; !ok {
			residue = append(residue, txid)
		}
	}

	// Un-mining is exempt from the postcondition below, because the interface says missing
	// entries are tolerated there: a reorg may un-mine a transaction the store has already
	// discarded. Tolerated means it does not error, NOT that the answer is empty. Transactions
	// that DO still exist must still appear, which the conformance suite checks.
	//
	// A block-path row has no identity row for unstampSQL to touch, so the residue here also
	// gets a best-effort delete against tx_mined. Absence is tolerated the same way.
	if info.UnsetMined {
		if len(residue) > 0 {
			if _, err := s.pool.Exec(ctx, deleteMinedRowSQL, residue,
				int32(info.BlockHeight), int32(info.BlockID)); err != nil { //nolint:gosec // heights and ids fit
				return nil, errors.NewStorageError("[utxoset][SetMinedMulti] unmine membership", err)
			}
		}

		return out, nil
	}

	// Block-path rows live in tx_mined, not tx_ident. A stamp for such a row is the retry path
	// (Phase 1.5 on ErrTxExists) or a sibling block at the same height. Append a membership row
	// for this block if the transaction has any membership row at all; the postcondition is
	// then answered from tx_mined. A transaction in neither table is genuinely unknown.
	if len(residue) > 0 {
		if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
			return nil, err
		}

		if _, err := s.pool.Exec(ctx, appendMinedSQL, residue,
			int32(info.BlockHeight), int32(info.BlockID), int32(info.SubtreeIdx)); err != nil { //nolint:gosec // heights and ids fit
			return nil, errors.NewStorageError("[utxoset][SetMinedMulti] append membership", err)
		}

		ids, err := s.minedIDsByTxid(ctx, residue)
		if err != nil {
			return nil, err
		}

		for h, v := range ids {
			out[h] = v
		}
	}

	for _, h := range hashes {
		if h == nil {
			continue
		}

		if _, ok := out[*h]; !ok {
			return nil, errors.NewTxNotFoundError("[utxoset][SetMinedMulti] %s", h.String())
		}
	}

	return out, nil
}

// minedIDsByTxid reads the block ids tx_mined records for a set of transactions, in insertion
// order. It reuses readMinedRows rather than a bespoke query, so the ids SetMinedMulti hands
// back for a block-path transaction and the ids an ordinary Get would report can never
// disagree about what "insertion order" means.
func (s *Store) minedIDsByTxid(ctx context.Context, txids [][]byte) (map[chainhash.Hash][]uint32, error) {
	hashes := make([]chainhash.Hash, 0, len(txids))

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		hashes = append(hashes, h)
	}

	data := make(map[chainhash.Hash]*meta.Data, len(hashes))
	if err := s.readMinedRows(ctx, hashes, data); err != nil {
		return nil, err
	}

	out := make(map[chainhash.Hash][]uint32, len(data))
	for h, d := range data {
		out[h] = d.BlockIDs
	}

	return out, nil
}
