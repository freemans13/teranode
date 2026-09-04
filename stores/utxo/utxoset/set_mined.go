package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
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
                      size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
SELECT k.txid, $2, $3, $4, m.created_height, m.size_in_bytes, m.fee, m.tx_inpoints,
       m.locktime, m.created_at, m.flags
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT created_height, size_in_bytes, fee, tx_inpoints, locktime, created_at, flags
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m
ON CONFLICT (txid, mined_height, block_id) DO NOTHING`

// moveToMinedSQL moves identity rows that, after this block, name exactly one block into the
// membership table, deleting them from the identity table in the same statement.
//
// A row naming two or more blocks is left where it is with its marker cleared: the id-less
// mark-on-longest-chain call cannot later say which of them is main, so the row waits for an
// un-mine or a further stamp to reduce it to one.
//
// A row carrying conflicting children is left behind too, and that is a scope boundary rather
// than a second reading of the same rule. tx_mined has no column for that bookkeeping; it
// moves to a side table in a later task, and until it does, moving the row would lose the list
// of children this transaction conflicts with. Its marker is already cleared by the stamp,
// exactly as the multi-block case's is.
//
// The single-block test is an EQUALITY against the packed triple this stamp just appended, not
// a length test, and that does both halves of the job at once: it is true only when the
// membership column holds twelve bytes AND those twelve bytes are THIS block. A row whose one
// triple named some other block would otherwise move under this block's facts. The length test
// alone cannot go wrong today, because stampSQL has already appended this block by the time
// this statement runs, but the equality does not depend on that ordering holding.
//
// mined_height, block_id and subtree_idx come from the parameters rather than from decoding the
// membership bytes, because the equality above has already proved the two agree.
//
// THE KEYS ARE TWO ARRAY QUALS, NOT A JOIN AGAINST unnest, and that is the difference between
// one index descent per key and a read of the whole mempool. Written the obvious way --
// `DELETE FROM tx_ident i USING unnest($1, $2) AS k WHERE i.leaf = k.leaf AND i.txid = k.txid`
// -- the planner is free to hash-join the keys against the table, and at 40,000 identity rows
// it does: measured on this schema at 500 keys, the plan carried a Seq Scan on all eight leaf
// partitions and built a 40,000-row hash to answer 500 keys, 8.7 ms of the statement's 12.4.
// The array form plans as a bitmap index scan per surviving partition, 0.6 ms at the same
// width, and its cost is a function of the batch rather than of the mempool. The LATERAL
// fence the read statements use is not available here: a DELETE cannot laterally reference
// its own target.
//
// txid = ANY is EXACT despite dropping the pairing. txid is the full 32 bytes and tx_ident_ck
// makes leaf a function of it, so no row can satisfy the txid qual under a leaf other than its
// own. The leaf array is therefore redundant as a filter and load-bearing as an access path:
// it is what prunes partitions and what makes the primary key usable, since txid is its second
// column. It must list the leaf of every txid in the batch, which is why both come from the
// same loop.
//
// It RETURNS the txids it moved, and that is what keeps the postcondition cheap. provePresentSQL
// reads tx_ident only, so without this every row the move just settled would be reported absent
// and fall into the residue set -- an appendMinedSQL and a minedIDsByTxid at full block width on
// every longest-chain stamp, which is the ordinary tip path. The ids returned are the ones that
// LEFT tx_ident, taken from the DELETE's own RETURNING rather than the INSERT's: an insert that
// hits the membership key (the same block replayed) reports nothing, yet the identity row is
// gone all the same, so keying off the insert would leak those rows back into the residue.
const moveToMinedSQL = `
WITH moved AS (
    DELETE FROM tx_ident i
     WHERE i.leaf = ANY($1::smallint[])
       AND i.txid = ANY($2::bytea[])
       AND i.membership = $6::bytea
       AND i.conflicting_children IS NULL
    RETURNING i.txid, i.created_height, i.size_in_bytes, i.fee, i.tx_inpoints,
              i.locktime, i.created_at, i.flags
),
settled AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT m.txid, $3::int, $4::int, $5::int, m.created_height, m.size_in_bytes, m.fee,
           m.tx_inpoints, m.locktime, m.created_at, m.flags
      FROM moved m
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
)
SELECT txid FROM moved`

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
//
// Three write paths, and the flags on info decide which runs rather than what the store
// holds. A block ON THE LONGEST CHAIN settles the transactions it names, so its stamp is also
// a move (stampAndMove). An UN-MINE takes the block back off the identity row (unstampOnly).
// Anything else is a fork stamp, which only appends the block (forkStamp). Un-mining wins
// over the longest-chain flag when a caller sets both, as it did before the move existed.
//
// Each path answers the postcondition the interface states -- every hash asked about appears
// in the result, or the call fails -- from three sources: the identity row (provePresentSQL),
// the identity row this call MOVED into the membership table, and the membership rows a
// transaction created by the block path already had.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	// Built once and used by every statement, so the set a stamp acted on and the set the
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

	var (
		out map[chainhash.Hash][]uint32
		err error
	)

	switch {
	case info.UnsetMined:
		out, err = s.unstampOnly(ctx, leaves, txids, entry, info)
	case info.OnLongestChain:
		out, err = s.stampAndMove(ctx, leaves, txids, entry, info)
	default:
		out, err = s.forkStamp(ctx, leaves, txids, entry, info)
	}

	if err != nil {
		return nil, err
	}

	// Un-mining is exempt from the postcondition below, because the interface says missing
	// entries are tolerated there: a reorg may un-mine a transaction the store has already
	// discarded. Tolerated means it does not error, NOT that the answer is empty. Transactions
	// that DO still exist must still appear, which the conformance suite checks.
	if info.UnsetMined {
		return out, nil
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

// stampAndMove appends this block to every listed identity row, clears their mempool markers,
// moves the rows the block has settled into the membership table, and answers for the rest.
//
// The statements are ONE TRANSACTION, and that is a correctness rule rather than a saved
// round trip. The read path stops at an identity hit (see lookup.go), so between a committed
// delete from tx_ident and a committed insert into tx_mined a concurrent reader finds the
// transaction in neither table and reports it missing -- which makes the validator reject
// every child of a parent that is merely being mined. Inside one transaction the two states
// are the only two a reader can see.
//
// ensureTxMinedPartition runs BEFORE the transaction opens, because the DDL needs its own pool
// connection; the same rule the create path follows. It is also what lets the residue append
// live inside the transaction: the window it inserts into is the one this call just created
// for the move, so the append needs no DDL of its own.
func (s *Store) stampAndMove(ctx context.Context, leaves []int16, txids [][]byte, entry []byte,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
		return nil, err
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] begin", err)
	}

	out, residue, err := s.stampMoveAndAppend(ctx, dbTx, leaves, txids, entry, info)
	if err != nil {
		_ = dbTx.Rollback(ctx)

		return nil, err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] commit stamp and move", err)
	}

	// The block ids of an appended row are read after the commit, on the pool, and only for
	// the hashes that actually needed appending -- which on the ordinary tip path is none,
	// because moveToMinedSQL already answered for every row it moved. Reading committed state
	// through the ordinary membership step keeps "insertion order" a single definition; the
	// write it reports on is already durable, so nothing needs it inside the transaction.
	if len(residue) > 0 {
		ids, err := s.minedIDsByTxid(ctx, residue)
		if err != nil {
			return nil, err
		}

		for h, v := range ids {
			out[h] = v
		}
	}

	return out, nil
}

// stampMoveAndAppend is the body of the longest-chain transaction, split out so that every
// failure inside it leaves through one rollback. It returns the answers it established and the
// transactions still to be answered from the membership table.
//
// The order is fixed. stampSQL first, so moveToMinedSQL's single-block test sees the row as
// this block leaves it: a row that only ever saw this block moves, one that already carried a
// fork triple now carries two and stays. provePresentSQL after the move, so a row cannot be
// counted both as moved and as present.
func (s *Store) stampMoveAndAppend(ctx context.Context, dbTx pgx.Tx, leaves []int16,
	txids [][]byte, entry []byte, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32,
	[][]byte, error) {
	if _, err := dbTx.Exec(ctx, stampSQL, leaves, txids, entry, true); err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
	}

	out := make(map[chainhash.Hash][]uint32, len(txids))

	// Every row the move settled named exactly one block -- this one, which is what
	// moveToMinedSQL's equality against the stamped triple proves -- so its answer is known
	// without reading the membership table back.
	moved, err := queryTxids(ctx, dbTx, moveToMinedSQL, distinctLeaves(txids), txids,
		int32(info.BlockHeight), int32(info.BlockID), int32(info.SubtreeIdx), entry) //nolint:gosec // heights and ids fit
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] move to membership", err)
	}

	for _, h := range moved {
		out[h] = []uint32{info.BlockID}
	}

	if err := provePresentInto(ctx, dbTx, leaves, txids, out); err != nil {
		return nil, nil, err
	}

	// The residue: every transaction neither moved nor present on the identity table, because
	// it never held an identity row (the block path never writes one) or has since lost it.
	// A stamp for such a row is the retry path (Phase 1.5 on ErrTxExists) or a sibling block
	// at the same height. Append a membership row for this block if the transaction has any
	// membership row at all; a transaction in neither table is genuinely unknown.
	residue := absentTxids(txids, out)
	if len(residue) == 0 {
		return out, nil, nil
	}

	if _, err := dbTx.Exec(ctx, appendMinedSQL, residue, int32(info.BlockHeight),
		int32(info.BlockID), int32(info.SubtreeIdx)); err != nil { //nolint:gosec // heights and ids fit
		return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] append membership", err)
	}

	return out, residue, nil
}

// forkStamp records a block that is NOT on the longest chain: every listed identity row gains
// the block and keeps its mempool marker, and nothing moves.
//
// No transaction wraps the statements, and that is a real difference from the longest-chain
// path rather than an oversight. Nothing here deletes. The stamp only appends to tx_ident, the
// append only inserts into tx_mined, and they act on disjoint sets of transactions, so there
// is no instant at which a reader finds a transaction in neither table -- which is the whole
// reason stampAndMove needs one.
func (s *Store) forkStamp(ctx context.Context, leaves []int16, txids [][]byte, entry []byte,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if _, err := s.pool.Exec(ctx, stampSQL, leaves, txids, entry, false); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
	}

	out := make(map[chainhash.Hash][]uint32, len(txids))
	if err := provePresentInto(ctx, s.pool, leaves, txids, out); err != nil {
		return nil, err
	}

	residue := absentTxids(txids, out)
	if len(residue) == 0 {
		return out, nil
	}

	if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
		return nil, err
	}

	if _, err := s.pool.Exec(ctx, appendMinedSQL, residue, int32(info.BlockHeight),
		int32(info.BlockID), int32(info.SubtreeIdx)); err != nil { //nolint:gosec // heights and ids fit
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] append membership", err)
	}

	ids, err := s.minedIDsByTxid(ctx, residue)
	if err != nil {
		return nil, err
	}

	for h, v := range ids {
		out[h] = v
	}

	return out, nil
}

// unstampOnly takes one block off every listed identity row and puts the row back in the
// mempool set with a fresh clock.
//
// A transaction whose stamp lives only in the membership table has no identity row for the
// unstamp to touch, so the residue gets a best-effort delete of that block's row. Absence is
// tolerated, exactly as the interface allows for an un-mine.
func (s *Store) unstampOnly(ctx context.Context, leaves []int16, txids [][]byte, entry []byte,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	// A fresh clock from the current tip. See unstampSQL.
	height := int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32

	if _, err := s.pool.Exec(ctx, unstampSQL, leaves, txids, entry, height); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] unstamp", err)
	}

	out := make(map[chainhash.Hash][]uint32, len(txids))
	if err := provePresentInto(ctx, s.pool, leaves, txids, out); err != nil {
		return nil, err
	}

	residue := absentTxids(txids, out)
	if len(residue) == 0 {
		return out, nil
	}

	if _, err := s.pool.Exec(ctx, deleteMinedRowSQL, residue,
		int32(info.BlockHeight), int32(info.BlockID)); err != nil { //nolint:gosec // heights and ids fit
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] unmine membership", err)
	}

	return out, nil
}

// provePresentInto records, for every listed transaction that still holds an identity row,
// the blocks that row names. See provePresentSQL for why this is a statement of its own.
func provePresentInto(ctx context.Context, q querier, leaves []int16, txids [][]byte,
	out map[chainhash.Hash][]uint32) error {
	rows, err := q.Query(ctx, provePresentSQL, leaves, txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] prove", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid       []byte
			membership []byte
		)

		if err := rows.Scan(&txid, &membership); err != nil {
			return errors.NewStorageError("[utxoset][SetMinedMulti] scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		ids, _, _ := unpackMembership(membership)
		out[h] = ids
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] rows", err)
	}

	return nil
}

// absentTxids is the set of listed transactions the answer does not yet cover.
func absentTxids(txids [][]byte, out map[chainhash.Hash][]uint32) [][]byte {
	var residue [][]byte

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		if _, ok := out[h]; !ok {
			residue = append(residue, txid)
		}
	}

	return residue
}

// queryTxids runs a statement whose result is one txid column and collects the hashes.
func queryTxids(ctx context.Context, q querier, stmt string, args ...any) ([]chainhash.Hash, error) {
	rows, err := q.Query(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var out []chainhash.Hash

	for rows.Next() {
		var txid []byte

		if err := rows.Scan(&txid); err != nil {
			return nil, err
		}

		var h chainhash.Hash

		copy(h[:], txid)

		out = append(out, h)
	}

	return out, rows.Err()
}

// distinctLeaves is the set of leaf partitions a batch of transaction ids touches.
//
// moveToMinedSQL takes it rather than the per-key leaf array stampSQL takes, because there it
// is a partition list and not a key: eight values at most, and a batch smaller than eight
// prunes the partitions it cannot contain. Passing the per-key array would work and would
// restate one leaf value a thousand times inside the plan.
func distinctLeaves(txids [][]byte) []int16 {
	var seen [NumLeaves]bool

	out := make([]int16, 0, NumLeaves)

	for _, txid := range txids {
		leaf := LeafFor(txid)
		if seen[leaf] {
			continue
		}

		seen[leaf] = true
		out = append(out, leaf)
	}

	return out
}

// minedIDsByTxid reads the block ids tx_mined records for a set of transactions, in insertion
// order. It goes through the read path's own membership step rather than a bespoke query, so
// the ids SetMinedMulti hands back and the ids an ordinary Get would report can never disagree
// about what "insertion order" means.
//
// A row that will not decode fails THIS CALL rather than being reported as a transaction
// claiming no blocks, which is the conservative reading here: refusing the stamp is
// recoverable, quietly stamping nothing is not. That is why the per-transaction failures are
// collected and returned instead of being handed back alongside the answers, as they are on
// the BatchDecorate path this result type was written for.
func (s *Store) minedIDsByTxid(ctx context.Context, txids [][]byte) (map[chainhash.Hash][]uint32, error) {
	hashes := make([]chainhash.Hash, 0, len(txids))

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		hashes = append(hashes, h)
	}

	res := newLookupResult(len(hashes))
	if err := s.readMinedInto(ctx, hashes, &res); err != nil {
		return nil, err
	}

	for _, err := range res.failed {
		return nil, err
	}

	out := make(map[chainhash.Hash][]uint32, len(res.found))
	for h, d := range res.found {
		out[h] = d.BlockIDs
	}

	return out, nil
}
