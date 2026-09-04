package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// The read order is identity table, then membership by transaction id, then the coin, and
// the order is a correctness rule rather than a tuning. A coin holds ONE block id, and on the
// ordinary two-step reorg (a fork block stamped as not on the longest chain, then a later
// block making it the main chain) nothing rewrites the coins of transactions shared between
// the two blocks. A coin-first read would then hand block validation an id that lost, and the
// parent check stores a valid block as invalid. The membership table holds every id while the
// window lives, so the coin is consulted only once the window is gone, by which time its
// block is at least 1440 deep and its coin-carried id is the settled one.
//
// The spec also names a journal step between membership and coin. While membership and journal
// retention are both 1440 blocks the journal can only find a spend whose parent's membership
// row also still exists, so that step is omitted here. Revisit if the two retentions diverge.

// identByTxidSQL reads the identity rows for a set of transactions, joining each body only if
// it is still inside its window.
//
// The join is LEFT, and that is the point rather than caution. The body window is dropped
// after 288 blocks while the identity row lives for as long as any of the transaction's
// outputs is unspent, at any age, so a body-less row is the ordinary steady state for an old
// transaction. An inner join would report every such transaction as missing, and a missing
// parent makes the validator reject its children.
//
// Both halves are found by (leaf, txid) and (created_height, txid) respectively, so this is
// two index probes per transaction and no scan. The body's height comes from the identity row,
// which is why created_height is immutable there: if it moved, the body could not be found.
const identByTxidSQL = `
SELECT i.txid, i.created_height, i.off_chain_since, i.membership, i.fee, i.size_in_bytes,
       i.tx_inpoints, i.locktime, i.created_at, i.conflicting_children, i.flags, b.raw_tx
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
  JOIN tx_ident i ON i.leaf = k.leaf AND i.txid = k.txid
  LEFT JOIN tx_body b ON b.created_height = i.created_height AND b.txid = i.txid`

// minedByTxidSQL reads every membership row for a set of transactions, across every live
// window, in insertion order. The primary key leads with txid, so this is one descent per
// window per transaction.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, and that is the difference
// between one descent per key and a read of every live window. Written as the plain
// `JOIN tx_mined m ON m.txid = k.txid` the planner is free to hash-join the keys against the
// whole partitioned table, and it does: measured on this schema at 40,000 transactions across
// six windows, 500 keys took 9.9 ms and the plan carried a Seq Scan on every one of the six
// windows, against 3.4 ms and none for the lateral. At 400,000 the planner happened to choose
// index scans for both, which is exactly why the fence has to be in the statement rather than
// left to the estimate: the hash-join cost grows with the size of the live membership set,
// which at 1440 blocks of mainnet is millions of rows, and this read is on the validator's
// parent-resolution path.
//
// OFFSET 0 is the fence itself, the same one coinFactsSQL relies on: it stops the planner
// pulling the subquery up into the outer join, which is what re-admits the hash join. The
// inner ORDER BY walks the primary key in order; the outer one is what actually guarantees the
// grouping the reader relies on, because the LEFT JOIN to the body may reorder rows.
const minedByTxidSQL = `
SELECT k.txid, m.mined_height, m.block_id, m.subtree_idx, m.size_in_bytes, m.fee,
       m.tx_inpoints, m.locktime, m.created_at, m.flags, b.raw_tx
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
          m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags, m.seq
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
   OFFSET 0
 ) AS m
  LEFT JOIN tx_body b ON b.created_height = m.created_height AND b.txid = k.txid
 ORDER BY k.txid, m.seq`

// coinFactsSQL reads one live coin per transaction, for the transactions nothing else knows.
// The LATERAL with ORDER BY and LIMIT 1 OFFSET 0 is the fence the planner needs to walk the
// packed-key index instead of scanning the coin table, and each half of it was measured:
// without the packed-key range bound the planner reads every leaf partition, and without the
// ORDER BY it materialises the whole range before the LIMIT can stop it. createMinedPlanSQL's
// duplicate-coin guard carries the identical fence, for the identical reason.
const coinFactsSQL = `
SELECT k.txid, hit.mined_height, hit.block_id, hit.flags, b.raw_tx
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT u.mined_height, u.block_id, u.created_height, u.flags
     FROM utxo u
    WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
    ORDER BY u.ukey LIMIT 1 OFFSET 0
 ) AS hit
  LEFT JOIN tx_body b ON b.created_height = hit.created_height AND b.txid = k.txid`

// lookupResult is one read of the store: what it found, and what it could not make sense of.
//
// The two maps are separate because a transaction the store HOLDS but cannot decode is not the
// same answer as one it does not hold, and the difference decides what the caller does. A miss
// makes the validator reject a child for a missing parent, which is recoverable and correct. A
// corrupt row is a storage fault on that one transaction, and it belongs on that transaction's
// own entry rather than on the whole batch: BatchDecorate's contract is that a transaction the
// store cannot serve is reported on ITS OWN entry, so one unreadable tx_inpoints must not
// reject every transaction that happened to travel with it.
type lookupResult struct {
	found  map[chainhash.Hash]*meta.Data
	failed map[chainhash.Hash]error
}

func newLookupResult(n int) lookupResult {
	return lookupResult{found: make(map[chainhash.Hash]*meta.Data, n), failed: nil}
}

// fail records a per-transaction fault. The hash counts as RESOLVED from here on, which is the
// point: a corrupt identity row must not fall through to the membership table or the coin, or
// the store would answer from a coin for a transaction whose real record it just refused to
// read, silently substituting a thinner answer for a fault.
func (r *lookupResult) fail(h chainhash.Hash, err error) {
	if r.failed == nil {
		r.failed = map[chainhash.Hash]error{}
	}

	r.failed[h] = err
}

// resolved reports whether any step has already answered for this hash, either way.
func (r *lookupResult) resolved(h chainhash.Hash) bool {
	if _, ok := r.found[h]; ok {
		return true
	}

	_, ok := r.failed[h]

	return ok
}

// lookupMany resolves a set of transactions in the read order. Misses are absent from both
// maps; a transaction whose stored row will not decode lands in failed rather than found.
//
// Each step asks only about the hashes the steps before it could not answer, so a batch of
// ordinary mined parents costs one membership probe each and never touches the coin table,
// and a batch of mempool parents never leaves the identity table.
//
// The returned error is for faults that are NOT per-transaction: a dead connection, a syntax
// error, a partition that vanished mid-read. Those really do fail every entry, because nothing
// was answered.
func (s *Store) lookupMany(ctx context.Context, hashes []chainhash.Hash) (lookupResult, error) {
	res := newLookupResult(len(hashes))
	if len(hashes) == 0 {
		return res, nil
	}

	// Step 1: the identity table (mempool and fork-limbo rows).
	//
	// One entry per DISTINCT hash. A batch can name the same parent twice, and asking twice
	// would return the row twice and waste the round trip this call exists to save.
	uniq := make([]chainhash.Hash, 0, len(hashes))
	seen := make(map[chainhash.Hash]struct{}, len(hashes))
	leaves := make([]int16, 0, len(hashes))
	txids := make([][]byte, 0, len(hashes))

	for i := range hashes {
		if _, dup := seen[hashes[i]]; dup {
			continue
		}

		seen[hashes[i]] = struct{}{}

		uniq = append(uniq, hashes[i])
		leaves = append(leaves, LeafFor(hashes[i][:]))
		txids = append(txids, hashes[i][:])
	}

	if err := s.readIdentRows(ctx, leaves, txids, &res); err != nil {
		return lookupResult{}, err
	}

	rest := stillMissing(uniq, &res)
	if len(rest) == 0 {
		return res, nil
	}

	// Step 2: membership by transaction id.
	if err := s.readMinedInto(ctx, rest, &res); err != nil {
		return lookupResult{}, err
	}

	rest = stillMissing(rest, &res)
	if len(rest) == 0 {
		return res, nil
	}

	// Step 3: the coin.
	if err := s.readCoinFacts(ctx, rest, &res); err != nil {
		return lookupResult{}, err
	}

	return res, nil
}

// stillMissing returns the hashes no step so far has answered.
//
// Named for the shadowing it avoids: MarkTransactionsOnLongestChain has a []error local called
// missing, and two different things called the same name in one package is how a reader ends
// up reasoning about the wrong one.
func stillMissing(hashes []chainhash.Hash, res *lookupResult) []chainhash.Hash {
	var rest []chainhash.Hash

	for _, h := range hashes {
		if !res.resolved(h) {
			rest = append(rest, h)
		}
	}

	return rest
}

// readIdentRows fills in every transaction that still holds an identity row: a mempool
// arrival, or one un-mined by a reorg and waiting again.
func (s *Store) readIdentRows(ctx context.Context, leaves []int16, txids [][]byte,
	res *lookupResult) error {
	rows, err := s.pool.Query(ctx, identByTxidSQL, leaves, txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] identity rows", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			r    metaRow
		)

		if err := rows.Scan(&txid, &r.createdHeight, &r.offChainSince, &r.membership,
			&r.fee, &r.sizeInBytes, &r.txInpoints, &r.locktime, &r.createdAt,
			&r.conflictingChildren, &r.flags, &r.rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] identity scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		// A row that will not decode is this transaction's fault alone. See lookupResult.
		data, derr := r.toMeta(&h)
		if derr != nil {
			res.fail(h, derr)

			continue
		}

		res.found[h] = data
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] identity rows", err)
	}

	return nil
}

// readMinedRows fills in every transaction a live membership window still names.
//
// One transaction can hold several rows -- one per block that contains it -- and they arrive
// grouped and in insertion order, which is what the conformance suite asserts about
// SubtreeIdxs. The scalars that describe the transaction rather than a block come off the
// FIRST row; the rows are written by one statement per block application, so they agree, and
// taking the first is the reading that does not depend on how many blocks claim it.
func (s *Store) readMinedInto(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	rows, err := s.pool.Query(ctx, minedByTxidSQL, txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] membership rows", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid        []byte
			minedHeight int32
			blockID     int32
			subtreeIdx  int32
			sizeInBytes *int32
			fee         *int64
			txInpoints  []byte
			locktime    *int32
			createdAt   *int64
			flags       int16
			rawTx       []byte
		)

		if err := rows.Scan(&txid, &minedHeight, &blockID, &subtreeIdx,
			&sizeInBytes, &fee, &txInpoints, &locktime, &createdAt, &flags, &rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] membership scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		// A row this loop already refused is not retried on its later rows: the first fault
		// is the answer for the transaction, and appending a second block to a record that
		// was never built would panic on a nil map entry.
		if _, bad := res.failed[h]; bad {
			continue
		}

		data := res.found[h]
		if data == nil {
			data = &meta.Data{
				IsCoinbase:  flags&FlagCoinbase != 0,
				Conflicting: flags&FlagConflicting != 0,
				Locked:      flags&FlagLocked != 0,
			}

			if sizeInBytes != nil {
				data.SizeInBytes = uint64(*sizeInBytes) //nolint:gosec // a size is never negative
			}

			// NULL for every row the block path wrote, and the fee the identity row carried
			// for one the tip's stamp moved here. See the fee column in schema.go.
			if fee != nil {
				data.Fee = uint64(*fee) //nolint:gosec // a fee is never negative
			}

			if locktime != nil {
				data.LockTime = uint32(*locktime) //nolint:gosec // a locktime is never negative
			}

			if createdAt != nil {
				data.CreatedAt = *createdAt
			}

			if len(txInpoints) > 0 {
				ip, ierr := subtree.NewTxInpointsFromBytes(txInpoints)
				if ierr != nil {
					res.fail(h, errors.NewStorageError("[utxoset][lookup] inpoints %s", h.String(), ierr))

					continue
				}

				data.TxInpoints = ip
			}

			res.found[h] = data
		}

		data.BlockIDs = append(data.BlockIDs, uint32(blockID))             //nolint:gosec // a block id is never negative
		data.BlockHeights = append(data.BlockHeights, uint32(minedHeight)) //nolint:gosec // a height is never negative
		data.SubtreeIdxs = append(data.SubtreeIdxs, int(subtreeIdx))

		// A body-less row is expected once its window has aged out, so this is a nil
		// transaction rather than an error, exactly as it is on the identity read.
		if data.Tx == nil && len(rawTx) > 0 {
			tx, terr := bt.NewTxFromBytes(rawTx)
			if terr != nil {
				delete(res.found, h)
				res.fail(h, errors.NewStorageError("[utxoset][lookup] decode body %s", h.String(), terr))

				continue
			}

			data.Tx = tx
		}
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] membership rows", err)
	}

	return nil
}

// readCoinFacts is the last step: a transaction nothing else knows about, answered from one of
// its own live coins.
//
// What comes back is deliberately thin. The coin carries its block and nothing about the
// transaction's fee, size, inputs or subtree position, which is exactly what a pruned SV Node
// can say about a parent whose block it no longer holds, and all the validator needs to check
// a child's inputs.
func (s *Store) readCoinFacts(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	leaves, ids, los, his := liveCoinArgs(txids)

	rows, err := s.pool.Query(ctx, coinFactsSQL, leaves, ids, los, his)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] coin facts", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid        []byte
			minedHeight int32
			blockID     int32
			flags       int16
			rawTx       []byte
		)

		if err := rows.Scan(&txid, &minedHeight, &blockID, &flags, &rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] coin facts scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		data := &meta.Data{
			IsCoinbase:  flags&FlagCoinbase != 0,
			Conflicting: flags&FlagConflicting != 0,
			Locked:      flags&FlagLocked != 0,
		}

		// mined_height 0 is the unconfirmed sentinel, and an unconfirmed coin means the
		// transaction claims no block at all. Reporting block id 0 for it would be a lie
		// that block validation cannot tell from genesis, whose id really is 0.
		if minedHeight > 0 {
			data.BlockIDs = []uint32{uint32(blockID)}         //nolint:gosec // a block id is never negative
			data.BlockHeights = []uint32{uint32(minedHeight)} //nolint:gosec // a height is never negative
			data.SubtreeIdxs = []int{0}
		}

		if len(rawTx) > 0 {
			tx, terr := bt.NewTxFromBytes(rawTx)
			if terr != nil {
				res.fail(h, errors.NewStorageError("[utxoset][lookup] decode body %s", h.String(), terr))

				continue
			}

			data.Tx = tx
		}

		res.found[h] = data
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] coin facts", err)
	}

	return nil
}

// liveCoinArgs expands transaction ids into the four parallel arrays coinFactsSQL takes: the
// partition key, the identity, and the packed-key range covering every output the transaction
// could have created.
//
// It is a named function rather than four lines inline so the plan tests can build exactly the
// arguments the production path builds. A test that explained a hand-written variant would be
// pinning the plan of a statement nothing runs.
func liveCoinArgs(txids [][]byte) (leaves []int16, ids [][]byte, los, his [][16]byte) {
	leaves = make([]int16, 0, len(txids))
	ids = make([][]byte, 0, len(txids))
	los = make([][16]byte, 0, len(txids))
	his = make([][16]byte, 0, len(txids))

	for _, id := range txids {
		leaves = append(leaves, LeafFor(id))
		ids = append(ids, id)
		los = append(los, Pack(id, 0))
		his = append(his, Pack(id, ^uint32(0)))
	}

	return leaves, ids, los, his
}
