package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// deleteTxSQL removes every trace of a transaction: its coins, the undo records for those
// coins, its serialized bytes and its identity row.
//
// One statement of arrays, so deleting one transaction and deleting a thousand run the same
// code, exactly as spendJournalSQL and createPlanSQL do on the write paths.
//
// Data-modifying common table expressions share one snapshot, so the four deletes commit
// together. That matters for the same reason it mattered to createPlanSQL: a half-deleted
// transaction, its identity gone and its coins left, is a live output that nothing can ever
// reclaim, because reclaim finds coins through their identity row.
//
// Coins are found by a RANGE over the packed key rather than by transaction id. The one index
// on that table is on the key, and the key packs the transaction id prefix first precisely so
// that "every output of parent P" is a range scan. See Pack in schema.go. The full 32-byte id
// is still rechecked, because the 96-bit prefix is non-unique by design and can locate a row
// but never authorise acting on one.
//
// The undo records this destroys are the ones where the deleted transaction is the PARENT,
// meaning the payloads for its own already-spent outputs. They must go. Left behind, a later
// unspend would restore a coin whose identity row no longer exists: spendable, invisible to
// reclaim, and permanent.
//
// Records where the deleted transaction is the SPENDER are deliberately left alone. They
// authorise restoring OTHER transactions' coins, and the offline rewind tool unspends before
// it deletes, so destroying them here would turn an ordering mistake into unrecoverable coin
// loss. They are inert anyway, and retire with their partition.
//
// The body is reached through the identity row's created_height rather than by transaction id,
// because created_height leads the body's primary key and the id alone cannot use it. This is
// createPlanSQL's claim-gates-the-body join, run backwards.
const deleteTxSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[])
        AS t(leaf, txid, lo, hi)
),
coins AS (
    DELETE FROM utxo u USING k
     WHERE u.leaf  = k.leaf
       AND u.ukey >= k.lo
       AND u.ukey <= k.hi
       AND u.txid  = k.txid
),
undo AS (
    DELETE FROM spend_journal j USING k
     WHERE j.ukey >= k.lo
       AND j.ukey <= k.hi
       AND j.txid  = k.txid
),
ident AS (
    DELETE FROM tx_ident i USING k
     WHERE i.leaf = k.leaf AND i.txid = k.txid
    RETURNING i.created_height, i.txid
),
body AS (
    DELETE FROM tx_body b USING ident d
     WHERE b.created_height = d.created_height AND b.txid = d.txid
)
SELECT count(*) FROM ident`

// Delete removes a transaction and everything the store holds about it.
//
// A transaction the store does not hold is SUCCESS rather than an error, and that is the
// contract both reference stores keep. Every caller depends on it. The offline rewind tool
// tolerates not-found explicitly, and block assembly's reorg tolerates ONLY a not-found error,
// so returning anything else here aborts a reorg.
//
// Not batched, deliberately. This fires on a reorg and from operator tools, never at validator
// rate, and a batcher would buy nothing but a window in which a caller believes a transaction
// is gone before it is.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if hash == nil {
		return errors.NewProcessingError("[utxoset][Delete] nil hash")
	}

	return s.deleteIn(ctx, s.pool, []chainhash.Hash{*hash})
}

// deleteIn is Delete against an arbitrary querier and over a batch, so a future multi-delete
// and any composition inside a database transaction share these predicates rather than growing
// a second copy of them. A single Delete is a batch of one.
func (s *Store) deleteIn(ctx context.Context, q querier, hashes []chainhash.Hash) error {
	if len(hashes) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(hashes))
	txids := make([][]byte, 0, len(hashes))
	los := make([][16]byte, 0, len(hashes))
	his := make([][16]byte, 0, len(hashes))

	for i := range hashes {
		leaves = append(leaves, LeafFor(hashes[i][:]))
		txids = append(txids, hashes[i][:])
		los = append(los, Pack(hashes[i][:], 0))
		his = append(his, Pack(hashes[i][:], ^uint32(0)))
	}

	// The count is read rather than discarded so that a future batch form can tell its caller
	// what went, but it is deliberately NOT compared against the number of hashes offered. A
	// transaction the store never held deletes nothing, and that is the contract rather than a
	// miss.
	var removed int64

	if err := q.QueryRow(ctx, deleteTxSQL, leaves, txids, los, his).Scan(&removed); err != nil {
		return errors.NewStorageError("[utxoset][Delete] remove", err)
	}

	return nil
}
