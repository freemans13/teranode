package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// batchGetSQL resolves many transactions in one statement.
//
// Subtree validation resolves thousands at a time, so one round trip each would dominate.
// Both other implementations of this interface funnel single reads into this call for that
// reason, and this store now does the same.
//
// The join to the body is LEFT, exactly as the single read's is, and for the same reason: a
// transaction whose bytes have aged out of their window is the ordinary steady state, not an
// error, and an inner join would report every one of them as missing.
const batchGetSQL = `
SELECT i.txid, i.created_height, i.off_chain_since, i.membership, i.fee, i.size_in_bytes,
       i.tx_inpoints, i.locktime, i.created_at, i.conflicting_children, i.flags, b.raw_tx
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
  JOIN tx_ident i ON i.leaf = k.leaf AND i.txid = k.txid
  LEFT JOIN tx_body b ON b.created_height = i.created_height AND b.txid = i.txid`

// BatchDecorate fills in metadata for many transactions at once.
//
// A transaction the store does not hold is reported on ITS OWN entry, not by failing the
// call. The validator turns a missing parent into a rejection for that transaction alone, so
// failing the batch would reject every transaction that happened to be resolved beside it.
//
// The field list is accepted and ignored, as it is on the single read: every column arrives
// on one row from one statement, so narrowing the projection would save a little on the wire
// and hand the caller a partly populated record it might dereference.
func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	if len(items) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(items))
	txids := make([][]byte, 0, len(items))

	// One entry per DISTINCT hash. A batch can name the same parent twice, and asking twice
	// would return two rows for it and waste the round trip this call exists to save.
	seen := make(map[chainhash.Hash]struct{}, len(items))

	for _, it := range items {
		if _, dup := seen[it.Hash]; dup {
			continue
		}

		seen[it.Hash] = struct{}{}

		leaves = append(leaves, LeafFor(it.Hash[:]))
		txids = append(txids, it.Hash[:])
	}

	rows, err := s.pool.Query(ctx, batchGetSQL, leaves, txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][BatchDecorate]", err)
	}

	found := make(map[chainhash.Hash]*metaRow, len(items))

	for rows.Next() {
		var (
			txid []byte
			r    metaRow
		)

		if err := rows.Scan(&txid, &r.createdHeight, &r.offChainSince, &r.membership,
			&r.fee, &r.sizeInBytes, &r.txInpoints, &r.locktime, &r.createdAt,
			&r.conflictingChildren, &r.flags, &r.rawTx); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][BatchDecorate] scan", err)
		}

		var h chainhash.Hash
		copy(h[:], txid)

		row := r
		found[h] = &row
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][BatchDecorate] rows", err)
	}

	for _, it := range items {
		row, ok := found[it.Hash]
		if !ok {
			it.Err = errors.NewTxNotFoundError("[utxoset][BatchDecorate] %s", it.Hash.String())
			continue
		}

		data, derr := row.toMeta(&it.Hash)
		if derr != nil {
			it.Err = derr
			continue
		}

		it.Data = data
	}

	return nil
}

// setLockedSQL flips the locked bit on the transaction row AND on every coin it created.
//
// Both, because they are read by different things. The transaction row is what a caller sees
// through Get; the coin row is what the spend path reads, and the spend path never looks at
// the transaction row. Setting only one would leave a transaction reporting itself locked
// while its coins were still spendable, or the reverse.
const setLockedSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::bytea[]) AS t(leaf, txid)
),
ident AS (
    UPDATE tx_ident i SET flags = CASE WHEN $3::boolean
                                       THEN i.flags | $4::smallint
                                       ELSE i.flags & ~$4::smallint END
      FROM k WHERE i.leaf = k.leaf AND i.txid = k.txid
)
UPDATE utxo u SET flags = CASE WHEN $3::boolean
                               THEN u.flags | $4::smallint
                               ELSE u.flags & ~$4::smallint END
  FROM k WHERE u.leaf = k.leaf AND u.txid = k.txid`

// SetLocked marks transactions as locked for spending, or releases them.
//
// The release is the two-phase commit path: a transaction created for the mempool is locked,
// and unlocked once it commits. That is one call per transaction on a hot path, which is why
// the sql store batches it, and why this takes a slice.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	// A single-hash call is the two-phase commit release, one per mempool transaction, and
	// that is what the batcher is for. A caller that already has many hashes is its own
	// batch and goes straight through.
	if s.lockBatcher != nil && len(txHashes) == 1 {
		errCh := make(chan error, 1)

		s.lockBatcher.PutCtx(ctx, &lockItem{hash: txHashes[0], value: value, errCh: errCh})

		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.setLockedDirect(ctx, txHashes, value)
}

// setLockedDirect issues the update, and is what both the direct and the batched path end at.
func (s *Store) setLockedDirect(ctx context.Context, txHashes []chainhash.Hash, value bool) error {

	leaves := make([]int16, 0, len(txHashes))
	txids := make([][]byte, 0, len(txHashes))

	for i := range txHashes {
		leaves = append(leaves, LeafFor(txHashes[i][:]))
		txids = append(txids, txHashes[i][:])
	}

	if _, err := s.pool.Exec(ctx, setLockedSQL, leaves, txids, value, FlagLocked); err != nil {
		return errors.NewStorageError("[utxoset][SetLocked]", err)
	}

	return nil
}
