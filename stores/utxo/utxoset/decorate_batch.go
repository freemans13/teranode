package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BatchDecorate fills in metadata for many transactions at once.
//
// A transaction the store does not hold is reported on ITS OWN entry, not by failing the
// call. The validator turns a missing parent into a rejection for that transaction alone, so
// failing the batch would reject every transaction that happened to be resolved beside it.
//
// The field list is accepted and ignored, as it is on the single read: whichever step answers
// returns every column it holds on one row, so narrowing the projection would save a little on
// the wire and hand the caller a partly populated record it might dereference.
//
// The read order lives in lookupMany, which this, Get and the get batcher all go through, so
// there is one answer to "where does a transaction come from" rather than three.
func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	if len(items) == 0 {
		return nil
	}

	hashes := make([]chainhash.Hash, 0, len(items))
	for _, it := range items {
		hashes = append(hashes, it.Hash)
	}

	res, err := s.lookupMany(ctx, hashes)
	if err != nil {
		return errors.NewStorageError("[utxoset][BatchDecorate]", err)
	}

	// One record per ENTRY, even where two entries name the same transaction. lookupMany
	// resolves a repeated hash once, and handing both entries the same pointer would let a
	// caller decorating one of them mutate the other's answer.
	given := make(map[chainhash.Hash]struct{}, len(items))

	for _, it := range items {
		// A row that would not decode is reported on its own entry, the same way a miss is.
		// That is this call's whole contract: one corrupt tx_inpoints must not reject every
		// transaction that happened to be resolved beside it.
		if derr, bad := res.failed[it.Hash]; bad {
			it.Err = derr
			continue
		}

		data, ok := res.found[it.Hash]
		if !ok {
			it.Err = errors.NewTxNotFoundError("[utxoset][BatchDecorate] %s", it.Hash.String())
			continue
		}

		if _, dup := given[it.Hash]; dup {
			copied := *data
			data = &copied
		} else {
			given[it.Hash] = struct{}{}
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
