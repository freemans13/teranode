package utxoset

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
)

// getSQL reads one transaction from the identity row, joining the body only if it is still
// inside its window.
//
// The join is LEFT, and that is the point rather than caution. The body window is dropped
// after 288 blocks while the identity row lives for as long as any of the transaction's
// outputs is unspent, at any age, so a body-less row is the ordinary steady state for an old
// transaction. An inner join would report every such transaction as missing, and a missing
// parent makes the validator reject its children.
//
// Both halves are found by (leaf, txid) and (created_height, txid) respectively, so this is
// two index probes and no scan. The body's height comes from the identity row, which is why
// created_height is immutable there: if it moved, the body could not be found.
const getSQL = `
SELECT i.created_height, i.off_chain_since, i.membership, i.fee, i.size_in_bytes,
       i.tx_inpoints, i.locktime, i.created_at, i.conflicting_children, i.flags, b.raw_tx
  FROM tx_ident i
  LEFT JOIN tx_body b ON b.created_height = i.created_height AND b.txid = i.txid
 WHERE i.leaf = $1 AND i.txid = $2`

// unpackMembership turns the packed 12-byte triples back into the three parallel slices the
// meta.Data carries, in the order they were written.
//
// Insertion order is load-bearing: the shared conformance suite requires subtree indexes to
// come back in the order they were written rather than sorted, so this must never reorder or
// deduplicate.
func unpackMembership(m []byte) (blockIDs, heights []uint32, subtreeIdxs []int) {
	n := len(m) / 12
	if n == 0 {
		return nil, nil, nil
	}

	blockIDs = make([]uint32, 0, n)
	heights = make([]uint32, 0, n)
	subtreeIdxs = make([]int, 0, n)

	for i := 0; i < n; i++ {
		e := m[i*12 : i*12+12]
		blockIDs = append(blockIDs, binary.BigEndian.Uint32(e[0:4]))
		heights = append(heights, binary.BigEndian.Uint32(e[4:8]))
		subtreeIdxs = append(subtreeIdxs, int(int32(binary.BigEndian.Uint32(e[8:12])))) //nolint:gosec // round-trips what create packed
	}

	return blockIDs, heights, subtreeIdxs
}

// Get returns everything the store holds about one transaction.
//
// The field list is accepted and deliberately ignored. Both halves arrive on a single row
// from one statement, so narrowing the projection would save a few bytes on the wire and
// nothing else, while giving a caller a partially populated meta.Data that it might
// dereference. Answering the whole question is cheaper than answering half of it carefully.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	if hash == nil {
		return nil, errors.NewProcessingError("[utxoset][Get] nil hash")
	}

	// Batched when configured. The validator resolves parents one at a time from many
	// goroutines, and each of those is otherwise its own round trip.
	if s.getBatcher != nil {
		return s.getBatched(ctx, hash)
	}

	var r metaRow

	err := s.pool.QueryRow(ctx, getSQL, LeafFor(hash[:]), hash[:]).Scan(
		&r.createdHeight, &r.offChainSince, &r.membership, &r.fee, &r.sizeInBytes,
		&r.txInpoints, &r.locktime, &r.createdAt, &r.conflictingChildren, &r.flags, &r.rawTx)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, errors.NewTxNotFoundError("[utxoset][Get] %s", hash.String())
	case err != nil:
		return nil, errors.NewStorageError("[utxoset][Get] %s", hash.String(), err)
	}

	data, err := r.toMeta(hash)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetMeta fills in an existing meta.Data from the store, which is what callers holding a
// record they want refreshed use.
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if data == nil {
		return errors.NewProcessingError("[utxoset][GetMeta] nil data")
	}

	got, err := s.Get(ctx, hash)
	if err != nil {
		return err
	}

	*data = *got

	return nil
}

// unpackHashes reads a packed run of 32-byte transaction ids.
func unpackHashes(b []byte) ([]chainhash.Hash, error) {
	if len(b) == 0 {
		return nil, nil
	}

	if len(b)%chainhash.HashSize != 0 {
		return nil, errors.NewProcessingError("packed hashes are %d bytes, not a multiple of %d",
			len(b), chainhash.HashSize)
	}

	out := make([]chainhash.Hash, 0, len(b)/chainhash.HashSize)

	for i := 0; i < len(b); i += chainhash.HashSize {
		var h chainhash.Hash

		copy(h[:], b[i:i+chainhash.HashSize])

		out = append(out, h)
	}

	return out, nil
}

// metaRow is one row as the identity table and its body return it, before it becomes the
// record a caller sees.
//
// It exists so the single read and the batched read share one conversion. Two copies of
// "what a stored transaction means" is how the two drift apart on a field nobody notices
// until something downstream reads a zero.
type metaRow struct {
	createdHeight       int32
	offChainSince       *int32
	membership          []byte
	fee                 *int64
	sizeInBytes         *int32
	txInpoints          []byte
	locktime            *int32
	createdAt           *int64
	conflictingChildren []byte
	flags               int16
	rawTx               []byte
}

// toMeta turns a stored row into the record callers read.
func (r *metaRow) toMeta(hash *chainhash.Hash) (*meta.Data, error) {
	data := &meta.Data{
		IsCoinbase:  r.flags&FlagCoinbase != 0,
		Conflicting: r.flags&FlagConflicting != 0,
		Locked:      r.flags&FlagLocked != 0,
	}

	data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = unpackMembership(r.membership)

	var err error
	if data.ConflictingChildren, err = unpackHashes(r.conflictingChildren); err != nil {
		return nil, errors.NewStorageError("[utxoset] conflicting children %s", hash.String(), err)
	}

	if r.offChainSince != nil {
		data.UnminedSince = uint32(*r.offChainSince) //nolint:gosec // a stored height is never negative
	}

	if r.fee != nil {
		data.Fee = uint64(*r.fee) //nolint:gosec // a fee is never negative
	}

	if r.sizeInBytes != nil {
		data.SizeInBytes = uint64(*r.sizeInBytes) //nolint:gosec // a size is never negative
	}

	if r.locktime != nil {
		data.LockTime = uint32(*r.locktime) //nolint:gosec // a locktime is never negative
	}

	if r.createdAt != nil {
		data.CreatedAt = *r.createdAt
	}

	if len(r.txInpoints) > 0 {
		ip, ierr := subtree.NewTxInpointsFromBytes(r.txInpoints)
		if ierr != nil {
			return nil, errors.NewStorageError("[utxoset] inpoints %s", hash.String(), ierr)
		}

		data.TxInpoints = ip
	}

	// A body-less row is expected once its window has aged out, so this is a nil transaction
	// rather than an error. Callers that genuinely need the bytes have to check.
	if len(r.rawTx) > 0 {
		tx, terr := bt.NewTxFromBytes(r.rawTx)
		if terr != nil {
			return nil, errors.NewStorageError("[utxoset] decode body %s", hash.String(), terr)
		}

		data.Tx = tx
	}

	return data, nil
}
