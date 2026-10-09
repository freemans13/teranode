package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// StoredTxProber is an optional store capability: a cheap answer to "which of these
// transactions did a create with no block information leave in the store".
//
// StoredSubset asks it on every block below the checkpoint, about transactions that are almost
// always absent, so a store whose general read walks several tables for a miss can answer this
// narrower question from the one place such a create writes. A store without it is asked
// through BatchDecorate. A wrapper store forwards it to the store it wraps.
type StoredTxProber interface {
	// StoredTxs returns the members of hashes the store holds a record of, in any order. A
	// hash it has never seen is left out, not reported as an error.
	StoredTxs(ctx context.Context, hashes []*chainhash.Hash) ([]chainhash.Hash, error)
}

// StoredSubset returns the members of hashes the store holds, in their original order, and
// drops those it has never seen.
//
// It is for transactions a block path chose not to store, those with no spendable outputs below
// the checkpoint. They are normally absent, but an earlier attempt at the same block that failed
// in subtree validation can have stored them as unmined, and nothing else would ever mark them
// mined: on 2026-09-24 that left 352 mined data transactions unmined for good on mainnet. The
// caller marks the returned ones mined with the rest of the block.
//
// Whether any were stored cannot be read off the rest of the block: an earlier attempt may have
// got only as far as a skipped transaction, so every skipped hash is asked about, on every
// attempt. A store that implements StoredTxProber answers that itself. Any other store is asked
// with one BatchDecorate, which every store answers with a miss on the entry itself rather than
// by failing the call. Either way a first attempt pays one batched read and no write. A store
// failure is returned.
func StoredSubset(ctx context.Context, store Store, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	if prober, ok := store.(StoredTxProber); ok {
		return storedSubsetByProbe(ctx, prober, hashes)
	}

	return StoredSubsetByDecorate(ctx, store, hashes)
}

func storedSubsetByProbe(ctx context.Context, prober StoredTxProber, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	asked := make([]*chainhash.Hash, 0, len(hashes))

	for _, h := range hashes {
		if h != nil {
			asked = append(asked, h)
		}
	}

	if len(asked) == 0 {
		return nil, nil
	}

	held, err := prober.StoredTxs(ctx, asked)
	if err != nil {
		return nil, err
	}

	if len(held) == 0 {
		return nil, nil
	}

	set := make(map[chainhash.Hash]struct{}, len(held))
	for _, h := range held {
		set[h] = struct{}{}
	}

	var stored []*chainhash.Hash

	for _, h := range asked {
		if _, ok := set[*h]; ok {
			stored = append(stored, h)
		}
	}

	return stored, nil
}

// StoredSubsetByDecorate is StoredSubset through BatchDecorate alone. It is what a store
// without StoredTxProber gets, and what a wrapper store uses to forward StoredTxs to a store it
// wraps that has no prober of its own.
func StoredSubsetByDecorate(ctx context.Context, store Store, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	items := make([]*UnresolvedMetaData, 0, len(hashes))

	for i, h := range hashes {
		if h == nil {
			continue
		}

		items = append(items, &UnresolvedMetaData{Hash: *h, Idx: i, Fields: []fields.FieldName{fields.BlockIDs}})
	}

	if len(items) == 0 {
		return nil, nil
	}

	if err := store.BatchDecorate(ctx, items, fields.BlockIDs); err != nil {
		return nil, err
	}

	var stored []*chainhash.Hash

	for _, it := range items {
		// Data as well as Err: a store that answers nothing at all has not found it either.
		if it.Err == nil && it.Data != nil {
			stored = append(stored, hashes[it.Idx])
		}
	}

	return stored, nil
}

// ForwardStoredTxs is StoredTxs for a wrapper store: it asks the wrapped store's own prober
// when it has one and falls back to BatchDecorate on the wrapped store otherwise, so the
// wrapper's own BatchDecorate, and any cache behind it, is never involved.
func ForwardStoredTxs(ctx context.Context, wrapped Store, hashes []*chainhash.Hash) ([]chainhash.Hash, error) {
	if prober, ok := wrapped.(StoredTxProber); ok {
		return prober.StoredTxs(ctx, hashes)
	}

	stored, err := StoredSubsetByDecorate(ctx, wrapped, hashes)
	if err != nil {
		return nil, err
	}

	out := make([]chainhash.Hash, 0, len(stored))
	for _, h := range stored {
		out = append(out, *h)
	}

	return out, nil
}
