package utxo_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/stretchr/testify/require"
)

// knownStore answers BatchDecorate for the hashes it holds and reports the rest not found on
// their own entries, as every real store does for a transaction it has never seen.
type knownStore struct {
	*nullstore.NullStore
	known map[chainhash.Hash]bool
	calls int
	fail  error
}

func (s *knownStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.calls++

	if s.fail != nil {
		return s.fail
	}

	for _, it := range items {
		if !s.known[it.Hash] {
			it.Err = errors.NewTxNotFoundError("%s", it.Hash.String())
			continue
		}

		it.Data = &meta.Data{}
	}

	return nil
}

// A block path that chose not to store some transactions learns which of them were stored
// anyway, by an earlier failed attempt at the same block, in one batched read.
func TestStoredSubsetKeepsOnlyWhatTheStoreHolds(t *testing.T) {
	held, never := chainhash.Hash{1}, chainhash.Hash{2}
	store := &knownStore{NullStore: &nullstore.NullStore{}, known: map[chainhash.Hash]bool{held: true}}

	got, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&never, nil, &held})
	require.NoError(t, err, "a transaction the store has never seen is the normal case")
	require.Equal(t, []*chainhash.Hash{&held}, got)
	require.Equal(t, 1, store.calls, "one read for the whole set")
}

func TestStoredSubsetAsksNothingOfAnEmptySet(t *testing.T) {
	store := &knownStore{NullStore: &nullstore.NullStore{}}

	got, err := utxo.StoredSubset(context.Background(), store, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, store.calls)
}

func TestStoredSubsetReturnsAStoreFailure(t *testing.T) {
	h := chainhash.Hash{3}
	store := &knownStore{NullStore: &nullstore.NullStore{}, fail: errors.NewStorageError("down")}

	_, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&h})
	require.Error(t, err)
}

// A store that answers nothing at all, as the null store does, holds nothing.
func TestStoredSubsetTreatsNoAnswerAsAbsent(t *testing.T) {
	h := chainhash.Hash{4}

	got, err := utxo.StoredSubset(context.Background(), &nullstore.NullStore{}, []*chainhash.Hash{&h})
	require.NoError(t, err)
	require.Empty(t, got)
}

// proberStore answers StoredTxs itself and counts any BatchDecorate call through knownStore.
type proberStore struct {
	knownStore
	probes int
}

func (s *proberStore) StoredTxs(_ context.Context, hashes []*chainhash.Hash) ([]chainhash.Hash, error) {
	s.probes++

	var out []chainhash.Hash

	// Answered in reverse, to show StoredSubset restores the caller's order.
	for i := len(hashes) - 1; i >= 0; i-- {
		if s.known[*hashes[i]] {
			out = append(out, *hashes[i])
		}
	}

	return out, nil
}

// A store with its own cheap probe is asked through it, once, and never through BatchDecorate.
func TestStoredSubsetUsesTheStoresProbe(t *testing.T) {
	a, b, never := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}
	store := &proberStore{knownStore: knownStore{NullStore: &nullstore.NullStore{}, known: map[chainhash.Hash]bool{a: true, b: true}}}

	got, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&a, &never, nil, &b})
	require.NoError(t, err)
	require.Equal(t, []*chainhash.Hash{&a, &b}, got, "the caller's order")
	require.Equal(t, 1, store.probes)
	require.Zero(t, store.calls, "BatchDecorate is not used when the store can probe")
}

// A wrapper forwards to the wrapped store's probe when it has one and to the wrapped store's
// BatchDecorate when it does not.
func TestForwardStoredTxs(t *testing.T) {
	a, never := chainhash.Hash{1}, chainhash.Hash{2}

	prober := &proberStore{knownStore: knownStore{NullStore: &nullstore.NullStore{}, known: map[chainhash.Hash]bool{a: true}}}
	got, err := utxo.ForwardStoredTxs(context.Background(), prober, []*chainhash.Hash{&a, &never})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{a}, got)
	require.Equal(t, 1, prober.probes)
	require.Zero(t, prober.calls)

	plain := &knownStore{NullStore: &nullstore.NullStore{}, known: map[chainhash.Hash]bool{a: true}}
	got, err = utxo.ForwardStoredTxs(context.Background(), plain, []*chainhash.Hash{&a, &never})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{a}, got)
	require.Equal(t, 1, plain.calls)
}
