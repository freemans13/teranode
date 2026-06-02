package model

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/subtreecache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// buildParsedSubtree returns a complete 2-leaf subtree (coinbase placeholder +
// one tx) with a resolvable root hash, and its meta.
func buildParsedSubtree(t *testing.T) (*subtreepkg.Subtree, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	var txHash chainhash.Hash
	txHash[0] = 0xAB
	require.NoError(t, st.AddNode(txHash, 1, 100))
	require.NotNil(t, st.RootHash())

	return st, subtreepkg.NewSubtreeMeta(st)
}

func newBlockForSubtree(t *testing.T, subtreeHash *chainhash.Hash) *Block {
	t.Helper()

	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	b, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{subtreeHash}, 2, 1000, 0, 0)
	require.NoError(t, err)

	return b
}

// GetAndValidateSubtrees must serve a cached parsed subtree without touching the
// disk: the backing store is empty, so success can only come from the cache.
func TestGetAndValidateSubtrees_ServedFromCache(t *testing.T) {
	st, metaData := buildParsedSubtree(t)
	store := subtreecache.NewStore(memory.New(), 1<<20) // empty disk
	store.PutParsedSubtree(*st.RootHash(), st, metaData)

	b := newBlockForSubtree(t, st.RootHash())

	err := b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, store, 1)
	require.NoError(t, err, "must succeed from cache despite empty disk")
	require.Len(t, b.SubtreeSlices, 1)
	require.Same(t, st, b.SubtreeSlices[0], "subtree should be the cached object")
	require.Equal(t, uint64(2), b.TransactionCount, "txCount accumulator must be fed on the cache path")
}

// With no cache and an empty disk, GetAndValidateSubtrees must still fall back to
// the disk read (and fail to find it) — proving the cache is optional.
func TestGetAndValidateSubtrees_FallsBackToDiskOnMiss(t *testing.T) {
	st, _ := buildParsedSubtree(t)
	store := subtreecache.NewStore(memory.New(), 1<<20) // cache enabled but empty

	b := newBlockForSubtree(t, st.RootHash())

	err := b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, store, 1)
	require.Error(t, err, "cache miss must fall back to disk, which is empty")
}

// getSubtreeMetaSlice must serve a cached meta when its provenance matches the
// subtree being validated (cachedMeta.Subtree == subtree).
func TestGetSubtreeMetaSlice_ServedFromCacheWhenProvenanceMatches(t *testing.T) {
	st, metaData := buildParsedSubtree(t)
	store := subtreecache.NewStore(memory.New(), 1<<20) // empty disk
	store.PutParsedSubtree(*st.RootHash(), st, metaData)

	b := newBlockForSubtree(t, st.RootHash())

	got, err := b.getSubtreeMetaSlice(context.Background(), store, *st.RootHash(), st)
	require.NoError(t, err, "must serve meta from cache despite empty disk")
	require.Same(t, metaData, got)
}

// If the subtree being validated is NOT the cached subtree (different provenance),
// the cached meta must be ignored and the disk read attempted.
func TestGetSubtreeMetaSlice_IgnoresCacheOnProvenanceMismatch(t *testing.T) {
	st, metaData := buildParsedSubtree(t)
	store := subtreecache.NewStore(memory.New(), 1<<20) // empty disk
	store.PutParsedSubtree(*st.RootHash(), st, metaData)

	b := newBlockForSubtree(t, st.RootHash())

	// A different subtree object with the same root hash content.
	other, _ := buildParsedSubtree(t)

	_, err := b.getSubtreeMetaSlice(context.Background(), store, *st.RootHash(), other)
	require.Error(t, err, "provenance mismatch must bypass cache and hit the empty disk")
}
