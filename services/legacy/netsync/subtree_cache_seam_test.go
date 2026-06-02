package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/subtreecache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The shared subtree-store decorator must keep satisfying the netsync-side
// capability interfaces; a signature drift should fail the build rather than
// silently make the type-asserts miss and disable the cache.
var (
	_ parsedSubtreePutter  = (*subtreecache.Store)(nil)
	_ parsedSubtreeEvicter = (*subtreecache.Store)(nil)
)

// buildTestSubtree returns a small complete subtree (coinbase + one tx) with a
// resolvable root hash, plus its meta.
func buildTestSubtree(t *testing.T) (*subtreepkg.Subtree, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	var txHash chainhash.Hash
	txHash[0] = 0xAB
	require.NoError(t, st.AddNode(txHash, 1, 100))

	require.NotNil(t, st.RootHash(), "subtree should have a resolvable root hash")

	return st, subtreepkg.NewSubtreeMeta(st)
}

func TestCacheParsedSubtree_PopulatesStoreCache(t *testing.T) {
	store := subtreecache.NewStore(memory.New(), 1<<20)
	sm := &SyncManager{logger: ulogger.TestLogger{}, subtreeStore: store}

	st, metaData := buildTestSubtree(t)

	sm.cacheParsedSubtree(st, metaData)

	gotSt, ok := store.CachedSubtree(*st.RootHash())
	require.True(t, ok, "writer seam should populate the parsed cache")
	require.Same(t, st, gotSt)

	gotMeta, ok := store.CachedSubtreeMeta(*st.RootHash())
	require.True(t, ok)
	require.Same(t, metaData, gotMeta)
}

func TestCacheParsedSubtree_NoopWhenStoreHasNoCache(t *testing.T) {
	// A plain blob store (no decorator) must be handled gracefully — no panic,
	// just a no-op.
	sm := &SyncManager{logger: ulogger.TestLogger{}, subtreeStore: memory.New()}

	st, metaData := buildTestSubtree(t)
	require.NotPanics(t, func() {
		sm.cacheParsedSubtree(st, metaData)
	})
}

func TestCacheParsedSubtree_NilSubtreeNoPanic(t *testing.T) {
	store := subtreecache.NewStore(memory.New(), 1<<20)
	sm := &SyncManager{logger: ulogger.TestLogger{}, subtreeStore: store}

	require.NotPanics(t, func() {
		sm.cacheParsedSubtree(nil, nil)
	})
}

func TestEvictCachedSubtrees_RemovesBlockSubtrees(t *testing.T) {
	store := subtreecache.NewStore(memory.New(), 1<<20)
	sm := &SyncManager{logger: ulogger.TestLogger{}, subtreeStore: store}

	st1, m1 := buildTestSubtree(t)
	store.PutParsedSubtree(*st1.RootHash(), st1, m1)

	// Build a second distinct subtree so the block has two entries to evict.
	st2, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st2.AddCoinbaseNode())
	var tx2 chainhash.Hash
	tx2[0] = 0xCD
	require.NoError(t, st2.AddNode(tx2, 1, 100))
	store.PutParsedSubtree(*st2.RootHash(), st2, subtreepkg.NewSubtreeMeta(st2))

	h1, h2 := *st1.RootHash(), *st2.RootHash()
	block := &model.Block{Subtrees: []*chainhash.Hash{&h1, &h2}}

	sm.evictCachedSubtrees(block)

	_, ok := store.CachedSubtree(h1)
	require.False(t, ok, "subtree 1 should be evicted after finalize")
	_, ok = store.CachedSubtree(h2)
	require.False(t, ok, "subtree 2 should be evicted after finalize")
}

func TestEvictCachedSubtrees_NoopWhenStoreHasNoCache(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}, subtreeStore: memory.New()}
	var h chainhash.Hash
	h[0] = 0x07
	block := &model.Block{Subtrees: []*chainhash.Hash{&h}}

	require.NotPanics(t, func() { sm.evictCachedSubtrees(block) })
}
