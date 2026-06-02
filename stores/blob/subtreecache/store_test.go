package subtreecache

import (
	"context"
	"io"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// the decorator must remain usable anywhere a blob.Store is expected.
var _ blob.Store = (*Store)(nil)

// and it must satisfy the optional cache interface the model read paths assert,
// so a signature drift fails the build rather than silently disabling the cache.
var _ model.ParsedSubtreeCache = (*Store)(nil)

func TestStore_PutThenCachedHit(t *testing.T) {
	s := NewStore(memory.New(), 1<<20)

	h := hashN(1)
	st := &subtreepkg.Subtree{SizeInBytes: 500}
	meta := &subtreepkg.Meta{}

	s.PutParsedSubtree(h, st, meta)

	gotSt, ok := s.CachedSubtree(h)
	require.True(t, ok)
	require.Same(t, st, gotSt)

	gotMeta, ok := s.CachedSubtreeMeta(h)
	require.True(t, ok)
	require.Same(t, meta, gotMeta)
}

func TestStore_EvictCached(t *testing.T) {
	s := NewStore(memory.New(), 1<<20)

	h := hashN(1)
	s.PutParsedSubtree(h, &subtreepkg.Subtree{SizeInBytes: 500}, &subtreepkg.Meta{})

	s.EvictCachedSubtree(h)

	_, ok := s.CachedSubtree(h)
	require.False(t, ok)
}

func TestStore_DisabledCapZero(t *testing.T) {
	s := NewStore(memory.New(), 0)

	s.PutParsedSubtree(hashN(1), &subtreepkg.Subtree{SizeInBytes: 500}, &subtreepkg.Meta{})

	_, ok := s.CachedSubtree(hashN(1))
	require.False(t, ok, "cap=0 disables caching")
}

func TestStore_BlobPassThrough(t *testing.T) {
	s := NewStore(memory.New(), 1<<20)

	ctx := context.Background()
	key := []byte("some-subtree-key")
	payload := []byte("payload-bytes")

	require.NoError(t, s.Set(ctx, key, fileformat.FileTypeSubtree, payload))

	rc, err := s.GetIoReader(ctx, key, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestStore_MissingSizeFallsBackToNodeCount(t *testing.T) {
	// A subtree with no SizeInBytes must still be accounted (non-zero) so it
	// participates in cap eviction rather than being treated as free.
	s := NewStore(memory.New(), 1<<20)

	st := &subtreepkg.Subtree{Nodes: make([]subtreepkg.Node, 10)} // SizeInBytes == 0
	s.PutParsedSubtree(hashN(1), st, &subtreepkg.Meta{})

	require.Positive(t, s.cache.Bytes(), "zero-SizeInBytes entry must still be accounted")
}
