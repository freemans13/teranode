package subtreecache

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// hashN returns a deterministic distinct hash for a small integer.
func hashN(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	return h
}

func TestCache_PutGet_ReturnsStoredObjects(t *testing.T) {
	c := New(1 << 20) // 1 MiB

	h := hashN(1)
	st := &subtreepkg.Subtree{}
	meta := &subtreepkg.Meta{}

	c.Put(h, st, meta, 100)

	gotSt, ok := c.Subtree(h)
	require.True(t, ok)
	require.Same(t, st, gotSt)

	gotMeta, ok := c.Meta(h)
	require.True(t, ok)
	require.Same(t, meta, gotMeta)

	require.Equal(t, 100, c.Bytes())
	require.Equal(t, 1, c.Len())
}

func TestCache_Miss_ReturnsFalse(t *testing.T) {
	c := New(1 << 20)

	gotSt, ok := c.Subtree(hashN(9))
	require.False(t, ok)
	require.Nil(t, gotSt)

	gotMeta, ok := c.Meta(hashN(9))
	require.False(t, ok)
	require.Nil(t, gotMeta)
}

func TestCache_Disabled_WhenCapZero(t *testing.T) {
	c := New(0)

	c.Put(hashN(1), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)

	_, ok := c.Subtree(hashN(1))
	require.False(t, ok, "cap=0 must disable the cache (always miss)")
	require.Equal(t, 0, c.Bytes())
	require.Equal(t, 0, c.Len())
}

func TestCache_EvictsOldestWhenOverCap(t *testing.T) {
	c := New(250) // holds two 100-byte entries, third forces eviction

	c.Put(hashN(1), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)
	c.Put(hashN(2), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)
	c.Put(hashN(3), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)

	// hash1 (oldest) evicted to make room.
	_, ok := c.Subtree(hashN(1))
	require.False(t, ok, "oldest entry should be evicted")

	_, ok = c.Subtree(hashN(2))
	require.True(t, ok)
	_, ok = c.Subtree(hashN(3))
	require.True(t, ok)

	require.LessOrEqual(t, c.Bytes(), 250, "byte usage must never exceed the cap")
}

func TestCache_SingleEntryLargerThanCap_NotStored(t *testing.T) {
	c := New(250)

	c.Put(hashN(1), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 5000) // bigger than cap

	_, ok := c.Subtree(hashN(1))
	require.False(t, ok, "an entry larger than the cap must not be stored (disk fallback)")
	require.Equal(t, 0, c.Bytes())
}

func TestCache_Evict_RemovesEntryAndFreesBytes(t *testing.T) {
	c := New(1 << 20)

	c.Put(hashN(1), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)
	c.Put(hashN(2), &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)

	c.Evict(hashN(1))

	_, ok := c.Subtree(hashN(1))
	require.False(t, ok)
	_, ok = c.Subtree(hashN(2))
	require.True(t, ok)

	require.Equal(t, 100, c.Bytes())
	require.Equal(t, 1, c.Len())
}

func TestCache_RePutSameHash_ReplacesWithoutDoubleCounting(t *testing.T) {
	c := New(1 << 20)

	h := hashN(1)
	c.Put(h, &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)

	st2 := &subtreepkg.Subtree{}
	c.Put(h, st2, &subtreepkg.Meta{}, 150)

	got, ok := c.Subtree(h)
	require.True(t, ok)
	require.Same(t, st2, got, "re-put must replace the stored object")
	require.Equal(t, 150, c.Bytes(), "re-put must not double-count bytes")
	require.Equal(t, 1, c.Len())
}

func TestCache_ConcurrentAccess(t *testing.T) {
	c := New(1 << 20)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n byte) {
			defer wg.Done()
			h := hashN(n)
			c.Put(h, &subtreepkg.Subtree{}, &subtreepkg.Meta{}, 100)
			_, _ = c.Subtree(h)
			_, _ = c.Meta(h)
			c.Evict(h)
		}(byte(i))
	}
	wg.Wait()
}
