// Package subtreecache provides a bounded, in-process cache of parsed subtree
// artifacts (the *Subtree node list and its *Meta) keyed by subtree root hash.
//
// During legacy below-checkpoint IBD the netsync block handler (PhaseA) parses
// and writes each subtree to the blob store, then block finalization (PhaseB,
// in blockvalidation) reads the same files straight back and re-deserializes
// them for the merkle/order checks. When both run in one process and share the
// subtree store, this cache lets PhaseA's parsed objects satisfy PhaseB's reads
// without a disk round-trip or a second deserialize.
//
// The cache never holds the (potentially multi-gigabyte) subtree *data* — only
// the comparatively small node list and meta. It is bounded by a byte cap with
// FIFO eviction, and a single entry larger than the cap is simply not stored.
// It is purely an optimization: every miss or eviction falls back to the blob
// store, so correctness never depends on cache state.
package subtreecache

import (
	"container/list"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// entry is one cached subtree, tracked in the FIFO eviction list.
type entry struct {
	hash    chainhash.Hash
	subtree *subtreepkg.Subtree
	meta    *subtreepkg.Meta
	size    int
}

// Cache is a bounded, thread-safe parsed-subtree cache with FIFO eviction.
// The zero value is not usable; construct with New.
type Cache struct {
	mu       sync.Mutex
	capBytes int
	curBytes int
	items    map[chainhash.Hash]*list.Element
	order    *list.List // front = oldest, back = newest; values are *entry
}

// New returns a cache bounded to capBytes total. A capBytes of 0 (or negative)
// disables the cache entirely: Put is a no-op and every lookup misses.
func New(capBytes int) *Cache {
	return &Cache{
		capBytes: capBytes,
		items:    make(map[chainhash.Hash]*list.Element),
		order:    list.New(),
	}
}

// Put stores the parsed subtree and meta under hash, accounting for size bytes.
// If an entry for hash already exists it is replaced (without double-counting).
// Entries larger than the whole cap are not stored. Oldest entries are evicted
// as needed to keep total usage within the cap. No-op when the cache is
// disabled (cap <= 0).
func (c *Cache) Put(hash chainhash.Hash, subtree *subtreepkg.Subtree, meta *subtreepkg.Meta, size int) {
	if c.capBytes <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Replace an existing entry in place.
	if el, ok := c.items[hash]; ok {
		e := el.Value.(*entry)
		c.curBytes -= e.size
		e.subtree = subtree
		e.meta = meta
		e.size = size
		c.curBytes += size
		c.order.MoveToBack(el)
		c.evictToCap()
		return
	}

	// An entry that can never fit is not worth evicting everything else for.
	if size > c.capBytes {
		return
	}

	e := &entry{hash: hash, subtree: subtree, meta: meta, size: size}
	el := c.order.PushBack(e)
	c.items[hash] = el
	c.curBytes += size

	c.evictToCap()
}

// evictToCap drops oldest entries until usage fits the cap. Caller holds mu.
func (c *Cache) evictToCap() {
	for c.curBytes > c.capBytes {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}
		c.removeElement(oldest)
	}
}

// removeElement unlinks el and frees its bytes. Caller holds mu.
func (c *Cache) removeElement(el *list.Element) {
	e := el.Value.(*entry)
	c.order.Remove(el)
	delete(c.items, e.hash)
	c.curBytes -= e.size
}

// Subtree returns the cached subtree for hash, if present.
func (c *Cache) Subtree(hash chainhash.Hash) (*subtreepkg.Subtree, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[hash]
	if !ok {
		return nil, false
	}
	return el.Value.(*entry).subtree, true
}

// Meta returns the cached meta for hash, if present.
func (c *Cache) Meta(hash chainhash.Hash) (*subtreepkg.Meta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[hash]
	if !ok {
		return nil, false
	}
	return el.Value.(*entry).meta, true
}

// Evict removes the entry for hash if present. Used to release a block's
// subtrees as soon as finalization has consumed them.
func (c *Cache) Evict(hash chainhash.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[hash]; ok {
		c.removeElement(el)
	}
}

// Bytes returns the current total accounted size of cached entries.
func (c *Cache) Bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
