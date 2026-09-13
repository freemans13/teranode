package netsync

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// headerCache holds the block hashes for a contiguous run of heights above the
// best block processed, taken from one getheaders reply.
//
// It is a cache and not a work queue, and the difference is the whole point. It
// can be thrown away at any instant with no consequence, because everything in
// it can be asked for again in one message. The structure it replaces, a linked
// list of every header ever received, could not be thrown away, so every bound
// on it was a decision to discard something, and every discard needed a
// mechanism somewhere else to put it back. On Hetzner mainnet that list reached
// 933,062 entries with a stale flag on its front, and only losing the memory it
// lived in ever cleared it.
//
// A fill replaces the contents outright. Merging would let a run from a chain
// this node has left name a height on the chain it is on.
type headerCache struct {
	mu       sync.Mutex
	byHeight map[int32]chainhash.Hash
	// byHash is the reverse of byHeight, maintained alongside it. It exists for
	// pipelineParentHeight, which is handed a block's parent hash — off the
	// wire, before that parent is committed — and needs the height that hash
	// names, not the other way round. The old header list answered this from
	// its own hash-keyed index; a cache keyed only by height could not, so this
	// is kept in step with byHeight at every Fill and Discard.
	byHash map[chainhash.Hash]int32
	top    int32
	filled bool
}

func newHeaderCache() *headerCache {
	return &headerCache{
		byHeight: make(map[int32]chainhash.Hash),
		byHash:   make(map[chainhash.Hash]int32),
	}
}

// Fill replaces the cache with headers, where headers[0] sits at baseHeight and
// its parent is parent. It reports whether the batch was accepted.
//
// It refuses a batch that does not link, in either of the two ways a batch can
// fail to. If the first header names a different parent, the batch describes a
// chain this node is not on. If any later header does not name the one before
// it, every height after the break is a guess, and a height is exactly what
// this structure exists to provide, so a guess is worse than nothing.
//
// Refusing changes nothing. The caller asks again, of the same peer or another.
func (c *headerCache) Fill(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) bool {
	if c == nil || len(headers) == 0 {
		return false
	}

	if !headers[0].PrevBlock.IsEqual(&parent) {
		return false
	}

	// Walk the whole batch before touching the map, so a refusal leaves the
	// previous contents intact rather than half-replaced.
	hashes := make([]chainhash.Hash, 0, len(headers))
	prev := parent

	for _, header := range headers {
		if !header.PrevBlock.IsEqual(&prev) {
			return false
		}

		hash := header.BlockHash()
		hashes = append(hashes, hash)
		prev = hash
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.byHeight = make(map[int32]chainhash.Hash, len(hashes))
	c.byHash = make(map[chainhash.Hash]int32, len(hashes))

	for i, hash := range hashes {
		height := baseHeight + int32(i) //nolint:gosec // a batch index, bounded by the wire limit
		c.byHeight[height] = hash
		c.byHash[hash] = height
	}

	c.top = baseHeight + int32(len(hashes)) - 1 //nolint:gosec // as above
	c.filled = true

	return true
}

// At returns the hash this cache names for height, and whether it names one.
func (c *headerCache) At(height int32) (chainhash.Hash, bool) {
	if c == nil {
		return chainhash.Hash{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	hash, ok := c.byHeight[height]

	return hash, ok
}

// HeightOf is At's reverse: the height this cache names for hash, and whether
// it names one. Used by pipelineParentHeight to resolve a not-yet-committed
// parent's height from its hash alone, which is all a block streaming off the
// wire ever hands over.
func (c *headerCache) HeightOf(hash chainhash.Hash) (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	height, ok := c.byHash[hash]

	return height, ok
}

// Top returns the highest height the cache names. A pass that has reached it has
// run off the end and needs another getheaders.
func (c *headerCache) Top() (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.filled || len(c.byHeight) == 0 {
		return 0, false
	}

	return c.top, true
}

// Len returns how many heights are named.
func (c *headerCache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.byHeight)
}

// Discard empties the cache. Nothing is lost that one message cannot replace.
func (c *headerCache) Discard() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.byHeight = make(map[int32]chainhash.Hash)
	c.byHash = make(map[chainhash.Hash]int32)
	c.top = 0
	c.filled = false
}
