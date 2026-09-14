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

// Fill replaces the cache with the part of headers that sits above parent,
// where the first header above parent sits at baseHeight. It reports whether
// anything was accepted.
//
// parent is the node's committed tip, and the batch is a linked run answering a
// getheaders that was asked from that tip as it stood when the question went
// out. Those are not the same instant. On Hetzner mainnet the node commits
// around 18 blocks a second, so by the time a reply crosses the network the tip
// has moved perhaps 20 blocks into the batch. Requiring headers[0] to name the
// current tip as its parent therefore threw away the entire 2,000-header reply
// because its first 20 entries had gone behind us, and the cache could only be
// refilled once the node had already stopped committing — which is exactly the
// dead air between refills that this rule removes. Eleven getheaders went out
// on 2026-09-14 between 13:11:36 and 13:12:26, across eight peers, and not one
// of the replies was kept; the twelfth landed within a second of its own
// request, once commits had stopped.
//
// So the batch is searched for where it meets the tip rather than being judged
// on its front. Two shapes, and they are the same case:
//
//   - headers[0] names parent as its parent, so the whole batch is usable and
//     headers[0] sits at baseHeight. This is the cold-start shape, and the only
//     shape the rule this replaces would accept.
//   - some headers[i] hashes to parent, so headers[i+1] onward are usable and
//     headers[i+1] sits at baseHeight. The prefix up to and including i is
//     behind the committed tip and is dropped.
//
// This is not a weaker check than judging the front. The batch is verified to
// be one internally linked run before anything is kept, and parent is a
// specific 32-byte value: a header whose PrevBlock is that value builds on this
// node's tip, wherever in the run it happens to fall.
//
// It refuses a batch that does not link, in any of the three ways a batch can
// fail to. If parent appears nowhere in the run and is not headers[0]'s parent,
// the batch describes a chain this node is not on, or one it has run clean past.
// If parent is the run's own last header, the usable suffix is empty and there
// is nothing to cache. If any header does not name the one before it, every
// height after the break is a guess, and a height is exactly what this structure
// exists to provide, so a guess is worse than nothing.
//
// Refusing changes nothing. The caller asks again, of the same peer or another.
func (c *headerCache) Fill(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) bool {
	if c == nil || len(headers) == 0 {
		return false
	}

	// Walk the whole batch before touching the map, so a refusal leaves the
	// previous contents intact rather than half-replaced. The walk does both
	// jobs in one pass: it proves every header names the one before it, and it
	// finds where the run meets the committed tip.
	hashes := make([]chainhash.Hash, 0, len(headers))

	// start is the index of the first usable header. Set to 0 up front when the
	// batch's front already builds on the tip, so the loop's search below is
	// skipped; otherwise the loop sets it to i+1 at the header that hashes to
	// parent. Left negative if the run never meets the tip.
	start := -1
	if headers[0].PrevBlock.IsEqual(&parent) {
		start = 0
	}

	var prev chainhash.Hash

	for i, header := range headers {
		// i == 0 has nothing before it to link to: whether its own parent is
		// the tip is the start check above, and a batch whose front is behind
		// the tip is precisely what this rule exists to accept.
		if i > 0 && !header.PrevBlock.IsEqual(&prev) {
			return false
		}

		hash := header.BlockHash()

		if start < 0 && hash.IsEqual(&parent) {
			start = i + 1
		}

		hashes = append(hashes, hash)
		prev = hash
	}

	// start == len(hashes) is the tip being the batch's own last header: the run
	// is honest and connects, there is simply nothing above the tip in it. A
	// clean refusal, not an empty success — writing an empty map here would
	// throw away a perfectly good previous batch in exchange for nothing.
	if start < 0 || start >= len(hashes) {
		return false
	}

	hashes = hashes[start:]

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
