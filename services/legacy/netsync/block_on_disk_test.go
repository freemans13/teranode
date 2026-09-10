package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockOnDiskMsg covers the consumer's side of the streaming path.
//
// A streamed block's bytes reach the park's store before the park has any record
// of them, so this is what gives it one and then asks for a drain. It runs on the
// consumer goroutine for the same reason every other park and drain decision
// does: the drain queue is that goroutine's own state, and five separate stalls
// this week came from touching this machinery from somewhere else.
func TestHandleBlockOnDiskMsg(t *testing.T) {
	bodyFor := func(prev chainhash.Hash, size int64) peerpkg.BlockBody {
		header := wire.BlockHeader{Version: 1, PrevBlock: prev}

		return peerpkg.BlockBody{
			Header:  header,
			TxCount: 3,
			Size:    size,
			Hash:    header.BlockHash(),
		}
	}

	// drainAsync is what the consumer loop sets while it is running, and it is
	// the production setting. Left false, scheduleDrain walks the drain stack
	// synchronously instead of queueing, which is correct for a manager with no
	// consumer but exercises the whole drain rather than this hand-off.
	withConsumer := func(t *testing.T) *parkWiringHarness {
		t.Helper()

		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		return h
	}

	t.Run("the block is adopted and a drain is asked for", func(t *testing.T) {
		h := withConsumer(t)

		parent := chainhash.Hash{0xaa}
		body := bodyFor(parent, 1<<20)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.True(t, h.sm.blockPark.Has(body.Hash),
			"the bytes are already on disk, so the park must know about them or nothing will ever read them")
		require.Equal(t, int64(1<<20), h.sm.blockPark.Bytes(),
			"an adopted block must be charged, or the park's accounting drifts from its disk")

		require.Len(t, h.sm.drainQueue, 1,
			"a streamed block whose parent is already in the chain must not wait for the 30-second sweep")
		require.Equal(t, parent, h.sm.drainQueue[0].parent)
	})

	t.Run("the delivering peer is recorded", func(t *testing.T) {
		h := withConsumer(t)

		body := bodyFor(chainhash.Hash{0xbb}, 4096)
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		entry, ok := h.sm.blockPark.Take(body.Hash)
		require.True(t, ok)
		require.Equal(t, h.peer, entry.peer,
			"post-commit actions ask the delivering peer for more, so losing it costs a peer's worth of progress")
	})

	t.Run("a re-delivered body is not adopted twice", func(t *testing.T) {
		h := withConsumer(t)

		body := bodyFor(chainhash.Hash{0xcc}, 2048)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.Equal(t, 1, h.sm.blockPark.Len())
		require.Equal(t, int64(2048), h.sm.blockPark.Bytes(),
			"charging a duplicate twice would have the park believe it holds bytes it does not")
	})

	t.Run("a park that refuses the entry has the orphaned body deleted", func(t *testing.T) {
		h := withConsumer(t)

		// Fill to the ceiling so adoption is refused for a reason other than
		// already holding it.
		h.sm.blockPark.mu.Lock()
		for i := 0; i < maxParkedEntries; i++ {
			var hash chainhash.Hash
			hash[0] = byte(i)
			hash[1] = byte(i >> 8)
			hash[2] = 0xfe
			stored := parkedBlock{hash: hash}
			h.sm.blockPark.entries[hash] = &stored
		}
		h.sm.blockPark.mu.Unlock()

		body := bodyFor(chainhash.Hash{0xdd}, 512)
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.False(t, h.sm.blockPark.Has(body.Hash))
		require.Empty(t, h.sm.drainQueue,
			"nothing was adopted, so asking for a drain would send the consumer looking for work that is not there")
	})

	t.Run("no park means the message is dropped rather than panicking", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.blockPark = nil

		require.NotPanics(t, func() {
			h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: bodyFor(chainhash.Hash{0xee}, 8), peer: h.peer})
		}, "a nil park is how every other call site reads the park being switched off")
	})
}
