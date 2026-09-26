package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// The download budget is what stops the node holding unbounded decoded blocks.
// It is acquired AFTER a block has been read off the wire, in OnBlock, and until
// this change it was released only when the block's reply came back, which is
// after validation. So one block held budget for its whole validation, and every
// peer that had finished reading a block sat blocked in the acquire, unable to
// read its next message.
//
// Measured on mainnet at height 752,100 with the budget at 256 MiB: one or two
// blocks in flight and five or six peer read loops blocked, with the link
// delivering 27 MB/s. Freeing the block-queue consumer cannot help with that,
// because the queue it can now service is empty by construction: nothing else
// was allowed to download.
//
// The fix is to release the BYTES once the block's memory has been charged to
// whichever budget owns it next, and to keep the DEDUP HASH until the reply.
// After a dispatch that is the window's own byte charge; after a park admission
// it is the park's byte budget. Both are real bounds, so nothing becomes
// unbounded; what changes is that the download budget stops double-counting
// memory another budget is already accounting for.

// prefetchHarness is a manager with a real download budget and nothing else it
// does not need.
func prefetchHarness(t *testing.T, budget int64) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:                   ulogger.TestLogger{},
		blockPrefetchBudgetBytes: budget,
		blockPrefetchBudget:      semaphore.NewWeighted(budget),
		inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
	}

	return sm
}

// TestPrefetch_TheBytesAreGivenBackAtHandOffAndTheHashAtTheReply is the split
// this change rests on.
//
// The two halves of the admission gate had one lifetime, deliberately, so that a
// copy of a hash could not be re-admitted until the block had left the pipeline.
// The dedup half still works that way. The byte half does not need to: once the
// block is charged to the window or to the park, holding the download budget as
// well counts the same memory twice and it is the second count that shuts the
// peers out.
func TestPrefetch_TheBytesAreGivenBackAtHandOffAndTheHashAtTheReply(t *testing.T) {
	// Four times the per-block floor, so the numbers below are the shape a real
	// node has: one block can take the whole budget, and a second block still
	// needs at least the floor.
	const budget = 4 * minInFlightBlockWeight

	sm := prefetchHarness(t, budget)

	first := chainhash.HashH([]byte("the block being validated"))
	second := chainhash.HashH([]byte("a block another peer has ready"))

	// A block that takes the whole budget, which is what a block at or over the
	// budget does: AcquireBlockPrefetch clamps it and admits it alone.
	weight, err := sm.AcquireBlockPrefetch(context.Background(), nil, first, budget)
	require.NoError(t, err)
	require.Equal(t, int64(budget), weight)

	// Another peer has finished reading its own block and wants in. There is no
	// room, which is the state five or six read loops were measured in.
	require.False(t, sm.blockPrefetchBudget.TryAcquire(minInFlightBlockWeight),
		"precondition: the budget is fully committed to the first block")

	// The first block is handed over to whichever budget owns it next.
	sm.ReleaseBlockPrefetchBytes(first, weight)

	require.True(t, sm.blockPrefetchBudget.TryAcquire(minInFlightBlockWeight),
		"once the block is charged elsewhere its download bytes must come back, or peers stay shut out for the whole validation")
	sm.blockPrefetchBudget.Release(minInFlightBlockWeight)

	// The dedup half is untouched: a second copy of the same hash is still
	// refused while the block is in the pipeline.
	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, first, minInFlightBlockWeight)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight,
		"the hash must be held until the block leaves the pipeline, or a duplicate is admitted and validated twice")

	// A different hash is admitted, which is the whole point.
	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, second, minInFlightBlockWeight)
	require.NoError(t, err)

	// Exactly once. Releasing the same weight twice panics the semaphore, and the
	// panic would land on a peer's read loop, so the second call must be a no-op.
	// The peer makes both calls on the hand-off path: one at the hand-off and one
	// from its deferred cleanup.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetchBytes(first, weight) },
		"a second release of the same block's bytes must be a no-op, not a panic")

	require.False(t, sm.blockPrefetchBudget.TryAcquire(budget),
		"and it must not have handed the budget back twice either")

	sm.ReleaseBlockPrefetchHash(first)

	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, first, minInFlightBlockWeight)
	require.NoError(t, err, "once the block has left the pipeline its hash is free again")
}

// TestPrefetch_ADispatchedBlockGivesItsDownloadBytesBack and
// TestPrefetch_AParkedBlockGivesItsDownloadBytesBack used to drive this split
// through the consumer loop: a decoded blockQueueMsg carrying its own
// handedOff channel, released early at dispatch or at park admission while its
// dedup hash was held until the reply. Both are gone. The park is now
// mandatory, and admitPipelineSink's own doc comment (streaming_install.go)
// says why AcquireBlockPrefetch is no longer reachable from that route at
// all: with the park enabled every block arrives as *peer.MsgBlockOnDisk, not
// a *wire.MsgBlock, so OnBlock's decode-in-memory admission check — the one
// blockQueueMsg.handedOff signalled an early release from — never runs. The
// pipeline's own admission (admitPipelineSink) acquires the whole budget
// around the ENTIRE conversion and releases it once, combined
// (sm.ReleaseBlockPrefetch), only after pipelineBlockSink returns — there is
// no early hand-off moment on this route to pin. TestPrefetch_TheBytesAreGivenBackAtHandOffAndTheHashAtTheReply
// above still exercises the split release primitives directly, which remain
// live API even though nothing currently calls them apart from each other.
