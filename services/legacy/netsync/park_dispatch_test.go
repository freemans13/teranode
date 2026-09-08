package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests cover a dispatch that commits a parked block off disk: the shape it
// must arrive in, the two steps it runs, and the three ways it can end. The drain
// that produces such a dispatch lands in a later commit; here the dispatch is
// built by hand, which is also how a reviewer can read what the drain must build.

// parkedDispatchFor builds the dispatch the drain will build, for the harness
// block at the given index, with its park entry already admitted and its blob on
// disk.
func (h *parkWiringHarness) parkedDispatchFor(t *testing.T, index int) (*blockDispatch, parkedBlock) {
	t.Helper()

	msgBlock := h.blocks[index].MsgBlock()

	entry := parkedBlock{
		hash:      msgBlock.BlockHash(),
		prevBlock: msgBlock.Header.PrevBlock,
		height:    int32(index + 1),
		peer:      h.peer,
	}

	stored, admitted := h.sm.blockPark.Admit(entry, msgBlock)
	require.Equal(t, admitRegistered, admitted)
	require.Equal(t, parkAccepted, h.sm.blockPark.WriteAdmitted(context.Background(), stored, msgBlock))
	h.sm.blockPark.FinishWrite(stored.hash)

	// The drain takes the entry out of the index before it dispatches, so the
	// dispatch owns it and every path out settles it.
	taken, ok := h.sm.blockPark.Take(stored.hash)
	require.True(t, ok)

	return &blockDispatch{parked: &taken, bytes: taken.size}, taken
}

// TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail is the happy
// path end to end through the dispatcher: the worker reads the blob and validates
// it, and the parked tail runs the post-commit bookkeeping.
func TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	// The chain holds everything, which is how every park test fakes a commit.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	d, entry := h.parkedDispatchFor(t, 1)

	require.True(t, bd.canDispatch(d), "an unwindowed dispatch is admissible into an empty frontier")

	bd.dispatch(d)
	require.Len(t, bd.frontier, 1, "the parked dispatch is in the frontier")
	require.Equal(t, entry.hash, bd.frontier[0].hash, "the entry is keyed by the parked block's own hash, not by a queue message")

	drainCompletionsUntilEmpty(t, bd)

	require.Zero(t, h.sm.blockPark.Len(), "the entry is settled, not left in the index")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, entry.hash.String(), "a committed block's blob is deleted by the disposition")
	}

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "a committed block is not marked as having failed")
}

// TestParkDispatch_AReadFailureKeepsTheBlockAndIsNotJudged is the reason the read
// error is carried in a field of its own.
//
// The two classifications default opposite ways: a read failure keeps the block,
// because the store may simply have had no permit free, and a commit failure
// judges it and blames a peer. Collapse them and a node under ordinary store load
// destroys fully downloaded blocks and evicts the peers that sent them.
func TestParkDispatch_AReadFailureKeepsTheBlockAndIsNotJudged(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	d, entry := h.parkedDispatchFor(t, 1)

	// An unclassified failure from the store, which is what a driver error looks
	// like: not a missing blob, not a context error.
	h.store.failReadsWith(errors.NewProcessingError("the store is having a bad day"))

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.NotNil(t, d.readErr, "the read failure is recorded apart from the completion error")
	require.Equal(t, 1, h.sm.blockPark.Len(), "an unclassified read failure keeps the block")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "and does not judge it")
}

// TestParkDispatch_ACommitFailureIsJudgedByTheCommitTable is the other default:
// a fault of the block itself gives the block up.
func TestParkDispatch_ACommitFailureIsJudgedByTheCommitTable(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The block's own existence check fails with a fault of the block, which is
	// the same route the park's own recovery tests use to drive a judged commit.
	h.client.On("GetBlockExists", mock.Anything, &entry.hash).
		Return(false, errors.NewBlockInvalidError("this block is not one we can take"))
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Nil(t, d.readErr, "the blob read succeeded; this is a commit failure")
	require.Zero(t, h.sm.blockPark.Len(), "a judged block does not stay parked")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.True(t, failed, "and it is marked so its descendants are suppressed")
}

// TestParkDispatch_ADrainedBlockPassesANilParentSoTheWorkerLooksItUp is the
// invariant the whole design rests on, and the one edit that would break it
// silently.
//
// Legacy must never hand block validation a block whose parent is not committed.
// A parked dispatch enforces that in the worker rather than on a promise from the
// consumer: it passes a nil parent, so HandleBlockDirect performs its own
// GetBlockHeader on the previous hash and refuses the block if the parent is not
// there. Passing &inflightParent{height} instead would look like an optimisation,
// would skip that lookup, and would take the height on trust from a park entry
// whose height came off the header list.
//
// So this test puts the parent NOT in the chain, which is the state the guard
// exists for, and requires that the lookup happened and the block was kept rather
// than judged. Under the mutation the lookup never runs.
func TestParkDispatch_ADrainedBlockPassesANilParentSoTheWorkerLooksItUp(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The block itself is not stored, so HandleBlockDirect goes on to the parent.
	// The harness answers every header lookup with "no such block", so the parent
	// is missing, which is what makes the lookup observable.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.Nil(t, d.parent, "a parked dispatch carries no resolved parent")

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	h.client.AssertCalled(t, "GetBlockHeader", mock.Anything, &entry.prevBlock)

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a parent that is gone keeps the block, so the sweep can retry it when the parent lands")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "and a missing parent is not a judgement on the block")
}

// TestParkDispatch_ADispatchedParkedBlockNeverTouchesTheBacklog pins the field
// that makes the two kinds of dispatch structurally different.
//
// A parked dispatch has no queue message. Nothing incremented the backlog for it
// and nobody waits on a reply, so reaching finishBlockMsg would decrement a
// counter that was never incremented. That counter gates
// localReadBackpressured's first arm, which suppresses the sync-peer stall check
// while the pipeline is busy; underflow it once and the arm reads as empty for
// the life of the process.
func TestParkDispatch_ADispatchedParkedBlockNeverTouchesTheBacklog(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	// A live block is in the pipeline, so the counter is where a running node
	// keeps it and an underflow would be visible rather than clamped at zero.
	h.sm.blockBacklog.Add(1)

	d, _ := h.parkedDispatchFor(t, 1)

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Equal(t, int64(1), h.sm.blockBacklog.Load(),
		"a parked dispatch must not decrement the backlog; the live block still in the pipeline owns that slot")
}

// TestParkDispatch_TheWrongShapeIsRefusedAndTheEntryRestored covers the guard in
// dispatch, whose two failure modes are both silent.
//
// A resolved parent would make HandleBlockDirect take the parent's word for the
// height and skip the chain lookup, and that lookup is the thing that enforces
// "never hand block validation a block whose parent is not committed". A
// non-empty frontier would mean the server-side window already holds a legacy
// entry, so an unwindowed parked block would be refused admission there.
func TestParkDispatch_TheWrongShapeIsRefusedAndTheEntryRestored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(d *blockDispatch, bd *blockDispatcher)
	}{
		{
			name: "a resolved parent",
			break_: func(d *blockDispatch, _ *blockDispatcher) {
				d.parent = &inflightParent{height: 750_699}
			},
		},
		{
			name:   "marked windowed",
			break_: func(d *blockDispatch, _ *blockDispatcher) { d.windowed = true },
		},
		{
			name: "a non-empty frontier",
			break_: func(_ *blockDispatch, bd *blockDispatcher) {
				bd.frontier = append(bd.frontier, &frontierEntry{
					hash:       chainhash.HashH([]byte("something already in flight")),
					height:     750_699,
					rpcStarted: make(chan struct{}),
					settled:    make(chan struct{}),
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)
			bd := h.withDispatcher(t)

			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

			d, entry := h.parkedDispatchFor(t, 1)
			require.Zero(t, h.sm.blockPark.Len(), "precondition: the drain has taken the entry")

			tc.break_(d, bd)

			// Sampled after the case has set the scene: the non-empty-frontier case
			// puts an entry there itself, and that entry must survive the refusal.
			before := len(bd.frontier)

			bd.dispatch(d)

			require.Len(t, bd.frontier, before, "the malformed dispatch is refused")
			require.True(t, h.sm.blockPark.Has(entry.hash),
				"and its entry is put back, or the blob is stranded with its bytes charged and no index entry")
		})
	}
}

// TestParkDispatch_ShutdownRestoresAParkedDispatchInsteadOfReplying covers the
// quit arm of the consumer loop.
func TestParkDispatch_ShutdownRestoresAParkedDispatchInsteadOfReplying(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.quit = make(chan struct{})
	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true

	release := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error {
		<-release

		return nil
	}

	d, entry := h.parkedDispatchFor(t, 1)
	bd.dispatch(d)

	h.sm.blockBacklog.Add(1)

	// Nothing is queued on purpose. With an empty queue and no pending block the
	// quit arm is the only ready one, so the loop takes it as soon as it reaches
	// its select, which is the arm under test. Feeding a message instead would
	// have it head-processed, which is a different test.
	queue := make(chan *blockQueueMsg, 1)

	done := make(chan struct{})

	go func() {
		h.sm.dispatchBlocks(queue)
		close(done)
	}()

	close(h.sm.quit)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer did not return on quit")
	}

	close(release)

	require.True(t, h.sm.blockPark.Has(entry.hash),
		"a parked dispatch in flight at shutdown must have its entry restored, not replied to")
	require.Equal(t, int64(1), h.sm.blockBacklog.Load(),
		"and the backlog must be untouched by it")
}

// drainCompletionsUntilEmpty pumps the completions channel the way the consumer
// goroutine does, until the frontier is empty.
func drainCompletionsUntilEmpty(t *testing.T, bd *blockDispatcher) {
	t.Helper()

	deadline := time.After(10 * time.Second)

	for len(bd.frontier) > 0 {
		select {
		case c := <-bd.completions:
			bd.complete(c)
		case <-deadline:
			t.Fatal("timed out waiting for the frontier to drain")
		}
	}
}
