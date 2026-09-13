package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// assignPassDepth is both the read-ahead depth and the per-peer cap the harness
// runs with, and the two are deliberately equal. A pass then hands the single
// peer exactly its whole cap, which is the state the re-ask rule has to work in:
// a peer that has gone quiet holding a full slice has no budget left until
// somebody forgives it.
const assignPassDepth = 8

// reset forgets everything recorded so far, so a test can measure one pass
// without the previous pass's getdata in the total. Callers must first wait for
// the previous pass's message to have landed: the send is asynchronous, and a
// reset that races it drops the wrong pass's hashes.
func (r *getDataRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.hashes = nil
	r.msgs = 0
}

// assignHarness builds a manager the assignment pass can run against: headers
// for every height in [from, to], one connected peer that records every getdata
// it is sent, and a live download ledger.
//
// Settings come from the real loader and are then narrowed to assignPassDepth,
// so the numbers under test are small enough to reason about while the code
// path is the one an unconfigured node takes.
func assignHarness(t *testing.T, from, to int32) (*SyncManager, *getDataRecorder) {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	sm.settings.Legacy.BlockDownloadLowerWindow = assignPassDepth
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = assignPassDepth

	sm.headerMu.Lock()

	for h := from; h <= to; h++ {
		hash := chainhash.Hash{}
		hash[0] = byte(h)
		hash[1] = byte(h >> 8)

		element := sm.headerList.PushBack(&headerNode{height: h, hash: &hash})
		sm.indexHeaderLocked(element, hash)
	}

	sm.headerMu.Unlock()

	_, rec := schedulerPeer(t, sm, 1, to+1000)

	return sm, rec
}

// waitForPass blocks until the pass's getdata has reached the peer's remote end,
// so a following reset cannot swallow it. The send is asynchronous, so sampling
// the recorder straight after the call would be sampling a race.
func waitForPass(t *testing.T, rec *getDataRecorder) {
	t.Helper()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"the pass's getdata should have reached the peer")
}

// TestAssignWantedBlocks_AsksForWhatIsWantedAndNotOwed is the ordinary pass.
func TestAssignWantedBlocks_AsksForWhatIsWantedAndNotOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	sm.lastCommittedHeight.Store(10)

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a node with headers above its best block must ask somebody for them")
	require.LessOrEqual(t, rec.count(), assignPassDepth,
		"and it must not ask beyond the read-ahead depth")
}

// TestAssignWantedBlocks_DoesNotReAskForABlockAlreadyOwed pins the duplicate
// guard. Asking again inside the retry window wastes a slot the pass could have
// spent on a block nobody owes.
func TestAssignWantedBlocks_DoesNotReAskForABlockAlreadyOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	sm.lastCommittedHeight.Store(10)

	sm.assignWantedBlocks()
	waitForPass(t, rec)

	first := rec.all()
	require.NotEmpty(t, first, "the first pass must ask for something for the second to have anything to skip")

	rec.reset()
	sm.assignWantedBlocks()

	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, 2*time.Second),
		"a second pass with nothing delivered and nothing expired must ask for nothing")
}

// TestAssignWantedBlocks_ReAsksWhenTheOwnerHasGoneQuiet is the help-a-struggling-peer
// rule. A block owed past the retry window goes to somebody else as well; the
// first copy home wins and the late one arrives owned, so no honest peer is
// punished for it.
func TestAssignWantedBlocks_ReAsksWhenTheOwnerHasGoneQuiet(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	sm.lastCommittedHeight.Store(10)

	sm.assignWantedBlocks()
	waitForPass(t, rec)

	rec.reset()

	// The tracker's clock is already injectable; setting the field is how the
	// package ages an assignment without sleeping a minute. Safe unsynchronised
	// here because the manager is not running and this goroutine is the only one
	// touching the ledger.
	sm.blockDownloads.now = func() time.Time {
		return time.Now().Add(blockRequestRetryInterval + time.Second)
	}

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a block whose owner has gone quiet past the retry window must be asked of somebody else")
}

// TestAssignWantedBlocks_TerminatesWhenEverythingIsOwed is the spin found in the
// shipped binary on 2026-09-12: a pass whose every candidate was owed by a peer
// requested nothing, reported more work to do, and repeated for ever at one
// blockchain round trip per candidate.
func TestAssignWantedBlocks_TerminatesWhenEverythingIsOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	sm.lastCommittedHeight.Store(10)

	sm.assignWantedBlocks()
	waitForPass(t, rec)

	done := make(chan struct{})

	go func() {
		sm.assignWantedBlocks()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the assignment pass did not finish: it has no loop to spin in, so this means one was added")
	}
}
