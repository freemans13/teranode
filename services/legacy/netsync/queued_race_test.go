package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A block the chain is about to need can sit at a peer behind other blocks that peer is still
// sending. It is not slow and not quiet, so neither the slow-peer race nor the quiet rule asks
// anyone else for it, and the chain waits for the queue ahead of it. SV Node asks a second peer
// for a block in flight 30 seconds or more. This asks one when the block has had no bytes for
// that long, its peer is busy sending others, the chain reaches it within a minute, and another
// peer owes fewer blocks.

// queuedRaceManager has a committed tip at 10, a commit rate of one block a second, and headers
// 1 to 120. owner has been asked for the block at 11 forty seconds ago and is sending the block
// at 12.
func queuedRaceManager(t *testing.T) (*SyncManager, *peerpkg.Peer, *peerpkg.Peer, *getDataRecorder, chainhash.Hash) {
	t.Helper()

	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	sm.commitRate = newCommitRateTracker()

	now := time.Now()
	for i := 10; i >= 0; i-- {
		sm.commitRate.note(now.Add(-time.Duration(i) * time.Second))
	}

	mockCommittedTip(t, sm, 10, 0)

	owner, _ := schedulerPeer(t, sm, 1, 2000)
	idle, idleRec := schedulerPeer(t, sm, 2, 2000)

	next, ok := sm.headerCache.At(11)
	require.True(t, ok)

	sending, ok := sm.headerCache.At(12)
	require.True(t, ok)

	asked := now.Add(-40 * time.Second)
	sm.blockDownloads.now = func() time.Time { return asked }
	require.True(t, sm.blockDownloads.Add(owner, sending))
	require.True(t, sm.blockDownloads.Add(owner, next))
	sm.blockDownloads.now = time.Now

	s := sm.streams.start(sending, 12, owner, 900<<20, now.Add(-30*time.Second))
	s.read.Store(300 << 20)
	s.lastRead.Store(now.UnixNano())

	return sm, owner, idle, idleRec, next
}

func TestABlockQueuedBehindABusyPeerIsAskedOfAnotherPeer(t *testing.T) {
	sm, owner, idle, idleRec, next := queuedRaceManager(t)

	sm.maybeRaceQueuedBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return idleRec.count() == 1 }, 5*time.Second), "the idle peer is asked for the queued block")
	require.Equal(t, []chainhash.Hash{next}, idleRec.all())
	require.ElementsMatch(t, []*peerpkg.Peer{owner, idle}, sm.blockDownloads.OwnersOf(next),
		"both peers owe it, so whichever copy lands second is not taken as unrequested")

	sm.maybeRaceQueuedBlock(time.Now())
	require.Equal(t, 1, idleRec.count(), "a block is raced once")
}

func TestAQueuedBlockAskedForRecentlyIsNotRaced(t *testing.T) {
	sm, owner, _, idleRec, next := queuedRaceManager(t)

	sm.blockDownloads.RemoveOwner(owner, next)
	require.True(t, sm.blockDownloads.Add(owner, next))

	sm.maybeRaceQueuedBlock(time.Now())
	require.Zero(t, idleRec.count(), "under 30 seconds in its peer's queue")
}

func TestAQueuedBlockAtAQuietPeerIsLeftToTheQuietRule(t *testing.T) {
	sm, _, _, idleRec, _ := queuedRaceManager(t)

	for s := range sm.streams.active {
		s.lastRead.Store(time.Now().Add(-2 * blockRequestRetryInterval).UnixNano())
	}

	sm.maybeRaceQueuedBlock(time.Now())
	require.Zero(t, idleRec.count(), "a peer sending nothing is handled by the download pass, not raced")
}

func TestAQueuedBlockTheChainReachesLaterIsNotRaced(t *testing.T) {
	sm, _, _, idleRec, _ := queuedRaceManager(t)

	// One block every two minutes: the chain reaches height 11 in two minutes.
	sm.commitRate = newCommitRateTracker()
	now := time.Now()
	sm.commitRate.note(now.Add(-2 * time.Minute))
	sm.commitRate.note(now)

	sm.maybeRaceQueuedBlock(now)
	require.Zero(t, idleRec.count(), "the chain is not about to wait on it")
}

func TestAQueuedBlockIsNotRacedToAPeerOwingAsMuch(t *testing.T) {
	sm, _, idle, idleRec, _ := queuedRaceManager(t)

	require.True(t, sm.blockDownloads.Add(idle, chainhash.Hash{0xe7, 1}))
	require.True(t, sm.blockDownloads.Add(idle, chainhash.Hash{0xe7, 2}))

	sm.maybeRaceQueuedBlock(time.Now())
	require.Zero(t, idleRec.count(), "its queue is as long as the owner's, so it would not be sooner")
}
