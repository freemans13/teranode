package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The queued re-ask: the lowest block owed and not yet arriving, held at a peer whose queue
// will make the chain wait for it, is asked of one other peer that would deliver it at least
// twice as soon. The owner keeps its request and its connection. A block that is arriving is
// the race's, never this rule's, so a large block coming in at a healthy rate is never doubled.

const reaskTypicalBlock = 300_000_000

// reaskSetup builds a manager with the committed tip at 10, a typical block of 300 MB, and two
// peers: a slow owner at 3.5 MB/s and a fast idle peer at 80 MB/s.
func reaskSetup(t *testing.T) (sm *SyncManager, owner, fast *peerpkg.Peer, fastRec *getDataRecorder) {
	t.Helper()

	sm = assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	sm.blockPark = &blockPark{}
	sm.commitRate = newCommitRateTracker()
	mockCommittedTip(t, sm, 10, 0)

	for range 4 {
		sm.blockSizeTracker.addBlockSize(reaskTypicalBlock)
	}

	owner, _ = schedulerPeer(t, sm, 1, 2000)
	fast, fastRec = schedulerPeer(t, sm, 2, 2000)

	sm.streams.rates[owner] = 3_500_000
	sm.streams.rates[fast] = 80_000_000

	return sm, owner, fast, fastRec
}

// heightHash is the header cache's hash at height.
func heightHash(t *testing.T, sm *SyncManager, height int32) chainhash.Hash {
	t.Helper()

	h, ok := sm.headerCache.At(height)
	require.True(t, ok)

	return h
}

// askAt records a request of h from p as made at `at`.
func askAt(t *testing.T, sm *SyncManager, p *peerpkg.Peer, h chainhash.Hash, at time.Time) {
	t.Helper()

	prev := sm.blockDownloads.now
	sm.blockDownloads.now = func() time.Time { return at }
	require.True(t, sm.blockDownloads.Add(p, h))
	sm.blockDownloads.now = prev
}

// The case behind the 20-minute wait at 705,725: the block after the tip sits behind other
// blocks at a peer delivering 3.5 MB/s, which is busy, so it is neither struggling on this block
// nor quiet. A fast idle peer is asked for it too, and the owner keeps its request.
func TestQueuedReaskAsksAFastPeerForTheBlockBehindASlowQueue(t *testing.T) {
	sm, owner, fast, fastRec := reaskSetup(t)
	now := time.Now()

	// The owner was asked for 13, 14 and 15 first and 11 last, so it sends 11 last. 13 is
	// arriving now with 250 MB to go.
	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	s := sm.streams.start(heightHash(t, sm, 13), 13, owner, reaskTypicalBlock, now.Add(-15*time.Second))
	s.read.Store(50_000_000)

	sm.maybeReaskQueuedBlock(now)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.HasOwner(fast, next), "the fast peer is asked for the block the chain needs")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its request")
	require.True(t, WaitUntil(func() bool { return fastRec.count() == 1 }, 5*time.Second))
	require.Equal(t, []chainhash.Hash{next}, fastRec.all())
	require.True(t, owner.Connected(), "the owner is not dropped: it is working, just slowly")
}

// The reason this rule never looks at an arriving block. A 4 GB block at 40 MB/s takes 100 s;
// any timer on it alone fired for every large block and downloaded each one twice.
func TestQueuedReaskNeverDoublesALargeBlockThatIsArriving(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-90*time.Second))

	sm.streams.rates[owner] = 40_000_000
	s := sm.streams.start(next, 11, owner, 4_000_000_000, now.Add(-60*time.Second))
	s.read.Store(2_400_000_000)

	sm.maybeReaskQueuedBlock(now)
	sm.maybeRaceSlowBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, next), "a 4 GB block arriving at 40 MB/s is never asked of a second peer")
}

// A block the owner will deliver before the chain reaches it is left alone.
func TestQueuedReaskLeavesABlockThatArrivesInTime(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	// Six blocks to apply before the chain needs 16, at one block every 20 s: 120 s. The owner
	// at 30 MB/s delivers it, with one 300 MB block ahead, in 20 s.
	for i := range 64 {
		sm.commitRate.note(now.Add(time.Duration(i-63) * 20 * time.Second))
	}

	sm.streams.rates[owner] = 30_000_000

	for _, h := range []int32{17, 16} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.maybeReaskQueuedBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 16)))
}

// SV Node's timer runs from the request, for 30 s.
func TestQueuedReaskWaitsThirtySecondsFromTheRequest(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-20*time.Second))
	}

	sm.maybeReaskQueuedBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)), "20 s after the request is too soon")
}

// Another copy only helps if it comes clearly sooner. A fast peer with a long queue of its own
// would not deliver the block in half the owner's time, so it is not asked.
func TestQueuedReaskNeedsAPeerAtLeastTwiceAsFast(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	// Owner: 1 block ahead, 600 MB at 3.5 MB/s, about 171 s.
	for _, h := range []int32{13, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	// Fast peer: 30 blocks queued, 9.3 GB at 80 MB/s, about 116 s. Sooner, not twice as soon.
	for h := int32(40); h < 70; h++ {
		askAt(t, sm, fast, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.maybeReaskQueuedBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// One extra request per block. A second pass asks nobody else, whoever else is free.
func TestQueuedReaskAsksOnce(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	third, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[third] = 70_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	sm.maybeReaskQueuedBlock(now)
	require.True(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))

	sm.maybeReaskQueuedBlock(now.Add(time.Minute))
	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "the block has had its one extra request")
}
