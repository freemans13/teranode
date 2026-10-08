package netsync

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The queued re-ask: the lowest block owed, held at peers whose queues will make the chain wait
// for it, is asked of one other peer that would deliver it at least twice as soon as the soonest
// owner. The owner keeps its request and its connection. A block arriving under 100 KB/s is the
// race's. One arriving faster is judged on its own rate against a fresh copy of the whole block,
// so a large block coming in at a healthy rate is never doubled.

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

// One extra request per block while its owners are on time. A second pass judges both owners: the
// fast peer just asked lands the block soonest, and a peer at 70 MB/s is not twice as soon.
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

	sm.maybeReaskQueuedBlock(now.Add(10 * time.Second))
	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "the extra copy has its 30 s")

	sm.maybeReaskQueuedBlock(now.Add(time.Minute))
	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "the fast owner will land it soonest")
}

// A block owed by two peers, the owner and the peer the re-ask added, used to stream with no single
// owner, and its bytes counted for nobody. On 2026-10-07 the fast peer sending re-asked block
// 707,857, 2 GB in 64 s, therefore looked silent for a minute, and the quiet-owner rule let it off
// all 15 blocks in its queue; it delivered them anyway and each was downloaded twice. The peer
// sending the copy counts as sending, and the other owner, which sends nothing, does not.
func TestAPeerSendingABlockTwoPeersOweIsNotQuiet(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-3*time.Minute))
	askAt(t, sm, fast, block, now.Add(-2*time.Minute))

	queued := heightHash(t, sm, 30)
	askAt(t, sm, fast, queued, now.Add(-2*time.Minute))

	var stream *blockStream

	sink := sm.trackBlockStreams(func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		sm.streams.mu.Lock()
		for s := range sm.streams.active {
			stream = s
		}
		sm.streams.mu.Unlock()

		stream.lastRead.Store(time.Now().UnixNano())

		require.False(t, sm.streams.lastBlockBytes(fast).IsZero(), "the peer sending a block two peers owe is sending")
		require.True(t, sm.ownerStillSending(queued), "so the rest of its queue is not let off")
		require.True(t, sm.streams.lastBlockBytes(owner).IsZero(), "the other owner sends nothing and does not look busy")

		return true, nil
	})

	_, err := sink(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(nil), fast), 0)
	require.NoError(t, err)
}

// A block arriving at a steady rate above the race's 100 KB/s floor is still re-asked when it
// will be late and a fresh copy from another peer, started from the first byte, would land in
// half the time. On 2026-10-07 a peer at 1.9 MB/s had 1,083 MB left of a block while peers at 26
// to 38 MB/s carried the rest. The owner keeps its copy; whichever completes first is converted.
func TestQueuedReaskAsksForASlowArrivingBlockThatWillBeLate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-2*time.Minute))

	s := sm.streams.start(next, 11, owner, 1_083_000_000, now.Add(-95*time.Second))
	s.read.Store(180_000_000)

	sm.maybeReaskQueuedBlock(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "1.9 MB/s with 903 MB left: 475 s, against about 14 s for a fresh copy")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its copy")
	require.True(t, owner.Connected())
}

// A block arriving at a healthy rate is not re-asked when a fresh copy would not land in half
// the time, however late it is.
func TestQueuedReaskLeavesAnArrivingBlockAFreshCopyWouldNotBeat(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-2*time.Minute))

	// 2 GB at 30 MB/s, 1.05 GB in after 35 s: 32 s to go. A fresh copy at 80 MB/s needs 25 s,
	// not half of 32.
	sm.streams.rates[owner] = 30_000_000
	s := sm.streams.start(next, 11, owner, 2_000_000_000, now.Add(-10*time.Second))
	s.read.Store(1_050_000_000)

	sm.maybeReaskQueuedBlock(now.Add(25 * time.Second))

	require.False(t, sm.blockDownloads.HasOwner(fast, next))
}

// A queued block's own size is not known, so it is counted at the largest recent block, not the
// average. With the average small, an idle slower peer looked sooner than a faster peer with a
// queue, though for a large block the faster peer is far sooner.
func TestQueuedReaskCountsTheQueuedBlockAtTheLargestRecentSize(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	medium, _ := schedulerPeer(t, sm, 3, 2000)
	now := time.Now()

	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockSizeTracker.addBlockSize(2_000_000_000)
	for range 9 {
		sm.blockSizeTracker.addBlockSize(10_000_000)
	}

	// Owner 3.5, fast 50, medium 12 MB/s: 80% of 65.5 needs 50 and 12, so both are active.
	sm.streams.rates[owner] = 3.5 * mb
	sm.streams.rates[fast] = 50 * mb
	sm.streams.rates[medium] = 12 * mb

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	for h := int32(40); h < 45; h++ {
		askAt(t, sm, fast, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.maybeReaskQueuedBlock(now)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.HasOwner(fast, next), "a 2 GB block lands in about 41 s at 50 MB/s behind five blocks")
	require.False(t, sm.blockDownloads.HasOwner(medium, next), "and in about 167 s at the idle 12 MB/s peer")
}
