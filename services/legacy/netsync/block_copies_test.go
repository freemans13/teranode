package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// A block more than one peer owes. Both rules that ask another peer for a block used to skip it:
// the queued re-ask returned when a block had more than one owner, and the race skipped a stream
// with no single owner. After one extra copy, the block the chain waited on could wait for the
// peer layer's deadline. Each copy is judged now, as SV Node judges each peer a block is in flight
// from, and a further copy is asked for when every copy is late, up to maxBlockCopies owners.

// Two owners, each with the block behind a slow queue, and the extra copy asked for a minute ago.
// A fast idle peer is asked as the third owner. A fourth is never asked.
func TestQueuedReaskAsksAThirdPeerWhenBothOwnersAreLate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	second, _ := schedulerPeer(t, sm, 3, 2000)
	fourth, _ := schedulerPeer(t, sm, 4, 2000)
	sm.streams.rates[second] = 3_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-3*time.Minute))
	}

	for _, h := range []int32{20, 21, 22, 11} {
		askAt(t, sm, second, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	next := heightHash(t, sm, 11)
	sm.streams.markRaced(next, now.Add(-time.Minute))

	sm.maybeReaskQueuedBlock(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "both owners are minutes away; the idle fast peer is asked")
	require.True(t, sm.blockDownloads.HasOwner(owner, next))
	require.True(t, sm.blockDownloads.HasOwner(second, next))
	require.True(t, owner.Connected())
	require.True(t, second.Connected())

	// The third owner turns out slow too, and a fourth peer would land the block in 3 s.
	sm.streams.rates[fast] = 1_000_000
	sm.streams.rates[fourth] = 90_000_000

	sm.maybeReaskQueuedBlock(now.Add(time.Minute))
	require.False(t, sm.blockDownloads.HasOwner(fourth, next), "three peers owe the block: no fourth copy")
	require.Len(t, sm.blockDownloads.OwnersOf(next), 3)
}

// One owner that will land the block soon keeps every other peer from being asked, however late the
// other owner is.
func TestQueuedReaskLeavesABlockOneOwnerWillLandSoon(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	quick, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[quick] = 80_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-3*time.Minute))
	}

	next := heightHash(t, sm, 11)
	askAt(t, sm, quick, next, now.Add(-time.Minute))
	sm.streams.markRaced(next, now.Add(-time.Minute))

	sm.maybeReaskQueuedBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, next), "quick has it at the head of its queue at 80 MB/s")
}

// Each copy gets SV Node's 30 s: a second owner asked 10 s ago is not judged yet.
func TestQueuedReaskGivesEachCopyThirtySeconds(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	second, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[second] = 3_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-3*time.Minute))
	}

	for _, h := range []int32{20, 21, 22, 11} {
		askAt(t, sm, second, heightHash(t, sm, h), now.Add(-10*time.Second))
	}

	sm.maybeReaskQueuedBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// A block with one healthy copy is not raced, whatever its other copy does.
func TestRaceLeavesABlockWhileOneCopyIsHealthy(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, 1_000_000, now.Add(-40*time.Second))
	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.2:8333"), 300_000_000, 200_000_000, now.Add(-40*time.Second))

	_, _, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "the second copy arrives at 5 MB/s")
}

// Two owners each sending the block under 100 KB/s for 40 s: a third peer is asked and both
// stalling owners are dropped. With three owners nobody else is asked.
func TestRaceAsksAThirdPeerWhenEveryCopyStruggles(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, _ := schedulerPeer(t, sm, 1, 2000)
	b, _ := schedulerPeer(t, sm, 2, 2000)
	_, racerRec := schedulerPeer(t, sm, 3, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))
	require.True(t, sm.blockDownloads.Add(b, next))

	now := time.Now()
	sm.streams.markRaced(next, now.Add(-time.Minute))

	for _, p := range []struct {
		s    *blockStream
		read int64
	}{
		{sm.streams.start(next, 11, a, 300_000_000, now.Add(-40*time.Second)), 1_000_000},
		{sm.streams.start(next, 11, b, 300_000_000, now.Add(-40*time.Second)), 2_000_000},
	} {
		p.s.read.Store(p.read)
	}

	sm.maybeRaceSlowBlock(now)

	require.True(t, WaitUntil(func() bool { return racerRec.count() == 1 }, 5*time.Second), "a third peer is asked")
	require.Equal(t, []chainhash.Hash{next}, racerRec.all())
	require.True(t, WaitUntil(func() bool { return !a.Connected() && !b.Connected() }, 5*time.Second), "and both stalling owners are dropped")
}

// The race never asks a fourth peer.
func TestRaceNeverAsksForAFourthCopy(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	next := heightHash(t, sm, 11)
	now := time.Now()

	// Three owners, SV Node's DEFAULT_MAX_BLOCK_PARALLEL_FETCH, written out so a changed bound
	// fails here.
	for i := range 3 {
		p, _ := schedulerPeer(t, sm, uint8(i+1), 2000)
		require.True(t, sm.blockDownloads.Add(p, next))

		s := sm.streams.start(next, 11, p, 300_000_000, now.Add(-40*time.Second))
		s.read.Store(1_000_000)
	}

	_, fourthRec := schedulerPeer(t, sm, 9, 2000)

	sm.maybeRaceSlowBlock(now)

	require.False(t, WaitUntil(func() bool { return fourthRec.count() > 0 }, 300*time.Millisecond))
	require.Len(t, sm.blockDownloads.OwnersOf(next), 3)
}
