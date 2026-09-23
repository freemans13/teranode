package netsync

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The race asks a second peer for a block only when the chain is about to wait on it: its
// estimated finish is later than when the chain will reach it, AND the peer sending it is well
// below the median rate of the other peers. A slow peer with enough lead is left alone, and so
// is a fast peer on a huge block. On 2026-09-23 mainnet waited 49 seconds on block 634,643, a
// 309 MB block arriving from one peer at 4.3 MB/s, with nothing to ask anyone else for it.

func hashN(n byte) chainhash.Hash { return chainhash.Hash{n} }

// streamAt registers a stream that started at start, has received read of total bytes, and
// belongs to owner at height.
func streamAt(r *streamRegistry, n byte, height int32, owner *peerpkg.Peer, total, read int64, start time.Time) *blockStream {
	s := r.start(hashN(n), height, owner, total, start)
	s.read.Store(read)

	return s
}

func TestRaceFiresWhenTheChainIsAboutToWaitOnASlowPeer(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	start := now.Add(-20 * time.Second)

	slowPeer := newTestPeer(t, "10.0.0.1:8333")
	fastA := newTestPeer(t, "10.0.0.2:8333")
	fastB := newTestPeer(t, "10.0.0.3:8333")

	// 309 MB at 4 MB/s after 20 s: 80 MB in, 229 MB left, about 57 s to go.
	slow := streamAt(r, 1, 1001, slowPeer, 309_000_000, 80_000_000, start)
	// Two other peers streaming at 20 MB/s.
	streamAt(r, 2, 1003, fastA, 900_000_000, 400_000_000, start)
	streamAt(r, 3, 1004, fastB, 900_000_000, 400_000_000, start)

	// The chain is at 1000 and commits 4 blocks a second: it needs 1001 in a quarter of a second.
	got, c, ok := r.pickRace(now, 1000, 4)
	require.True(t, ok)
	require.Same(t, slow, got)
	require.Greater(t, c.eta, c.need)
	require.Less(t, c.rate, raceSlowFraction*c.median)
}

func TestRaceLeavesASlowPeerAloneWhenTheLeadCoversIt(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	start := now.Add(-20 * time.Second)

	streamAt(r, 1, 1500, newTestPeer(t, "10.0.0.1:8333"), 309_000_000, 80_000_000, start)
	streamAt(r, 2, 1003, newTestPeer(t, "10.0.0.2:8333"), 900_000_000, 400_000_000, start)
	streamAt(r, 3, 1004, newTestPeer(t, "10.0.0.3:8333"), 900_000_000, 400_000_000, start)

	// 500 blocks ahead at 4 a second is 125 s of lead against about 57 s to finish.
	_, _, ok := r.pickRace(now, 1000, 4)
	require.False(t, ok)
}

func TestRaceLeavesAPeerAtTheMedianRateAlone(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	start := now.Add(-20 * time.Second)

	// A huge block the chain needs now, arriving as fast as every other peer delivers.
	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 4_000_000_000, 400_000_000, start)
	streamAt(r, 2, 1003, newTestPeer(t, "10.0.0.2:8333"), 900_000_000, 400_000_000, start)
	streamAt(r, 3, 1004, newTestPeer(t, "10.0.0.3:8333"), 900_000_000, 400_000_000, start)

	_, _, ok := r.pickRace(now, 1000, 4)
	require.False(t, ok, "asking a second peer only helps when the first is the slow one")
}

func TestRaceNeedsTwoOtherPeersToJudgeAgainst(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	start := now.Add(-20 * time.Second)

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 309_000_000, 80_000_000, start)
	streamAt(r, 2, 1003, newTestPeer(t, "10.0.0.2:8333"), 900_000_000, 400_000_000, start)

	_, _, ok := r.pickRace(now, 1000, 4)
	require.False(t, ok, "one other peer is no median")
}

func TestRaceWaitsUntilAStreamHasBeenMeasured(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 309_000_000, 1_000, now.Add(-time.Second))
	streamAt(r, 2, 1003, newTestPeer(t, "10.0.0.2:8333"), 900_000_000, 400_000_000, now.Add(-20*time.Second))
	streamAt(r, 3, 1004, newTestPeer(t, "10.0.0.3:8333"), 900_000_000, 400_000_000, now.Add(-20*time.Second))

	_, _, ok := r.pickRace(now, 1000, 4)
	require.False(t, ok, "a second of data is not a rate")
}

func TestRaceUsesCompletedPeerRatesAsTheMedian(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	fastA := newTestPeer(t, "10.0.0.2:8333")
	fastB := newTestPeer(t, "10.0.0.3:8333")

	// Two peers finished blocks at 20 MB/s earlier; nothing else is in flight now.
	for _, p := range []*peerpkg.Peer{fastA, fastB} {
		s := streamAt(r, 9, 900, p, 200_000_000, 200_000_000, now.Add(-time.Minute))
		r.finish(s, now.Add(-50*time.Second), true)
	}

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 309_000_000, 80_000_000, now.Add(-20*time.Second))

	_, _, ok := r.pickRace(now, 1000, 4)
	require.True(t, ok)
}

func TestRaceIsAskedOncePerBlockAndClearedWhenItLands(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	start := now.Add(-20 * time.Second)

	slow := streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 309_000_000, 80_000_000, start)
	streamAt(r, 2, 1003, newTestPeer(t, "10.0.0.2:8333"), 900_000_000, 400_000_000, start)
	streamAt(r, 3, 1004, newTestPeer(t, "10.0.0.3:8333"), 900_000_000, 400_000_000, start)

	_, _, ok := r.pickRace(now, 1000, 4)
	require.True(t, ok)

	r.markRaced(slow.hash, now)

	_, _, ok = r.pickRace(now, 1000, 4)
	require.False(t, ok, "one race per block, and one race at a time")

	r.finish(slow, now, true)
	require.False(t, r.racing(), "the race clears when the block lands")
}

func TestFinishedStreamRecordsItsPeersRate(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	p := newTestPeer(t, "10.0.0.1:8333")

	s := streamAt(r, 1, 1001, p, 100_000_000, 100_000_000, now.Add(-10*time.Second))
	r.finish(s, now, true)

	require.InDelta(t, 10_000_000.0, r.peerRate(p), 1)

	// An aborted stream says nothing about the peer's bandwidth.
	s = streamAt(r, 2, 1002, p, 100_000_000, 1_000, now.Add(-10*time.Second))
	r.finish(s, now, false)
	require.InDelta(t, 10_000_000.0, r.peerRate(p), 1)
}

func TestCountingReaderCountsWhatTheSinkReads(t *testing.T) {
	r := newStreamRegistry()
	s := r.start(hashN(1), 1001, nil, 11, time.Now())

	got, err := io.ReadAll(countingReader{r: bytes.NewReader([]byte("hello world")), s: s})
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
	require.Equal(t, int64(11), s.read.Load())
}

func TestChooseRacerPrefersTheFastestPeerThatDoesNotOwnTheBlock(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	owner := newTestPeer(t, "10.0.0.1:8333")
	slowOther := newTestPeer(t, "10.0.0.2:8333")
	fastOther := newTestPeer(t, "10.0.0.3:8333")
	unknown := newTestPeer(t, "10.0.0.4:8333")

	for p, bps := range map[*peerpkg.Peer]int64{owner: 50_000_000, slowOther: 2_000_000, fastOther: 30_000_000} {
		s := streamAt(r, 9, 900, p, bps*10, bps*10, now.Add(-10*time.Second))
		r.finish(s, now, true)
	}

	queued := func(*peerpkg.Peer) int { return 0 }

	got := r.chooseRacer([]*peerpkg.Peer{owner, slowOther, fastOther, unknown}, []*peerpkg.Peer{owner}, queued)
	require.Same(t, fastOther, got, "the owner is never its own racer, and a measured fast peer beats an unmeasured one")

	require.Nil(t, r.chooseRacer([]*peerpkg.Peer{owner}, []*peerpkg.Peer{owner}, queued), "no one else to ask")

	// With no measurements at all, the peer with the fewest blocks queued goes first.
	fresh := newStreamRegistry()
	counts := map[*peerpkg.Peer]int{slowOther: 9, fastOther: 3, unknown: 5}
	got = fresh.chooseRacer([]*peerpkg.Peer{slowOther, fastOther, unknown}, nil, func(p *peerpkg.Peer) int { return counts[p] })
	require.Same(t, fastOther, got)
}
