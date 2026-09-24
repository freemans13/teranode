package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A peer's queue is measured in time, not blocks. Counting blocks treated a 1.3 GB block like a
// 100 MB one, so peers were handed minutes of work and the block the chain needed next began
// arriving five and a half minutes after it was asked for. Racing those blocks to a second peer
// then downloaded seven of them twice in eleven minutes. Now each peer's queue is estimated as
// the bytes left on what it is sending plus its other owed blocks at the average block size,
// over its measured rate; a peer with queueWorkCap of work is given no more, and each block goes
// to the peer expected to reach it soonest.

const qMB = int64(1) << 20

// queueTimeManager has two peers and an average block size of 100 MB.
func queueTimeManager(t *testing.T) (*SyncManager, *peerpkg.Peer, *getDataRecorder, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	sm := schedulerManager(t)
	sm.ctx = context.Background()
	sm.blockPark, _ = newTestPark(t, "")
	sm.streams = newStreamRegistry()
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16

	for i := 0; i < 3; i++ {
		sm.blockSizeTracker.addBlockSize(100 * qMB)
	}

	a, aRec := schedulerPeer(t, sm, 130, 1000)
	sm.storeSyncPeer(a, &syncPeerState{})

	b, bRec := schedulerPeer(t, sm, 131, 1000)

	return sm, a, aRec, b, bRec
}

// busy gives p a block it is part way through sending, with remaining bytes still to come.
func busy(t *testing.T, sm *SyncManager, p *peerpkg.Peer, salt byte, remaining int64) {
	t.Helper()

	h := chainhash.Hash{0xb0, salt}
	require.True(t, sm.blockDownloads.Add(p, h))

	s := sm.streams.start(h, 0, p, remaining, time.Now())
	s.lastRead.Store(time.Now().UnixNano())
}

func owes(t *testing.T, sm *SyncManager, p *peerpkg.Peer, salt byte, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		require.True(t, sm.blockDownloads.Add(p, chainhash.Hash{0xc0, salt, byte(i)}))
	}
}

func TestABlockGoesToThePeerThatReachesItSoonest(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf5}
	msg, hashes := linkedHeaders(anchor, 1, &nonce)

	sm, a, aRec, b, bRec := queueTimeManager(t)

	// a owes one block, but has 500 MB of it left at 10 MB/s: 50 s.
	sm.streams.rates[a] = float64(10 * qMB)
	busy(t, sm, a, 1, 500*qMB)

	// b owes three blocks, 300 MB at 20 MB/s: 15 s.
	sm.streams.rates[b] = float64(20 * qMB)
	owes(t, sm, b, 2, 3)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return bRec.count() == 1 }, 5*time.Second))
	require.Equal(t, hashes, bRec.all(), "the peer owing more blocks but less time takes it")
	require.Zero(t, aRec.count())
}

func TestAPeerWithAMinuteOfWorkIsGivenNoMore(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf6}
	msg, hashes := linkedHeaders(anchor, 2, &nonce)

	sm, a, aRec, b, bRec := queueTimeManager(t)

	// a: 1 GB left at 10 MB/s, 100 s. b: eight blocks, 800 MB at 10 MB/s, 80 s.
	sm.streams.rates[a] = float64(10 * qMB)
	busy(t, sm, a, 1, 1024*qMB)
	sm.streams.rates[b] = float64(10 * qMB)
	owes(t, sm, b, 2, 8)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return aRec.count()+bRec.count() > 0 }, 300*time.Millisecond),
		"both peers have over a minute of work, so neither is given more")

	next, ok := nextCandidateHash(t, sm)
	require.True(t, ok)
	require.Equal(t, hashes[0], next, "the block waits for the next pass rather than being lost")
}

func TestAnIdlePeerStillTakesOneBlockLargerThanTheCap(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf7}
	msg, hashes := linkedHeaders(anchor, 3, &nonce)

	sm, a, aRec, b, bRec := queueTimeManager(t)

	// 2 GB average at 10 MB/s: one block is over three minutes of work.
	sm.blockSizeTracker = newBlockSizeTracker(10)
	for i := 0; i < 3; i++ {
		sm.blockSizeTracker.addBlockSize(2048 * qMB)
	}

	sm.streams.rates[a] = float64(10 * qMB)
	sm.streams.rates[b] = float64(10 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return aRec.count()+bRec.count() == 2 }, 5*time.Second))
	require.Equal(t, hashes[0:1], aRec.all(), "each idle peer takes one block, however big")
	require.Equal(t, hashes[1:2], bRec.all())
}

// Before any block has completed there is no average to estimate from, and right after a restart
// that is when every peer's queue is filled. On mainnet on 2026-09-24 that start handed one peer
// 15 blocks and another almost four minutes of work. Until an average exists each peer is asked
// for one block at a time.
func TestWithNoAverageYetEachPeerIsAskedForOneBlock(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf8}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm, a, aRec, _, bRec := queueTimeManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return aRec.count()+bRec.count() == 2 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return aRec.count()+bRec.count() > 2 }, 300*time.Millisecond),
		"one block each until an average block size exists")
	require.Equal(t, hashes[0:1], aRec.all())
	require.Equal(t, hashes[1:2], bRec.all())
}

// Block sizes at height 708,000 run from a few megabytes to 2 GB, so the average of the last ten
// said 8 MB right after a run of small ones, and a peer was handed nine of the next blocks the
// chain needed: a one-minute queue looked like room for dozens. The queue is now estimated from
// the largest recent block, so one big block in the window keeps every queue shallow.
func TestTheLargestRecentBlockSetsTheQueueDepth(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf9}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm, a, aRec, b, bRec := queueTimeManager(t)

	sm.blockSizeTracker = newBlockSizeTracker(10)
	for i := 0; i < 9; i++ {
		sm.blockSizeTracker.addBlockSize(8 * qMB)
	}

	sm.blockSizeTracker.addBlockSize(800 * qMB)

	sm.streams.rates[a] = float64(10 * qMB)
	sm.streams.rates[b] = float64(10 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return aRec.count()+bRec.count() == 2 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return aRec.count()+bRec.count() > 2 }, 300*time.Millisecond),
		"an 800 MB block at 10 MB/s is over a minute, so each peer takes one")
	require.Equal(t, hashes[0:1], aRec.all())
	require.Equal(t, hashes[1:2], bRec.all())
}
