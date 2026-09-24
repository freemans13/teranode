package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// Design B (docs/superpowers/specs/2026-09-24-byte-bounded-block-download-design.md). At
// height 709,000 download is the limit: a 2 GB block took 265 s to arrive and 1.9 s to process.
// The chain can only move at the peers' combined speed, and it loses time only when peers go
// idle. So, with the park on, the read-ahead limit is a byte budget, a peer holds at most two
// large blocks, and the lowest block goes to the fastest peer with room.

const qMB = int64(1) << 20

// budgetManager has the park on, a stream registry, and two peers a and b.
func budgetManager(t *testing.T) (*SyncManager, *peerpkg.Peer, *getDataRecorder, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	sm := schedulerManager(t)
	sm.ctx = context.Background()
	sm.blockPark, _ = newTestPark(t, "")
	sm.streams = newStreamRegistry()
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16

	a, aRec := schedulerPeer(t, sm, 130, 1000)
	sm.storeSyncPeer(a, &syncPeerState{})

	b, bRec := schedulerPeer(t, sm, 131, 1000)

	return sm, a, aRec, b, bRec
}

func recentBlocks(sm *SyncManager, size int64) {
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockSizeTracker.addBlockSize(size)
}

func requested(recs ...*getDataRecorder) int {
	n := 0
	for _, r := range recs {
		n += r.count()
	}

	return n
}

func TestTheByteBudgetStopsRequestsAtTwentyGiB(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	// 19 GiB is arriving from a third peer already, so there is room for one more 1 GiB block.
	sm.streams.start(chainhash.Hash{0xe2}, 0, newTestPeer(t, "10.0.0.9:8333"), 19<<30, time.Now())

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 1 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 1 }, 300*time.Millisecond),
		"one more 1 GiB block fits under the 20 GiB budget, and no more")
}

func TestAParkedBlockCountsAtItsWireSize(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe3}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	// Converted, so the park charges its few-hundred-byte record, but the block is 19 GiB.
	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: chainhash.Hash{0xe4}, prevBlock: chainhash.Hash{0xe5}, converted: true, size: 300, wireSize: 19 << 30}))

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 1 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 1 }, 300*time.Millisecond),
		"the parked block's 19 GiB counts, not its record")
}

func TestAPeerHoldsAtMostTwoLargeBlocks(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe6}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 4 }, 300*time.Millisecond))
	require.Equal(t, 2, aRec.count(), "one sending and one queued")
	require.Equal(t, 2, bRec.count())
}

func TestTheLowestBlockGoesToTheFastestPeerWithRoom(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe7}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm, a, aRec, b, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	sm.streams.rates[a] = float64(10 * qMB)
	sm.streams.rates[b] = float64(50 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second))
	require.Equal(t, hashes[0:2], bRec.all(), "the faster peer takes the lowest blocks until it is full")
	require.Equal(t, hashes[2:4], aRec.all())
}

// At 07:35Z on 2026-09-24 three peers sat idle with 7 blocks parked and 5 owed: the pass trimmed
// the wanted range to the number of requests it could make before skipping what was already
// parked or owed, so the trimmed range held nothing to ask for.
func TestIdlePeersAreAskedForBlocksPastThoseAlreadyParkedOrOwed(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe8}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)

	// a is full with the first two; the next four are parked.
	require.True(t, sm.blockDownloads.Add(a, hashes[0]))
	require.True(t, sm.blockDownloads.Add(a, hashes[1]))

	for i := 2; i < 6; i++ {
		require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hashes[i], prevBlock: hashes[i-1], converted: true, size: 300, wireSize: 200 * qMB}))
	}

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return bRec.count() == 2 }, 5*time.Second), "the idle peer is asked for the next two nobody holds")
	require.Equal(t, hashes[6:8], bRec.all())
	require.Zero(t, aRec.count())
}

// A peer holds at most two blocks whatever their size. Any limit that depends on block size
// fails when small blocks come first: after the 08:29Z restart on 2026-09-24 the first blocks to
// complete were small, the limit rose to 16, and the sync peer was handed ten blocks up to 1 GB
// while five peers sat idle.
func TestAPeerHoldsAtMostTwoBlocksWhateverTheirSize(t *testing.T) {
	for _, size := range []int64{0, 5 * qMB, 50 * qMB} {
		var nonce uint32

		anchor := chainhash.Hash{0xe9, byte(size >> 20)}
		msg, _ := linkedHeaders(anchor, 10, &nonce)

		sm, a, aRec, _, bRec := budgetManager(t)
		if size > 0 {
			recentBlocks(sm, size)
		}

		seedFetchHeaders(t, sm, a, anchor, msg)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second), "size %d", size)
		require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 4 }, 300*time.Millisecond), "size %d", size)
		require.Equal(t, 2, aRec.count())
		require.Equal(t, 2, bRec.count())
	}
}

// The largest recent block is taken over the last largestSizeSamples blocks, not the last ten.
// Small blocks finish quickly, so ten of them pushed a 1.8 GB block out of the window within 90
// seconds on 2026-09-24, the per-peer limit jumped to 16, and the node owed 106 blocks.
func TestOneLargeBlockStaysTheLargestThroughARunOfSmallOnes(t *testing.T) {
	bst := newBlockSizeTracker(10)
	bst.addBlockSize(1800 * qMB)

	for i := 0; i < 50; i++ {
		bst.addBlockSize(5 * qMB)
	}

	require.Equal(t, 1800*qMB, bst.largestRecentSize())

	for i := 0; i < largestSizeSamples; i++ {
		bst.addBlockSize(5 * qMB)
	}

	require.Equal(t, 5*qMB, bst.largestRecentSize(), "until it is further back than the window")
}

// The budget bounds how far ahead the node reaches, never the filling of a gap below blocks it
// already holds. On 2026-09-24 the park held about 42 blocks counted at 1 GB each after a restart,
// bytes ahead read 45 GB against the 20 GiB budget, and nothing was requested: not even the one
// missing block just above the tip that would have let the park drain. The chain stopped.
func TestTheBudgetNeverStopsAGapBelowParkedBlocksBeingFilled(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xea}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	seedFetchHeaders(t, sm, a, anchor, msg)

	// Everything from the second block to the sixth is parked, far over the budget.
	for i := 1; i < 6; i++ {
		require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hashes[i], prevBlock: hashes[i-1], converted: true, size: 300, wireSize: 19 << 30}))
	}

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 1 }, 5*time.Second), "the missing block is asked for")
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 1 }, 300*time.Millisecond), "and nothing past what is parked")
	require.Equal(t, []chainhash.Hash{hashes[0]}, append(aRec.all(), bRec.all()...))
}

// A peer is refilled the moment its block arrives, not at the next commit. While download is the
// limit blocks arrive out of order and park behind a missing one, and nothing asked the delivering
// peer for more until a later commit or the 30-second sweep: on 2026-09-24 four of eight peers
// were idle in some reports with the byte budget half used.
func TestAPeerIsRefilledWhenItsBlockArrivesAndParks(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xeb}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, _ := budgetManager(t)
	recentBlocks(sm, 200*qMB)
	sm.settings.Legacy.MultiPeerBlockDownload = true

	seedFetchHeaders(t, sm, a, anchor, msg)

	// a is asked for the second block, which will park: the first is still missing.
	require.True(t, sm.blockDownloads.Add(a, hashes[1]))

	body := peerpkg.BlockBody{Hash: hashes[1], Size: 200 * qMB, TxCount: 1, Converted: false}
	body.Header.PrevBlock = hashes[0]

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: a})

	require.True(t, WaitUntil(func() bool { return aRec.count() > 0 }, 5*time.Second), "the delivering peer is asked for more straight away")
}
