package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The next frontBlockCount blocks the chain needs are each put at the top of a different peer's
// queue. A peer sends the blocks it is asked for in order and no message moves one up, so a
// block's place is decided when it is handed out. On 2026-09-24 at height 705,000, with blocks of
// 175 MB to 2 GB, the next block's bytes began 1m37s to 5m55s after it was requested, because the
// assigner filled one peer at a time and the block waited behind others at the same peer; the
// chain spent 192 to 274 s of every 300 waiting.

// frontHarness is the assignment harness with n connected peers and blocks large enough that the
// ladder narrows every peer to 5 in flight, the regime mainnet was in.
func frontHarness(t *testing.T, peers int, from, to int32) (*SyncManager, []*peerpkg.Peer) {
	t.Helper()

	sm := assignManager(t, from, to)
	sm.blockPark, _ = newTestPark(t, "")
	if sm.streams == nil {
		sm.streams = newStreamRegistry()
	}
	if sm.ctx == nil {
		sm.ctx = context.Background()
	}
	sm.blockSizeTracker.addBlockSize(300 << 20)
	sm.settings.Legacy.BlockDownloadLowerWindow = 64
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16

	out := make([]*peerpkg.Peer, 0, peers)

	for i := 0; i < peers; i++ {
		p, _ := schedulerPeer(t, sm, uint8(i+1), to+1000) //nolint:gosec // a small test count
		out = append(out, p)
	}

	return sm, out
}

func heightHash(t *testing.T, sm *SyncManager, h int32) chainhash.Hash {
	t.Helper()

	hash, ok := sm.headerCache.At(h)
	require.True(t, ok)

	return hash
}

func TestFrontBlocksEachGoToTheTopOfADifferentPeer(t *testing.T) {
	sm, _ := frontHarness(t, 10, 1, 120)
	mockCommittedTip(t, sm, 10, 0)

	sm.assignWantedBlocks()

	owners := make(map[*peerpkg.Peer]int32)

	for h := int32(11); h < 11+frontBlockCount; h++ {
		owner := sm.blockDownloads.TopOwner(heightHash(t, sm, h))
		require.NotNil(t, owner, "block %d is at the top of some peer's queue", h)

		prev, taken := owners[owner]
		require.False(t, taken, "blocks %d and %d share a peer", prev, h)
		owners[owner] = h
	}
}

// With fewer peers than frontBlockCount, the front shrinks to the peers there are, so a small
// node is not held to one block at a time.
func TestTheFrontShrinksToThePeersThereAre(t *testing.T) {
	sm, _ := frontHarness(t, 3, 1, 120)
	mockCommittedTip(t, sm, 10, 0)

	sm.assignWantedBlocks()

	for h := int32(11); h < 14; h++ {
		require.NotNil(t, sm.blockDownloads.TopOwner(heightHash(t, sm, h)), "three peers, three front blocks at the top")
	}

	require.NotEmpty(t, sm.blockDownloads.OwnersOf(heightHash(t, sm, 14)), "and the read-ahead carries on behind them")
}

// When a front block has no peer it would be first at, nothing further ahead is handed out, so a
// peer drains and comes free for it.
func TestFarBlocksWaitWhileAFrontBlockHasNoPeer(t *testing.T) {
	sm, peers := frontHarness(t, 2, 1, 120)
	mockCommittedTip(t, sm, 10, 0)

	// Both peers are busy with blocks far ahead, handed out earlier.
	for i, p := range peers {
		require.True(t, sm.blockDownloads.Add(p, heightHash(t, sm, int32(60+i)))) //nolint:gosec // a small test count
	}

	sm.assignWantedBlocks()

	for h := int32(11); h < 13; h++ {
		require.NotEmpty(t, sm.blockDownloads.OwnersOf(heightHash(t, sm, h)),
			"block %d is still asked for, behind a busy peer's queue, rather than not at all", h)
	}

	for h := int32(13); h <= 120; h++ {
		if h == 60 || h == 61 {
			continue
		}

		require.Empty(t, sm.blockDownloads.OwnersOf(heightHash(t, sm, h)),
			"block %d is not asked for while the next blocks have no peer to be first at", h)
	}
}

func TestAFrontBlockBehindOthersIsAskedOfAnEmptyPeer(t *testing.T) {
	sm, peers := frontHarness(t, 2, 1, 120)
	// The chain is at 11, so 12 and 13 are the first two front blocks.
	mockCommittedTip(t, sm, 11, 0)

	busy, empty := peers[0], peers[1]
	twelve, thirteen := heightHash(t, sm, 12), heightHash(t, sm, 13)

	// Handed out while they were far ahead: 13 sits behind 12 at the busy peer.
	base := time.Now().Add(-10 * time.Second)
	sm.blockDownloads.now = func() time.Time { return base }
	require.True(t, sm.blockDownloads.Add(busy, twelve))
	sm.blockDownloads.now = func() time.Time { return base.Add(time.Second) }
	require.True(t, sm.blockDownloads.Add(busy, thirteen))
	sm.blockDownloads.now = time.Now

	sm.assignWantedBlocks()

	require.Same(t, busy, sm.blockDownloads.TopOwner(twelve), "12 is already at the top of the busy peer")
	require.True(t, sm.blockDownloads.HasOwner(empty, thirteen), "13 is behind 12, so it is also asked of the empty peer")
	require.Same(t, empty, sm.blockDownloads.TopOwner(thirteen))
}

// A peer is only given up on for a block when its connection has gone silent, not when a fixed
// time has passed since the request. With large blocks a peer can spend minutes sending the
// blocks queued ahead of this one; asking a second peer then downloads the block twice.
func TestABlockAtABusyPeerIsNotAskedOfAnother(t *testing.T) {
	sm, peers := frontHarness(t, 2, 1, 120)
	mockCommittedTip(t, sm, 10, 0)

	far := heightHash(t, sm, 60)

	past := time.Now().Add(-2 * blockRequestRetryInterval)
	sm.blockDownloads.now = func() time.Time { return past }
	require.True(t, sm.blockDownloads.Add(peers[0], far))
	sm.blockDownloads.now = time.Now

	receiving := peers[0]

	// The peer is part way through sending an earlier block.
	earlier := sm.streams.start(heightHash(t, sm, 59), 59, receiving, 400<<20, time.Now().Add(-30*time.Second))
	earlier.read.Store(100 << 20)
	earlier.lastRead.Store(time.Now().UnixNano())

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 60, hash: far}}),
		"its peer is still sending block bytes, so the block is not handed to anyone else")
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(receiving), "and the peer is not forgiven it")

	// The stream stops arriving: no block bytes for longer than the retry window.
	earlier.lastRead.Store(time.Now().Add(-2 * blockRequestRetryInterval).UnixNano())

	require.Len(t, sm.unownedBlocks([]wantedBlock{{height: 60, hash: far}}), 1,
		"once the peer has sent no block bytes past the retry window, the block is asked for again")
}
