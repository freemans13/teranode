package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestMaybeRequestMoreHeaders_AsksForMoreOnceTheCacheReachesItsTop drives the
// real path: fill the header cache with one getheaders batch's worth of
// headers, commit the node all the way up to the top of that batch, run the
// download pass, and check a fresh getheaders went out.
//
// This is the state a node reaches the moment it finishes what its last
// getheaders reply named. Before this fix, nothing was left to notice that
// moment: wantedBlocksFromCache came back empty, assignWantedBlocks requested
// nothing, and the node sat there — the exact stall this whole rewrite exists
// to remove, reached simply by making progress.
//
// The locator itself is asserted twice, from two directions. GetBlockLocator
// must be called with the committed tip's own hash and height as its
// arguments — never anything derived from the cache or from a header merely
// downloaded, which is what had a peer answer from genesis and lose its
// connection for answering honestly on 2026-09-11. And the getheaders that
// goes out on the wire must carry exactly what that call returned, proving
// there is no second, competing way this path could have built a locator.
func TestMaybeRequestMoreHeaders_AsksForMoreOnceTheCacheReachesItsTop(t *testing.T) {
	fromChain := []*chainhash.Hash{{0x99}}

	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(fromChain, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 220, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x70}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))

	// The committed tip has caught all the way up to the last header the cache
	// names: nothing is left in the cache above it.
	tip := hashes[len(hashes)-1]
	sm.noteCommittedHeight(int32(len(hashes)), tip) //nolint:gosec // a small test count

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return headers.count() == 1 }, 5*time.Second),
		"a cache with nothing left above the committed tip must produce a getheaders")

	client.AssertCalled(t, "GetBlockLocator", mock.Anything, &tip, uint32(len(hashes))) //nolint:gosec // a small test count

	sent := headers.last()
	require.NotNil(t, sent)
	require.Equal(t, *fromChain[0], *sent.BlockLocatorHashes[0],
		"the getheaders sent must carry exactly the locator the chain's own GetBlockLocator returned")
}

// TestMaybeRequestMoreHeaders_DoesNotAskTwiceWithinTheRateLimit pins the floor
// that keeps a commit-driven pass from turning into a getheaders storm. Every
// commit can call assignWantedBlocks, and a cache that has run dry stays dry
// for as long as the reply is still in flight — without a floor, every one of
// those calls would send its own request to whichever peer it happened to
// pick.
func TestMaybeRequestMoreHeaders_DoesNotAskTwiceWithinTheRateLimit(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 221, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x71}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	sm.noteCommittedHeight(int32(len(hashes)), hashes[len(hashes)-1]) //nolint:gosec // a small test count

	// Twenty passes in a row, the way twenty rapid commits would each call
	// fetchHeaderBlocks while the first reply is still on the wire.
	for i := 0; i < 20; i++ {
		sm.fetchHeaderBlocks()
	}

	require.True(t, WaitUntil(func() bool { return headers.count() > 0 }, 5*time.Second),
		"sanity: at least the first pass must have asked")
	require.Equal(t, 1, headers.count(),
		"twenty passes inside the rate limit's window must produce exactly one getheaders")
	client.AssertNumberOfCalls(t, "GetBlockLocator", 1)
}
