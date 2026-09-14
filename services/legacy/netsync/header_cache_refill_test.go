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

// TestMaybeRequestMoreHeaders_RotatesAcrossEligiblePeers pins the fix for the
// single-usable-peer stall: PushGetHeadersMsg drops a repeat of the same
// (locator, stop hash) pair from one peer, and while the header cache is
// empty neither half of that pair can move — the stop hash is always the
// zero hash, and the locator is built from the committed tip, which cannot
// advance until a block commits. Sending to peers[0] on every call therefore
// sends exactly once and is filtered forever after: with one usable peer, the
// node never asks again once that first reply is unusable.
//
// This drives the refill repeatedly against three connected, eligible peers
// and requires that more than one of them actually received a request. Before
// the fix, every one of these passes would have named the same peer and every
// send after the first would have been silently dropped by that peer's own
// dedup filter.
func TestMaybeRequestMoreHeaders_RotatesAcrossEligiblePeers(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headersA := demotionPeer(t, sm, 240, 1000)
	_, _, headersB := demotionPeer(t, sm, 241, 1000)
	_, _, headersC := demotionPeer(t, sm, 242, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x72}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	sm.noteCommittedHeight(int32(len(hashes)), hashes[len(hashes)-1]) //nolint:gosec // a small test count

	// Repeated refills, as repeated commit-driven passes would produce while a
	// single unusable reply leaves the cache dry. The rate limit's floor is
	// reset before each pass so every one of them actually reaches a peer,
	// standing in for headerCacheRefillInterval having elapsed on a live node.
	for i := 0; i < 3; i++ {
		want := i + 1

		sm.lastHeaderRequestAt.Store(0)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool {
			return headersA.count()+headersB.count()+headersC.count() >= want
		}, 5*time.Second), "pass %d must have sent a getheaders", i)
	}

	distinct := 0

	for _, r := range []*getHeadersRecorder{headersA, headersB, headersC} {
		if r.count() > 0 {
			distinct++
		}
	}

	require.Greater(t, distinct, 1,
		"three refills against three connected, eligible peers must not all land on the same one")
}

// TestMaybeRequestMoreHeaders_SinglePeerAsksAgainAfterRateLimitLapses pins the
// half of the stall the peer rotation above does not reach. Rotation only
// ever changes which peer is asked; with exactly one eligible peer every
// retry names that same peer again, and PushGetHeadersMsg's own dedup filter
// on that peer sees the same (locator, stop hash) pair every time — the stop
// hash is always the zero hash, and the locator cannot move until a block
// commits — so it drops every retry after the first while logging success.
// Before the fix this test would see exactly one getheaders reach the wire
// no matter how many times the refill ran.
func TestMaybeRequestMoreHeaders_SinglePeerAsksAgainAfterRateLimitLapses(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 250, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x73}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	sm.noteCommittedHeight(int32(len(hashes)), hashes[len(hashes)-1]) //nolint:gosec // a small test count

	// Repeated refills against the one eligible peer, as repeated
	// commit-driven passes would produce while a single unusable reply
	// leaves the cache dry. The rate limit's floor is reset before each pass
	// to stand in for headerCacheRefillInterval having elapsed on a live
	// node — the actual gate under test is the peer's own dedup filter, not
	// this one.
	for i := 0; i < 3; i++ {
		want := i + 1

		sm.lastHeaderRequestAt.Store(0)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool {
			return headers.count() >= want
		}, 5*time.Second), "pass %d must have sent a getheaders", i)
	}

	require.Greater(t, headers.count(), 1,
		"a single eligible peer must still be asked again once the rate limit lapses")
}
