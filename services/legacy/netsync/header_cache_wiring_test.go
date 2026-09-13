package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHeadersRoundLocator_UsesTheChain pins the change that ended a seven-hour
// mainnet stall. The locator it replaces was anchored at the back of a header
// list, far above what the node had committed, so a peer that had not reached
// that point recognised nothing in it. That list is gone now, along with
// everything that could anchor a locator on it; this pins that the chain's own
// locator is what goes out.
func TestHeadersRoundLocator_UsesTheChain(t *testing.T) {
	tip := chainhash.Hash{0x11}
	fromChain := []*chainhash.Hash{{0x22}, {0x33}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return(fromChain, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.settings.Legacy.MultiPeerBlockDownload = true

	got, err := sm.headersRoundLocator(&tip, 100)
	require.NoError(t, err)

	require.Len(t, got, 2, "the locator must come from the chain")
	require.Equal(t, fromChain[0], got[0])
}

// TestNew_BuildsAHeaderCache is a guard against the cache being forgotten in
// the constructor. A nil cache is not a crash, because every method on it is
// nil safe. It is worse: a silently empty wanted range for ever.
//
// This calls the real constructor rather than a helper that pokes the field in
// afterwards. The earlier version of this test called newRaceManagerWithCache,
// which set sm.headerCache itself and then asserted that it was set — so it
// could not fail no matter what New did. Commenting out the constructor's own
// "sm.headerCache = newHeaderCache()" line left the whole package green, which
// is how the reviewer caught it.
func TestNew_BuildsAHeaderCache(t *testing.T) {
	// Cancellable rather than context.Background(): New starts a goroutine
	// (startKafkaListeners) that polls sm.blockchainClient.IsFSMCurrentState
	// once a second for as long as ctx is alive, and this test's mock has
	// nothing else it needs to keep answering once the test is done.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 800_000}, nil)
	// startKafkaListeners' ticker goroutine calls this every second regardless
	// of what this test is pinning; without a stub the mock panics the whole
	// test binary the first time it fires.
	client.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)

	config := &Config{
		ChainParams: &chaincfg.MainNetParams,
		// Checkpoints are irrelevant to this test and pull in a real header walk
		// against bestHeader, which is a bare stand-in and not a chain the
		// checkpoint tables know about.
		DisableCheckpoints: true,
	}

	sm, err := New(
		ctx,
		ulogger.TestLogger{},
		&settings.Settings{},
		client,
		&validator.MockValidator{},
		&utxo.MockUtxostore{},
		blob_memory.New(),
		nil,
		&subtreevalidation.MockSubtreeValidation{},
		&blockvalidation.MockBlockValidation{},
		blockassembly.NewMock(),
		config,
	)
	require.NoError(t, err)

	require.NotNil(t, sm.headerCache, "the manager must start with a cache to fill")
}

// newHeaderCacheManager builds a manager with a header cache ready to fill —
// the state New leaves it in — without paying for New's other eleven
// dependencies. Whether lastCommittedTip is also set is each test's own call,
// since the unset case is one of the three behaviours under test below.
func newHeaderCacheManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.headerCache = newHeaderCache()

	return sm
}

// TestFillHeaderCache_ABatchThatLinksToTheCommittedTipIsAccepted is the ordinary
// case: a batch whose first header names the committed tip as its parent lands
// in the cache at the heights just above it.
func TestFillHeaderCache_ABatchThatLinksToTheCommittedTipIsAccepted(t *testing.T) {
	sm := newHeaderCacheManager(t)

	tipHash := chainhash.Hash{0x50}
	sm.lastCommittedTip.Store(&committedTip{height: 100, hash: tipHash})

	peer, _, _ := connectRacePeer(t, 210, 1000)

	var nonce uint32
	msg, hashes := linkedHeaders(tipHash, 3, &nonce)

	sm.fillHeaderCache(peer, msg)

	for i, want := range hashes {
		height := int32(101 + i) //nolint:gosec // i is bounded by the 3 headers built above
		got, ok := sm.headerCache.At(height)
		require.True(t, ok, "height %d must be named", height)
		require.Equal(t, want, got)
	}
}

// TestFillHeaderCache_ABatchThatDoesNotLinkIsDroppedAndThePeerKeepsItsConnection
// is the headline behaviour of the whole task. A peer that answers honestly
// from an ancestor older than this node's committed tip is not a peer to
// punish — the 2026-09-11 stall was exactly a disconnect on this path, taken
// against a peer that had told the truth.
func TestFillHeaderCache_ABatchThatDoesNotLinkIsDroppedAndThePeerKeepsItsConnection(t *testing.T) {
	sm := newHeaderCacheManager(t)
	sm.lastCommittedTip.Store(&committedTip{height: 100, hash: chainhash.Hash{0x50}})

	peer, _, _ := connectRacePeer(t, 211, 1000)

	var nonce uint32
	// Hangs off a parent that is not the committed tip: an older shared
	// ancestor, not a doctored chain.
	msg, _ := linkedHeaders(chainhash.Hash{0x77}, 3, &nonce)

	sm.fillHeaderCache(peer, msg)

	_, ok := sm.headerCache.Top()
	require.False(t, ok, "a batch that does not link must not be cached")

	require.True(t, peer.Connected(),
		"an honest reply from an older shared ancestor must not cost the peer its connection")
}

// TestFillHeaderCache_ABatchArrivingWithNoCommittedTipIsDroppedWithoutDisconnecting
// covers the state before the first seed or commit, now that fillHeaderCache no
// longer asks the blockchain service for the tip and so has no RPC failure to
// report: an unset tip reads the same as any other batch that cannot be
// checked, dropped with nobody blamed for it.
func TestFillHeaderCache_ABatchArrivingWithNoCommittedTipIsDroppedWithoutDisconnecting(t *testing.T) {
	sm := newHeaderCacheManager(t)
	// lastCommittedTip left unset on purpose.

	peer, _, _ := connectRacePeer(t, 212, 1000)

	var nonce uint32
	msg, _ := linkedHeaders(chainhash.Hash{0x88}, 3, &nonce)

	sm.fillHeaderCache(peer, msg)

	_, ok := sm.headerCache.Top()
	require.False(t, ok, "nothing can be cached before the committed tip is known")

	require.True(t, peer.Connected(), "an unset tip must not cost the peer its connection either")
}
