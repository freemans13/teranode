// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the svnode-aligned header-chain retention on sync-peer rotation.
//
// The live bug: during headers-first IBD, when the sync peer dropped,
// handleDonePeerMsg -> updateSyncPeer called resetHeaderState, which ran
// headerList.Init() and DISCARDED the entire downloaded, PoW/checkpoint-verified
// header chain (observed live: 13,780 headers to height 719402 wiped, rewound to
// the committed tip 705619). Because the disk-fed in-order commit walk
// (drainValidateFromDisk) is header-list driven — it feeds blocks by walking
// headerList from the download frontier — an empty header list starved it, so the
// committed height froze for ~6 minutes until a fresh getheaders round-trip
// re-downloaded the headers, while hundreds of already-downloaded, directly
// committable block bodies sat stranded on disk.
//
// The fix: updateSyncPeer now decides retain-vs-reset. When a validated header
// prefix above the committed tip is still present it RETAINS headerList /
// headerHeightIndex / headerListSeed / headerCheckpoint and resumes getheaders
// from the CURRENT header frontier (so a new peer supplies only headers ABOVE what
// we hold). resetHeaderState is reserved for the cases that genuinely need the
// rewind: an empty/seed-only list, or a stale checkpoint cursor.
//
// These tests use blockchain2.Mock rather than the sqlitememory store, following
// the same rationale documented in unconnecting_headers_test.go: the only
// blockchain calls on the updateSyncPeer/startSync path are the two trivial reads
// (GetBestBlockHeader / GetBlockLocator) plus the FSM transitions
// (CatchUpBlocks / Run), and every sibling test that drives this path in this
// package mocks exactly those. Reusing the package's proven SyncManager
// scaffolding is worth more here than store fidelity — the assertions are about
// the header-state DECISION, not blockchain state.

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// seedRetainHeaderList seeds sm.headerList with a leading seed node (seedHash at
// seedHeight, never fetched) followed by `hashes` as fetchable header nodes at
// ascending heights, populating headerHeightIndex and headerListSeed exactly as
// production does. It returns the frontier (tail) hash.
func seedRetainHeaderList(sm *SyncManager, seedHash chainhash.Hash, seedHeight int32, hashes []chainhash.Hash) chainhash.Hash {
	seedCopy := seedHash
	seed := sm.headerList.PushBack(&headerNode{height: seedHeight, hash: &seedCopy})
	sm.headerListSeed = seed
	sm.headerHeightIndex[seedCopy] = seedHeight

	for i := range hashes {
		h := hashes[i]
		height := seedHeight + int32(i+1) //nolint:gosec
		sm.headerList.PushBack(&headerNode{height: height, hash: &h})
		sm.headerHeightIndex[h] = height
	}

	return hashes[len(hashes)-1]
}

// TestCanRetainHeaderChain_Decision locks in the retain-vs-reset boundary at the
// decision function directly (no peers, no mocks). Retain only when a real
// validated prefix ABOVE the committed tip is present; every degenerate shape
// falls back to reset.
func TestCanRetainHeaderChain_Decision(t *testing.T) {
	cpHash := chainhash.Hash{0x9c}
	checkpoint := &chaincfg.Checkpoint{Height: 100000, Hash: &cpHash}

	newSM := func() *SyncManager {
		return &SyncManager{
			logger:            ulogger.TestLogger{},
			headerList:        list.New(),
			headerHeightIndex: make(map[chainhash.Hash]int32),
			headerCheckpoint:  checkpoint,
		}
	}

	base := chainhash.Hash{0xa0}
	_, hashes := buildHeaderChain(base, 20, 40000)

	t.Run("populated prefix above tip retains", func(t *testing.T) {
		sm := newSM()
		seedRetainHeaderList(sm, base, 1, hashes)
		require.True(t, sm.canRetainHeaderChain(1),
			"a validated prefix well above the committed tip must be retained")
	})

	t.Run("empty list resets", func(t *testing.T) {
		sm := newSM()
		require.False(t, sm.canRetainHeaderChain(1),
			"an empty header list must fall back to reset")
	})

	t.Run("seed-only list resets", func(t *testing.T) {
		sm := newSM()
		seedCopy := base
		sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 1, hash: &seedCopy})
		sm.headerHeightIndex[seedCopy] = 1
		require.False(t, sm.canRetainHeaderChain(1),
			"a seed-only header list has no runway above the tip and must reset")
	})

	t.Run("frontier not above committed tip resets", func(t *testing.T) {
		sm := newSM()
		frontier := seedRetainHeaderList(sm, base, 1, hashes) // frontier height 21
		_ = frontier
		require.False(t, sm.canRetainHeaderChain(1_000_000),
			"when the committed tip has caught up to the frontier there is nothing to retain")
	})

	t.Run("nil header checkpoint resets", func(t *testing.T) {
		sm := newSM()
		seedRetainHeaderList(sm, base, 1, hashes)
		sm.headerCheckpoint = nil
		require.False(t, sm.canRetainHeaderChain(1),
			"past the final checkpoint (nil cursor) there is no interval to head toward; reset")
	})
}

// retainRotationHarness builds a SyncManager mid-headers-first-IBD with a stored
// (about-to-drop) sync peer and a separate connected candidate peer whose outbound
// getheaders are observable, so a test can drive updateSyncPeer and inspect the
// resume request.
type retainRotationHarness struct {
	sm            *SyncManager
	oldSyncPeer   *peer.Peer
	candidate     *peer.Peer
	getHeaders    chan *wire.MsgGetHeaders
	committedHash *chainhash.Hash
	frontierHash  chainhash.Hash
	checkpoint    *chaincfg.Checkpoint
}

// newRetainRotationHarness wires the harness. seedHashes are the fetchable header
// nodes above the seed; pass nil for the seed-only (reset-fallback) shape.
func newRetainRotationHarness(t *testing.T, index uint8, seedHashes []chainhash.Hash) *retainRotationHarness {
	t.Helper()

	chainParams := chaincfg.MainNetParams
	// A checkpoint far above the committed tip so headers-first stays engaged
	// (headerCheckpoint non-nil, committed tip < checkpoint height).
	cpHash := chainhash.Hash{0x9c, index}
	chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}}
	checkpoint := &chainParams.Checkpoints[0]

	getHeaders := make(chan *wire.MsgGetHeaders, 8)
	candidateCounterpartCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, msg *wire.MsgGetHeaders) {
				select {
				case getHeaders <- msg:
				default:
				}
			},
			OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	plainCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {},
			OnGetData:    func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	// The candidate is the peer startSync will promote; its counterpart records
	// the resume getheaders.
	_, candidate, err := MakeConnectedPeers(t, candidateCounterpartCfg, plainCfg, index)
	require.NoError(t, err)
	t.Cleanup(func() { candidate.DisconnectWithInfo("test done") })

	// The old sync peer is disconnected by updateSyncPeer; it is NOT in peerStates
	// (handleDonePeerMsg removes it before updateSyncPeer runs), so startSync cannot
	// re-select it.
	_, oldSyncPeer, err := MakeConnectedPeers(t, plainCfg, plainCfg, index+1)
	require.NoError(t, err)
	t.Cleanup(func() { oldSyncPeer.DisconnectWithInfo("test done") })

	// The candidate looks far ahead of our committed tip (1) so startSync keeps it
	// in headers-first mode and elects it.
	candidate.UpdateLastBlockHeight(1_000_000)

	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	committedHash := bestHeader.Hash()

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{committedHash}, nil).Maybe()
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()
	blockchainClient.On("CatchUpBlocks", mock.Anything).Return(nil).Maybe()
	blockchainClient.On("Run", mock.Anything, mock.Anything).Return(nil).Maybe()

	candidateState := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(candidateState.requestedTxns.Stop)
	t.Cleanup(candidateState.requestedBlocks.Stop)

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          test.CreateBaseTestSettings(t),
		chainParams:       &chainParams,
		blockchainClient:  blockchainClient,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	t.Cleanup(sm.requestedBlocks.Stop)

	// Only the candidate lives in peerStates; the old sync peer is stored as the
	// current sync peer and will be disconnected by updateSyncPeer.
	sm.peerStates.Set(candidate, candidateState)
	sm.storeSyncPeer(oldSyncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.nextCheckpoint = checkpoint
	sm.headerCheckpoint = checkpoint

	base := chainhash.Hash{0x11, index}
	frontier := base
	if len(seedHashes) > 0 {
		frontier = seedRetainHeaderList(sm, base, 1, seedHashes)
	} else {
		// Seed-only shape for the reset-fallback test.
		seedCopy := base
		sm.headerListSeed = sm.headerList.PushBack(&headerNode{height: 1, hash: &seedCopy})
		sm.headerHeightIndex[seedCopy] = 1
	}

	return &retainRotationHarness{
		sm:            sm,
		oldSyncPeer:   oldSyncPeer,
		candidate:     candidate,
		getHeaders:    getHeaders,
		committedHash: committedHash,
		frontierHash:  frontier,
		checkpoint:    checkpoint,
	}
}

// TestUpdateSyncPeer_RetainsHeaderChainAndResumesFromFrontier is the GREEN
// lock-in for the fix. A populated, validated header prefix above the committed
// tip must survive the sync-peer rotation, and the successor peer's getheaders
// must anchor at the frontier — never re-requesting from the committed tip.
func TestUpdateSyncPeer_RetainsHeaderChainAndResumesFromFrontier(t *testing.T) {
	base := chainhash.Hash{0x11, 40}
	_, hashes := buildHeaderChain(base, 20, 50000)

	h := newRetainRotationHarness(t, 40, hashes)
	sm := h.sm

	listLenBefore := sm.headerList.Len()
	indexLenBefore := len(sm.headerHeightIndex)
	genBefore := sm.headerGen

	require.Equal(t, 21, listLenBefore, "precondition: seed + 20 headers")

	// Simulate the sync peer dropping: this is the updateSyncPeer path reached from
	// handleDonePeerMsg.
	sm.updateSyncPeer(nil, "sync peer disconnected")

	// (a) the header chain is NOT wiped.
	require.Equal(t, listLenBefore, sm.headerList.Len(),
		"header list must be retained across the rotation, not wiped")
	require.Equal(t, indexLenBefore, len(sm.headerHeightIndex),
		"headerHeightIndex must be retained across the rotation")

	// (b) headers-first mode stays on.
	require.True(t, sm.headersFirstMode.Load(),
		"headers-first mode must stay engaged on a retaining rotation")

	// The generation counter advanced, so any in-flight fetch walk that captured
	// the departing peer aborts cleanly (the disk feed does not read headerGen).
	require.Greater(t, sm.headerGen, genBefore,
		"retainHeaderStateForRotation must bump headerGen to abort in-flight fetch walks")

	// (d) the disk-fed feed can still find a live node to feed.
	sm.headerMu.Lock()
	frontierElem := sm.downloadFrontierAnchorLocked()
	sm.headerMu.Unlock()
	require.NotNil(t, frontierElem,
		"downloadFrontierAnchorLocked must return a live node so drainValidateFromDisk keeps feeding")

	// (c) a new sync peer is promoted and getheaders is anchored at the frontier,
	// NOT the committed tip.
	require.Equal(t, h.candidate, sm.loadSyncPeer(),
		"the connected candidate must be promoted to sync peer")

	select {
	case got := <-h.getHeaders:
		require.NotEmpty(t, got.BlockLocatorHashes, "resume getheaders must carry a locator")
		require.True(t, got.BlockLocatorHashes[0].IsEqual(&h.frontierHash),
			"resume getheaders must anchor at the header frontier %s, got %s",
			h.frontierHash, got.BlockLocatorHashes[0])
		require.False(t, got.BlockLocatorHashes[0].IsEqual(h.committedHash),
			"resume getheaders must NOT anchor at the committed tip (that re-requests headers we already hold)")
		require.True(t, got.HashStop.IsEqual(h.checkpoint.Hash),
			"resume getheaders must still head toward the retained header checkpoint")
	case <-time.After(3 * time.Second):
		t.Fatal("no resume getheaders was issued after the retaining rotation")
	}
}

// TestUpdateSyncPeer_ResetFallback_SeedOnlyList proves the safe fallback is
// preserved: a seed-only header list (no validated runway above the tip) still
// takes the full resetHeaderState rewind and re-requests headers from the
// committed tip, exactly as before the fix.
func TestUpdateSyncPeer_ResetFallback_SeedOnlyList(t *testing.T) {
	h := newRetainRotationHarness(t, 42, nil)
	sm := h.sm

	require.Equal(t, 1, sm.headerList.Len(), "precondition: seed-only list")
	require.False(t, sm.canRetainHeaderChain(1),
		"a seed-only list must not be retained")

	sm.updateSyncPeer(nil, "sync peer disconnected")

	// resetHeaderState re-seeds the list from the committed tip (nextCheckpoint is
	// non-nil), so it is seed-only again — never a retained multi-node prefix.
	require.Equal(t, 1, sm.headerList.Len(),
		"reset path must re-seed the header list from the committed tip, not retain a prefix")

	// The resume flag must not be left armed after a reset rotation.
	sm.headerMu.Lock()
	resumeArmed := sm.headerResumeFromFrontier
	sm.headerMu.Unlock()
	require.False(t, resumeArmed, "headerResumeFromFrontier must be cleared after startSync")

	require.Equal(t, h.candidate, sm.loadSyncPeer(), "a new sync peer must be promoted")

	select {
	case got := <-h.getHeaders:
		require.NotEmpty(t, got.BlockLocatorHashes, "getheaders must carry a locator")
		require.True(t, got.BlockLocatorHashes[0].IsEqual(h.committedHash),
			"reset-path getheaders must anchor at the committed tip %s, got %s",
			h.committedHash, got.BlockLocatorHashes[0])
	case <-time.After(3 * time.Second):
		t.Fatal("no getheaders was issued after the reset rotation")
	}
}
