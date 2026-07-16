// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the park-ahead runway cap and the ascending re-fetch drain, the two
// fixes for the multi-peer park-ahead livelock: under parallelFetchPeers>1 the
// monotonic fetch cursor climbed unboundedly ahead of a lagging block assembly,
// saturating the park; every overflow reject was requeued into a random-ordered
// re-fetch set, so the one contiguous tip+1 block that advances block assembly was
// starved and the tip froze. fetchRunwayHorizon bounds the frontier to the parkable
// horizon; drainRefetchBlocks now re-requests lowest-height-first.
import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestFetchRunwayHorizon verifies the gating of the fetch runway cap. It must be
// active only when parking is live, maxBehind/parkCap are positive, the BA cache is
// polled (cached>0), and we are below the checkpoint — and it must return exactly
// cachedBA + maxBehind + parkCap.
func TestFetchRunwayHorizon(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	newSM := func() *SyncManager {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20
		tSettings.Legacy.ParallelWindowMaxParkedBlocks = 1024

		return &SyncManager{
			logger:      ulogger.TestLogger{},
			settings:    tSettings,
			chainParams: &chainParams,
		}
	}

	t.Run("inactive when park-ahead off", func(t *testing.T) {
		sm := newSM()
		sm.cachedBlockAssemblyHeight.Store(500000)
		sm.baHeightPolled.Store(true)
		// parkAheadActive left false.
		_, capped := sm.fetchRunwayHorizon()
		require.False(t, capped, "cap must be inactive when park-ahead is not live")
	})

	t.Run("active below checkpoint returns cached+maxBehind+parkCap", func(t *testing.T) {
		sm := newSM()
		sm.parkAheadActive.Store(true)
		sm.cachedBlockAssemblyHeight.Store(500000)
		sm.baHeightPolled.Store(true)

		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped, "cap must be active below the checkpoint with a polled cache")
		require.Equal(t, uint32(500000+20+1024), horizon)
	})

	t.Run("inactive when cache unpolled", func(t *testing.T) {
		sm := newSM()
		sm.parkAheadActive.Store(true)
		// cachedBlockAssemblyHeight stays 0.
		_, capped := sm.fetchRunwayHorizon()
		require.False(t, capped, "must not cap against an unpolled (zero) BA height")
	})

	t.Run("inactive above the highest checkpoint", func(t *testing.T) {
		sm := newSM()
		sm.parkAheadActive.Store(true)
		sm.cachedBlockAssemblyHeight.Store(1_000_000_000) // above every mainnet checkpoint
		sm.baHeightPolled.Store(true)

		_, capped := sm.fetchRunwayHorizon()
		require.False(t, capped, "above the checkpoint parking is off, so no cap")
	})

	t.Run("inactive when maxBehind non-positive", func(t *testing.T) {
		sm := newSM()
		sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly = 0
		sm.parkAheadActive.Store(true)
		sm.cachedBlockAssemblyHeight.Store(500000)
		sm.baHeightPolled.Store(true)

		_, capped := sm.fetchRunwayHorizon()
		require.False(t, capped, "a non-positive maturity window disables the cap")
	})
}

// TestDrainRefetchBlocks_AscendingHeightFirst locks in the ascending drain: given a
// tight per-pass budget and a re-fetch set holding orphans at mixed heights, the
// LOWEST-height orphan (the commit-blocking one) must win the budget, not a random
// map-range pick.
func TestDrainRefetchBlocks_AscendingHeightFirst(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          test.CreateBaseTestSettings(t),
		chainParams:       &chainParams,
		blockchainClient:  blockchainClient,
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
	}
	defer sm.requestedBlocks.Stop()

	// Three orphans at mixed heights; the lowest (hLow, height 100) must go first.
	hHigh := chainhash.Hash{0xaa}
	hLow := chainhash.Hash{0xbb}
	hMid := chainhash.Hash{0xcc}
	sm.refetchBlocks[hHigh] = struct{}{}
	sm.refetchBlocks[hLow] = struct{}{}
	sm.refetchBlocks[hMid] = struct{}{}
	sm.headerHeightIndex[hHigh] = 900
	sm.headerHeightIndex[hLow] = 100
	sm.headerHeightIndex[hMid] = 500

	tgt := &fetchTarget{peer: &peer.Peer{}, spare: 8}

	// Budget of exactly 1 slot: only the single lowest-height orphan may be picked.
	remaining := sm.drainRefetchBlocks([]*fetchTarget{tgt}, 1)

	require.Equal(t, 0, remaining, "the one budget slot must be consumed")
	require.Len(t, tgt.pending, 1, "exactly one orphan assigned under a 1-slot budget")
	require.Equal(t, hLow, *tgt.pending[0], "the LOWEST-height orphan must win the budget")

	// With a 2-slot budget the two lowest (hLow=100 then hMid=500) go, never hHigh.
	tgt2 := &fetchTarget{peer: &peer.Peer{}, spare: 8}
	sm.drainRefetchBlocks([]*fetchTarget{tgt2}, 2)
	require.Len(t, tgt2.pending, 2)
	got := []chainhash.Hash{*tgt2.pending[0], *tgt2.pending[1]}
	require.Equal(t, hLow, got[0], "lowest first")
	require.Equal(t, hMid, got[1], "second-lowest next")
	require.NotContains(t, got, hHigh, "the highest orphan must not be picked before the lower two")
}
