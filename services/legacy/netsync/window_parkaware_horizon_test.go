// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the PARK-AWARE runway cap: the fix for the 8-peer park-ahead livelock
// where the STATIC runway (cached+maxBehind+parkCap) kept both the forward walk and
// the refetch drain pulling far-ahead blocks even when the park was count-full — so
// those blocks were park-rejected, requeued, and re-fetched forever, starving the
// in-gate tip+1 that would advance block assembly. When the park is count-full the
// horizon now clamps to the maturity gate, and drainRefetchBlocks skips (not drops)
// beyond-gate orphans, preserving the pass budget for the in-gate block.
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

// fullPark returns a parkStore already at its count cap (countFull()==true).
func fullPark() *parkStore {
	return &parkStore{entries: make([]windowEntry, 1), budget: 1 << 30, maxBlocks: 1}
}

// TestFetchRunwayHorizon_ParkFullClampsToGate: a count-full park clamps the runway
// to the maturity gate; a non-full park keeps the full static parkCap runway.
func TestFetchRunwayHorizon_ParkFullClampsToGate(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	newSM := func() *SyncManager {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20
		tSettings.Legacy.ParallelWindowMaxParkedBlocks = 1024

		sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings, chainParams: &chainParams}
		sm.parkAheadActive.Store(true)
		sm.cachedBlockAssemblyHeight.Store(585737) // below checkpoint 600000
		return sm
	}

	t.Run("park not full → full static cap", func(t *testing.T) {
		sm := newSM()
		sm.parkRef.Store(newParkStore(1<<30, 1024)) // empty, not full
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, uint32(585737+20+1024), horizon, "non-full park keeps the whole prefetch runway")
	})

	t.Run("park count-full → clamp to maturity gate", func(t *testing.T) {
		sm := newSM()
		sm.parkRef.Store(fullPark())
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, uint32(585737+20), horizon, "count-full park clamps the frontier to the gate (no parkCap runway)")
	})

	t.Run("no parkRef published → full static cap (defensive)", func(t *testing.T) {
		sm := newSM() // parkRef never Stored
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, uint32(585737+20+1024), horizon)
	})
}

// TestDrainRefetchBlocks_ParkFullSkipsBeyondGateOrphan: with the park count-full
// (horizon clamped to the gate), the drain must SKIP the far-ahead orphan (leaving it
// queued) and still fetch the in-gate orphan — the budget must not be burned on the
// un-parkable block. This is the RED/GREEN that pins the un-starving of tip+1.
func TestDrainRefetchBlocks_ParkFullSkipsBeyondGateOrphan(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20
	tSettings.Legacy.ParallelWindowMaxParkedBlocks = 1024

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		blockchainClient:  blockchainClient,
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
	}
	defer sm.requestedBlocks.Stop()

	// Park-ahead live, below checkpoint, cache polled, park COUNT-FULL → horizon = gate = 585757.
	sm.parkAheadActive.Store(true)
	sm.cachedBlockAssemblyHeight.Store(585737)
	sm.parkRef.Store(fullPark())

	inGate := chainhash.Hash{0x01}   // 585738 <= gate 585757 → must be fetched
	farAhead := chainhash.Hash{0x02} // 585894 > gate → must be skipped while park full
	sm.refetchBlocks[inGate] = struct{}{}
	sm.refetchBlocks[farAhead] = struct{}{}
	sm.headerHeightIndex[inGate] = 585738
	sm.headerHeightIndex[farAhead] = 585894

	tgt := &fetchTarget{peer: &peer.Peer{}, spare: 8}

	remaining := sm.drainRefetchBlocks([]*fetchTarget{tgt}, 8)

	assigned := map[chainhash.Hash]bool{}
	for _, h := range tgt.pending {
		assigned[*h] = true
	}
	require.True(t, assigned[inGate], "in-gate orphan (585738 <= gate) must be re-fetched")
	require.False(t, assigned[farAhead], "beyond-gate orphan (585894 > gate) must be SKIPPED while park is full")
	require.Less(t, remaining, 8, "the in-gate orphan consumed budget")

	// The skipped far-ahead orphan stays queued (no strand) for a later pass.
	sm.assignedMu.Lock()
	_, stillQueued := sm.refetchBlocks[farAhead]
	sm.assignedMu.Unlock()
	require.True(t, stillQueued, "skipped beyond-gate orphan must remain in refetchBlocks (re-sent when the park drains)")
}
