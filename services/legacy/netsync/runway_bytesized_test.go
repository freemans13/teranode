// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the byte-sized fetch runway (legacy_parkRunwayByteSized). The prior
// fetchRunwayHorizon clamp is BINARY: park full → span collapses to the maturity
// gate, otherwise span springs to maxBehind + the full static parkCap. Under fat
// blocks that flaps, flooding the fetch budget with un-parkable far-ahead blocks
// that get park-rejected and re-requested — churning the frontier re-fetch. When
// the flag is on the span instead retreats SMOOTHLY, sized by the park's REMAINING
// byte room: span = maxBehind + clamp(remainingBytes/avgSize, 1, parkCap). The
// frontier stays requestable (floor 1 keeps the horizon strictly above the gate)
// and no un-parkable far-ahead block is pulled.

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestFetchRunwayHorizon_ByteSized(t *testing.T) {
	chainParams := chaincfg.MainNetParams
	const (
		cached    = uint32(500000)
		maxBehind = 20
		parkCap   = 1024
	)

	newSM := func(byteSized bool) *SyncManager {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = maxBehind
		tSettings.Legacy.ParallelWindowMaxParkedBlocks = parkCap
		tSettings.Legacy.ParkRunwayByteSized = byteSized

		sm := &SyncManager{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			chainParams:      &chainParams,
			blockSizeTracker: newBlockSizeTracker(10),
		}
		sm.parkAheadActive.Store(true)
		sm.cachedBlockAssemblyHeight.Store(cached)
		sm.baHeightPolled.Store(true)

		return sm
	}

	// seedAvg drives blockSizeTracker.getAverageSize() to avg via one sample.
	seedAvg := func(sm *SyncManager, avg int64) { sm.blockSizeTracker.addBlockStats(avg, 1) }

	// parkWith builds a parkStore with the given byte budget and accumulated bytes.
	parkWith := func(budget, accum int64) *parkStore {
		ps := newParkStore(budget, parkCap)
		ps.bytesAccum = accum
		return ps
	}

	t.Run("flag OFF keeps the binary full static span", func(t *testing.T) {
		sm := newSM(false)
		seedAvg(sm, 100)
		sm.parkRef.Store(parkWith(1000, 600)) // not at capacity
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+parkCap, horizon,
			"flag off must be byte-identical: full static runway")
	})

	t.Run("flag ON sizes the runway by remaining park room", func(t *testing.T) {
		sm := newSM(true)
		seedAvg(sm, 100)
		sm.parkRef.Store(parkWith(1000, 600)) // remaining 400, avg 100 → N=4
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+4, horizon,
			"runway = maturity gate + remainingBytes/avgSize")
	})

	t.Run("flag ON retreats to gate+1 when the park is full (never below)", func(t *testing.T) {
		sm := newSM(true)
		seedAvg(sm, 100)
		sm.parkRef.Store(parkWith(1000, 1000)) // remaining 0 → N clamped to floor 1
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+1, horizon,
			"a full park retreats to gate+1 so the frontier stays requestable")
	})

	t.Run("flag ON caps at parkCap when the park is near-empty", func(t *testing.T) {
		sm := newSM(true)
		seedAvg(sm, 100)
		sm.parkRef.Store(parkWith(1_000_000, 0)) // remaining/avg = 10000 > parkCap
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+parkCap, horizon,
			"N never exceeds ParallelWindowMaxParkedBlocks")
	})

	t.Run("flag ON falls back to full static cap when byte budget disabled", func(t *testing.T) {
		sm := newSM(true)
		seedAvg(sm, 100)
		sm.parkRef.Store(parkWith(0, 0)) // ParallelWindowParkedMemoryFraction=0 → count-only
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+parkCap, horizon,
			"budget<=0 (count-only mode) must use the full static cap, not collapse to 1")
	})

	t.Run("flag ON falls back to full static cap on cold start (no size samples)", func(t *testing.T) {
		sm := newSM(true)
		// no seedAvg → avgSize 0
		sm.parkRef.Store(parkWith(1000, 600))
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+parkCap, horizon,
			"avgSize 0 (cold start) must use the full static cap, byte-identical to today")
	})

	t.Run("flag ON with no park uses the full static span", func(t *testing.T) {
		sm := newSM(true)
		seedAvg(sm, 100)
		// parkRef left nil
		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, cached+maxBehind+parkCap, horizon,
			"nil park (park not yet created) keeps the full static runway")
	})
}
