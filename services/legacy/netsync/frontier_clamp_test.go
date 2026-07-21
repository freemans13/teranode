// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the FRONTIER-FIRST CLAMP (legacy_frontierLeadClamp): the fix for the
// residual measured live 2026-07-21 on top of svnode-style parallel block fetch.
//
// THE RESIDUAL. fetchRunwayHorizon sizes the forward fetch runway from the CACHED
// block-assembly height (cached + maxBehind + parkCap), NOT from the committed
// contiguous frontier. So while the next-needed block (committedFrontier+1) is
// stuck in flight, the forward walk keeps buying SUCCESSOR blocks far past the gap.
// Those successors arrive, cannot commit (the in-order rule), pile into the park as
// strays, and when the park caps they are DROPPED ("park full; dropping stray") and
// re-fetched — ~150-204 wasted multi-MB fetches/hr that STEAL peer bandwidth from
// the raced frontier block, so committed freezes ~240-360s then bursts forward.
//
// THE CLAMP. While a gap is genuinely OPEN — the park holds a stranded stray within
// the maturity gate but the frontier block itself is not yet parked — cap the
// horizon at committedFrontier + lead, where lead = max(frontierLeadClamp,
// calculateMaxInFlightBlocks()). That frees peer bandwidth for the frontier block
// (raced by checkHeadStall/addRacerForHead) so the gap closes faster and the park
// stops churning. It is a MIN against the existing span (only ever narrows), engages
// ONLY on an open gap, always keeps the gap block itself fetchable (no wedge), and
// floors the lead at the dynamic in-flight depth so it can never starve prefetch
// (no feed-idle regression). frontierLeadClamp==0 short-circuits the whole block.
//
// These tests drive fetchRunwayHorizon directly (the same seam the sibling
// window_parkaware_horizon_test.go / runway_horizon_test.go tests use), because the
// clamp lives entirely in the runway-sizing math that assignBlocksAcrossPeers and
// drainRefetchBlocks both consult (node.height > horizon → not fetched).
import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// clampCached is a below-checkpoint (< mainnet 600000) block-assembly height used as
// the horizon base across the clamp tests, matching the sibling horizon tests.
const clampCached uint32 = 585737

// newClampManager builds the minimal SyncManager fetchRunwayHorizon needs to reach
// the clamp branch: park-ahead live, cache polled, below checkpoint, a real
// blockSizeTracker (its calculateMaxInFlightBlocks() is the lead floor — fresh, with
// no samples, it returns noSampleInFlightDefault == 20). Callers set maxBehind,
// parkCap, frontierLeadClamp, lastHandedWindowEnd and the park before calling.
func newClampManager(t *testing.T) *SyncManager {
	t.Helper()

	chainParams := chaincfg.MainNetParams
	tSettings := test.CreateBaseTestSettings(t)
	// Deterministic span: maxBehind + parkCap, with ParkRunwayByteSized off (default)
	// so span never depends on average block size.
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20
	tSettings.Legacy.ParallelWindowMaxParkedBlocks = 1024

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockSizeTracker: newBlockSizeTracker(100),
	}
	sm.parkAheadActive.Store(true)
	sm.cachedBlockAssemblyHeight.Store(clampCached)
	sm.baHeightPolled.Store(true)

	return sm
}

// parkWithMinHeights builds a parkStore whose minHeight() is the lowest of the given
// heights, with REAL blocks (minHeight scans entries[i].block.Height, so the entries
// must carry non-nil blocks — unlike the count-only fullPark() helper). The park is
// given ample count and byte room so atCapacity() is false and the runway span stays
// the full static maxBehind+parkCap (isolating the clamp as the sole narrower).
func parkWithMinHeights(heights ...uint32) *parkStore {
	ps := &parkStore{
		entries:   make([]windowEntry, 0, len(heights)),
		budget:    1 << 30, // large: never byte-full
		maxBlocks: 1 << 20, // large: never count-full
	}
	for _, h := range heights {
		b := &model.Block{Height: h, SizeInBytes: 1000}
		ps.entries = append(ps.entries, windowEntry{block: b})
		ps.bytesAccum += int64(b.SizeInBytes)
	}

	return ps
}

// TestFrontierClamp_BoundsSuccessorOverfetch is the core RED/GREEN: with the clamp
// enabled and a gap OPEN, the runway horizon is bounded at committedFrontier+lead —
// the scheduler will NOT walk successors beyond it — well below the un-clamped
// cached+maxBehind+parkCap. Neutralising the clamp (frontierLeadClamp=0) re-opens the
// horizon to the full span, which is what the assertion pins.
func TestFrontierClamp_BoundsSuccessorOverfetch(t *testing.T) {
	const (
		maxBehind = uint32(20)
		parkCap   = uint32(1024)
		lead      = uint32(50) // > the fresh-tracker floor (20), so the SETTING is the binding lead
	)
	span := maxBehind + parkCap
	unclamped := clampCached + span // 586781: the horizon with no clamp

	sm := newClampManager(t)
	sm.settings.Legacy.FrontierLeadClamp = int(lead)
	// lastHandedWindowEnd == 0 → committedFrontier == cached == 585737.
	// Gap OPEN: frontier block 585738 is MISSING, but a stray sits parked at 585750,
	// inside the maturity gate (585737+20 = 585757). releaseParkedBlocks cannot free
	// it because the block below it never arrived.
	sm.parkRef.Store(parkWithMinHeights(585750))

	horizon, capped := sm.fetchRunwayHorizon()
	require.True(t, capped, "clamp path still returns a capped horizon")

	// The horizon is exactly committedFrontier + lead — the over-fetch is bounded.
	require.Equal(t, clampCached+lead, horizon,
		"open gap must clamp the runway to committedFrontier+lead, not the full parkCap runway")

	// And it is strictly tighter than the un-clamped span: this is the assertion that
	// goes RED the instant the clamp is neutralised (it would return unclamped instead).
	require.Less(t, horizon, unclamped,
		"the clamped horizon must be strictly below cached+maxBehind+parkCap (RED if the clamp is removed)")
}

// TestFrontierClamp_FloorKeepsMinimumLead guards the "do NOT starve prefetch" rule:
// even when frontierLeadClamp is set tiny (1), the effective lead is FLOORED at the
// dynamic in-flight depth (calculateMaxInFlightBlocks() == 20 on a fresh tracker), so
// the horizon is committedFrontier+20, never committedFrontier+1. A clamp that
// collapsed to the raw setting would re-introduce feed-idle stalls.
func TestFrontierClamp_FloorKeepsMinimumLead(t *testing.T) {
	sm := newClampManager(t)
	sm.settings.Legacy.FrontierLeadClamp = 1 // deliberately below the floor
	sm.parkRef.Store(parkWithMinHeights(585750))

	floor := uint32(sm.blockSizeTracker.calculateMaxInFlightBlocks()) //nolint:gosec // fresh tracker == 20
	require.Equal(t, uint32(20), floor, "a fresh tracker floors the lead at noSampleInFlightDefault")

	horizon, capped := sm.fetchRunwayHorizon()
	require.True(t, capped)

	require.Equal(t, clampCached+floor, horizon,
		"the lead must be floored at the dynamic in-flight depth, not the tiny setting")
	require.Greater(t, horizon, clampCached+1,
		"a floored lead keeps a sensible prefetch depth (never collapses to committedFrontier+1)")
}

// TestFrontierClamp_GapBlockAlwaysFetchable is the no-wedge guarantee: the gap block
// (committedFrontier+1) must ALWAYS stay at or below the horizon, or drainRefetchBlocks
// would skip it (height > horizon) and commit would deadlock. This exercises the
// load-bearing base choice max(cached, lastHandedWindowEnd): lastHandedWindowEnd runs
// ahead of cached, so the TRUE gap block is lastHandedWindowEnd+1. Were the base raw
// cached, cached+lead would fall BELOW the gap block here and wedge the sync.
func TestFrontierClamp_GapBlockAlwaysFetchable(t *testing.T) {
	sm := newClampManager(t)
	// Widen the gate so lastHandedWindowEnd can sit 30 ahead of cached and still leave
	// an in-gate stray above it.
	sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly = 40
	sm.settings.Legacy.FrontierLeadClamp = 1 // floored to 20 — the tightest realistic lead

	// committedFrontier = max(cached, lastHandedWindowEnd) = cached+30 = 585767.
	sm.lastHandedWindowEnd = clampCached + 30
	committedFrontier := clampCached + 30
	gapBlock := committedFrontier + 1 // 585768 — the block that MUST stay fetchable

	// Gap OPEN: gapBlock missing, a stray parked at cached+35 (inside the widened gate
	// cached+40 = 585777).
	sm.parkRef.Store(parkWithMinHeights(clampCached + 35))

	horizon, capped := sm.fetchRunwayHorizon()
	require.True(t, capped)

	// The gap block is at or below the horizon → drainRefetchBlocks/assignBlocksAcrossPeers
	// can still fetch it. With a raw-cached base (585737+20 = 585757) it would be ABOVE
	// the horizon and this assertion would fail — the wedge the max() base prevents.
	require.GreaterOrEqual(t, horizon, gapBlock,
		"the gap block (committedFrontier+1) must remain fetchable under the clamp — no sync wedge")

	// Exactly committedFrontier + floored lead (20).
	require.Equal(t, committedFrontier+20, horizon,
		"horizon is committedFrontier+lead, keyed on the committed frontier not raw cached")
}

// TestFrontierClamp_NoGapNoReduction pins the "engage ONLY on an open gap" rule: at
// healthy contiguous progress (no stranded stray) and during legitimate deep
// beyond-gate park-ahead, the clamp must NOT narrow the horizon — the full in-flight
// depth is preserved, so there is no feed-idle regression.
func TestFrontierClamp_NoGapNoReduction(t *testing.T) {
	const (
		maxBehind = uint32(20)
		parkCap   = uint32(1024)
	)
	full := clampCached + maxBehind + parkCap // 586781: the un-narrowed horizon

	t.Run("empty park (contiguous progress) keeps the full runway", func(t *testing.T) {
		sm := newClampManager(t)
		sm.settings.Legacy.FrontierLeadClamp = 50
		// Park empty → minHeight() reports no stray → no gap → clamp is skipped.
		sm.parkRef.Store(parkWithMinHeights())

		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, full, horizon,
			"a healthy contiguous run (empty park) must keep the full prefetch depth")
	})

	t.Run("deep beyond-gate park-ahead keeps the full runway", func(t *testing.T) {
		sm := newClampManager(t)
		sm.settings.Legacy.FrontierLeadClamp = 50
		// parkMin far ABOVE the maturity gate (cached+maxBehind = 585757): this is
		// legitimate park-ahead, not a stranded contiguity stray, so gapOpen is false.
		sm.parkRef.Store(parkWithMinHeights(clampCached + 500))

		horizon, capped := sm.fetchRunwayHorizon()
		require.True(t, capped)
		require.Equal(t, full, horizon,
			"legitimate deep park-ahead (parkMin beyond the gate) must not be throttled")
	})
}

// TestFrontierClamp_ZeroDisablesByteIdentical pins the rollback lever: with
// frontierLeadClamp==0 the whole clamp block short-circuits, so even the exact
// open-gap scenario that clamps at 50 returns the full un-narrowed span — byte-for-byte
// the pre-feature horizon (identical to the same manager with no park at all).
func TestFrontierClamp_ZeroDisablesByteIdentical(t *testing.T) {
	const (
		maxBehind = uint32(20)
		parkCap   = uint32(1024)
	)
	full := clampCached + maxBehind + parkCap // 586781

	// The SAME open-gap park that TestFrontierClamp_BoundsSuccessorOverfetch clamps.
	smClampOff := newClampManager(t)
	smClampOff.settings.Legacy.FrontierLeadClamp = 0
	smClampOff.parkRef.Store(parkWithMinHeights(585750))

	horizonOff, cappedOff := smClampOff.fetchRunwayHorizon()
	require.True(t, cappedOff)
	require.Equal(t, full, horizonOff,
		"frontierLeadClamp==0 must leave the horizon at the full pre-feature span")

	// Reference: the identical manager with NO park published takes the same
	// three-branch span path and returns the same horizon — proving clamp==0 is a no-op.
	smNoPark := newClampManager(t)
	smNoPark.settings.Legacy.FrontierLeadClamp = 0
	horizonRef, cappedRef := smNoPark.fetchRunwayHorizon()
	require.True(t, cappedRef)
	require.Equal(t, horizonRef, horizonOff,
		"clamp==0 with an open-gap park is byte-identical to the no-park path (true rollback)")
}
