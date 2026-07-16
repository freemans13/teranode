package netsync

// Contiguity-gate regression tests (the stall-burst sync defect).
//
// Production evidence (mainnet, 2026-07-16 ~16:35Z, tip 609470): peer churn
// lost heights 609471-609480 before admission. Every window flushed afterwards
// started ABOVE the hole, so ProcessBlockWindow failed "previous block not
// found" / "output not found", burned three serial bounded-recovery passes,
// escalated to a sync-peer disconnect, and only the resulting rotation
// re-fetched the hole — a 4-5 minute stall, then a burst, then the next stall.
//
// The park path already enforces the needed invariant (releaseParkedBlocks
// only feeds the committer a contiguous ascending run); the direct window path
// did not. gateContiguousWindow closes that: at flush time the drained job is
// split — the contiguous ascending run continuing the last handed height is
// committed, and any post-gap strays are routed into the park, where the
// existing release machinery feeds them back in order once the hole fills.

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newGateTestManager() *SyncManager {
	return &SyncManager{
		logger:            ulogger.TestLogger{},
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
	}
}

func ownedGateBlocks(t *testing.T, sm *SyncManager, heights ...uint32) []*model.Block {
	t.Helper()

	blocks := make([]*model.Block, 0, len(heights))

	for _, h := range heights {
		b := newOwnedTestBlock(t, h)
		sm.claimWindowBlock(*b.Hash(), h)
		blocks = append(blocks, b)
	}

	return blocks
}

// TestGateContiguousWindow_ContiguousPassesThrough: a healthy streaming window
// that continues the last handed height is handed unchanged, and the tracker
// advances to its last height.
func TestGateContiguousWindow_ContiguousPassesThrough(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 500
	park := newParkStore(0, 1024)

	blocks := ownedGateBlocks(t, sm, 501, 502, 503)
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Len(t, out.blocks, 3, "a contiguous continuation must be handed whole")
	require.Equal(t, uint32(503), sm.lastHandedWindowEnd, "tracker advances to the handed run's end")
	require.Equal(t, 0, park.len(), "nothing parked for a contiguous window")
}

// TestGateContiguousWindow_InternalGapSplitsJob: the run before an internal
// hole is handed; everything after the hole is parked (ownership retained, so
// admission keeps skipping re-deliveries while the strays wait in the park).
func TestGateContiguousWindow_InternalGapSplitsJob(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 500
	park := newParkStore(0, 1024)

	// 501..503 contiguous, hole at 504, strays 505..506.
	blocks := ownedGateBlocks(t, sm, 501, 502, 503, 505, 506)
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Len(t, out.blocks, 3, "only the pre-hole run may be handed")
	require.Equal(t, uint32(503), out.blocks[len(out.blocks)-1].Height)
	require.Equal(t, uint32(503), sm.lastHandedWindowEnd)
	require.Equal(t, 2, park.len(), "post-hole strays must be parked, not committed")

	for _, b := range blocks[3:] {
		require.True(t, sm.windowBlockOwned(*b.Hash()),
			"a parked stray stays owned so re-deliveries keep being skipped while it waits")
	}
}

// TestGateContiguousWindow_BeyondGapParksEverything is the mainnet stall case:
// the whole job sits above a hole (first height > lastHanded+1). Nothing may
// be handed — committing would fail on a missing parent and burn the slow
// recovery/rotation cycle. All blocks are parked for ordered release.
func TestGateContiguousWindow_BeyondGapParksEverything(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 609470 // committed frontier
	park := newParkStore(0, 1024)

	// The gap 609471..609480 was lost; this window starts at 609481.
	blocks := ownedGateBlocks(t, sm, 609481, 609482, 609483)
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Empty(t, out.blocks, "a wholly-beyond-gap window must not reach the committer")
	require.Equal(t, 3, park.len(), "beyond-gap blocks are parked for ordered release")
	require.Equal(t, uint32(609470), sm.lastHandedWindowEnd, "tracker unchanged when nothing is handed")
}

// TestGateContiguousWindow_ReseedAtOrBelowLastHanded: after a fatal commit the
// pipeline rotates and re-syncs from the committed best-block, so a re-delivered
// window legitimately starts at or below the last handed height. The gate must
// accept it (idempotent re-commit) and re-seed the tracker, never wedge on a
// stale high-water mark.
func TestGateContiguousWindow_ReseedAtOrBelowLastHanded(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 800 // stale: the fatal window 795.. never committed
	park := newParkStore(0, 1024)

	blocks := ownedGateBlocks(t, sm, 795, 796, 797)
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Len(t, out.blocks, 3, "a re-sync window at/below the tracker must be handed (idempotent)")
	require.Equal(t, uint32(797), sm.lastHandedWindowEnd, "tracker re-seeds to the re-synced run")
	require.Equal(t, 0, park.len())
}

// TestGateContiguousWindow_FirstFlushSeeds: with no prior hand-off (tracker 0)
// the first job seeds the tracker and is handed whole.
func TestGateContiguousWindow_FirstFlushSeeds(t *testing.T) {
	sm := newGateTestManager()
	park := newParkStore(0, 1024)

	blocks := ownedGateBlocks(t, sm, 900, 901)
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Len(t, out.blocks, 2)
	require.Equal(t, uint32(901), sm.lastHandedWindowEnd)
}

// TestGateContiguousWindow_NilParkPassesThrough: with parking disabled there is
// nowhere to hold strays, so the gate is a byte-identical pass-through (the
// pre-gate behaviour, including its recovery semantics).
func TestGateContiguousWindow_NilParkPassesThrough(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 500

	blocks := ownedGateBlocks(t, sm, 505, 506) // beyond-gap, but no park
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, nil)

	require.Len(t, out.blocks, 2, "no park -> unsplit pass-through (old behaviour)")
}

// TestGateContiguousWindow_StrayOverflowDropsAndReleases: a stray that does not
// fit the park's caps is dropped WITH its ownership released, so the existing
// refetch machinery can re-buy it — a full park must never wedge the gap-fill.
func TestGateContiguousWindow_StrayOverflowDropsAndReleases(t *testing.T) {
	sm := newGateTestManager()
	sm.lastHandedWindowEnd = 500
	park := newParkStore(50, 0) // tiny byte budget: newOwnedTestBlock is 100 bytes

	blocks := ownedGateBlocks(t, sm, 502, 503) // beyond-gap strays
	out := sm.gateContiguousWindow(windowFlushJob{blocks: blocks}, park)

	require.Empty(t, out.blocks)
	require.Equal(t, 0, park.len(), "oversized strays are dropped, not parked")

	for _, b := range blocks {
		require.False(t, sm.windowBlockOwned(*b.Hash()),
			"a dropped stray must have ownership released so it can be re-fetched")
	}
}
