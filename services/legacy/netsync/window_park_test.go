package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func mkBlock(height uint32, size uint64) *model.Block {
	return &model.Block{Height: height, SizeInBytes: size}
}

// TestBlockAssemblyGateAdmitsCached proves the non-blocking predicate: the
// far-ahead case that used to block for 30s now returns (admit=false,
// evaluable=true) INSTANTLY with zero gRPC.
func TestBlockAssemblyGateAdmitsCached(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20) // maxBehind = 20

	sm.cachedBlockAssemblyHeight.Store(1000)

	// Within the gate: 1000 + 20 = 1020 >= 1015.
	admit, evaluable := sm.blockAssemblyGateAdmitsCached(1015)
	require.True(t, evaluable)
	require.True(t, admit)

	// Beyond the gate (the old 30s-block case): 1020 < 1200 -> park, not block.
	admit, evaluable = sm.blockAssemblyGateAdmitsCached(1200)
	require.True(t, evaluable)
	require.False(t, admit)

	// Unpolled cache -> not evaluable (caller falls back to blocking wait).
	sm.cachedBlockAssemblyHeight.Store(0)
	_, evaluable = sm.blockAssemblyGateAdmitsCached(1200)
	require.False(t, evaluable)

	// Above the checkpoint -> never evaluable (fresh-gRPC slow path).
	sm.cachedBlockAssemblyHeight.Store(1000)
	_, evaluable = sm.blockAssemblyGateAdmitsCached(cacheTestCheckpointHeight + 500)
	require.False(t, evaluable)

	// No gRPC anywhere in this test.
	require.Equal(t, int64(0), spy.calls.Load())
}

// TestParkStore_Caps proves count + byte caps and byte accounting.
func TestParkStore_Caps(t *testing.T) {
	ps := newParkStore(0, 2) // count cap 2, byte cap disabled
	require.False(t, ps.countFull())
	ps.add(mkBlock(10, 100))
	ps.add(mkBlock(11, 100))
	require.True(t, ps.countFull())
	require.Equal(t, int64(200), ps.bytesAccum)

	psb := newParkStore(150, 0) // byte cap 150, count cap disabled
	require.False(t, psb.full(100))
	psb.add(mkBlock(10, 100))
	require.True(t, psb.full(100)) // 100 + 100 > 150
	require.False(t, psb.full(40)) // 100 + 40 <= 150
}

// TestReleaseParkedBlocks_ContiguousWithinGate is the core safety+liveness proof.
// It asserts: (a) only the contiguous run from cached+1 within the ceiling is
// released, (b) release is ascending, (c) a gap above the run stays parked (no
// release above a hole), (d) blocks beyond the ceiling stay parked (coinbase
// ceiling preserved), and (e) advancing block assembly releases the next run.
func TestReleaseParkedBlocks_ContiguousWithinGate(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20)   // maxBehind = 20
	sm.cachedBlockAssemblyHeight.Store(100) // committed tip 100, ceiling 120

	wa := newWindowAccumulator(1<<30, 0) // huge budget, no count cap: never auto-flushes
	park := newParkStore(0, 1024)

	// Parked (inserted out of order to prove the sort): a contiguous run 101..108,
	// then a GAP (109 missing), then 110, then 200 (beyond ceiling 120).
	for _, h := range []uint32{104, 101, 200, 103, 108, 102, 110, 107, 105, 106} {
		park.add(mkBlock(h, 10))
	}

	var flushes, arms int
	flushWindow := func() { flushes++ }
	armTimer := func() { arms++ }

	sm.releaseParkedBlocks(park, wa, flushWindow, armTimer)

	// Released the contiguous run 101..108 only. 110 (gap at 109) and 200 (beyond
	// ceiling) stay parked.
	require.Len(t, wa.entries, 8)
	for i, e := range wa.entries {
		require.Equal(t, uint32(101+i), e.block.Height, "released blocks must be ascending and contiguous")
	}
	require.Equal(t, 2, park.len(), "110 (post-gap) and 200 (beyond ceiling) stay parked")
	require.Equal(t, int64(20), park.bytesAccum, "byte accounting tracks survivors")
	require.GreaterOrEqual(t, arms, 1, "timer armed after admitting blocks")

	// Block assembly advances past the hole: commit 108, then 109 arrives+commits
	// so cached rises to 110, ceiling to 130. Now 110 is contiguous (cached+1) and
	// 200 is still beyond the ceiling.
	sm.cachedBlockAssemblyHeight.Store(109) // 110 == cached+1
	sm.releaseParkedBlocks(park, wa, flushWindow, armTimer)
	require.Len(t, wa.entries, 9, "110 released once contiguous")
	require.Equal(t, uint32(110), wa.entries[8].block.Height)
	require.Equal(t, 1, park.len(), "200 still beyond ceiling")
}

// TestReleaseParkedBlocks_NoContiguousHead proves nothing releases when the lowest
// parked block is not cached+1 (the anti-hole guard): parked 105.. while cached=100
// leaves a gap 101..104, so NOTHING is admitted even though 105 <= ceiling.
func TestReleaseParkedBlocks_NoContiguousHead(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20)
	sm.cachedBlockAssemblyHeight.Store(100) // ceiling 120

	wa := newWindowAccumulator(1<<30, 0)
	park := newParkStore(0, 1024)
	for _, h := range []uint32{105, 106, 107} { // 101..104 missing (in flight)
		park.add(mkBlock(h, 10))
	}

	sm.releaseParkedBlocks(park, wa, func() {}, func() {})

	require.Len(t, wa.entries, 0, "must not release above a hole")
	require.Equal(t, 3, park.len(), "all stay parked until the gap fills and cached rises")
}

// TestReleaseParkedBlocks_FlushOnFull proves a full window flushes mid-release and
// that a single pass admits at most maxBehind blocks (no drain monopolisation).
func TestReleaseParkedBlocks_FlushOnFull(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20)
	sm.cachedBlockAssemblyHeight.Store(100) // ceiling 120

	wa := newWindowAccumulator(1<<30, 4) // count cap 4 -> fills fast
	park := newParkStore(0, 1024)
	for h := uint32(101); h <= 140; h++ { // more than a ceiling's worth parked
		park.add(mkBlock(h, 10))
	}

	var flushes int
	sm.releaseParkedBlocks(park, wa, func() { flushes++ }, func() {})

	// Ceiling caps the pass at 20 blocks (101..120); 121..140 stay parked.
	require.Equal(t, 20, park.len(), "pass bounded by the ceiling, not the whole parked set")
	require.GreaterOrEqual(t, flushes, 1, "full window flushed during release")
}
