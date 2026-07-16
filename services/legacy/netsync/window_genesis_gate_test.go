package netsync

// Regression tests for the genesis sentinel collision (mainnet from-scratch
// resync, 2026-07-16 21:15Z: node wedged at height 100 exactly).
//
// cachedBlockAssemblyHeight == 0 was used to mean "poller hasn't reported
// yet" — but on a fresh node block assembly's height genuinely IS 0, so
// after a reset the maturity gate was never evaluable, parking never engaged,
// and a far-ahead out-of-order delivery (multi-peer fetch) put the drain
// goroutine into the BLOCKING WaitForBlockAssemblyReady. Blocks 101+ queued
// behind it, block assembly could not advance (nothing committed), and the
// node lurched only on wait timeouts — stuck at exactly the gate width (100)
// from genesis. The fix records "the poller has reported" as its own signal
// (baHeightPolled) so a real height of 0 arms the gate like any other height.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyGate_GenesisEvaluableOncePolled: with the poller having
// reported a genuine height of 0, the gate must be evaluable — blocks within
// the gate admit, blocks beyond it PARK (evaluable && !admit) instead of
// falling into the drain-blocking wait.
func TestBlockAssemblyGate_GenesisEvaluableOncePolled(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20) // maxBehind = 20

	// Poller reported: block assembly genuinely at genesis (height 0).
	sm.baHeightPolled.Store(true)
	sm.cachedBlockAssemblyHeight.Store(0)

	admit, evaluable := sm.blockAssemblyGateAdmitsCached(15) // 0+20 >= 15
	require.True(t, evaluable, "a polled genesis height must arm the gate")
	require.True(t, admit, "heights within the gate admit from genesis")

	admit, evaluable = sm.blockAssemblyGateAdmitsCached(150) // far ahead
	require.True(t, evaluable, "a polled genesis height must arm the gate for far-ahead blocks too")
	require.False(t, admit, "far-ahead blocks must PARK (not block the drain) on a fresh node")

	// No poller report yet: not evaluable (unchanged fallback to the blocking
	// wait; a stale zero must not be trusted).
	sm.baHeightPolled.Store(false)

	_, evaluable = sm.blockAssemblyGateAdmitsCached(150)
	require.False(t, evaluable, "without a poller report the gate stays unarmed")
}

// TestReleaseParkedBlocks_ReleasesFromGenesis: the park release path must be
// able to feed heights 1..ceiling on a fresh node (cached height 0). Before
// the fix its cached==0 guard returned immediately, so nothing parked at
// genesis could ever be released.
func TestReleaseParkedBlocks_ReleasesFromGenesis(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20)

	sm.baHeightPolled.Store(true)
	sm.cachedBlockAssemblyHeight.Store(0) // genesis

	wa := newWindowAccumulator(1<<30, 0)
	park := newParkStore(0, 1024)

	for _, h := range []uint32{3, 1, 5, 2, 4} {
		park.add(mkBlock(h, 10))
	}

	sm.releaseParkedBlocks(park, wa, func() {}, func() {})

	require.Len(t, wa.entries, 5, "the contiguous run 1..5 must release from genesis (cached 0)")
	for i, e := range wa.entries {
		require.Equal(t, uint32(i+1), e.block.Height)
	}
	require.Equal(t, 0, park.len())
}
