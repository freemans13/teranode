package netsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func heights(jobs []*finalizeJob) []uint32 {
	out := make([]uint32, len(jobs))
	for i, j := range jobs {
		out[i] = j.blockHeight
	}
	return out
}

// TestFinalizeReorderBuffer_InOrder: contiguous in-order submissions each become
// ready immediately, one at a time.
func TestFinalizeReorderBuffer_InOrder(t *testing.T) {
	b := newFinalizeReorderBuffer()
	b.setStart(100)

	require.Equal(t, []uint32{100}, heights(b.add(&finalizeJob{blockHeight: 100})))
	require.Equal(t, []uint32{101}, heights(b.add(&finalizeJob{blockHeight: 101})))
	require.Equal(t, []uint32{102}, heights(b.add(&finalizeJob{blockHeight: 102})))
	require.Equal(t, 0, b.len())
}

// TestFinalizeReorderBuffer_OutOfOrderCompletion: the start height is authoritative
// (set at dispatch), so even when PhaseA for 101/102 completes before 100, the
// buffer holds them until 100 arrives — it does NOT mistake the first completion
// for the start and drop 100.
func TestFinalizeReorderBuffer_OutOfOrderCompletion(t *testing.T) {
	b := newFinalizeReorderBuffer()
	b.setStart(100)

	require.Empty(t, b.add(&finalizeJob{blockHeight: 101}))
	require.Empty(t, b.add(&finalizeJob{blockHeight: 102}))
	require.Equal(t, 2, b.len(), "101 and 102 buffered behind the missing 100")

	require.Equal(t, []uint32{100, 101, 102}, heights(b.add(&finalizeJob{blockHeight: 100})))
	require.Equal(t, 0, b.len())
}

// TestFinalizeReorderBuffer_GapHeldUntilFilled: a gap in the middle holds later
// blocks until the missing height arrives.
func TestFinalizeReorderBuffer_GapHeldUntilFilled(t *testing.T) {
	b := newFinalizeReorderBuffer()
	b.setStart(100)

	require.Equal(t, []uint32{100}, heights(b.add(&finalizeJob{blockHeight: 100})))
	require.Empty(t, b.add(&finalizeJob{blockHeight: 102}))
	require.Equal(t, 1, b.len())
	require.Equal(t, []uint32{101, 102}, heights(b.add(&finalizeJob{blockHeight: 101})))
}

// TestFinalizeReorderBuffer_StaleDropped: a height at or below an already
// finalized height is stale (its block is already on chain) and must be dropped,
// never re-finalized.
func TestFinalizeReorderBuffer_StaleDropped(t *testing.T) {
	b := newFinalizeReorderBuffer()
	b.setStart(100)

	require.Equal(t, []uint32{100}, heights(b.add(&finalizeJob{blockHeight: 100})))
	require.Equal(t, []uint32{101}, heights(b.add(&finalizeJob{blockHeight: 101})))

	require.Empty(t, b.add(&finalizeJob{blockHeight: 100}), "100 already finalized")
	require.Equal(t, 0, b.len())

	require.Equal(t, []uint32{102}, heights(b.add(&finalizeJob{blockHeight: 102})))
}

// TestFinalizeReorderBuffer_SetStartOnlyOnce: setStart is idempotent so a late or
// repeated call cannot move the finalize cursor backwards or forwards.
func TestFinalizeReorderBuffer_SetStartOnlyOnce(t *testing.T) {
	b := newFinalizeReorderBuffer()
	b.setStart(100)
	b.setStart(50)  // ignored
	b.setStart(200) // ignored

	h, started := b.waitingFor()
	require.True(t, started)
	require.Equal(t, uint32(100), h)
}

// TestFinalizeReorderBuffer_FallbackToFirstAdd: if setStart was never called the
// first job added establishes the start (defensive fallback).
func TestFinalizeReorderBuffer_FallbackToFirstAdd(t *testing.T) {
	b := newFinalizeReorderBuffer()

	require.Equal(t, []uint32{105}, heights(b.add(&finalizeJob{blockHeight: 105})))
}

// TestFinalizeReorderBuffer_WaitingFor reports the next height the buffer needs to
// make progress, so a watchdog can detect a stalled gap.
func TestFinalizeReorderBuffer_WaitingFor(t *testing.T) {
	b := newFinalizeReorderBuffer()

	_, started := b.waitingFor()
	require.False(t, started, "no start height before setStart or first add")

	b.setStart(100)
	h, started := b.waitingFor()
	require.True(t, started)
	require.Equal(t, uint32(100), h, "waiting for the start height")

	b.add(&finalizeJob{blockHeight: 100})
	h, _ = b.waitingFor()
	require.Equal(t, uint32(101), h, "advanced after finalizing 100")

	b.add(&finalizeJob{blockHeight: 102}) // gap at 101
	h, _ = b.waitingFor()
	require.Equal(t, uint32(101), h, "still waiting for the gap")
}
