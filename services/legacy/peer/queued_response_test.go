package peer

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// A reply queued behind blocks the peer is still sending is not a stall. A getdata can ask for
// several blocks, and when the first arrives the whole block-reply group is cleared, so the
// headers deadline counts down again while the peer sends the next one. At 1 GB and 5 to
// 10 MB/s that is longer than the 90 s headers budget: on 2026-09-24 mainnet disconnected peers
// for "headers timeout" while they were sending blocks, and lost the block one of them had just
// finished, which stopped the chain at 708,114.
func TestAReplyQueuedBehindAHealthyDownloadIsNotAStall(t *testing.T) {
	const budget = 20 * time.Minute

	require.True(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, 0, budget), "bytes are arriving at a healthy rate")
	require.False(t, shouldDeferQueuedResponse(wire.CmdHeaders, false, 0, budget), "nothing arriving is a stall")
	require.False(t, shouldDeferQueuedResponse(wire.CmdBlock, true, 0, budget), "block replies keep their own extension rule")
}

// The healthy rate is the whole association's, so transaction relay alone kept it up with no
// block in flight, and a getheaders the peer never answered was deferred for good (review of
// 2026-10-12). A reply deferred for the block download budget is a stall.
func TestAQueuedReplyIsDeferredOnlyForTheBlockBudget(t *testing.T) {
	const budget = 20 * time.Minute

	require.True(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, budget-time.Second, budget))
	require.False(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, budget, budget), "deferred for the whole budget")

	now := time.Now()
	require.Zero(t, deferredDuration(time.Time{}, false, now))
	require.Equal(t, time.Minute, deferredDuration(now.Add(-time.Minute), true, now))
}
