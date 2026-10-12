package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A copy fast early and slow past half is judged on its live rate, as the race judges it. On
// its average since the start it kept a short estimate and got no helper: here 150 of 200 MB
// came in 120 s, 1.25 MB/s on average and 40 s to go, but only 4.5 MB in the last 30 s,
// 150 KB/s and more than five minutes to go (review of 2026-10-12).
func TestWatchJudgesACopyPastHalfOnItsLiveRate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-3*time.Minute))

	s := sm.streams.start(next, 11, owner, 200_000_000, now.Add(-120*time.Second))
	s.read.Store(145_500_000)
	sm.streams.sampleStreams(now.Add(-liveRateWindow))
	s.read.Store(150_000_000)
	sm.streams.sampleStreams(now)

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "50 MB at 150 KB/s is past watchPastHalfWait")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its copy")
}
