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

// With no block size, seeding failed or the chain starts at genesis, the watcher cannot judge a
// queued block and does nothing until the first block converts. It says so once rather than
// silently (review of 2026-10-12).
func TestWatchSaysOnceThatItHasNoBlockSize(t *testing.T) {
	sm, owner, _, _ := reaskSetup(t)
	logs := &warnCaptureLogger{}
	sm.logger = logs
	sm.blockSizeTracker = newBlockSizeTracker(10)
	now := time.Now()

	askAt(t, sm, owner, heightHash(t, sm, 11), now.Add(-2*time.Minute))

	sm.watchOwedBlocks(now)
	sm.watchOwedBlocks(now.Add(5 * time.Second))

	require.Len(t, logs.warnings, 1, "said once, not on every tick")
	require.Contains(t, logs.warnings[0], "no recent block size")

	sm.blockSizeTracker.addBlockSize(reaskTypicalBlock)
	sm.watchOwedBlocks(now.Add(10 * time.Second))
	require.False(t, sm.watchNoSizeLogged.Load(), "cleared once a size is known, so a later episode is said again")
}
