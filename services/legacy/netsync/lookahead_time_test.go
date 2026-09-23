package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// The read-ahead depth is measured in time as well as in blocks. On 2026-09-23 mainnet sat at
// height 634,642 for 49 seconds waiting on block 634,643, a 309 MB block among blocks of under
// 1 MB. The depth was the configured 128 blocks, which at the 4 blocks a second the chain was
// committing is 32 seconds of lead, and the block took 72 seconds to arrive from its one peer.
// Every block behind it was already parked. A depth that covers minutes of commits would have
// asked for it long before the chain reached it.

const (
	testMiB = int64(1024 * 1024)
	testGiB = 1024 * testMiB
)

// commitsAt feeds a manager's commit-rate tracker n commits spaced interval apart, ending now.
func commitsAt(sm *SyncManager, n int, interval time.Duration, now time.Time) {
	for i := n - 1; i >= 0; i-- {
		sm.commitRate.note(now.Add(-time.Duration(i) * interval))
	}
}

func timeLookaheadManager(avgBlockSize int64) *SyncManager {
	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockDownloadLowerWindow = 128
	tSettings.Legacy.BlockDownloadWindow = 1024

	sm := &SyncManager{
		settings:         tSettings,
		blockSizeTracker: newBlockSizeTracker(10),
		commitRate:       newCommitRateTracker(),
	}

	if avgBlockSize > 0 {
		sm.blockSizeTracker.addBlockSize(avgBlockSize)
	}

	return sm
}

func depthOf(t *testing.T, sm *SyncManager) int64 {
	t.Helper()

	ceiling, ok := sm.lookaheadCeilingLocked(1000)
	require.True(t, ok)

	return ceiling - 1000
}

// The case that stalled mainnet: small blocks committing at 4 a second. Five minutes of lead is
// 1,200 blocks, which the node-wide download window of 1,024 then clamps.
func TestLookaheadCoversMinutesOfCommitsOnSmallBlocks(t *testing.T) {
	sm := timeLookaheadManager(1 * testMiB)
	commitsAt(sm, 200, 250*time.Millisecond, time.Now())

	require.Equal(t, int64(1024), depthOf(t, sm),
		"at 4 blocks a second five minutes of lead is 1,200 blocks, clamped to the 1,024-block window")
}

// Before any commit has been seen the rate is unknown, and the depth is exactly what it was
// before this change: the configured count scaled by the block-size ladder.
func TestLookaheadWithNoCommitsKeepsTheConfiguredDepth(t *testing.T) {
	require.Equal(t, int64(128), depthOf(t, timeLookaheadManager(1*testMiB)))
	require.Equal(t, int64(32), depthOf(t, timeLookaheadManager(300*testMiB)), "300 MB blocks: ladder 5 of 20")
}

// The time depth is bounded by what the park can hold. At a 300 MB average, 20 GiB of park is
// 68 blocks, so five minutes at 4 blocks a second is cut to 68, which is still more than the
// ladder's 32.
func TestLookaheadTimeDepthIsCappedByParkBytes(t *testing.T) {
	sm := timeLookaheadManager(300 * testMiB)
	commitsAt(sm, 200, 250*time.Millisecond, time.Now())

	require.Equal(t, int64(68), depthOf(t, sm))
}

// Never shallower than the ladder's depth: at 3 GB blocks the park cap is 6 blocks, the same
// as the ladder, and the time depth cannot pull it below that.
func TestLookaheadIsNeverShallowerThanTheLadder(t *testing.T) {
	sm := timeLookaheadManager(3 * testGiB)
	commitsAt(sm, 20, 20*time.Second, time.Now())

	require.Equal(t, int64(6), depthOf(t, sm))
}

// A stall must not collapse the depth. The rate is measured over the last commits, however
// long ago the last one was, so the minute the chain spends waiting on one big block does not
// shrink the look-ahead back to 128 just when it matters.
func TestCommitRateSurvivesAStall(t *testing.T) {
	sm := timeLookaheadManager(1 * testMiB)
	commitsAt(sm, 200, 250*time.Millisecond, time.Now().Add(-2*time.Minute))

	require.Equal(t, int64(1024), depthOf(t, sm))
}

func TestCommitRateTracker(t *testing.T) {
	r := newCommitRateTracker()
	require.Zero(t, r.rate(), "no commits, no rate")

	now := time.Now()
	r.note(now)
	require.Zero(t, r.rate(), "one commit is not a rate")

	r.note(now.Add(time.Second))
	require.InDelta(t, 1.0, r.rate(), 1e-9)

	for i := 2; i <= 1000; i++ {
		r.note(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}

	require.InDelta(t, 10.0, r.rate(), 1e-6, "the rate follows the most recent commits, not the whole history")

	var nilTracker *commitRateTracker
	require.Zero(t, nilTracker.rate(), "a manager built without a tracker reads no rate")
	nilTracker.note(now)
}
