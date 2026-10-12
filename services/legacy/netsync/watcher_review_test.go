package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
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

// A fast owner that ignored one getdata entry and delivered the blocks asked after it has passed
// the block over. Its queue estimate stayed at a typical block at its rate, under watchMinETA,
// and any block it delivered kept its deadline and rate up, so nothing helped the block until the
// ledger's TTL (review of 2026-10-12). The watcher counts it as one the owner will not deliver.
func TestWatchHelpsABlockItsOwnerPassedOver(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	sm.streams.rates[owner] = 80_000_000
	sm.streams.rates[fast] = 200_000_000

	next := heightHash(t, sm, 11)
	later := heightHash(t, sm, 12)

	askAt(t, sm, owner, next, now.Add(-2*time.Minute))
	askAt(t, sm, owner, later, now.Add(-2*time.Minute))
	sm.blockDownloads.Delivered(owner, later)

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "the passed-over block gets a helper")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its request")
}

// A block with a forgiven owner that sends nothing and one active owner is still judged. The
// owner count included the forgiven owner, so once a block had one the watcher never judged it
// again (review of 2026-10-12).
func TestWatchJudgesABlockWhoseOtherOwnerWasLetOff(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	letOff, _ := schedulerPeer(t, sm, 3, 2000)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	next := heightHash(t, sm, 11)
	askAt(t, sm, letOff, next, now.Add(-5*time.Minute))

	// Let only the earlier owner off: forgive both, then put the slow owner back on the hook.
	sm.blockDownloads.ForgiveOwners(next, blockRequestRetryInterval)

	sm.blockDownloads.now = func() time.Time { return now.Add(-2 * time.Minute) }
	require.True(t, sm.blockDownloads.ReassertOwner(owner, next))
	sm.blockDownloads.now = time.Now

	active, _ := sm.blockDownloads.ActiveOwners(next)
	require.Equal(t, []*peerpkg.Peer{owner}, active, "harness check: one active owner, one let off")

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "the slow active owner's block gets a helper")
}

// The watcher shares the race's cap on extra copies of a block over raceExpiry. It checked only
// the live owner count and wasRaced's 30 s, so a helper that disconnected, leaving the ledger,
// let it ask a new one every 30 s (review of 2026-10-12).
func TestWatchKeepsTheRaceCapOnExtraCopies(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	next := heightHash(t, sm, 11)

	// Two extra copies were asked a minute ago, and both helpers have since gone.
	for range maxBlockCopies - 1 {
		sm.streams.markRaced(next, now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, next), "the cap is spent for raceExpiry")
}

// The watcher asks under assignMu, so a download pass never asks for a block beside it. It ran
// without the lock (review of 2026-10-12). It takes the lock with TryLock and skips the tick
// while a pass holds it, because it runs on the frontier-race ticker and waiting out a slow
// pass would stop everything else on that ticker too (review of 2026-10-12, 16:58).
func TestWatchAsksOnlyBetweenDownloadPasses(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	next := heightHash(t, sm, 11)

	sm.assignMu.Lock()

	done := make(chan struct{})

	go func() {
		sm.watchOwedBlocks(now)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		sm.assignMu.Unlock()
		t.Fatal("the watcher must skip the tick while a pass holds assignMu, not wait for the pass")
	}

	require.False(t, sm.blockDownloads.HasOwner(fast, next), "no helper is asked while a pass holds assignMu")

	sm.assignMu.Unlock()

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "and the next tick asks once the pass is done")
}

// A copy whose bytes have all arrived and that this node is still converting is this node's
// wait, not the peer's. From its last byte the peer looked silent: its rate decayed, and after
// the retry window its other blocks could be re-asked, while a 2 GB takeover converted for 90 s
// (review of 2026-10-12).
func TestAPeerWhoseCopyIsBeingConvertedIsNotQuiet(t *testing.T) {
	sm, _, fast, _ := reaskSetup(t)
	now := time.Now()

	askAt(t, sm, fast, heightHash(t, sm, 13), now.Add(-10*time.Minute))

	h := heightHash(t, sm, 12)
	s := sm.streams.start(h, 12, fast, 2_000_000_000, now.Add(-3*time.Minute))
	s.read.Store(2_000_000_000 - wire.MaxBlockHeaderPayload)
	s.lastRead.Store(now.Add(-90 * time.Second).UnixNano())
	require.True(t, s.complete(), "harness check: every byte has arrived")

	sm.decayQuietRates(now)

	require.InDelta(t, 80_000_000.0, sm.streams.peerRate(fast), 1, "no decay while its copy converts")
	require.Less(t, time.Since(sm.streams.lastBlockBytes(fast)), time.Second, "and it counts as sending now")
}

// The ledger's passed-over mark: a peer that delivered a block asked after h, while it still
// owes h, passed h over. A block asked after the delivered one is not passed over, and the mark
// goes with the peer.
func TestTheLedgerKnowsABlockAPeerPassedOver(t *testing.T) {
	ledger := newBlockDownloadTracker(time.Hour)
	p := newTestPeer(t, "10.0.0.1:8333")

	first, second, third := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}

	require.True(t, ledger.Add(p, first))
	require.True(t, ledger.Add(p, second))
	require.False(t, ledger.PassedOver(p, first), "nothing delivered yet")

	ledger.Delivered(p, second)
	require.False(t, ledger.HasOwner(p, second), "a delivered block is discharged")
	require.True(t, ledger.PassedOver(p, first), "second was asked after first and came first")

	require.True(t, ledger.Add(p, third))
	require.False(t, ledger.PassedOver(p, third), "asked after the delivered block")

	ledger.ClearPeer(p)
	require.False(t, ledger.PassedOver(p, first))
}
