package sql

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rebuildFault stands in for the on_main_chain rebuild transaction. While failing is set,
// or for the first failFirst calls, it returns an error without touching the database;
// otherwise it runs the real transaction. It records the full and floor arguments of every
// call, and whether slowPathMu was held, so a test can see which walk the repair asked for.
type rebuildFault struct {
	failing   atomic.Bool
	failFirst int32
	calls     atomic.Int32

	mu     sync.Mutex
	fulls  []bool
	floors []int64
	locked []bool
}

// installRebuildFault wires a rebuildFault into the store, failing every call until the
// test clears failing.
func installRebuildFault(s *SQL) *rebuildFault {
	f := &rebuildFault{}
	f.failing.Store(true)

	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		n := f.calls.Add(1)

		// TryLock fails exactly when someone, in practice the caller, holds slowPathMu.
		held := !s.slowPathMu.TryLock()
		if !held {
			s.slowPathMu.Unlock()
		}

		f.mu.Lock()
		f.fulls = append(f.fulls, full)
		f.floors = append(f.floors, floor)
		f.locked = append(f.locked, held)
		f.mu.Unlock()

		if f.failing.Load() || n <= f.failFirst {
			return errors.NewStorageError("injected rebuild failure")
		}

		return s.rebuildOnMainChainFlagTx(ctx, full, floor)
	}

	return f
}

func (f *rebuildFault) walks() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.fulls...)
}

func (f *rebuildFault) floorsSeen() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int64(nil), f.floors...)
}

func (f *rebuildFault) lockHeld() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.locked...)
}

// setRepairBackoff shortens the background repair's first backoff so a test does not
// wait out the production second.
func setRepairBackoff(t *testing.T, s *SQL, d time.Duration) {
	t.Helper()

	s.onMainChainRepair.mu.Lock()
	s.onMainChainRepair.backoff = d
	s.onMainChainRepair.mu.Unlock()
}

// waitForRepair blocks until the store no longer owes a flag repair, which it signals by
// dropping mainChainRebuilding back to zero.
func waitForRepair(t *testing.T, s *SQL) {
	t.Helper()

	require.Eventually(t, func() bool {
		return s.mainChainRebuilding.Load() == 0
	}, 10*time.Second, time.Millisecond, "the background repair must release its guard exactly once")
}

// A successful operator rebuild can acknowledge the repair after the worker has
// selected its generation but before the worker accounts for its own success.
// The acknowledgment leaves the record to the running attempt, so the guard is
// released only once.
func TestOnMainChainRepairDoesNotClearAnAlreadyRepairedGeneration(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block1.Hash().CloneBytes())
	require.NoError(t, err)

	acknowledged := make(chan struct{})
	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		if err := s.rebuildOnMainChainFlagTx(ctx, full, floor); err != nil {
			return err
		}

		// Model the external acknowledgment in the worker's accounting window,
		// after a real full rebuild has repaired the persisted flags.
		s.onMainChainRepaired(true)
		close(acknowledged)

		return nil
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))
	select {
	case <-acknowledged:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair did not reach the acknowledgment")
	}

	// Join the worker before examining its final accounting, including underflow.
	close(s.backgroundDone)
	select {
	case <-s.onMainChainRepair.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair loop did not stop")
	}

	require.Zero(t, s.mainChainRebuilding.Load(), "the same repair must not release its guard twice")
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "the persisted flag was repaired")
	require.Equal(t, gaugeBefore, repairPending(t), "the pending gauge is released once")
}

func repairPending(t *testing.T) float64 {
	t.Helper()

	return testutil.ToFloat64(prometheusOnMainChainRepairPending)
}

func writeFailures(t *testing.T, path string) float64 {
	t.Helper()

	return testutil.ToFloat64(prometheusOnMainChainWriteFailures.WithLabelValues(path))
}

// TestInvalidateBlockFailedRebuildIsRepairedInProcess is the reason the store keeps a
// record of a failed flag write.
//
// InvalidateBlock commits its UPDATE and then runs the full on_main_chain rebuild. When the
// rebuild fails it logs and returns success, and before this change nothing retried it: the
// guard dropped with the call, every flag reader went back to trusting the column, and the
// losing branch stayed flagged true while the winner stayed false until a restart or the
// next operator action. The block below the invalidated one is that stale true, and the
// fork that becomes the best chain is that stale false.
func TestInvalidateBlockFailedRebuildIsRepairedInProcess(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, 10*time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3 -> 4, and a fork 1 -> alt2 -> alt3 that loses.
	block4 := createBlock3OnFork(block3)
	alt3 := createBlock3OnFork(blockAlternative2)
	storeBlocks(t, s, block1, block2, block3, block4, blockAlternative2, alt3)
	require.True(t, getOnMainChain(t, s, block4.Hash().CloneBytes()), "block4 is the best block")
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "alt3 is a fork")

	failuresBefore := writeFailures(t, "invalidate_block")
	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	// Invalidating block3 takes block4 with it, so the best chain is now 1 -> alt2 -> alt3.
	_, err := s.InvalidateBlock(ctx, block3.Hash())
	require.NoError(t, err, "InvalidateBlock logs a failed rebuild and still returns success")

	// The UPDATE committed and the rebuild did not run, so the flags are stale both ways.
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 is a stale true: it is no longer on the best chain")
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "alt3 is a stale false: it is the best block")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block4.Hash().CloneBytes()))

	// The guard must stay up for as long as the repair is owed, so no reader trusts the
	// column, and the failure must be visible.
	require.Positive(t, s.mainChainRebuilding.Load(), "the guard stays raised while a repair is owed")
	require.Equal(t, gaugeBefore+1, repairPending(t), "the pending gauge counts this store's owed repair")
	require.Equal(t, failuresBefore+1, writeFailures(t, "invalidate_block"), "the failure is counted once")

	// Readers answer from the flag-free walk while the guard is up.
	onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, block2.Hash().CloneBytes())})
	require.NoError(t, err)
	require.False(t, onChain, "block2 is flagged true but is not on the best chain")

	onChain, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, alt3.Hash().CloneBytes())})
	require.NoError(t, err)
	require.True(t, onChain, "alt3 is flagged false but is the best block")

	byHeight, err := s.GetBlockByHeight(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, alt3.Hash().String(), byHeight.Hash().String(), "GetBlockByHeight walks the chain while the guard is up")

	// Let the database "recover" and the background repair land.
	fault.failing.Store(false)
	waitForRepair(t, s)

	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair clears the stale true")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "the repair flags the winning branch")
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "the repair flags the new best block")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block4.Hash().CloneBytes()))

	require.Equal(t, gaugeBefore, repairPending(t), "the pending gauge drops once the repair lands")
	require.GreaterOrEqual(t, fault.calls.Load(), int32(2), "at least one failed attempt and the one that landed")

	// With the guard down the flag fast path now agrees with the walk.
	byHeight, err = s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, blockAlternative2.Hash().String(), byHeight.Hash().String())
}

// TestRevalidateBlockFailedRebuildIsRepairedInProcess covers the other operator path with
// the same shape: the UPDATE that makes a block valid again commits, the full rebuild that
// would move the flags onto the revalidated branch fails, and nothing retried it.
func TestRevalidateBlockFailedRebuildIsRepairedInProcess(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, 10*time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3, then a fork 1 -> alt2 -> alt3 -> alt4 that wins.
	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()), "alt4 won the reorg")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	// Park alt4 with a working rebuild. block3 and alt3 tie on work and block3 has the lower
	// id, so block3 is the best again.
	_, err := s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))

	failuresBefore := writeFailures(t, "revalidate_block")
	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()), "RevalidateBlock logs a failed rebuild and still returns success")

	// alt4 is valid and the best block again, but the flags still describe block3's chain.
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 is a stale true")
	require.False(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()), "alt4 is a stale false")

	require.Positive(t, s.mainChainRebuilding.Load(), "the guard stays raised while a repair is owed")
	require.Equal(t, gaugeBefore+1, repairPending(t))
	require.Equal(t, failuresBefore+1, writeFailures(t, "revalidate_block"))

	onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, block3.Hash().CloneBytes())})
	require.NoError(t, err)
	require.False(t, onChain, "block3 is flagged true but is not on the best chain")

	onChain, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, alt4.Hash().CloneBytes())})
	require.NoError(t, err)
	require.True(t, onChain, "alt4 is flagged false but is the best block")

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestOnMainChainRepairKeepsTheRecordedFloorAsTheTipMoves pins the floor. A bounded
// repair's own window is measured from the tip at the time of the attempt, and blocks keep
// arriving while the repair keeps failing. Without the floor the mark recorded, a retry
// after the tip moved walks a window that no longer reaches the stale row, succeeds,
// releases the guard, and leaves the row wrong.
//
// CoinbaseMaturity 0 makes the bounded window the tip alone, so a single new block is
// enough to carry it past the stale row.
func TestOnMainChainRepairKeepsTheRecordedFloorAsTheTipMoves(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, 20*time.Millisecond)

	storeBlocks(t, s, block1, block2)

	// block2, at the tip, is the row a failed StoreBlock write left wrong.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	fault := installRebuildFault(s)

	s.markOnMainChainDirty(false, int64(block2.Height), "test", errors.NewStorageError("injected"))

	require.Eventually(t, func() bool { return fault.calls.Load() >= 1 }, 5*time.Second, time.Millisecond, "the first attempt runs")

	// The tip moves while the repair is failing.
	storeBlocks(t, s, block3)

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair reaches the stale row below the moved window")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	for i, full := range fault.walks() {
		require.False(t, full, "attempt %d stays bounded: the floor covers the stale row", i+1)
	}

	for i, floor := range fault.floorsSeen() {
		require.EqualValues(t, block2.Height, floor, "attempt %d keeps the recorded floor", i+1)
	}

	for i, held := range fault.lockHeld() {
		require.True(t, held, "bounded attempt %d runs under slowPathMu", i+1)
	}
}

// TestOnMainChainRepairWithoutAFloorWalksToGenesis: a bounded mark that cannot name the
// lowest stale height cannot be covered by any bounded window, so it is owed a full walk.
// That walk holds slowPathMu on SQLite, where a long write would otherwise make StoreBlock
// fail on the busy timeout, and runs without it on PostgreSQL.
func TestOnMainChainRepairWithoutAFloorWalksToGenesis(t *testing.T) {
	for _, tc := range []struct {
		name     string
		unlocked bool
	}{
		{name: "sqlite holds the lock", unlocked: false},
		{name: "postgres runs without the lock", unlocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newOnMainChainTestStore(t)
			require.False(t, s.unlockedFullRepair, "a SQLite store holds the lock for a full repair")
			s.unlockedFullRepair = tc.unlocked

			setRepairBackoff(t, s, time.Millisecond)
			storeBlocks(t, s, block1, block2)

			fault := installRebuildFault(s)
			fault.failing.Store(false)

			s.markOnMainChainDirty(false, noRepairFloor, "test", errors.NewStorageError("injected"))
			waitForRepair(t, s)

			require.Equal(t, []bool{true}, fault.walks())
			require.Equal(t, []bool{!tc.unlocked}, fault.lockHeld())
		})
	}
}

// TestOnMainChainRepairRerunsWhenAChainWriteOverlapsIt: a full walk that runs without
// slowPathMu reads its rows from a snapshot taken when it began. RevalidateBlock can commit
// in the meantime, its own rebuild can land and acknowledge the repair, and then the walk
// commits its older view. On PostgreSQL the two touch different rows, so neither aborts.
// The repair must not release the guard on that walk. It must run again.
//
// SQLite cannot hold two writers open at once, so this test stands in for the stale
// commit: the loop's first attempt waits until RevalidateBlock has finished and then writes
// what a walk from the old snapshot would write. The PostgreSQL test below runs the real
// walk.
func TestOnMainChainRepairRerunsWhenAChainWriteOverlapsIt(t *testing.T) {
	s := newOnMainChainTestStore(t)
	s.unlockedFullRepair = true
	setRepairBackoff(t, s, time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3, and a fork 1 -> alt2 -> alt3 -> alt4 that wins
	// once alt4 is valid. Park alt4, so block3 is the best.
	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	_, err := s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	var calls atomic.Int32

	started := make(chan struct{})
	release := make(chan struct{})

	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		if calls.Add(1) != 1 {
			return s.rebuildOnMainChainFlagTx(ctx, full, floor)
		}

		close(started)
		<-release

		// The walk's snapshot predates the revalidation: block3's chain was the best.
		_, err := s.db.ExecContext(ctx, `UPDATE blocks SET on_main_chain = (hash IN ($1, $2)) WHERE height IN (2, 3, 4)`,
			block2.Hash().CloneBytes(), block3.Hash().CloneBytes())

		return err
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair's first attempt did not start")
	}

	// alt4 becomes the best again, and RevalidateBlock's own rebuild lands while the
	// loop's attempt is still in flight.
	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.Positive(t, s.mainChainRebuilding.Load(), "an acknowledgment during an attempt does not drop the guard")

	close(release)
	waitForRepair(t, s)

	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the rerun undoes the stale walk's write")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.EqualValues(t, 3, calls.Load(), "the stale attempt, RevalidateBlock's rebuild, and the rerun")
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestStartupBoundedRebuildFailureKeepsItsWindowAsTheFloor: a failed bounded startup
// rebuild is owed a bounded repair that still reaches the bottom of the window the startup
// rebuild meant to cover, however far the tip moves before the repair lands.
func TestStartupBoundedRebuildFailureKeepsItsWindowAsTheFloor(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, 20*time.Millisecond)

	storeBlocks(t, s, block1, block2)

	// block2, at the tip at startup, is the row the previous process left wrong.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	fault := installRebuildFault(s)

	s.startupRebuildOnMainChain(context.Background(), false)
	require.Positive(t, s.mainChainRebuilding.Load(), "a repair is owed")

	require.Eventually(t, func() bool { return fault.calls.Load() >= 2 }, 5*time.Second, time.Millisecond, "the loop's first attempt runs")

	// The tip moves past the startup window while the repair is failing.
	storeBlocks(t, s, block3)

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair reaches the startup window below the moved tip")

	walks := fault.walks()
	floors := fault.floorsSeen()

	// The first call is the startup rebuild itself; the rest are the loop's attempts.
	for i := 1; i < len(walks); i++ {
		require.False(t, walks[i], "attempt %d stays bounded", i)
		require.EqualValues(t, block2.Height, floors[i], "attempt %d keeps the startup window's bottom as its floor", i)
	}
}

// TestOnMainChainMarkDropsCachedAnswers: GetBlockByHeight answers from the response cache
// before it consults the guard, so a reply cached from the stale column before the mark
// would keep being served while the repair is owed.
func TestOnMainChainMarkDropsCachedAnswers(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, time.Hour)
	ctx := context.Background()

	storeBlocks(t, s, block1, block2, blockAlternative2)

	// A failed write leaves the fork flagged and the main-chain block not.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = (hash = $1) WHERE height = 2`, blockAlternative2.Hash().CloneBytes())
	require.NoError(t, err)

	cached, err := s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, blockAlternative2.Hash().String(), cached.Hash().String(), "the stale column answers before the mark")

	installRebuildFault(s)
	s.markOnMainChainDirty(false, 2, "test", errors.NewStorageError("injected"))

	byHeight, err := s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, block2.Hash().String(), byHeight.Hash().String(), "after the mark the answer comes from the flag-free walk")
}

// TestOnMainChainMarkIsSkippedWhileClosing: a write cancelled by Close is not a fault to
// repair. It must not raise the guard, count a failure or set the pending gauge.
func TestOnMainChainMarkIsSkippedWhileClosing(t *testing.T) {
	s := newOnMainChainTestStore(t)

	failuresBefore := writeFailures(t, "test_closing")
	gaugeBefore := repairPending(t)

	close(s.backgroundDone)
	s.markOnMainChainDirty(true, noRepairFloor, "test_closing", context.Canceled)

	require.Zero(t, s.mainChainRebuilding.Load())
	require.Equal(t, failuresBefore, writeFailures(t, "test_closing"))
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestALaterFullRebuildClearsThePendingRepair: a full rebuild that an operator path runs
// successfully while a repair is owed satisfies that repair, because it leaves every flag
// right. The background loop then has nothing left to do, and the store's shutdown must not
// wait for its backoff to run out.
func TestALaterFullRebuildClearsThePendingRepair(t *testing.T) {
	s := newOnMainChainTestStore(t)
	// Long enough that the loop cannot retry on its own during the test.
	setRepairBackoff(t, s, time.Hour)

	ctx := context.Background()

	block4 := createBlock3OnFork(block3)
	alt3 := createBlock3OnFork(blockAlternative2)
	storeBlocks(t, s, block1, block2, block3, block4, blockAlternative2, alt3)

	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	_, err := s.InvalidateBlock(ctx, block4.Hash())
	require.NoError(t, err)
	require.Positive(t, s.mainChainRebuilding.Load(), "a repair is owed")

	// Let the loop make its one attempt and go to sleep.
	deadline := time.Now().Add(5 * time.Second)
	for fault.calls.Load() < 2 {
		require.False(t, time.Now().After(deadline), "the loop never made its first attempt")
		time.Sleep(time.Millisecond)
	}

	require.Positive(t, s.mainChainRebuilding.Load(), "still owed after the failed attempt")

	// Now an operator invalidates block3 and the full rebuild works.
	fault.failing.Store(false)

	_, err = s.InvalidateBlock(ctx, block3.Hash())
	require.NoError(t, err)

	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "the successful full rebuild clears the owed repair at once")
	require.Equal(t, gaugeBefore, repairPending(t))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))

	// Shutdown must not wait out the hour-long backoff. Close signals backgroundDone first,
	// so do that half here and leave the rest to the test cleanup's Close.
	close(s.backgroundDone)

	select {
	case <-s.onMainChainRepair.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair loop did not stop on shutdown")
	}
}

// TestOnMainChainRepairRacingRevalidate_PostgreSQL runs the overlap with the real walk on
// the engine where the full repair runs without slowPathMu. The loop's walk is held after it
// has taken its snapshot, with block3's chain as the best and block3's flag left false by
// the failed write it is owed for. RevalidateBlock then makes alt4 the best and its own
// rebuild commits. The held walk resumes and sets block3 true from its old snapshot. The
// two rebuilds update different rows, so neither aborts, and without the rerun the table
// ends with both block3 and alt3 flagged at height 3.
func TestOnMainChainRepairRacingRevalidate_PostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	t.Cleanup(func() { _ = teardown() })

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })
	waitForStartupRebuild(t, s)

	require.True(t, s.unlockedFullRepair, "a PostgreSQL store runs the full repair without slowPathMu")
	setRepairBackoff(t, s, time.Millisecond)

	ctx := context.Background()

	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	_, err = s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	// The stale flag the failed write left behind.
	_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block3.Hash().CloneBytes())
	require.NoError(t, err)

	var snapshots atomic.Int32

	started := make(chan struct{})
	release := make(chan struct{})

	s.rebuildSnapshotHook = func(bool) {
		if snapshots.Add(1) != 1 {
			return
		}

		close(started)
		<-release
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the repair's walk did not reach its snapshot")
	}

	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))

	close(release)
	waitForRepair(t, s)

	var atHeight3 int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM blocks WHERE height = 3 AND on_main_chain = true`).Scan(&atHeight3))
	require.Equal(t, 1, atHeight3, "one block per height on the main chain")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the rerun undoes the held walk's write")
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.GreaterOrEqual(t, snapshots.Load(), int32(3), "the held walk, RevalidateBlock's rebuild, and the rerun")
	require.Equal(t, gaugeBefore, repairPending(t))
}
