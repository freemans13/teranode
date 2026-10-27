package sql

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rebuildFault stands in for the on_main_chain rebuild transaction. While failing is set,
// or for the first failFirst calls, it returns an error without touching the database;
// otherwise it runs the real transaction. It records the full argument of every call so a
// test can see which walk the repair asked for.
type rebuildFault struct {
	failing   atomic.Bool
	failFirst int32
	calls     atomic.Int32

	mu    sync.Mutex
	fulls []bool
}

// installRebuildFault wires a rebuildFault into the store, failing every call until the
// test clears failing.
func installRebuildFault(s *SQL) *rebuildFault {
	f := &rebuildFault{}
	f.failing.Store(true)

	s.rebuildTxHook = func(ctx context.Context, full bool) error {
		n := f.calls.Add(1)

		f.mu.Lock()
		f.fulls = append(f.fulls, full)
		f.mu.Unlock()

		if f.failing.Load() || n <= f.failFirst {
			return errors.NewStorageError("injected rebuild failure")
		}

		return s.rebuildOnMainChainFlagTx(ctx, full)
	}

	return f
}

func (f *rebuildFault) walks() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.fulls...)
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
// Both acknowledgments must release the pending repair's guard only once.
func TestOnMainChainRepairDoesNotClearAnAlreadyRepairedGeneration(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block1.Hash().CloneBytes())
	require.NoError(t, err)

	acknowledged := make(chan struct{})
	s.rebuildTxHook = func(ctx context.Context, full bool) error {
		if err := s.rebuildOnMainChainFlagTx(ctx, full); err != nil {
			return err
		}

		// Model the external acknowledgment in the worker's accounting window,
		// after a real full rebuild has repaired the persisted flags.
		s.onMainChainRepaired(true)
		close(acknowledged)

		return nil
	}

	s.markOnMainChainDirty(true, "test", errors.NewStorageError("injected"))
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
	require.Zero(t, repairPending(t))
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
	require.Equal(t, 1.0, repairPending(t), "the pending gauge is 1 while a repair is owed")
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

	require.Equal(t, 0.0, repairPending(t), "the pending gauge drops once the repair lands")
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
	fault := installRebuildFault(s)

	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()), "RevalidateBlock logs a failed rebuild and still returns success")

	// alt4 is valid and the best block again, but the flags still describe block3's chain.
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 is a stale true")
	require.False(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()), "alt4 is a stale false")

	require.Positive(t, s.mainChainRebuilding.Load(), "the guard stays raised while a repair is owed")
	require.Equal(t, 1.0, repairPending(t))
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
	require.Equal(t, 0.0, repairPending(t))
}

// TestOnMainChainRepairEscalatesToAFullWalkAfterRepeatedFailures pins the escalation rule.
// A bounded repair walks a window measured from the current tip, and blocks keep arriving
// while the repair keeps failing, so after onMainChainRepairEscalateAfter failed bounded
// attempts the next one walks to genesis, where nothing can have sunk out of reach.
func TestOnMainChainRepairEscalatesToAFullWalkAfterRepeatedFailures(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, time.Millisecond)
	storeBlocks(t, s, block1, block2)

	fault := installRebuildFault(s)
	fault.failing.Store(false)
	fault.failFirst = onMainChainRepairEscalateAfter

	s.markOnMainChainDirty(false, "test", errors.NewStorageError("injected"))
	require.Positive(t, s.mainChainRebuilding.Load())

	waitForRepair(t, s)

	want := make([]bool, 0, onMainChainRepairEscalateAfter+1)
	for i := 0; i < onMainChainRepairEscalateAfter; i++ {
		want = append(want, false)
	}

	want = append(want, true)
	require.Equal(t, want, fault.walks(), "bounded walks until the escalation threshold, then one full walk")
	require.Equal(t, 0.0, repairPending(t))
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
	require.Equal(t, 0.0, repairPending(t))
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
