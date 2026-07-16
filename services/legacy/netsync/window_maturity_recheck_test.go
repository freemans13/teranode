package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitForBlockAssemblyReadyCached_BelowCP_ReleasesOnCacheAdvance proves the
// smoothed release: for a below-checkpoint block whose cache is initially behind
// the bound, the wait polls the cached height at the fixed recheck interval and
// returns nil promptly once the poller-refreshed cache advances past the bound —
// WITHOUT any gRPC round-trip (the exponential-backoff fresh-gRPC path is not
// taken below the checkpoint).
//
// RED against the old code: the old below-cp fallthrough called the exponential
// WaitForBlockAssemblyReady, so with a spy that always reports "behind" it would
// either call gRPC (calls > 0) or block on 20ms->...->5s backoff and never see
// the cache advance. GREEN: zero gRPC calls, returns within ~one recheck interval
// of the cache satisfying the bound.
func TestWaitForBlockAssemblyReadyCached_BelowCP_ReleasesOnCacheAdvance(t *testing.T) {
	spy := &stateSpy{}
	// Spy reports a height that never clears the bound, so any use of the
	// exponential fresh-gRPC path would spin/backoff or error rather than
	// release. The release must come from the cache only.
	spy.height.Store(1) // 1 + 100 = 101 << blockHeight below
	sm := newCacheTestManager(t, spy, 100)

	const blockHeight = uint32(1500)
	// Initially behind: cached(1000) + maxBehind(100) = 1100 < 1500.
	sm.cachedBlockAssemblyHeight.Store(1000)
	sm.baHeightPolled.Store(true)

	// Simulate the background poller advancing the cache past the bound after a
	// short delay (well under the old 5s exponential step).
	go func() {
		time.Sleep(150 * time.Millisecond)
		sm.cachedBlockAssemblyHeight.Store(1400) // 1400 + 100 = 1500 >= 1500
		sm.baHeightPolled.Store(true)
	}()

	start := time.Now()
	err := sm.waitForBlockAssemblyReadyCached(context.Background(), blockHeight)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, int64(0), spy.calls.Load(),
		"below-checkpoint recheck must poll the cache, never the exponential fresh-gRPC path")
	// Must release shortly after the cache advances (~150ms + one recheck), and
	// well before the old exponential path's 5s step.
	require.Less(t, elapsed, 2*time.Second,
		"must release promptly on cache advance, not on exponential backoff")
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond,
		"cannot release before the cache actually advances")
}

// TestWaitForBlockAssemblyReadyCached_BelowCP_BoundedEscalation proves the wait
// is bounded: if the cache never advances (a genuine block-assembly stall), the
// below-checkpoint recheck loop does not hang — it returns an error after the
// bounded max wait so the existing recover/escalation path can fire.
//
// RED against a naive fix that loops forever on the cache: that would hang here.
func TestWaitForBlockAssemblyReadyCached_BelowCP_BoundedEscalation(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(1) // fresh gRPC would also never clear the bound
	sm := newCacheTestManager(t, spy, 100)

	const blockHeight = uint32(1500)
	// Behind and never advances.
	sm.cachedBlockAssemblyHeight.Store(1000)
	sm.baHeightPolled.Store(true)

	done := make(chan error, 1)
	go func() {
		done <- sm.waitForBlockAssemblyReadyCached(context.Background(), blockHeight)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a genuine stall must escalate with an error, not pass")
		require.Equal(t, int64(0), spy.calls.Load(),
			"below-checkpoint bounded wait polls the cache, never the fresh gRPC")
	case <-time.After(windowMaturityMaxWait + 5*time.Second):
		t.Fatal("below-checkpoint recheck hung: bounded escalation did not fire")
	}
}

// TestWaitForBlockAssemblyReadyCached_BelowCP_CtxCancelReturns proves the loop is
// context-aware: cancelling ctx while waiting returns promptly (does not wait out
// the full bounded max wait).
func TestWaitForBlockAssemblyReadyCached_BelowCP_CtxCancelReturns(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(1)
	sm := newCacheTestManager(t, spy, 100)

	const blockHeight = uint32(1500)
	sm.cachedBlockAssemblyHeight.Store(1000)
	sm.baHeightPolled.Store(true)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- sm.waitForBlockAssemblyReadyCached(ctx, blockHeight)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err, "ctx cancel while waiting must return an error")
	case <-time.After(2 * time.Second):
		t.Fatal("recheck loop did not observe ctx cancellation")
	}
}

// TestWaitForBlockAssemblyReadyCached_BelowCP_QuitReturns proves the loop watches
// sm.quit (shutdown) and returns promptly on close.
func TestWaitForBlockAssemblyReadyCached_BelowCP_QuitReturns(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(1)
	sm := newCacheTestManager(t, spy, 100)

	const blockHeight = uint32(1500)
	sm.cachedBlockAssemblyHeight.Store(1000)
	sm.baHeightPolled.Store(true)

	done := make(chan error, 1)
	go func() {
		done <- sm.waitForBlockAssemblyReadyCached(context.Background(), blockHeight)
	}()

	time.Sleep(100 * time.Millisecond)
	close(sm.quit)

	select {
	case err := <-done:
		require.Error(t, err, "closing quit while waiting must return an error")
	case <-time.After(2 * time.Second):
		t.Fatal("recheck loop did not observe sm.quit")
	}
}
