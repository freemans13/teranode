package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// cacheTestCheckpointHeight is the highest hardcoded checkpoint used by the
// cache-test SyncManager: blocks at or below it are inside the reorg-safe prefix
// where the cached-height fast path is allowed; blocks above it must always take
// the fresh-gRPC slow path.
const cacheTestCheckpointHeight = 100_000

// stateSpy is a minimal blockassembly.ClientI that counts GetBlockAssemblyState
// calls and returns a configurable height/error. All other interface methods are
// inherited from the embedded (nil) interface and must not be invoked by the code
// under test — the cached maturity check only ever calls GetBlockAssemblyState.
type stateSpy struct {
	blockassembly.ClientI // embedded nil: only GetBlockAssemblyState is exercised

	calls  atomic.Int64
	height atomic.Uint32
	err    atomic.Bool
}

func (s *stateSpy) GetBlockAssemblyState(_ context.Context) (*blockassembly_api.StateMessage, error) {
	s.calls.Add(1)

	if s.err.Load() {
		return nil, errors.NewProcessingError("spy: forced error")
	}

	return &blockassembly_api.StateMessage{CurrentHeight: s.height.Load()}, nil
}

// newCacheTestManager builds a bare SyncManager with just the fields the cached
// maturity check and poller touch.
func newCacheTestManager(t *testing.T, ba blockassembly.ClientI, maxBehind int) *SyncManager {
	t.Helper()

	s := &settings.Settings{}
	s.BlockValidation.MaxBlocksBehindBlockAssembly = maxBehind

	// A single hardcoded checkpoint so model.BelowCheckpoint has a real boundary:
	// heights at or below cacheTestCheckpointHeight are in the reorg-safe prefix
	// where the fast path may engage; heights above it must take the slow path.
	params := &chaincfg.Params{
		Checkpoints: []chaincfg.Checkpoint{{Height: cacheTestCheckpointHeight}},
	}

	sm := &SyncManager{
		ctx:           context.Background(),
		logger:        ulogger.TestLogger{},
		settings:      s,
		chainParams:   params,
		blockAssembly: ba,
		quit:          make(chan struct{}),
	}

	// Model a poller that has reported (the common case). Tests exercising the
	// pre-poll state clear this explicitly — a cached height of 0 is a REAL
	// height (fresh node at genesis), not an unpolled sentinel.
	sm.baHeightPolled.Store(true)

	return sm
}

// TestWaitForBlockAssemblyReadyCached_FastPath proves that when the cached
// height already satisfies the bound, the per-block check returns nil WITHOUT
// any gRPC round-trip (zero GetBlockAssemblyState calls).
func TestWaitForBlockAssemblyReadyCached_FastPath(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 100)

	// cached(1000) + maxBehind(100) = 1100 >= blockHeight(1050): bound holds.
	sm.cachedBlockAssemblyHeight.Store(1000)

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), 1050)
	require.NoError(t, err)
	require.Equal(t, int64(0), spy.calls.Load(), "fast path must not call GetBlockAssemblyState")
}

// TestWaitForBlockAssemblyReadyCached_SlowPath proves that a block on the
// genuine remaining fresh-gRPC slow path (here ABOVE the checkpoint, where a
// reorg could leave a stale-HIGH cache so the cache is never trusted) still
// calls GetBlockAssemblyState and surfaces that path's result. Below-checkpoint
// blocks with a usable-but-behind cache no longer take this path — they use the
// fixed-interval cache-poll loop with no gRPC (see the _BelowCP_ tests).
func TestWaitForBlockAssemblyReadyCached_SlowPath(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(cacheTestCheckpointHeight + 2000) // fresh gRPC clears the bound
	sm := newCacheTestManager(t, spy, 100)

	// Above-checkpoint block: cache is bypassed regardless of its value, so the
	// fresh gRPC is used and reports a satisfying height.
	aboveCP := uint32(cacheTestCheckpointHeight + 1500)
	sm.cachedBlockAssemblyHeight.Store(cacheTestCheckpointHeight + 1000)

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), aboveCP)
	require.NoError(t, err)
	require.GreaterOrEqual(t, spy.calls.Load(), int64(1), "slow path must call GetBlockAssemblyState")
}

// TestWaitForBlockAssemblyReadyCached_UnpolledFallsThrough proves a zero
// (never-polled) cache forces the slow path rather than passing on cached==0.
func TestWaitForBlockAssemblyReadyCached_UnpolledFallsThrough(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(5000)
	sm := newCacheTestManager(t, spy, 100)

	// Never polled: modelled by the flag (a zero HEIGHT with a reported poller
	// is a real genesis height and legitimately serves the fast path).
	sm.baHeightPolled.Store(false)

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), 100)
	require.NoError(t, err)
	require.GreaterOrEqual(t, spy.calls.Load(), int64(1), "unpolled cache must take the slow path")
}

// TestWaitForBlockAssemblyReadyCached_StaleLowSafe proves the safety invariant is
// preserved by the cache-poll path: a stale-LOW cache (cached < true height) that
// does not itself clear the bound must NOT pass immediately — the gate only passes
// when the cached lower bound satisfies cached+maxBehind >= blockHeight. Below the
// checkpoint the wait now polls the (monotonic, stale-low) cache instead of the
// fresh gRPC, so it must NOT call GetBlockAssemblyState and must release only once
// the cache itself advances past the bound.
func TestWaitForBlockAssemblyReadyCached_StaleLowSafe(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(1490) // true height clears the bound, but the gRPC must be unused
	sm := newCacheTestManager(t, spy, 100)

	// Stale-low cache: 900+100 = 1000 < 1500, so the gate must NOT pass yet.
	sm.cachedBlockAssemblyHeight.Store(900)

	// Simulate the poller catching the cache up to the true height mid-wait.
	go func() {
		time.Sleep(150 * time.Millisecond)
		sm.cachedBlockAssemblyHeight.Store(1490) // 1490+100 = 1590 >= 1500
	}()

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), 1500)
	require.NoError(t, err)
	require.Equal(t, int64(0), spy.calls.Load(), "stale-low cache below checkpoint must poll the cache, not the fresh gRPC")
}

// TestWaitForBlockAssemblyReadyCached_AboveCheckpointForcesSlowPath proves the
// reorg-safety restriction: for a block ABOVE the highest hardcoded checkpoint
// the fast path must NOT engage even when the cached height would satisfy the
// bound, because a reorg could lower block-assembly height and leave a stale-HIGH
// cache. The check must defer to the fresh gRPC (GetBlockAssemblyState is called).
func TestWaitForBlockAssemblyReadyCached_AboveCheckpointForcesSlowPath(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(cacheTestCheckpointHeight + 10) // fresh gRPC clears the bound
	sm := newCacheTestManager(t, spy, 100)

	// Cache would satisfy the bound if the fast path were allowed:
	// cached+maxBehind = (above-cp+5)+100 >= (above-cp+1). But the block is above
	// the checkpoint, so the fast path is skipped regardless.
	aboveCP := uint32(cacheTestCheckpointHeight + 1)
	sm.cachedBlockAssemblyHeight.Store(cacheTestCheckpointHeight + 5)

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), aboveCP)
	require.NoError(t, err)
	require.GreaterOrEqual(t, spy.calls.Load(), int64(1), "above-checkpoint block must take the fresh-gRPC slow path")
}

// TestWaitForBlockAssemblyReadyCached_BelowCheckpointTakesFastPath is the
// counterpart: an at/below-checkpoint block with a satisfying cache takes the
// fast path (no GetBlockAssemblyState call).
func TestWaitForBlockAssemblyReadyCached_BelowCheckpointTakesFastPath(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 100)

	// blockHeight == checkpoint height (inclusive boundary); cache clears the bound.
	belowCP := uint32(cacheTestCheckpointHeight)
	sm.cachedBlockAssemblyHeight.Store(cacheTestCheckpointHeight)

	err := sm.waitForBlockAssemblyReadyCached(context.Background(), belowCP)
	require.NoError(t, err)
	require.Equal(t, int64(0), spy.calls.Load(), "below-checkpoint block with satisfying cache must take the fast path")
}

// TestBlockAssemblyHeightPoller_PopulatesAndRefreshes proves the poller fills
// the cache from GetBlockAssemblyState and picks up subsequent height changes,
// and that it stops cleanly on shutdown with no leaked goroutine.
func TestBlockAssemblyHeightPoller_PopulatesAndRefreshes(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(4200)
	sm := newCacheTestManager(t, spy, 100)

	done := make(chan struct{})
	go func() {
		sm.blockAssemblyHeightPoller(sm.ctx)
		close(done)
	}()

	// Primed immediately on start.
	require.Eventually(t, func() bool {
		return sm.cachedBlockAssemblyHeight.Load() == 4200
	}, 2*time.Second, 5*time.Millisecond, "poller should prime the cache")

	// Height moves; poller should refresh within a few intervals.
	spy.height.Store(4250)
	require.Eventually(t, func() bool {
		return sm.cachedBlockAssemblyHeight.Load() == 4250
	}, 2*time.Second, 5*time.Millisecond, "poller should refresh the cache")

	// Shutdown: closing quit must stop the poller cleanly (no leak).
	close(sm.quit)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop on quit (goroutine leak)")
	}
}

// TestBlockAssemblyHeightPoller_KeepsLastOnError proves a poll error is logged
// and leaves the previously cached height untouched (never zeroed).
func TestBlockAssemblyHeightPoller_KeepsLastOnError(t *testing.T) {
	spy := &stateSpy{}
	spy.height.Store(7000)
	sm := newCacheTestManager(t, spy, 100)

	done := make(chan struct{})
	go func() {
		sm.blockAssemblyHeightPoller(sm.ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return sm.cachedBlockAssemblyHeight.Load() == 7000
	}, 2*time.Second, 5*time.Millisecond, "poller should prime the cache")

	// Force errors; cache must retain 7000 across several intervals.
	spy.err.Store(true)
	require.Never(t, func() bool {
		return sm.cachedBlockAssemblyHeight.Load() != 7000
	}, 3*blockAssemblyHeightPollInterval, 5*time.Millisecond, "error must not change the cached height")

	close(sm.quit)
	<-done
}
