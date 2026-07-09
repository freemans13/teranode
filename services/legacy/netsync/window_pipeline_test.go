package netsync

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/stretchr/testify/require"
)

// pipelineTestTimeout bounds every blocking receive in the pipeline concurrency
// tests so a missing hand-off or commit surfaces as a clear failure rather than
// a hung test run.
const pipelineTestTimeout = 2 * time.Second

// pipelineSpyBlockValidation records the order and content of every
// ProcessBlockWindow commit and can gate each commit on a per-call release
// channel supplied by the test, so a test can hold a window's commit open while
// asserting that the drain side keeps making progress (fill overlaps commit).
//
// It can also be told to fail a specific window's commit unrecoverably (fatal
// error on both ProcessBlockWindow and every recovery ProcessBlock) to exercise
// the poison-and-drain fatal path.
type pipelineSpyBlockValidation struct {
	blockvalidation.MockBlockValidation

	mu sync.Mutex
	// committedFirstHeights records, in commit order, the height of the first
	// block of each window that ProcessBlockWindow was called with.
	committedFirstHeights []uint32

	// entered is signalled (non-blocking best-effort) as each ProcessBlockWindow
	// call is entered, carrying the first-block height of that window.
	entered chan uint32

	// gate, when non-nil, blocks every ProcessBlockWindow call until the test
	// sends on it (one send released per commit).
	gate chan struct{}

	// fatalFirstHeight, when non-zero, makes the window whose first block is at
	// that height fail unrecoverably (fatal on ProcessBlockWindow and recovery).
	fatalFirstHeight uint32

	processWindowCalls atomic.Int32
}

func (s *pipelineSpyBlockValidation) ProcessBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	s.processWindowCalls.Add(1)

	var first uint32
	if len(blocks) > 0 {
		first = blocks[0].Height
	}

	if s.entered != nil {
		select {
		case s.entered <- first:
		default:
		}
	}

	if s.gate != nil {
		<-s.gate
	}

	if s.fatalFirstHeight != 0 && first == s.fatalFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal window commit")
	}

	s.mu.Lock()
	s.committedFirstHeights = append(s.committedFirstHeights, first)
	s.mu.Unlock()

	return nil
}

// ProcessBlock backs recoverWindowCommit. For the fatal window it always returns
// a non-infra (fatal) error so recovery escalates immediately.
func (s *pipelineSpyBlockValidation) ProcessBlock(_ context.Context, block *model.Block, _ uint32, _, _ string, _ uint32) error {
	if s.fatalFirstHeight != 0 && block != nil && block.Height == s.fatalFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal recovery")
	}

	return nil
}

func (s *pipelineSpyBlockValidation) committedOrder() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]uint32, len(s.committedFirstHeights))
	copy(out, s.committedFirstHeights)

	return out
}

var _ blockvalidation.Interface = (*pipelineSpyBlockValidation)(nil)

// buildWindowJob drains a fresh accumulator seeded with n blocks starting at
// firstHeight into a windowFlushJob.
func buildWindowJob(t *testing.T, firstHeight uint32, n int) windowFlushJob {
	t.Helper()

	wa := newWindowAccumulator(1<<40, 100)
	for i := 0; i < n; i++ {
		wa.add(newMinimalModelBlock(t, firstHeight+uint32(i)))
	}

	job, ok := wa.drainJob()
	require.True(t, ok, "drainJob must return a job for a non-empty accumulator")
	require.True(t, wa.empty(), "drainJob must drain the accumulator")

	return job
}

// TestPipeline_DrainJob_EmptyAccumulator asserts drainJob reports ok=false for an
// empty accumulator (the pipeline-off wrapper relies on this to skip commits).
func TestPipeline_DrainJob_EmptyAccumulator(t *testing.T) {
	wa := newWindowAccumulator(1<<40, 20)

	_, ok := wa.drainJob()
	require.False(t, ok, "empty accumulator must yield ok=false")
}

// TestPipeline_DrainJob_SortsAscending asserts drainJob sorts the drained blocks
// ascending by height regardless of arrival order.
func TestPipeline_DrainJob_SortsAscending(t *testing.T) {
	wa := newWindowAccumulator(1<<40, 20)
	wa.add(newMinimalModelBlock(t, 503))
	wa.add(newMinimalModelBlock(t, 501))
	wa.add(newMinimalModelBlock(t, 502))

	job, ok := wa.drainJob()
	require.True(t, ok)
	require.Len(t, job.blocks, 3)
	require.Equal(t, uint32(501), job.blocks[0].Height)
	require.Equal(t, uint32(502), job.blocks[1].Height)
	require.Equal(t, uint32(503), job.blocks[2].Height)
}

// TestPipeline_Overlap is the core property: with a gated commit, the worker is
// stuck committing window 1 while the drain side is free to build and hand off
// window 2. Against a synchronous flush the drain side would itself be blocked
// inside the commit and could not build window 2, so this test would deadlock /
// time out. With the async worker, window 2 is built and the second commit is
// observed to enter only after we release window 1.
func TestPipeline_Overlap(t *testing.T) {
	spy := &pipelineSpyBlockValidation{
		gate:    make(chan struct{}),
		entered: make(chan uint32, 4),
	}
	sm := newAckTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	go sm.flushWorker(sm.ctx, jobs)

	// Hand off window 1 (worker will enter its commit and block on the gate).
	jobs <- buildWindowJob(t, 500, 2)

	// Confirm the worker entered window 1's commit.
	select {
	case first := <-spy.entered:
		require.Equal(t, uint32(500), first, "worker must enter window 1 commit first")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter window 1 commit")
	}

	// The drain side (this goroutine, standing in for it) is FREE to build and
	// hand off window 2 into the depth-1 channel even though window 1 is still
	// committing. This is the overlap the pipeline exists to create.
	handoffDone := make(chan struct{})
	go func() {
		jobs <- buildWindowJob(t, 600, 2)
		close(handoffDone)
	}()

	select {
	case <-handoffDone:
		// window 2 handed into the buffered slot while window 1 still committing.
	case <-time.After(pipelineTestTimeout):
		t.Fatal("could not hand off window 2 while window 1 commit in flight (no overlap)")
	}

	// Window 2's commit must NOT have started while window 1 is gated (FIFO).
	select {
	case <-spy.entered:
		t.Fatal("window 2 committed before window 1 released — FIFO violated")
	case <-time.After(50 * time.Millisecond):
	}

	// Release window 1; window 2's commit must then enter.
	spy.gate <- struct{}{}
	spy.gate <- struct{}{}

	select {
	case first := <-spy.entered:
		require.Equal(t, uint32(600), first, "window 2 commit must enter after window 1 released")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("window 2 commit did not enter after window 1 released")
	}

	close(jobs)
}

// TestPipeline_FIFOOrder asserts windows commit in the exact order they were
// handed to the worker.
func TestPipeline_FIFOOrder(t *testing.T) {
	spy := &pipelineSpyBlockValidation{}
	sm := newAckTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)

	done := make(chan struct{})
	go func() {
		sm.flushWorker(sm.ctx, jobs)
		close(done)
	}()

	starts := []uint32{500, 520, 540, 560}
	for _, h := range starts {
		jobs <- buildWindowJob(t, h, 3)
	}

	close(jobs)

	select {
	case <-done:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not drain all jobs")
	}

	require.Equal(t, starts, spy.committedOrder(), "windows must commit in produced (ascending) order")
}

// TestPipeline_Depth1BackPressure proves the depth-1 channel bounds in-flight
// windows to two: with the worker gated on window 1, the drain side can hand off
// window 2 (into the buffered slot) but a THIRD hand-off blocks until window 1
// commits and frees the slot.
func TestPipeline_Depth1BackPressure(t *testing.T) {
	spy := &pipelineSpyBlockValidation{
		gate:    make(chan struct{}),
		entered: make(chan uint32, 4),
	}
	sm := newAckTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	go sm.flushWorker(sm.ctx, jobs)

	// Window 1 -> worker picks it up and blocks on the gate.
	jobs <- buildWindowJob(t, 500, 2)
	select {
	case <-spy.entered:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter window 1 commit")
	}

	// Window 2 -> occupies the single buffered slot (non-blocking).
	jobs <- buildWindowJob(t, 600, 2)

	// Window 3 -> must BLOCK: worker busy on window 1, slot holds window 2.
	third := make(chan struct{})
	go func() {
		jobs <- buildWindowJob(t, 700, 2)
		close(third)
	}()

	select {
	case <-third:
		t.Fatal("third hand-off succeeded while channel full — back-pressure violated")
	case <-time.After(100 * time.Millisecond):
		// expected: blocked
	}

	// Release commits; the third hand-off must now unblock.
	spy.gate <- struct{}{} // window 1
	spy.gate <- struct{}{} // window 2

	select {
	case <-third:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("third hand-off did not unblock after commits drained")
	}

	spy.gate <- struct{}{} // window 3

	close(jobs)
}

// TestPipeline_FatalDiscardsQueuedNoGap is the consensus-critical proof: when
// window W's commit fails unrecoverably, the worker poisons itself, disconnects
// the sync peer, and commits NO already-queued later window. The committed set
// must contain neither W nor W+1 (a committed gap would be a consensus bug).
func TestPipeline_FatalDiscardsQueuedNoGap(t *testing.T) {
	const fatalFirst = uint32(500)

	spy := &pipelineSpyBlockValidation{
		gate:             make(chan struct{}),
		entered:          make(chan uint32, 4),
		fatalFirstHeight: fatalFirst,
	}
	sm := newAckTestSyncManager(t, spy)
	syncP := newConnectedSyncPeer(t, sm)

	jobs := make(chan windowFlushJob, 1)

	done := make(chan struct{})
	go func() {
		sm.flushWorker(sm.ctx, jobs)
		close(done)
	}()

	// Window W (fatal) -> worker enters and blocks on the gate.
	jobs <- buildWindowJob(t, fatalFirst, 2)
	select {
	case first := <-spy.entered:
		require.Equal(t, fatalFirst, first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter fatal window commit")
	}

	// Queue window W+1 into the buffered slot BEFORE releasing the fatal commit,
	// so it is already in flight when W fails. It must never be committed.
	jobs <- buildWindowJob(t, 600, 2)

	// Release the fatal commit. Recovery runs (fatal ProcessBlock) and escalates.
	spy.gate <- struct{}{}

	// The next window (W+1) must NOT enter ProcessBlockWindow — the worker is
	// poisoned and drains it without committing. If it did enter, it would block
	// on the gate; we assert it does not enter within a window.
	select {
	case first := <-spy.entered:
		t.Fatalf("poisoned worker committed queued window (first height %d) after a fatal gap", first)
	case <-time.After(200 * time.Millisecond):
		// expected: W+1 discarded, not committed
	}

	close(jobs)
	select {
	case <-done:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not exit after channel close")
	}

	require.False(t, syncP.Connected(),
		"fatal window commit must disconnect the sync peer to rotate the pipeline")

	committed := spy.committedOrder()
	require.NotContains(t, committed, fatalFirst, "fatal window W must not be recorded as committed")
	require.NotContains(t, committed, uint32(600), "queued window W+1 must not be committed after a fatal gap")
	require.Empty(t, committed, "no window may commit once a fatal gap occurs")
}

// TestPipeline_ShutdownCtxDone_NoCommitOfBuffered isolates the flushWorker's
// ctx.Done() branch (distinct from the channel-close path): when the worker's
// ctx is cancelled and a later window is subsequently buffered in jobs, the
// worker must DRAIN and DISCARD that buffered window WITHOUT committing it, then
// exit on channel close. Under a normal Stop() this branch never fires (Stop
// closes sm.quit, not sm.ctx); this test drives the branch directly.
//
// To isolate the ctx.Done() branch deterministically we make jobs EMPTY at the
// top-of-loop select at the moment of cancellation: window 1 is gated inside its
// commit (worker not in the select), we cancel, release window 1 so the worker
// returns to the select with an empty channel and a done ctx (ctx.Done() is then
// the sole ready case), and only THEN buffer window 2 for the drain loop to
// discard.
func TestPipeline_ShutdownCtxDone_NoCommitOfBuffered(t *testing.T) {
	before := runtime.NumGoroutine()

	spy := &pipelineSpyBlockValidation{
		gate:    make(chan struct{}),
		entered: make(chan uint32, 4),
	}
	sm := newAckTestSyncManager(t, spy)

	ctx, cancel := context.WithCancel(context.Background())

	jobs := make(chan windowFlushJob, 1)

	done := make(chan struct{})
	go func() {
		sm.flushWorker(ctx, jobs)
		close(done)
	}()

	// Window 1 -> worker enters its commit and blocks on the gate (it is now
	// inside ProcessBlockWindow, NOT in the top-of-loop select).
	jobs <- buildWindowJob(t, 500, 2)
	select {
	case first := <-spy.entered:
		require.Equal(t, uint32(500), first, "worker must be mid-commit on window 1")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter window 1 commit")
	}

	// Cancel while the worker is parked in the gated commit (jobs is empty).
	cancel()

	// Release window 1: its commit completes and is recorded, then the worker
	// returns to the top-of-loop select where jobs is empty and ctx is done, so
	// it deterministically takes the ctx.Done() branch and enters the range-drain.
	spy.gate <- struct{}{}

	// Wait until window 1's commit is recorded: the worker has now left the
	// commit and, with jobs empty and ctx done, takes ctx.Done() and parks in the
	// range-drain loop. Only after that do we buffer window 2, so it is delivered
	// straight to the drain loop (read+discarded), never to a competing select.
	require.Eventually(t, func() bool {
		return len(spy.committedOrder()) == 1
	}, pipelineTestTimeout, time.Millisecond, "window 1 commit must record before we buffer window 2")

	// Buffer window 2 for the ctx.Done() drain loop to read and DISCARD (never
	// commit), then close so the drain loop terminates and the worker returns.
	jobs <- buildWindowJob(t, 600, 2)
	close(jobs)

	select {
	case <-done:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not exit after ctx cancel + channel close")
	}

	// Window 1 (accepted before cancel) commits; window 2, buffered after cancel,
	// is drained and discarded WITHOUT committing on the ctx.Done() path.
	committed := spy.committedOrder()
	require.Equal(t, []uint32{500}, committed,
		"ctx.Done() must drain the later-buffered window WITHOUT committing it")
	require.NotContains(t, committed, uint32(600),
		"buffered window must never be committed on the ctx.Done() shutdown path")

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+1
	}, pipelineTestTimeout, 10*time.Millisecond, "worker goroutine leaked after ctx-cancel shutdown")
}

// TestPipeline_PromptShutdown_AbandonsPendingWindow proves the fix: when
// shutdown lands while the single worker is mid-commit (window 1) AND the
// depth-1 slot is already occupied (window 2), the drain goroutine's shutdown
// hand-off (shutdownFlushHandoff) must NOT block on the full slot. It abandons
// the pending window, closes jobs, and returns promptly; the worker then exits
// cleanly once its in-flight commit completes and the channel is drained. The
// abandoned window is never committed.
//
// Against the old blocking hand-off (jobs <- j) this would block for up to a
// full per-block commit deadline, so this test times out RED before the fix.
func TestPipeline_PromptShutdown_AbandonsPendingWindow(t *testing.T) {
	before := runtime.NumGoroutine()

	spy := &pipelineSpyBlockValidation{
		gate:    make(chan struct{}),
		entered: make(chan uint32, 4),
	}
	sm := newAckTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)

	workerDone := make(chan struct{})
	go func() {
		sm.flushWorker(sm.ctx, jobs)
		close(workerDone)
	}()

	// Window 1 -> worker picks it up and blocks mid-commit on the gate.
	jobs <- buildWindowJob(t, 500, 2)
	select {
	case first := <-spy.entered:
		require.Equal(t, uint32(500), first, "worker must be mid-commit on window 1")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter window 1 commit")
	}

	// Window 2 -> occupies the single buffered slot. The slot is now FULL while
	// the worker is still committing window 1.
	jobs <- buildWindowJob(t, 600, 2)

	// A pending window 3 sits in the accumulator at shutdown time. The slot is
	// full and the worker is busy, so the hand-off must abandon it, not block.
	wa := newWindowAccumulator(1<<40, 20)
	wa.add(newMinimalModelBlock(t, 700))
	wa.add(newMinimalModelBlock(t, 701))

	// Drive the REAL production shutdown path and assert it returns PROMPTLY
	// (does not block on the full slot for the per-block commit deadline).
	handoffReturned := make(chan struct{})
	go func() {
		sm.shutdownFlushHandoff(wa, jobs)
		close(handoffReturned)
	}()

	select {
	case <-handoffReturned:
		// prompt: the hand-off abandoned the pending window instead of blocking.
	case <-time.After(pipelineTestTimeout):
		t.Fatal("shutdown hand-off blocked on the full worker slot (not prompt)")
	}

	// The pending window 3 was abandoned (dropped), so the accumulator drained it.
	require.True(t, wa.empty(), "shutdown hand-off must drain the pending window from the accumulator")

	// Let the worker finish window 1 and then commit the already-queued window 2,
	// then exit on the (now closed) channel. No panic on send-after-close because
	// only the drain path closes jobs and it has already returned.
	spy.gate <- struct{}{} // release window 1
	spy.gate <- struct{}{} // release window 2

	select {
	case <-workerDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not exit cleanly after in-flight commit + channel close")
	}

	// Windows 1 and 2 were already in the pipeline and commit normally; the
	// ABANDONED window 3 must never be committed.
	committed := spy.committedOrder()
	require.NotContains(t, committed, uint32(700),
		"abandoned pending window must never be committed (it is re-synced on restart)")

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+1
	}, pipelineTestTimeout, 10*time.Millisecond, "worker goroutine leaked after prompt shutdown")
}

// TestPipeline_FlagOff_SynchronousFlush proves the pipeline sub-flag default
// (false) preserves the byte-identical synchronous path: flush commits inline on
// the calling goroutine via drainJob + commitWindowJob, and no worker is needed.
func TestPipeline_FlagOff_SynchronousFlush(t *testing.T) {
	tSettings, _ := newOutpointOnlySettings(t, true, true, int32(1000))
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	require.False(t, tSettings.Legacy.ParallelWindowPipeline,
		"pipeline sub-flag must default to false")

	spy := &pipelineSpyBlockValidation{}
	sm := newAckTestSyncManager(t, spy)

	wa := newWindowAccumulator(1<<40, 20)
	wa.add(newMinimalModelBlock(t, 500))
	wa.add(newMinimalModelBlock(t, 501))

	// Synchronous flush: commits inline, drains the accumulator, no goroutine.
	wa.flush(sm.ctx, sm)

	require.True(t, wa.empty(), "synchronous flush must drain the accumulator")
	require.Equal(t, int32(1), spy.processWindowCalls.Load(),
		"synchronous flush must commit exactly one window inline")
	require.Equal(t, []uint32{500}, spy.committedOrder())
}
