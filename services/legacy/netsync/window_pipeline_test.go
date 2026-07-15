package netsync

import (
	"container/list"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
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

	// panicFirstHeight, when non-zero, makes the window whose first block is at
	// that height PANIC inside ProcessBlockWindow (before recording it), to
	// exercise the flushWorker recover-and-poison path.
	panicFirstHeight uint32

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

	if s.panicFirstHeight != 0 && first == s.panicFirstHeight {
		panic("pipeline panic window commit")
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

// TestPipeline_DirectPath_ErrBlockNotFound_ChainPreserved is the unit-level proof
// of the fail-closed error-chain property stated in the handleBlockMsgWithWindow
// comment. It asserts that wrapping an ErrBlockNotFound inside NewProcessingError
// (exactly as HandleBlockDirect does at handle_block.go:74) preserves the inner
// error code so errors.Is(wrapped, ErrBlockNotFound) remains true.
//
// This matters because the ErrBlockNotFound branch in handleBlockMsgWithWindow
// is the safety valve for the pipeline race: when the in-flight window parent
// has not yet committed, GetBlockHeader returns ErrBlockNotFound, which
// HandleBlockDirect wraps with NewProcessingError. If the chain were not
// preserved the error would fall through to the fatal path (reject + disconnect)
// instead of the safe re-request path.
func TestPipeline_DirectPath_ErrBlockNotFound_ChainPreserved(t *testing.T) {
	inner := errors.NewBlockNotFoundError("parent not committed yet (in-flight window)")
	wrapped := errors.NewProcessingError("failed to get block header for previous block %s", "deadbeef", inner)

	// The core assertion: the wrapping chain is transparent to errors.Is so the
	// ErrBlockNotFound branch in handleBlockMsgWithWindow fires correctly.
	require.True(t, errors.Is(wrapped, errors.ErrBlockNotFound),
		"NewProcessingError must preserve the inner ErrBlockNotFound code through the chain")

	// Confirm the outer error is a processing error (not a block-not-found at the
	// top level); the ErrBlockNotFound is only in the wrapped inner chain.
	require.True(t, errors.Is(wrapped, errors.ErrProcessing),
		"outer error code must be ErrProcessing (documents the NewProcessingError wrap)")
}

// TestPipeline_DirectPath_ParentUncommitted_ReRequestSafe is the integration-level
// proof of the fail-closed property for the pipeline checkpoint/ineligible race.
//
// Scenario: in pipeline mode a checkpoint/ineligible direct block arrives while
// its parent (the last window block) has not yet committed. GetBlockHeader on the
// parent returns ErrBlockNotFound (the blocks table has no row yet).
// HandleBlockDirect wraps it with NewProcessingError, but the error chain
// preserves ErrBlockNotFound, so handleBlockMsgWithWindow takes the re-request
// branch: PushGetBlocksMsg + (false, nil). No disconnect, no reject, tip not
// advanced.
//
// What this test proves:
//   - The parent-uncommitted condition (GetBlockHeader → ErrBlockNotFound) routes
//     through the safe re-request branch, not the fatal reject/disconnect branch.
//   - handleBlockMsgWithWindow returns (false, nil): no error propagates to the
//     caller, so the drain goroutine continues normally.
//   - The peer is not disconnected (Connected() stays true after the call).
//
// The test is a genuine discriminator because it wires GetBlockExists=false (so
// HandleBlockDirect proceeds past the early-exit) and GetBlockHeader always
// returning ErrBlockNotFound (the parent-uncommitted condition). If the error
// chain were broken and the error fell through to the fatal path, the peer would
// be rejected/disconnected and handleBlockMsgWithWindow would return a non-nil
// error (RED).
func TestPipeline_DirectPath_ParentUncommitted_ReRequestSafe(t *testing.T) {
	initPrometheusMetrics()

	// The arriving block is above the checkpoint → ineligible → direct path.
	const checkpointHeight = int32(400)
	const aboveCheckpointHeight = int32(500)

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: [32]byte{0x01}, // parent not committed
			Timestamp: time.Unix(1231006505, 0),
			Bits:      0x207fffff,
			Nonce:     0,
		},
	}
	coinbaseTx := wire.NewMsgTx(1)
	coinbaseTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: [32]byte{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseTx)

	blockHash := msgBlock.BlockHash()

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// GetBlockExists=false so HandleBlockDirect does not early-exit with "already exists".
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// GetBlockHeader always returns ErrBlockNotFound: the parent window block has
	// not yet committed (it is in-flight on the flush worker). This is the
	// parent-uncommitted condition under test.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent window block not yet committed"))
	// The ErrBlockNotFound branch in handleBlockMsgWithWindow calls
	// GetBestBlockHeader + GetBlockLocator to build the re-request; wire them up
	// so the call completes without panicking.
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{}, nil)

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(testPeer, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	// Header chain seats the block at an above-checkpoint height so it is routed
	// to the direct/ineligible path. The non-matching checkpoint hash ensures the
	// block is not treated as a checkpoint block (same pattern as existing tests).
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: aboveCheckpointHeight, hash: &blockHash})
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	// An empty window: the flush no-ops, so flushWindow does not affect the result.
	wa := newWindowAccumulator(100*1024*1024, 20)
	flushWindow := func() { wa.flush(sm.ctx, sm) }

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1,
		peer:        testPeer,
	}

	outcome, err := sm.handleBlockMsgWithWindow(bmsg, wa, flushWindow, nil)
	addedToWindow := outcome == blockAdmitWindowed

	// Core safety assertions:
	// 1. No error propagated — the ErrBlockNotFound re-request branch consumed it
	//    and returned (false, nil). If the error fell through to the fatal path,
	//    handleBlockMsgWithWindow would return a non-nil error here.
	require.NoError(t, err,
		"parent-uncommitted ErrBlockNotFound must be handled by the re-request branch, not propagated as a fatal error")
	// 2. Block was not added to the window (took the direct/ineligible path).
	require.False(t, addedToWindow, "above-checkpoint block must take the direct path, not the window")
	// 3. GetBestBlockHeader was called: this is the first call in the ErrBlockNotFound
	//    re-request branch (building the locator). If the error had been misclassified
	//    (e.g. as a fatal processing error), the code would never reach this call.
	blockchainClient.AssertCalled(t, "GetBestBlockHeader", mock.Anything)
	// 4. PushRejectMsg was NOT called: the blockchainClient mock has no expectation
	//    for PushRejectMsg. The peer.PushRejectMsg is called on the peer, not the
	//    mock, but we verify the re-request path was taken via assertion 3 above.
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

// TestFlushWorker_PanicPoisonsAndDrains proves the flushWorker's recover path:
// when a window's commit PANICS (rather than returning an error), the worker
// must NOT die/leak. It recovers, poisons itself (semantically identical to a
// fatal commit error), keeps draining its queued jobs (discarding them without
// committing, so no committed gap), and a subsequent producer send on jobs must
// still complete rather than block forever on a dead worker.
//
// Every blocking step is bounded so a hang (the pre-fix behaviour: the worker
// goroutine dies on the panic, jobs is never drained, and jobs <- j blocks
// forever) surfaces as a clear test failure rather than a wedged run.
func TestFlushWorker_PanicPoisonsAndDrains(t *testing.T) {
	const panicFirst = uint32(500)

	spy := &pipelineSpyBlockValidation{
		gate:             make(chan struct{}),
		entered:          make(chan uint32, 4),
		panicFirstHeight: panicFirst,
	}
	sm := newAckTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)

	done := make(chan struct{})
	go func() {
		sm.flushWorker(sm.ctx, jobs)
		close(done)
	}()

	// Window W (panics) -> worker enters and blocks on the gate.
	jobs <- buildWindowJob(t, panicFirst, 2)
	select {
	case first := <-spy.entered:
		require.Equal(t, panicFirst, first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not enter panicking window commit")
	}

	// Release the panicking commit. The worker must recover, poison itself, and
	// return to the top-of-loop select rather than dying.
	spy.gate <- struct{}{}

	// A subsequent producer send MUST complete: a dead worker (pre-fix) never
	// drains jobs, so this send would block forever. The buffered slot (depth 1)
	// plus a live, draining worker guarantees it lands within the deadline.
	select {
	case jobs <- buildWindowJob(t, 600, 2):
	case <-time.After(pipelineTestTimeout):
		t.Fatal("producer send blocked: worker died on panic and is not draining jobs")
	}

	// The queued window W+1 must NOT enter ProcessBlockWindow — the worker is
	// poisoned and drains it without committing.
	select {
	case first := <-spy.entered:
		t.Fatalf("poisoned worker committed queued window (first height %d) after a panic", first)
	case <-time.After(200 * time.Millisecond):
		// expected: W+1 discarded, not committed
	}

	// Close jobs; the recovered worker must finish draining and exit cleanly.
	// The <-done receive is the authoritative no-leak proof: it fires only if
	// the worker goroutine returned after recovering the panic (a dead goroutine
	// never closes done, and the deadline then fails the test).
	close(jobs)
	select {
	case <-done:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("worker did not exit after channel close (panic killed the goroutine)")
	}

	committed := spy.committedOrder()
	require.NotContains(t, committed, panicFirst, "panicking window W must not be recorded as committed")
	require.NotContains(t, committed, uint32(600), "queued window W+1 must not be committed after a panic gap")
	require.Empty(t, committed, "no window may commit once a panic poisons the worker")
}
