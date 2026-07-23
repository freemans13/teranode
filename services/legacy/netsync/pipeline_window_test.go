package netsync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// This file tests the two-stage prepare/commit window pipeline
// (legacy_pipelineWindowCommit=true): prepareWorker (jobs -> PrepareBlockWindow
// -> commitJobs) feeding commitWorker (commitJobs -> CommitBlockWindow). The
// flag-OFF single-worker path (flushWorker -> ProcessBlockWindow) is covered by
// window_pipeline_test.go and is untouched by this change (I6); these tests
// exercise only the new flag-ON machinery added alongside it.

// twoStagePipelineSpy records the order and content of PrepareBlockWindow and
// CommitBlockWindow calls independently, with per-stage gate/entered channels
// so a test can observe and control exactly when each stage's call starts and
// returns, and can inject an unrecoverable failure into either stage by the
// first block height of the window it should fail.
type twoStagePipelineSpy struct {
	blockvalidation.MockBlockValidation

	mu                    sync.Mutex
	preparedFirstHeights  []uint32
	committedFirstHeights []uint32
	processedFirstHeights []uint32

	// prepareEntered / commitEntered are signalled (non-blocking best-effort)
	// as each PrepareBlockWindow / CommitBlockWindow call is entered, carrying
	// the first-block height of that window.
	prepareEntered chan uint32
	commitEntered  chan uint32

	// prepareGate / commitGate, when non-nil, block every PrepareBlockWindow /
	// CommitBlockWindow call until the test sends on them (one send released
	// per call).
	prepareGate chan struct{}
	commitGate  chan struct{}

	// fatalPrepareFirstHeight / fatalCommitFirstHeight, when non-zero, make
	// the window whose first block is at that height fail UNRECOVERABLY at
	// that stage (and at the bounded per-block recovery, so no rescue is
	// possible — forcing the poison path).
	fatalPrepareFirstHeight uint32
	fatalCommitFirstHeight  uint32

	// transientPrepareFirstHeight, when non-zero, makes PrepareBlockWindow
	// fail with a retryable (ErrServiceError) error for the window whose first
	// block is at that height, for the first transientPrepareFailCount calls;
	// the call after that succeeds normally. Used to prove prepare-stage
	// recovery (recoverWindowPrepare) retries PrepareBlockWindow itself rather
	// than re-driving a real commit via ProcessBlock (adversarial review
	// Findings 2/3).
	transientPrepareFirstHeight uint32
	transientPrepareFailCount   int32

	prepareCalls      atomic.Int32
	commitCalls       atomic.Int32
	processedCalls    atomic.Int32
	processBlockCalls atomic.Int32
	transientAttempts atomic.Int32
}

func windowFirstHeight(blocks []*model.Block) uint32 {
	if len(blocks) == 0 {
		return 0
	}

	return blocks[0].Height
}

func (s *twoStagePipelineSpy) PrepareBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	s.prepareCalls.Add(1)

	first := windowFirstHeight(blocks)

	if s.prepareEntered != nil {
		select {
		case s.prepareEntered <- first:
		default:
		}
	}

	if s.prepareGate != nil {
		<-s.prepareGate
	}

	if s.fatalPrepareFirstHeight != 0 && first == s.fatalPrepareFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal prepare")
	}

	if s.transientPrepareFirstHeight != 0 && first == s.transientPrepareFirstHeight {
		if s.transientAttempts.Add(1) <= s.transientPrepareFailCount {
			return errors.NewServiceError("pipeline transient prepare infra error")
		}
	}

	s.mu.Lock()
	s.preparedFirstHeights = append(s.preparedFirstHeights, first)
	s.mu.Unlock()

	return nil
}

func (s *twoStagePipelineSpy) CommitBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	s.commitCalls.Add(1)

	first := windowFirstHeight(blocks)

	if s.commitEntered != nil {
		select {
		case s.commitEntered <- first:
		default:
		}
	}

	if s.commitGate != nil {
		<-s.commitGate
	}

	if s.fatalCommitFirstHeight != 0 && first == s.fatalCommitFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal commit")
	}

	s.mu.Lock()
	s.committedFirstHeights = append(s.committedFirstHeights, first)
	s.mu.Unlock()

	return nil
}

// ProcessBlockWindow backs the FLAG-OFF single flushWorker path (it never calls
// PrepareBlockWindow/CommitBlockWindow). Recording its own call count lets a
// test distinguish "flag off routed through flushWorker/ProcessBlockWindow"
// from "flag on routed through prepareWorker+commitWorker" without needing to
// reach into unexported worker state.
func (s *twoStagePipelineSpy) ProcessBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	s.processedCalls.Add(1)

	first := windowFirstHeight(blocks)

	s.mu.Lock()
	s.processedFirstHeights = append(s.processedFirstHeights, first)
	s.mu.Unlock()

	return nil
}

// ProcessBlock backs recoverWindowCommit, which is deliberately phase-agnostic
// (the same bounded per-block recovery serves a PrepareBlockWindow OR a
// CommitBlockWindow failure). It fails fatally for the first block of a window
// this spy has been told to fail unrecoverably at either stage, so recovery can
// never rescue an injected failure in these tests.
func (s *twoStagePipelineSpy) ProcessBlock(_ context.Context, block *model.Block, _ uint32, _, _ string, _ uint32) error {
	s.processBlockCalls.Add(1)

	if block == nil {
		return nil
	}

	if s.fatalPrepareFirstHeight != 0 && block.Height == s.fatalPrepareFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal recovery")
	}

	if s.fatalCommitFirstHeight != 0 && block.Height == s.fatalCommitFirstHeight {
		return errors.NewBlockInvalidError("pipeline fatal recovery")
	}

	return nil
}

func (s *twoStagePipelineSpy) preparedOrder() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]uint32, len(s.preparedFirstHeights))
	copy(out, s.preparedFirstHeights)

	return out
}

func (s *twoStagePipelineSpy) committedOrder() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]uint32, len(s.committedFirstHeights))
	copy(out, s.committedFirstHeights)

	return out
}

func (s *twoStagePipelineSpy) processedOrder() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]uint32, len(s.processedFirstHeights))
	copy(out, s.processedFirstHeights)

	return out
}

var _ blockvalidation.Interface = (*twoStagePipelineSpy)(nil)

// newPipelineTestSyncManager builds a minimal SyncManager wired to the given
// spy with legacy_pipelineWindowCommit enabled in settings (documentation only
// for these tests — prepareWorker/commitWorker are invoked directly, not via
// the settings-gated wiring in the drain goroutine's setup).
func newPipelineTestSyncManager(t *testing.T, spy blockvalidation.Interface) *SyncManager {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, int32(1000))
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	tSettings.Legacy.PipelineWindowCommit = true

	return &SyncManager{
		ctx:             context.Background(),
		settings:        tSettings,
		chainParams:     params,
		logger:          ulogger.TestLogger{},
		utxoStore:       &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		blockValidation: spy,
	}
}

// TestPipelineWindowCommit_DefaultsFalse asserts the new flag defaults to
// false, i.e. flag-off (byte-identical single flushWorker) is what every
// deployment gets unless explicitly opted in.
func TestPipelineWindowCommit_DefaultsFalse(t *testing.T) {
	tSettings, _ := newOutpointOnlySettings(t, true, int32(1000))
	require.False(t, tSettings.Legacy.PipelineWindowCommit,
		"legacy_pipelineWindowCommit must default to false so the flag-off path is byte-identical to today")
}

// TestStartWindowFlushWorkers_FlagOff_NoTwoStageMachineryConstructed is the
// strongest form of I6 this wiring code allows testing directly (see the
// extracted startWindowFlushWorkers helper's doc comment): with
// legacy_pipelineWindowCommit false, the two-stage machinery — the commitJobs
// channel, prepareWorker, commitWorker — is never constructed at all, and jobs
// is serviced by the single flushWorker calling ProcessBlockWindow, never
// PrepareBlockWindow/CommitBlockWindow.
func TestStartWindowFlushWorkers_FlagOff_NoTwoStageMachineryConstructed(t *testing.T) {
	spy := &twoStagePipelineSpy{}
	sm := newPipelineTestSyncManager(t, spy)
	sm.settings.Legacy.PipelineWindowCommit = false

	jobs := make(chan windowFlushJob, 1)
	commitJobs := sm.startWindowFlushWorkers(jobs)
	require.Nil(t, commitJobs, "flag off must never construct the commitJobs channel / two-stage workers")

	jobs <- buildWindowJob(t, 500, 2)

	require.Eventually(t, func() bool {
		return len(spy.processedOrder()) == 1
	}, pipelineTestTimeout, 5*time.Millisecond, "flag-off path must service the job via ProcessBlockWindow (flushWorker)")

	require.Empty(t, spy.preparedOrder(), "flag off must never call PrepareBlockWindow")
	require.Empty(t, spy.committedOrder(), "flag off must never call CommitBlockWindow")

	close(jobs)
}

// TestStartWindowFlushWorkers_FlagOn_ConstructsTwoStageMachinery is the
// flag-on counterpart: legacy_pipelineWindowCommit=true must return a genuine,
// usable commitJobs channel serviced by prepareWorker+commitWorker (via
// PrepareBlockWindow then CommitBlockWindow), and must never call
// ProcessBlockWindow directly.
func TestStartWindowFlushWorkers_FlagOn_ConstructsTwoStageMachinery(t *testing.T) {
	spy := &twoStagePipelineSpy{}
	sm := newPipelineTestSyncManager(t, spy) // flag already true, see newPipelineTestSyncManager

	jobs := make(chan windowFlushJob, 1)
	commitJobs := sm.startWindowFlushWorkers(jobs)
	require.NotNil(t, commitJobs, "flag on must construct the commitJobs channel")

	jobs <- buildWindowJob(t, 500, 2)

	require.Eventually(t, func() bool {
		return len(spy.committedOrder()) == 1
	}, pipelineTestTimeout, 5*time.Millisecond, "flag-on path must service the job via PrepareBlockWindow then CommitBlockWindow")

	require.Equal(t, []uint32{500}, spy.preparedOrder())
	require.Equal(t, []uint32{500}, spy.committedOrder())
	require.Empty(t, spy.processedOrder(), "flag on must never call ProcessBlockWindow")

	close(jobs)
}

// TestPipeline_TwoStage_PrepareStrictlyFIFO proves the prepare stage is
// strictly FIFO: window N+1's PrepareBlockWindow (which would spend outputs
// window N's PrepareBlockWindow created) never starts until window N's
// PrepareBlockWindow call has fully returned. This is the ordering guarantee
// the whole design depends on for correctness (only C1/C2(N+1) is allowed to
// overlap C3(N) — never C1/C2(N+1) with C1/C2(N)).
func TestPipeline_TwoStage_PrepareStrictlyFIFO(t *testing.T) {
	spy := &twoStagePipelineSpy{
		prepareEntered: make(chan uint32, 4),
		prepareGate:    make(chan struct{}),
	}
	sm := newPipelineTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	go sm.prepareWorker(sm.ctx, jobs, commitJobs)
	go sm.commitWorker(sm.ctx, commitJobs)

	// Window N (creates outputs) -> prepare enters and blocks on the gate.
	jobs <- buildWindowJob(t, 500, 2)

	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(500), first, "prepare(N) must enter first")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N) did not enter")
	}

	// Window N+1 (spends N's outputs) is handed into the buffered slot, but
	// must NOT start preparing while prepare(N) is still gated open.
	jobs <- buildWindowJob(t, 600, 2)

	select {
	case first := <-spy.prepareEntered:
		t.Fatalf("prepare(N+1) (first height %d) started before prepare(N) finished — FIFO prepare violated", first)
	case <-time.After(100 * time.Millisecond):
	}

	// Release prepare(N); prepare(N+1) must now proceed.
	spy.prepareGate <- struct{}{}

	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(600), first, "prepare(N+1) must enter only after prepare(N) finished")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N+1) did not enter after prepare(N) released")
	}

	spy.prepareGate <- struct{}{}

	close(jobs)
}

// TestPipeline_TwoStage_PrepareOverlapsCommit is the headline property this
// whole design exists to create: window N+1's prepare runs CONCURRENTLY with
// window N's still-in-flight commit. Against the flag-off single worker this
// would deadlock/serialize; the two-stage pipeline lets the drain side (here,
// the test) hand off window N+1 and observe its prepare start while window
// N's commit is still gated open. The commit stage remains the sole,
// strictly-FIFO committer throughout (I1).
func TestPipeline_TwoStage_PrepareOverlapsCommit(t *testing.T) {
	spy := &twoStagePipelineSpy{
		prepareEntered: make(chan uint32, 4),
		commitEntered:  make(chan uint32, 4),
		commitGate:     make(chan struct{}),
	}
	sm := newPipelineTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	go sm.prepareWorker(sm.ctx, jobs, commitJobs)
	go sm.commitWorker(sm.ctx, commitJobs)

	// Window N -> prepared immediately, forwarded, commit stage enters and
	// blocks on the gate.
	jobs <- buildWindowJob(t, 500, 2)

	select {
	case first := <-spy.commitEntered:
		require.Equal(t, uint32(500), first, "window N's commit must enter")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commit stage did not enter window N's commit")
	}

	// Drain window N's own (already-completed) prepareEntered signal so the
	// next receive below observes window N+1's, not this leftover one.
	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(500), first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N) did not signal entered")
	}

	// While commit(N) is gated open, hand off window N+1: the prepare stage
	// must be free to prepare it concurrently with commit(N) still in flight
	// — this IS the overlap the pipeline exists to create.
	jobs <- buildWindowJob(t, 600, 2)

	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(600), first, "prepare(N+1) must overlap commit(N)")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N+1) did not overlap commit(N)")
	}

	// Window N+1 must NOT have entered its own commit yet: commitWorker is
	// the sole, strictly-FIFO committer and is still busy with window N.
	select {
	case first := <-spy.commitEntered:
		t.Fatalf("window N+1 (first height %d) committed before window N — single-committer FIFO violated", first)
	case <-time.After(50 * time.Millisecond):
	}

	// Release commit(N); window N+1 must then proceed to commit, in order.
	spy.commitGate <- struct{}{}

	select {
	case first := <-spy.commitEntered:
		require.Equal(t, uint32(600), first, "window N+1 must commit only after window N")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("window N+1 did not commit after window N released")
	}

	spy.commitGate <- struct{}{}

	close(jobs)

	require.Eventually(t, func() bool {
		return len(spy.committedOrder()) == 2
	}, pipelineTestTimeout, 5*time.Millisecond)

	require.Equal(t, []uint32{500, 600}, spy.preparedOrder())
	require.Equal(t, []uint32{500, 600}, spy.committedOrder())
}

// TestPipeline_TwoStage_PoisonMidOverlap_HaltsBothStages is the consensus-
// critical proof for the pipelined path: a fatal commit(N) failure — while
// prepare(N+1) is genuinely in flight concurrently — poisons the SHARED latch
// and halts BOTH stages. Neither the already-prepared-and-forwarded window
// N+1 nor any later window may ever reach a commit; the committed set must
// never show a gap.
func TestPipeline_TwoStage_PoisonMidOverlap_HaltsBothStages(t *testing.T) {
	const fatalCommitFirst = uint32(500)

	spy := &twoStagePipelineSpy{
		prepareEntered:         make(chan uint32, 4),
		commitEntered:          make(chan uint32, 4),
		commitGate:             make(chan struct{}),
		fatalCommitFirstHeight: fatalCommitFirst,
	}
	sm := newPipelineTestSyncManager(t, spy)
	syncP := newConnectedSyncPeer(t, sm)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	prepDone := make(chan struct{})
	commitDone := make(chan struct{})

	go func() {
		sm.prepareWorker(sm.ctx, jobs, commitJobs)
		close(prepDone)
	}()
	go func() {
		sm.commitWorker(sm.ctx, commitJobs)
		close(commitDone)
	}()

	// Window N (fatal on commit) -> prepared fine, forwarded; commit worker
	// enters and blocks on the gate.
	jobs <- buildWindowJob(t, fatalCommitFirst, 2)

	select {
	case first := <-spy.commitEntered:
		require.Equal(t, fatalCommitFirst, first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commit worker did not enter window N's commit")
	}

	// Drain window N's own (already-completed) prepareEntered signal so the
	// next receive below observes window N+1's, not this leftover one.
	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, fatalCommitFirst, first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N) did not signal entered")
	}

	// While commit(N) is blocked, hand off window N+1 — prepare(N+1) runs
	// concurrently with the still-in-flight commit(N) (the overlap this test
	// needs to be a genuine "mid-overlap" poison, not a sequential one).
	jobs <- buildWindowJob(t, 600, 2)

	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(600), first, "prepare(N+1) must overlap commit(N) for this to be a genuine mid-overlap poison")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(N+1) did not overlap commit(N)")
	}

	// Release commit(N): CommitBlockWindow fails fatally, recovery
	// (ProcessBlock) also fails fatally, so the commit worker poisons the
	// SHARED latch and disconnects the sync peer.
	spy.commitGate <- struct{}{}

	require.Eventually(t, func() bool { return !syncP.Connected() }, pipelineTestTimeout, 10*time.Millisecond,
		"fatal commit must disconnect the sync peer")
	require.Eventually(t, func() bool { return sm.windowPoisoned.Load() }, pipelineTestTimeout, 10*time.Millisecond,
		"shared poison latch must be set")

	// Window N+1 — already prepared and forwarded to commitJobs BEFORE the
	// poison — must be discarded by the commit worker WITHOUT ever entering
	// CommitBlockWindow: prepared-but-never-committed work is safe, but it
	// must never actually commit past the gap.
	select {
	case first := <-spy.commitEntered:
		t.Fatalf("poisoned commit worker committed queued window (first height %d) after a fatal gap", first)
	case <-time.After(200 * time.Millisecond):
	}

	// A further window (N+2) handed to the prepare stage after the poison
	// must also never reach a commit.
	jobs <- buildWindowJob(t, 700, 2)

	select {
	case first := <-spy.commitEntered:
		t.Fatalf("poisoned pipeline committed a later window (first height %d)", first)
	case <-time.After(200 * time.Millisecond):
	}

	close(jobs)

	select {
	case <-prepDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare worker did not exit after channel close")
	}

	select {
	case <-commitDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commit worker did not exit after channel close")
	}

	require.Empty(t, spy.committedOrder(),
		"no window may commit once a fatal gap occurs, including one prepared concurrently with the failing commit")
}

// TestPipeline_TwoStage_BarrierDoneMeansCommitted proves flushWindowSync's
// barrier contract survives the two-stage split: a job's done channel must
// stay open for the ENTIRE journey through both stages and close only once
// CommitBlockWindow (the commit stage) has actually finished — never merely
// once PrepareBlockWindow has finished. Closing early would let a barrier
// waiter (e.g. a direct/checkpoint HandleBlockDirect relying on the parent
// being committed) proceed before the block is actually durable.
func TestPipeline_TwoStage_BarrierDoneMeansCommitted(t *testing.T) {
	spy := &twoStagePipelineSpy{
		prepareGate: make(chan struct{}),
		commitGate:  make(chan struct{}),
	}
	sm := newPipelineTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	go sm.prepareWorker(sm.ctx, jobs, commitJobs)
	go sm.commitWorker(sm.ctx, commitJobs)

	job := buildWindowJob(t, 500, 2)
	done := make(chan struct{})
	job.done = done

	jobs <- job

	// Barrier must not release while PrepareBlockWindow is still gated open.
	select {
	case <-done:
		t.Fatal("barrier released before prepare even finished")
	case <-time.After(100 * time.Millisecond):
	}

	// Release prepare — the job forwards to the commit stage, which gates on
	// CommitBlockWindow. The barrier must STILL not release: done means
	// committed, not merely prepared.
	spy.prepareGate <- struct{}{}

	select {
	case <-done:
		t.Fatal("barrier released before CommitBlockWindow ran — done must mean committed, not merely prepared")
	case <-time.After(150 * time.Millisecond):
	}

	// Release commit — only now must the barrier release.
	spy.commitGate <- struct{}{}

	select {
	case <-done:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("barrier did not release after CommitBlockWindow completed")
	}

	close(jobs)
}

// TestPipeline_TwoStage_ShutdownDrainsBothStagesWithoutDeadlock proves the
// shutdown hand-off is deadlock-free across both stages: closing jobs (the
// only channel the drain goroutine ever closes) must let prepareWorker drain
// to completion, close commitJobs itself (the single-writer-closes
// discipline), and let commitWorker drain to completion in turn — with every
// in-flight job's ownership released and its barrier (if any) unblocked.
func TestPipeline_TwoStage_ShutdownDrainsBothStagesWithoutDeadlock(t *testing.T) {
	spy := &twoStagePipelineSpy{}
	sm := newPipelineTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	prepDone := make(chan struct{})
	commitDone := make(chan struct{})

	go func() {
		sm.prepareWorker(sm.ctx, jobs, commitJobs)
		close(prepDone)
	}()
	go func() {
		sm.commitWorker(sm.ctx, commitJobs)
		close(commitDone)
	}()

	barrierJob := buildWindowJob(t, 500, 2)
	barrierDone := make(chan struct{})
	barrierJob.done = barrierDone

	jobs <- barrierJob

	select {
	case <-barrierDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("barrier job did not commit before shutdown")
	}

	close(jobs)

	select {
	case <-prepDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepareWorker did not exit on jobs close (possible deadlock)")
	}

	select {
	case <-commitDone:
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commitWorker did not exit once prepareWorker closed commitJobs (possible deadlock)")
	}

	require.Equal(t, []uint32{500}, spy.committedOrder())
}

// TestPipeline_TwoStage_PrepareRecovery_RetriesPrepareNotProcessBlock is the
// regression test for adversarial review Findings 2/3: a transient (infra)
// PrepareBlockWindow failure must be recovered by retrying PrepareBlockWindow
// itself, never by re-driving the per-block ProcessBlock commit path. The old
// design reused recoverWindowCommit (ProcessBlock-based) for prepare-stage
// failures, which is a REAL commit path — running it from prepareWorker while
// commitWorker's own CommitBlockWindow can be concurrently in flight for an
// earlier window made it a second, concurrent committer, and could silently
// mark a window "committed" when ProcessBlock declined to commit a block
// whose parent was not yet visible. Retrying PrepareBlockWindow instead is
// always safe (idempotent, no commit) regardless of what any other window's
// commit stage is doing.
func TestPipeline_TwoStage_PrepareRecovery_RetriesPrepareNotProcessBlock(t *testing.T) {
	const transientFirst = uint32(502)

	spy := &twoStagePipelineSpy{
		prepareEntered:              make(chan uint32, 8),
		commitEntered:               make(chan uint32, 8),
		transientPrepareFirstHeight: transientFirst,
		transientPrepareFailCount:   2, // fail attempts 1 and 2, succeed on attempt 3
	}
	sm := newPipelineTestSyncManager(t, spy)

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	go sm.prepareWorker(sm.ctx, jobs, commitJobs)
	go sm.commitWorker(sm.ctx, commitJobs)

	jobs <- buildWindowJob(t, transientFirst, 2)

	require.Eventually(t, func() bool {
		return len(spy.committedOrder()) == 1
	}, pipelineTestTimeout, 5*time.Millisecond, "the window must eventually commit once the transient prepare error clears")

	require.Equal(t, []uint32{transientFirst}, spy.committedOrder())
	require.EqualValues(t, 3, spy.transientAttempts.Load(), "PrepareBlockWindow must be retried until it succeeds (2 failures + 1 success)")
	require.EqualValues(t, 0, spy.processBlockCalls.Load(), "prepare-stage recovery must never call the per-block ProcessBlock commit path")
	require.False(t, sm.windowPoisoned.Load(), "a recovered transient prepare error must not poison the pipeline")

	close(jobs)
}

// TestWindowJobBelowPoisonedGap directly pins the pure decision helper
// commitWorker relies on for the Finding 1 fix: a window is "below the
// poisoned gap" (safe to commit without clearing the shared latch) only when
// there IS a recorded gap height and the window's LAST block sits strictly
// below it.
func TestWindowJobBelowPoisonedGap(t *testing.T) {
	below := buildWindowJob(t, 502, 2) // heights 502-503
	atGap := buildWindowJob(t, 504, 2) // heights 504-505
	past := buildWindowJob(t, 506, 2)  // heights 506-507

	require.True(t, windowJobBelowPoisonedGap(below, 504), "502-503 is entirely below gap height 504")
	require.False(t, windowJobBelowPoisonedGap(atGap, 504), "504-505 reaches the gap height, not below it")
	require.False(t, windowJobBelowPoisonedGap(past, 504), "506-507 is past the gap height, not below it")
	require.False(t, windowJobBelowPoisonedGap(below, 0), "gapHeight=0 means no active poison — never 'below' a gap")
	require.False(t, windowJobBelowPoisonedGap(windowFlushJob{}, 504), "an empty job has nothing to compare")
}

// TestPipeline_TwoStage_PrePoisonQueuedWindowCommitsWithoutClearingLatch is the
// regression test for adversarial review Finding 1: a window that was already
// prepared and forwarded to the commit stage BEFORE a later window's prepare
// failure poisoned the shared latch must still commit (matching what
// flushWorker's single-worker FIFO would already have done), but doing so must
// NOT clear the latch — the gap left by the failing window is still
// unresolved, so any later window must remain discarded until the genuine
// post-rotation resync arrives.
//
// Without the fix, commitWorker's un-poison check saw the pre-poison window as
// merely "tip-aligned" and cleared sm.windowPoisoned, after which a later,
// still-missing window would sail through both stages and commit off-chain —
// a permanent, silent below-checkpoint tip wedge.
func TestPipeline_TwoStage_PrePoisonQueuedWindowCommitsWithoutClearingLatch(t *testing.T) {
	const (
		fatalPrepareFirst = uint32(504) // window W3: fails unrecoverably at prepare
		laterGapFirst     = uint32(506) // window W4: already-downloaded, must stay discarded
	)

	spy := &twoStagePipelineSpy{
		prepareEntered:          make(chan uint32, 8),
		commitEntered:           make(chan uint32, 8),
		commitGate:              make(chan struct{}),
		fatalPrepareFirstHeight: fatalPrepareFirst,
	}
	sm := newPipelineTestSyncManager(t, spy)

	// GetBestBlockHeader is wired so that, were commitWorker to (incorrectly)
	// consult windowRelinksAfterPoison for window W2 (first height 502, last
	// committed height 501), it WOULD report tip-aligned — reproducing exactly
	// the stale signal the pre-fix code mistook for a genuine relink. The fix
	// must never reach that call for W2 at all (short-circuited by the
	// below-gap check), which is what this test proves.
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 501}, nil)
	sm.blockchainClient = blockchainClient

	jobs := make(chan windowFlushJob, 1)
	commitJobs := make(chan windowFlushJob, 1)

	go sm.prepareWorker(sm.ctx, jobs, commitJobs)
	go sm.commitWorker(sm.ctx, commitJobs)

	// Window W1 (500-501): prepares and forwards immediately; commit stage
	// enters and blocks on the gate.
	jobs <- buildWindowJob(t, 500, 2)

	select {
	case first := <-spy.commitEntered:
		require.Equal(t, uint32(500), first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commit(W1) did not enter")
	}

	// Drain window W1's own (already-completed) prepareEntered signal so the
	// next receive below observes window W2's, not this leftover one.
	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(500), first)
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(W1) did not signal entered")
	}

	// Window W2 (502-503): prepares immediately (commitJobs' single slot is
	// free — commitWorker already dequeued W1 and is blocked inside the gated
	// call) and sits forwarded, waiting behind W1's still-in-flight commit.
	jobs <- buildWindowJob(t, 502, 2)

	select {
	case first := <-spy.prepareEntered:
		require.Equal(t, uint32(502), first, "prepare(W2) must complete and forward while commit(W1) is still gated")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("prepare(W2) did not enter/complete")
	}

	// Window W3 (504-505): fails unrecoverably at prepare — poisons the shared
	// latch — while W2 is already sitting, prepared, in commitJobs.
	jobs <- buildWindowJob(t, fatalPrepareFirst, 2)

	require.Eventually(t, func() bool { return sm.windowPoisoned.Load() }, pipelineTestTimeout, 5*time.Millisecond,
		"prepare(W3)'s fatal failure must poison the shared latch")
	require.Equal(t, fatalPrepareFirst, sm.windowPoisonedFirstHeight.Load(),
		"the recorded gap height must be W3's first height")

	// Release commit(W1): it succeeds normally (W1 predates the poison
	// entirely and never depended on W3).
	spy.commitGate <- struct{}{}

	// commitWorker now dequeues W2 — poisoned, but entirely below the gap
	// (503 < 504) — and must still commit it.
	select {
	case first := <-spy.commitEntered:
		require.Equal(t, uint32(502), first,
			"a window queued before the poison and entirely below the gap must still commit")
	case <-time.After(pipelineTestTimeout):
		t.Fatal("commit(W2) did not enter — a safe pre-poison window must not be discarded")
	}

	// The crux of the fix: the latch must STILL be poisoned right now, before
	// W2's commit even finishes — committing W2 must never clear it.
	require.True(t, sm.windowPoisoned.Load(),
		"committing a pre-poison, below-gap window must NOT clear the shared latch")

	// Release commit(W2).
	spy.commitGate <- struct{}{}

	require.Eventually(t, func() bool {
		return len(spy.committedOrder()) == 2
	}, pipelineTestTimeout, 5*time.Millisecond)
	require.Equal(t, []uint32{500, 502}, spy.committedOrder())
	require.True(t, sm.windowPoisoned.Load(), "the latch must remain set after W2 commits — W3's gap is still open")

	// Window W4 (506-507): an already-downloaded window that arrives after the
	// poison and does NOT reach/cross the gap in a tip-aligned way (best is
	// now 503, so 506 is a real, unresolved gap) — must be discarded at the
	// prepare stage, never reaching a commit.
	jobs <- buildWindowJob(t, laterGapFirst, 2)

	select {
	case first := <-spy.commitEntered:
		t.Fatalf("window W4 (first height %d) must never commit while W3's gap remains open", first)
	case <-time.After(200 * time.Millisecond):
	}

	require.Empty(t, spy.preparedOrder()[2:], "W4 must be discarded before PrepareBlockWindow is even attempted")
	require.Equal(t, []uint32{500, 502}, spy.committedOrder(), "no window past the gap may ever commit")

	close(jobs)
}
