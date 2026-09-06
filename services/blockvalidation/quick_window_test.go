package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// chainOf returns n consecutive blocks whose headers link, heights 1..n, with distinct hashes.
func chainOf(t *testing.T, n int) []*model.Block {
	t.Helper()

	blocks := make([]*model.Block, 0, n)
	prev := chainhash.Hash{}

	for i := 1; i <= n; i++ {
		// prevHash is a fresh variable each iteration: HashPrevBlock is a pointer, and a
		// single reused variable across iterations would leave every header aliasing
		// whatever value the loop last wrote, corrupting all but the final block's parent
		// link once the loop has finished.
		prevHash := prev
		h := &model.BlockHeader{Version: 1, HashPrevBlock: &prevHash, HashMerkleRoot: &chainhash.Hash{}, Timestamp: uint32(i), Bits: model.NBit{}, Nonce: uint32(i)}
		b := &model.Block{Header: h, Height: uint32(i)}
		blocks = append(blocks, b)
		prev = *b.Hash()
	}

	return blocks
}

type recordingCommitter struct {
	mu     sync.Mutex
	order  []uint32
	failAt uint32 // height whose commit returns an error; 0 = never
}

func (r *recordingCommitter) commit(_ context.Context, e *windowEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failAt != 0 && e.Height() == r.failAt {
		return errors.NewProcessingError("commit failed at %d", e.Height())
	}

	r.order = append(r.order, e.Height())

	return nil
}

func newTestWindow(t *testing.T, depth int, c *recordingCommitter) (*quickWindow, context.CancelFunc) {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	w := newQuickWindow(ulogger.TestLogger{}, depth, 64, c.commit)
	w.Start(ctx)

	return w, cancel
}

func TestQuickWindow_CommitsInHeightOrderWhenStoreWorkCompletesOutOfOrder(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	e1, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.False(t, dup)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)
	e3, _, err := w.Admit(ctx, blocks[2])
	require.NoError(t, err)

	// Store work finishes 3, 2, 1.
	e3.StoreDone()
	e2.StoreDone()

	// Nothing may commit while the head is pending.
	select {
	case <-time.After(50 * time.Millisecond):
	case <-e3.committed:
		t.Fatal("block 3 committed before block 1")
	}

	e1.StoreDone()

	for _, e := range []*windowEntry{e1, e2, e3} {
		require.NoError(t, e.WaitCommitted(ctx))
		e.Leave()
	}

	c.mu.Lock()
	require.Equal(t, []uint32{1, 2, 3}, c.order)
	c.mu.Unlock()
}

func TestQuickWindow_AdmitRefusesABlockWhoseParentIsNotTheTail(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	_, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	// blocks[2]'s parent is blocks[1], which is not in flight.
	_, _, err = w.Admit(ctx, blocks[2])
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "a refused admission must be a local fault, got %v", err)
}

func TestQuickWindow_AdmitBlocksAtDepthAndResumesWhenAnEntryLeaves(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 1, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	admitted := make(chan struct{})
	go func() {
		defer close(admitted)
		_, _, err := w.Admit(ctx, blocks[1])
		require.NoError(t, err)
	}()

	select {
	case <-admitted:
		t.Fatal("second block admitted while the window was full at depth 1")
	case <-time.After(50 * time.Millisecond):
	}

	e1.StoreDone()
	require.NoError(t, e1.WaitCommitted(ctx))
	e1.Leave()

	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("second block was not admitted after the first left")
	}
}

func TestQuickWindow_DuplicateAdmitReturnsTheLiveEntry(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	e1, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.False(t, dup)

	e1again, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.True(t, dup)
	require.Same(t, e1, e1again)
}

func TestQuickWindow_GateReleasesWaiterOnlyWhenClosed(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	parent := chainhash.HashH([]byte("parent"))
	g, err := e1.RegisterBatch([]chainhash.Hash{parent})
	require.NoError(t, err)
	e1.RegistrationComplete()

	require.NoError(t, e2.WaitPredecessorsRegistered(ctx))
	require.Same(t, g, w.GateFor(e2, &parent), "block 2 must see block 1's open gate")
	require.Nil(t, w.GateFor(e1, &parent), "an entry never waits on its own gate")
	require.True(t, w.Registered(&parent))

	var released atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, g.Wait(ctx))
		released.Store(true)
	}()

	select {
	case <-done:
		t.Fatal("waiter released before the gate closed")
	case <-time.After(50 * time.Millisecond):
	}

	g.Close()
	<-done
	require.True(t, released.Load())
	require.Nil(t, w.GateFor(e2, &parent), "a closed gate is no longer open")
	require.True(t, w.Registered(&parent), "the retained set keeps the id until the entry leaves")
}

func TestQuickWindow_RegisterBatchRefusesATxidHeldByAnotherLiveEntry(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	txid := chainhash.HashH([]byte("shared"))
	_, err = e1.RegisterBatch([]chainhash.Hash{txid})
	require.NoError(t, err)
	_, err = e2.RegisterBatch([]chainhash.Hash{txid})
	require.Error(t, err)
}

func TestQuickWindow_HeadFailureAbortsSuccessorsWithAServiceErrorAndFailsTheirGates(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)
	e3, _, err := w.Admit(ctx, blocks[2])
	require.NoError(t, err)

	g2, err := e2.RegisterBatch([]chainhash.Hash{chainhash.HashH([]byte("b2"))})
	require.NoError(t, err)

	e3.StoreDone() // block 3 finished its store work before block 1 failed

	head := errors.NewProcessingError("spend hard-failed")
	e1.Fail(head)

	err = e1.WaitCommitted(ctx)
	require.ErrorIs(t, err, head, "the head returns its own error with its own class")

	err = e2.WaitCommitted(ctx)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "an aborted successor returns a service error, got %v", err)
	require.Error(t, e3.WaitCommitted(ctx))
	require.Error(t, g2.Wait(ctx), "a failed entry's gates release waiters with an error")
	require.Error(t, e2.Context().Err(), "an aborted entry's context is cancelled")

	c.mu.Lock()
	require.Empty(t, c.order, "nothing commits after the head failed")
	c.mu.Unlock()

	e1.Leave()
	e2.Leave()
	e3.Leave()

	// The window is empty again and admits fresh.
	_, _, err = w.Admit(ctx, blocks[0])
	require.NoError(t, err)
}

func TestQuickWindow_CommitFailureFailsTheEntryAndAbortsSuccessors(t *testing.T) {
	c := &recordingCommitter{failAt: 1}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	e1.StoreDone()
	e2.StoreDone()

	require.Error(t, e1.WaitCommitted(ctx))
	err = e2.WaitCommitted(ctx)
	require.True(t, errors.IsTransientLocalError(err))
}

func TestQuickWindow_IDAssignedChainOrdersSuccessors(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	require.NoError(t, e1.WaitPredecessorIDAssigned(ctx), "the first entry has no predecessor and never waits")

	waited := make(chan error, 1)
	go func() { waited <- e2.WaitPredecessorIDAssigned(ctx) }()

	select {
	case <-waited:
		t.Fatal("block 2 proceeded before block 1 assigned its id")
	case <-time.After(50 * time.Millisecond):
	}

	e1.IDAssigned()
	require.NoError(t, <-waited)
}

func TestQuickWindow_AwaitParentReturnsWhenTheParentIsAdmitted(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	require.Nil(t, w.AwaitParent(ctx, blocks[0].Hash(), 30*time.Millisecond))

	found := make(chan *windowEntry, 1)
	go func() { found <- w.AwaitParent(ctx, blocks[0].Hash(), 2*time.Second) }()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.Same(t, e1, <-found)
}
