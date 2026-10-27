package utxoset

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// overlapTracer holds the first statement it sees, once armed, until a second statement starts
// or the wait runs out, and records which happened. A batch whose two lookups run one after the
// other cannot start the second while the first is held, so it records no overlap.
type overlapTracer struct {
	armed      atomic.Bool
	started    atomic.Int32
	release    chan struct{}
	closeOnce  sync.Once
	overlapped atomic.Bool
}

func (o *overlapTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if !o.armed.Load() {
		return ctx
	}

	switch o.started.Add(1) {
	case 1:
		select {
		case <-o.release:
			o.overlapped.Store(true)
		case <-time.After(3 * time.Second):
		}
	case 2:
		o.closeOnce.Do(func() { close(o.release) })
	}

	return ctx
}

func (o *overlapTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestGetBatchRunsTheBodyAndNoBodyLookupsAtTheSameTime sends one batch with a read that wants
// the body and one that does not. The two halves are independent lookups, and run one after the
// other the body reader waits for the whole no-body lookup first, so they must be in flight
// together.
func TestGetBatchRunsTheBodyAndNoBodyLookupsAtTheSameTime(t *testing.T) {
	tracer := &overlapTracer{release: make(chan struct{})}
	poolTracer = tracer

	t.Cleanup(func() { poolTracer = nil })

	s, ctx := newTestStore(t)

	a := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, a, 100)
	require.NoError(t, err)

	b := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, b, 100)
	require.NoError(t, err)

	noBody := &getItem{hash: *a.TxIDChainHash(), done: make(chan getResult, 1)}
	withBody := &getItem{hash: *b.TxIDChainHash(), wantBody: true, done: make(chan getResult, 1)}

	tracer.armed.Store(true)
	s.sendGetBatch([]*getItem{noBody, withBody})
	tracer.armed.Store(false)

	r := <-noBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)

	r = <-withBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)

	require.True(t, tracer.overlapped.Load(), "the body lookup started only after the no-body lookup finished")
}
