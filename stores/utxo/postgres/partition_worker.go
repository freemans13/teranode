package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// shardSlot is a shared input channel + one dispatcher worker per
// (shard × op). Items routed to that slot (by Route(hash).Shard) land in
// the channel; the worker picks up items, accumulates a micro-batch, and
// dispatches against its held connection. Today NumShards=1 so there is
// one slot per op; with NumShards=N we get N slots per op, each owning
// one connection from the shard's pool.
//
// We dropped the partition layer from dispatch — postgres prunes the
// partition itself given a hash-keyed WHERE clause via the schema's
// PARTITION BY LIST declaration. Client-side partition fanout was
// over-engineering: the planner-level saving did not pay for the dispatch
// overhead at scale.
type shardSlot[T any] struct {
	shard   int
	input   chan T
	done    chan struct{}
	workers []*shardWorker[T]
}

// shardWorker is the dispatcher for one shardSlot. It owns one pgxpool
// connection for life, reads from the slot's input channel, accumulates a
// local batch, flushes on size or duration cap, and exits when `done`
// closes.
type shardWorker[T any] struct {
	shard     int
	pool      *pgxpool.Pool
	conn      *pgxpool.Conn
	logger    ulogger.Logger
	input     <-chan T
	batchSize int
	duration  time.Duration
	dispatch  func(conn *pgxpool.Conn, batch []T)
	done      <-chan struct{}
	wg        *sync.WaitGroup
}

// newShardSlot creates one (shard × op) slot with K worker goroutines
// sharing the same input channel. Each worker holds its own pgxpool
// connection for life and dispatches its own micro-batches. K parallel
// workers give K parallel pgx.Batch streams against K postgres backends —
// useful for read ops where per-backend serialization is the bottleneck.
// The input channel is buffered so callers don't block under bursty load.
func newShardSlot[T any](
	ctx context.Context,
	logger ulogger.Logger,
	pool *pgxpool.Pool,
	shard int,
	workers int,
	batchSize int,
	duration time.Duration,
	inputBuffer int,
	dispatch func(conn *pgxpool.Conn, batch []T),
	wg *sync.WaitGroup,
) (*shardSlot[T], error) {
	if workers < 1 {
		workers = 1
	}
	slot := &shardSlot[T]{
		shard: shard,
		input: make(chan T, inputBuffer),
		done:  make(chan struct{}),
	}
	for i := 0; i < workers; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			// Roll back any conns acquired so far + signal goroutines to exit.
			close(slot.done)
			for _, w := range slot.workers {
				if w.conn != nil {
					w.conn.Release()
					w.conn = nil
				}
			}
			return nil, err
		}
		w := &shardWorker[T]{
			shard:     shard,
			pool:      pool,
			conn:      conn,
			logger:    logger,
			input:     slot.input,
			batchSize: batchSize,
			duration:  duration,
			dispatch:  dispatch,
			done:      slot.done,
			wg:        wg,
		}
		slot.workers = append(slot.workers, w)
		wg.Add(1)
		go w.run()
	}
	return slot, nil
}

// Stop signals the worker to drain in-flight items and exit. Does not
// block; callers wait on the parent WaitGroup.
func (slot *shardSlot[T]) Stop() {
	close(slot.done)
}

// run is the worker loop. Accumulates items into a local batch, flushes
// when the batch fills or the per-batch timer fires, exits when `done`
// closes (releasing its connection on the way out).
func (w *shardWorker[T]) run() {
	defer w.wg.Done()
	defer w.releaseConn()

	batch := make([]T, 0, w.batchSize)
	var timer *time.Timer
	var timerCh <-chan time.Time

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if w.conn == nil {
			conn, err := w.pool.Acquire(context.Background())
			if err != nil {
				w.logger.Errorf("[shardWorker s=%d] failed to re-acquire connection: %v", w.shard, err)
				batch = batch[:0]
				return
			}
			w.conn = conn
		}
		w.dispatch(w.conn, batch)
		batch = batch[:0]
		if timer != nil {
			timer.Stop()
			timerCh = nil
		}
	}

	for {
		select {
		case <-w.done:
			flush()
			return
		case item := <-w.input:
			batch = append(batch, item)
			if len(batch) == 1 {
				if timer == nil {
					timer = time.NewTimer(w.duration)
				} else {
					timer.Reset(w.duration)
				}
				timerCh = timer.C
			}
			if len(batch) >= w.batchSize {
				flush()
			}
		case <-timerCh:
			timerCh = nil
			flush()
		}
	}
}

func (w *shardWorker[T]) releaseConn() {
	if w.conn != nil {
		w.conn.Release()
		w.conn = nil
	}
}
