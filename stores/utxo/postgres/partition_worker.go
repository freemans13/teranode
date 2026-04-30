package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkersPerPartition is the number of dispatcher goroutines (and pgxpool
// connections) per (operation × partition). Each (op × partition) maintains
// one shared input channel and K workers reading from it; each worker holds
// its own connection for life and dispatches batches independently. Total
// connections in use = 4 × NumPartitions × WorkersPerPartition.
//
// The single-worker-per-slot design (K=1) puts a hard parallelism cap of
// 4 × NumPartitions = 32 in-flight queries, which throttles total
// throughput regardless of how much load the caller offers. Raising K
// linearly raises in-flight query parallelism — the system's true ceiling
// is then whatever postgres can actually serve.
const WorkersPerPartition = 1

// partitionSlot is a shared input channel + K dispatcher workers for one
// (operation × partition) pair. Items routed to that slot land in the
// shared channel; whichever worker is free picks up the item, accumulates
// a micro-batch, and dispatches against its own held connection.
type partitionSlot[T any] struct {
	partition int
	input     chan T
	done      chan struct{}
	workers   []*partitionWorker[T]
}

// partitionWorker is one dispatcher within a slot. It owns one pgxpool
// connection for life, reads from the slot's shared input channel,
// accumulates a local batch, flushes on size or duration cap, and exits
// when the slot's `done` channel closes.
type partitionWorker[T any] struct {
	partition int
	pool      *pgxpool.Pool
	conn      *pgxpool.Conn
	logger    ulogger.Logger
	input     <-chan T // shared with the slot's other workers
	batchSize int
	duration  time.Duration
	dispatch  func(conn *pgxpool.Conn, partition int, batch []T)
	done      <-chan struct{}
	wg        *sync.WaitGroup
}

// newPartitionSlot creates one (op × partition) slot with K worker
// goroutines, each holding its own connection. The shared input channel is
// buffered so callers don't block under bursty load.
func newPartitionSlot[T any](
	ctx context.Context,
	logger ulogger.Logger,
	pool *pgxpool.Pool,
	partition int,
	workersPerSlot int,
	batchSize int,
	duration time.Duration,
	inputBuffer int,
	dispatch func(conn *pgxpool.Conn, partition int, batch []T),
	wg *sync.WaitGroup,
) (*partitionSlot[T], error) {
	slot := &partitionSlot[T]{
		partition: partition,
		input:     make(chan T, inputBuffer),
		done:      make(chan struct{}),
	}
	for i := 0; i < workersPerSlot; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			// Roll back any connections acquired so far.
			close(slot.done)
			for _, w := range slot.workers {
				if w.conn != nil {
					w.conn.Release()
					w.conn = nil
				}
			}
			return nil, err
		}
		w := &partitionWorker[T]{
			partition: partition,
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

// Stop signals all workers in the slot to drain in-flight items and exit.
// Does not block; callers wait on the parent WaitGroup.
func (slot *partitionSlot[T]) Stop() {
	close(slot.done)
}

// run is the worker loop. Accumulates items into a local batch, flushes
// when the batch fills or the per-batch timer fires, exits when `done`
// closes (releasing its connection on the way out).
func (w *partitionWorker[T]) run() {
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
				w.logger.Errorf("[partitionWorker p=%d] failed to re-acquire connection: %v", w.partition, err)
				batch = batch[:0]
				return
			}
			w.conn = conn
		}
		w.dispatch(w.conn, w.partition, batch)
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

func (w *partitionWorker[T]) releaseConn() {
	if w.conn != nil {
		w.conn.Release()
		w.conn = nil
	}
}
