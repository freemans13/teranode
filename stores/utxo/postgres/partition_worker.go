package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// partitionWorker is a long-lived goroutine that owns one pgxpool connection
// for its lifetime, accumulates items routed to its partition, and dispatches
// batches against that connection. There is one partitionWorker per
// (operation × partition) — e.g., 4 ops × 8 partitions = 32 workers. Items
// arrive on `input`, are coalesced up to `batchSize` or `duration`, and the
// `dispatch` callback runs on the worker's owned connection with the partition
// number it can use to address `<table>_pK` directly.
//
// Why not the global go-batcher: the global batcher fans out per batch into
// 8 goroutines that each acquire/release a pool connection, then synchronise
// via a WaitGroup before the next batch. Profiling showed that fan-out spends
// most of its time waiting on connection-open overhead and on the per-batch
// max-of-8 synchronisation barrier; postgres backends sit idle most of the
// time. Holding a connection per partition for life and dispatching
// continuously eliminates both.
type partitionWorker[T any] struct {
	partition int
	pool      *pgxpool.Pool
	conn      *pgxpool.Conn // held for the worker's life
	logger    ulogger.Logger
	input     chan T
	batchSize int
	duration  time.Duration
	dispatch  func(conn *pgxpool.Conn, partition int, batch []T)
	done      chan struct{}
	wg        *sync.WaitGroup
}

// newPartitionWorker acquires a connection from the pool, spawns a goroutine
// running the worker loop, and returns the handle. The connection is held
// until the worker exits via the `done` channel. inputBuffer caps the input
// channel so callers see backpressure if dispatch falls behind.
func newPartitionWorker[T any](
	ctx context.Context,
	logger ulogger.Logger,
	pool *pgxpool.Pool,
	partition int,
	batchSize int,
	duration time.Duration,
	inputBuffer int,
	dispatch func(conn *pgxpool.Conn, partition int, batch []T),
	wg *sync.WaitGroup,
) (*partitionWorker[T], error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	w := &partitionWorker[T]{
		partition: partition,
		pool:      pool,
		conn:      conn,
		logger:    logger,
		input:     make(chan T, inputBuffer),
		batchSize: batchSize,
		duration:  duration,
		dispatch:  dispatch,
		done:      make(chan struct{}),
		wg:        wg,
	}
	wg.Add(1)
	go w.run()
	return w, nil
}

// run is the worker loop. It accumulates items into a local batch, flushes
// when the batch fills or the per-batch timer fires, and exits when `done`
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
		// Re-acquire if the held connection has been zeroed (recovered from a
		// dispatch panic — rare but possible).
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

// Stop signals the worker to drain any in-flight items and exit. It does not
// block; the parent's WaitGroup is what callers wait on.
func (w *partitionWorker[T]) Stop() {
	close(w.done)
}
