package netsync

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// batcherLogger is a minimal logging interface accepted by SubtreeWriteBatcher.
// Callers may pass nil if no logging is required.
type batcherLogger interface {
	Errorf(format string, args ...interface{})
}

// SubtreeKind identifies which payload a SubtreeWriteItem represents so the flush
// function can route it to the right blob-store FileType.
type SubtreeKind int

const (
	SubtreeKindTree SubtreeKind = iota
	SubtreeKindData
	SubtreeKindMeta
)

// SubtreeWriteItem is one enqueued blob write.
type SubtreeWriteItem struct {
	Kind        SubtreeKind
	FileType    fileformat.FileType // resolved at Submit time; only meaningful for SubtreeKindTree (Data/Meta always map to fixed types inside the flush fn)
	RootHash    [32]byte
	Bytes       []byte
	DeleteAt    uint32 // DAH passed through to options.WithDeleteAt
	BlockHeight int32
}

// SubtreeWriteFlushFunc is called with the accumulated items when a flush trigger fires.
// Implementations must be resilient: a single item's failure must not silently skip others in the batch.
type SubtreeWriteFlushFunc func(ctx context.Context, items []SubtreeWriteItem) error

// SubtreeWriteBatcher accumulates blob-store write requests and flushes them in bulk.
//
// Flush triggers:
//  1. Item count reaches maxBlocks*3 entries (3 items per block: tree + data + meta).
//  2. Wall-clock time since the oldest pending item exceeds maxWait. This is a soft
//     lower bound, not a strict upper bound: the timer ticks at maxWait/2, so the
//     observed latency for a timer-triggered flush lies in [maxWait, 1.5*maxWait).
//     Callers that need a hard upper bound should call Stop() or size maxWait with
//     the tick tolerance in mind.
//  3. Stop() is called — all pending items flushed before Stop returns.
type SubtreeWriteBatcher struct {
	maxItems int
	maxWait  time.Duration
	flushFn  SubtreeWriteFlushFunc
	logger   batcherLogger

	mu        sync.Mutex
	buf       []SubtreeWriteItem
	oldest    time.Time
	stopCh    chan struct{}
	stopped   bool
	wg        sync.WaitGroup // timer goroutine
	inflight  sync.WaitGroup // tracks all in-progress flushFn invocations (Submit, timerLoop, Stop)
	lastErrMu sync.Mutex
	lastErr   error
}

// NewSubtreeWriteBatcher returns a running batcher. Call Stop() on shutdown to drain.
//
// maxBlocks is the block-count trigger; internally converted to 3×maxBlocks items.
// logger is optional — pass nil to suppress timer-path error logging.
func NewSubtreeWriteBatcher(maxBlocks int, maxWait time.Duration, logger batcherLogger, flushFn SubtreeWriteFlushFunc) *SubtreeWriteBatcher {
	if maxBlocks < 1 {
		maxBlocks = 1
	}
	if maxWait < 10*time.Millisecond {
		maxWait = 10 * time.Millisecond
	}
	b := &SubtreeWriteBatcher{
		maxItems: maxBlocks * 3,
		maxWait:  maxWait,
		flushFn:  flushFn,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
	b.wg.Add(1)
	go b.timerLoop()
	return b
}

// takeLastErr returns the last timer-path flush error and clears it.
func (b *SubtreeWriteBatcher) takeLastErr() error {
	b.lastErrMu.Lock()
	defer b.lastErrMu.Unlock()
	err := b.lastErr
	b.lastErr = nil
	return err
}

// Submit queues one write. May trigger a synchronous flush on count threshold.
// The provided ctx is forwarded to flushFn for any flush triggered by this call
// (count-threshold flushes only — timer-path flushes use context.Background()).
func (b *SubtreeWriteBatcher) Submit(ctx context.Context, item SubtreeWriteItem) error {
	if err := b.takeLastErr(); err != nil {
		return err
	}

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return errors.NewProcessingError("SubtreeWriteBatcher: submit after stop")
	}
	if len(b.buf) == 0 {
		b.oldest = time.Now()
	}
	b.buf = append(b.buf, item)
	shouldFlush := len(b.buf) >= b.maxItems
	var toFlush []SubtreeWriteItem
	if shouldFlush {
		toFlush = b.buf
		b.buf = nil
	}
	// Reserve an inflight slot under the mutex so Stop() (which sets b.stopped
	// and then waits on b.inflight) cannot race past us before we call flushFn.
	if toFlush != nil {
		b.inflight.Add(1)
	}
	b.mu.Unlock()

	if toFlush != nil {
		defer b.inflight.Done()
		return b.flushFn(ctx, toFlush)
	}
	return nil
}

// Stop drains and shuts down. Returns the error from the final flush, if any.
// Stop waits for every in-flight flush (count-threshold from Submit, timer-path
// from timerLoop, and the final pending flush here) before returning, so the
// "all pending items flushed before Stop returns" contract holds even if a
// Submit-triggered flush is racing with Stop.
func (b *SubtreeWriteBatcher) Stop(ctx context.Context) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return nil
	}
	b.stopped = true
	pending := b.buf
	b.buf = nil
	close(b.stopCh)
	b.mu.Unlock()

	// Wait for the timer goroutine to exit first so it cannot start a new flush
	// after we've decided to drain.
	b.wg.Wait()

	var finalErr error
	if len(pending) > 0 {
		b.inflight.Add(1)
		finalErr = b.flushFn(ctx, pending)
		b.inflight.Done()
	}

	// Wait for any flush triggered by a Submit call that observed !stopped
	// before we flipped the flag.
	b.inflight.Wait()

	// Surface any timer-path flush error captured during shutdown. Prefer the
	// final pending flush error if both are set, since the pending batch is
	// what the caller most directly relied on being persisted.
	if finalErr != nil {
		// Clear any captured timer error so it isn't returned by a future Stop.
		_ = b.takeLastErr()
		return finalErr
	}
	if err := b.takeLastErr(); err != nil {
		return err
	}
	return nil
}

func (b *SubtreeWriteBatcher) timerLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.maxWait / 2)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			if len(b.buf) == 0 {
				b.mu.Unlock()
				continue
			}
			if time.Since(b.oldest) < b.maxWait {
				b.mu.Unlock()
				continue
			}
			toFlush := b.buf
			b.buf = nil
			b.inflight.Add(1)
			b.mu.Unlock()
			// Timer-path flushes are not tied to a caller context; use
			// context.Background() and surface any error via lastErr for
			// the next Submit/Stop to observe.
			err := b.flushFn(context.Background(), toFlush)
			b.inflight.Done()
			if err != nil {
				if b.logger != nil {
					b.logger.Errorf("[SubtreeWriteBatcher] timer flush failed: %v", err)
				}
				b.lastErrMu.Lock()
				if b.lastErr == nil {
					b.lastErr = err
				}
				b.lastErrMu.Unlock()
			}
		}
	}
}
