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
	Kind     SubtreeKind
	FileType fileformat.FileType // resolved at Submit time; only meaningful for SubtreeKindTree (Data/Meta always map to fixed types inside the flush fn)
	RootHash [32]byte
	Bytes    []byte
	DeleteAt uint32 // DAH passed through to options.WithDeleteAt
}

// SubtreeWriteFlushFunc is called with the accumulated items when a flush trigger fires.
//
// Implementations must not silently skip items: every item in the batch must be
// either attempted or explicitly accounted for. Fail-fast cancellation of
// sibling item writes on a first error is acceptable (and matches the
// block-level fail-fast semantics in "Flush failure semantics" below): any
// returned error aborts the enclosing block, and the operator restarts
// catch-up — partial siblings are not expected to be preserved.
type SubtreeWriteFlushFunc func(ctx context.Context, items []SubtreeWriteItem) error

// SubtreeWriteBatcher accumulates blob-store write requests and flushes them in bulk.
//
// Flush triggers:
//  1. Item count reaches maxItems (derived as maxBlocks*3 — tree + data + meta per
//     block). Because meta may be omitted when the blob already exists (arrived via
//     P2P / created by block assembly), a block may contribute only 2 items in
//     practice, so maxItems is an *item* threshold and not a strict block-count
//     threshold. The effective block count per flush can therefore lie in
//     [maxBlocks, 1.5*maxBlocks]; this is acceptable because the purpose of the
//     threshold is to bound buffer memory, not to flush on exact block boundaries.
//  2. Wall-clock time since the oldest pending item exceeds maxWait. This is a soft
//     lower bound, not a strict upper bound: the timer ticks at maxWait/2, so the
//     observed latency for a timer-triggered flush lies in [maxWait, 1.5*maxWait).
//     Callers that need a hard upper bound should call Stop() or size maxWait with
//     the tick tolerance in mind.
//  3. Stop() is called — all pending items flushed before Stop returns.
//
// Flush failure semantics (fail-fast by design):
//
// When flushFn returns an error, the in-flight batch is NOT requeued — the items
// are dropped from the buffer and the error is surfaced to the caller via one of:
//   - Submit() returns the error directly (count-threshold path).
//   - The next Submit() or Stop() returns lastErr (timer path).
//   - Stop() returns the final-flush error if one occurs; otherwise it returns
//     the captured lastErr (and clears it).
//
// This is intentional. The caller (writeSubtree → prepareSubtrees → block
// processing) treats any Submit/Stop error as a block-level failure and aborts
// processing of the current block. The operator restarts catch-up from the last
// valid block height. Requeueing failed items into b.buf would conflate the
// rejected block's writes with the next block's writes and risk writing partial
// per-block blob sets under a different block context, which is worse than
// failing fast. Blind auto-retry of a persistent failure (disk full, permission
// denied, etc.) would also grow b.buf unboundedly.
//
// This matches the pre-batcher behaviour: a single failed blob write aborted the
// block; the batcher preserves that contract at the batch level.
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
// maxBlocks sets the flush threshold; internally converted to 3×maxBlocks items
// (3 is the maximum items-per-block at submit time: tree + data + meta). Because
// meta is optional in practice, the actual block count per flush can exceed
// maxBlocks — see the SubtreeWriteBatcher type doc.
// logger is optional — pass nil to suppress timer-path error logging.
func NewSubtreeWriteBatcher(maxBlocks int, maxWait time.Duration, logger batcherLogger, flushFn SubtreeWriteFlushFunc) *SubtreeWriteBatcher {
	if flushFn == nil {
		// Fail loudly at construction rather than at first flush. A nil flushFn
		// is always a programming error — there is no sensible default behaviour
		// for "discard all pending writes", so constructing the batcher is
		// unsafe and any Submit would silently lose data on first flush.
		panic("netsync: NewSubtreeWriteBatcher requires a non-nil flushFn")
	}
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
		// Fail-fast: if flushFn errors, toFlush is intentionally NOT requeued.
		// The caller aborts the current block and the operator restarts from the
		// last valid block. See the type-level "Flush failure semantics" doc for
		// why requeueing is worse than dropping here.
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
			// the next Submit/Stop to observe. toFlush is intentionally NOT
			// requeued on error — see the type-level "Flush failure
			// semantics" doc for the rationale.
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
