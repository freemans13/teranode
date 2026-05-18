package propagation

import (
	"context"
	"time"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// pendingTx is one entry waiting in the coalescer's batcher input.
// Each Submit caller creates one of these, hands it to the batcher,
// and blocks on Done until the batch completes (or its ctx cancels).
type pendingTx struct {
	Tx          *bt.Tx
	BlockHeight uint32
	Done        chan validator.ValidationResult // buffered 1; flusher never blocks
}

// TxCoalescer gathers concurrent single-tx submissions from the
// propagation handlers and dispatches them to validator.ValidateBatch
// in batches. Wraps go-batcher v2 with a per-item done-channel pattern
// so each Submit caller can block on its own result.
type TxCoalescer struct {
	logger ulogger.Logger
	b      *batcher.Batcher[pendingTx]
}

// NewTxCoalescer builds a coalescer wired to the supplied validator.
// The validator's ValidateBatch is invoked on each gathered batch.
//
// maxSize and maxWait control the gather thresholds; maxConcurrent
// caps the number of in-flight flush goroutines (0 = unbounded).
func NewTxCoalescer(
	ctx context.Context,
	logger ulogger.Logger,
	v validator.Interface,
	maxSize int,
	maxWait time.Duration,
	maxConcurrent int,
	drainMode bool,
) *TxCoalescer {
	c := &TxCoalescer{logger: logger}
	flush := func(items []*pendingTx) {
		c.flushBatch(ctx, v, items)
	}
	c.b = newBatcherInstance(logger, maxSize, maxWait, maxConcurrent, drainMode, flush)
	return c
}

// newTxCoalescerForTest wires a coalescer to a caller-supplied flush
// function, bypassing the validator. Used by unit tests.
func newTxCoalescerForTest(
	logger ulogger.Logger,
	maxSize int,
	maxWait time.Duration,
	maxConcurrent int,
	flush func(items []*pendingTx),
) *TxCoalescer {
	c := &TxCoalescer{logger: logger}
	c.b = newBatcherInstance(logger, maxSize, maxWait, maxConcurrent, false, flush)
	return c
}

func newBatcherInstance(
	logger ulogger.Logger,
	maxSize int,
	maxWait time.Duration,
	maxConcurrent int,
	drainMode bool,
	flush func(items []*pendingTx),
) *batcher.Batcher[pendingTx] {
	b := batcher.NewWithPool(
		maxSize,
		maxWait,
		flush,
		true,
		batcher.WithName("propagation_coalescer"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("propagation").OTelTracer()),
	)
	if maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}
	if drainMode {
		b.SetDrainMode(true)
	}
	return b
}

// Submit hands tx to the coalescer and blocks until either the batch
// has been validated (returns the per-tx ValidationResult) or ctx is
// cancelled (returns ctx.Err()).
//
// Per-caller ctx cancellation does NOT cancel the underlying batch —
// the validation work runs to completion, but the caller stops
// waiting for the result. This is deliberate: one caller's deadline
// must not cascade into other callers' tx failing.
func (c *TxCoalescer) Submit(ctx context.Context, tx *bt.Tx, blockHeight uint32) (validator.ValidationResult, error) {
	p := &pendingTx{
		Tx:          tx,
		BlockHeight: blockHeight,
		Done:        make(chan validator.ValidationResult, 1),
	}
	c.b.Put(p)

	select {
	case r := <-p.Done:
		return r, nil
	case <-ctx.Done():
		return validator.ValidationResult{}, ctx.Err()
	}
}

// flushBatch invokes validator.ValidateBatch for the gathered batch and
// fans results back to each waiting Submit caller via their Done channel.
// On a whole-batch error every caller receives the error. On success,
// per-tx results are delivered positionally (results[i] → items[i]).
func (c *TxCoalescer) flushBatch(ctx context.Context, v validator.Interface, items []*pendingTx) {
	if len(items) == 0 {
		return
	}

	txs := make([]*bt.Tx, len(items))
	bh := items[0].BlockHeight
	for i, p := range items {
		txs[i] = p.Tx
	}

	results, err := v.ValidateBatch(ctx, txs, bh)
	if err != nil {
		c.logger.Warnf("[TxCoalescer] ValidateBatch failed for batch of %d: %v", len(items), err)
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash(), Err: err}
		}
		return
	}

	for i, p := range items {
		p.Done <- results[i]
	}
}

// Close drains the in-progress batch and stops the batcher. Safe to
// call multiple times.
func (c *TxCoalescer) Close(_ context.Context) error {
	if c.b != nil {
		c.b.Trigger()
		c.b.Close()
	}
	return nil
}
