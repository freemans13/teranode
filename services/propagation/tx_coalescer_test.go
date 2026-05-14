package propagation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// dummyTx builds a positionally-distinct, recognisable *bt.Tx for tests.
// The hash bytes are derived from seed so test assertions can identify
// which tx came back.
func dummyTx(t testing.TB, seed byte) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	in := &bt.Input{}
	h := chainhash.Hash{seed, seed, seed, seed}
	require.NoError(t, in.PreviousTxIDAdd(&h))
	tx.Inputs = append(tx.Inputs, in)
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
	return tx
}

// newCoalescerWithStubFlush constructs a TxCoalescer where the flush
// function is provided by the test directly, bypassing the real
// validator. Used to exercise Submit/gather/dispatch mechanics in
// isolation.
func newCoalescerWithStubFlush(t testing.TB, maxSize int, maxWait time.Duration, maxConcurrent int, flush func(items []*pendingTx)) *TxCoalescer {
	t.Helper()
	logger := ulogger.TestLogger{}
	c := newTxCoalescerForTest(logger, maxSize, maxWait, maxConcurrent, flush)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestTxCoalescer_Submit_SingleTxRoundTrip(t *testing.T) {
	flush := func(items []*pendingTx) {
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 5*time.Millisecond, 0, flush)

	tx := dummyTx(t, 0xAA)
	res, err := c.Submit(context.Background(), tx, 0)
	require.NoError(t, err)
	require.NoError(t, res.Err)
	require.Equal(t, *tx.TxIDChainHash(), res.TxHash)
}

func TestTxCoalescer_Submit_NConcurrentTxFanInOneBatch(t *testing.T) {
	const N = 16
	var (
		flushedCount atomic.Int32
		seenItems    atomic.Int32
	)
	flush := func(items []*pendingTx) {
		flushedCount.Add(1)
		seenItems.Add(int32(len(items)))
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 100*time.Millisecond, 0, flush)

	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := dummyTx(t, byte(i+1))
			r, err := c.Submit(context.Background(), tx, 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()

	require.Equal(t, int32(N), seenItems.Load())
	require.GreaterOrEqual(t, flushedCount.Load(), int32(1))
	require.LessOrEqual(t, flushedCount.Load(), int32(3), "expected ~1 batch for an N=16 burst")
}

func TestTxCoalescer_Submit_SizeThresholdTriggersFlush(t *testing.T) {
	const N = 5
	flushCh := make(chan int, 10)
	flush := func(items []*pendingTx) {
		flushCh <- len(items)
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 2, 50*time.Millisecond, 0, flush)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	close(flushCh)
	total := 0
	maxSeen := 0
	for s := range flushCh {
		total += s
		if s > maxSeen {
			maxSeen = s
		}
	}
	require.Equal(t, N, total)
	require.Equal(t, 2, maxSeen, "no flush should exceed maxSize=2")
}

func TestTxCoalescer_Submit_TimeThresholdTriggersFlush(t *testing.T) {
	flushCh := make(chan int, 10)
	flush := func(items []*pendingTx) {
		flushCh <- len(items)
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 20*time.Millisecond, 0, flush)

	start := time.Now()
	_, err := c.Submit(context.Background(), dummyTx(t, 0xCC), 0)
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 18*time.Millisecond, "Submit should wait at least roughly maxWait")
	require.Less(t, elapsed, 200*time.Millisecond, "should not wait substantially longer than maxWait")
}

func TestTxCoalescer_Submit_CtxCancelReturnsEarly(t *testing.T) {
	flushed := make(chan struct{}, 1)
	flush := func(items []*pendingTx) {
		flushed <- struct{}{}
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, time.Hour, 0, flush)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Submit(ctx, dummyTx(t, 0xDD), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTxCoalescer_Submit_WholeBatchErrFannedToAllCallers(t *testing.T) {
	errBatch := terrors.NewProcessingError("upstream unreachable")
	flush := func(items []*pendingTx) {
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash(), Err: errBatch}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 5*time.Millisecond, 0, flush)

	const N = 4
	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()
	for i, r := range results {
		require.ErrorIs(t, r.Err, errBatch, "index %d", i)
	}
}
