package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestLastDequeueTime_InitialisedAtStart pins that LastDequeueTime() is
// seeded the moment Start() runs, before the goroutine's select loop has had
// a chance to execute even once. Without this, a fresh processor would read
// as "stalled since the epoch" (time.UnixMilli(0)) for whatever window
// elapses before the first iteration - a false positive on the very signal
// issue #1429 introduces.
func TestLastDequeueTime_InitialisedAtStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)

	before := time.Now()
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	// Deliberately NOT an IsZero() check. An unseeded atomic.Int64 decodes via
	// time.UnixMilli(0) to 1970, which is a perfectly non-zero time.Time - Go's
	// zero Time is year 1. So IsZero() would pass whether or not the seed
	// exists, and the real regression (a ~56-year staleness reading) would slip
	// straight through it. Assert the value is approximately now instead, which
	// is the property that actually matters and the one that fails when the
	// seed is removed.
	require.False(t, stp.LastDequeueTime().Before(before.Add(-time.Second)),
		"LastDequeueTime must be seeded to approximately now, not left at the Unix epoch")
}

// TestLastDequeueTime_SeededBeforeStart pins the constructor seed, which the
// two tests around it cannot: both call Start() immediately, whereas production
// does not. BlockAssembly.Init launches the metrics/stall updater and
// BlockAssembly.Start brings up the gRPC ingest path, both before
// BlockAssembler.Start ever reaches
// subtreeProcessor.Start, and loadUnminedTransactions runs in between and can
// take minutes on a large unmined set. Without a constructor seed the gauge
// publishes roughly 56 years of staleness for that whole window, on every node,
// at exactly the moment an operator is watching a restart - discrediting the
// signal this change exists to provide.
func TestLastDequeueTime_SeededBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := time.Now()
	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)

	require.False(t, stp.LastDequeueTime().Before(before.Add(-time.Second)),
		"LastDequeueTime must be seeded at construction, before Start() is ever called")
}

// TestLastDequeueTime_ReseededAtStart pins the second seed, which the two tests
// above cannot: with a constructor seed in place, removing the re-seed in Start()
// fails nothing, because the constructor value is already approximately now by the
// time either of them asserts.
//
// The re-seed earns its keep because the gap between the two is not small in
// production — Init brings up ingest, then loadUnminedTransactions runs, and only
// then does Start reach the consumer. Staleness accrued across that window belongs
// to the startup sequence, not to a consumer that did not exist yet; without the
// re-seed the first reading after Start would already be minutes old and could
// trip the stall warning against a consumer that had just begun working.
func TestLastDequeueTime_ReseededAtStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)

	constructed := stp.LastDequeueTime()

	// Stand in for the reload window between construction and Start. Short, but
	// comfortably larger than the assertion's margin.
	time.Sleep(100 * time.Millisecond)

	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	require.True(t, stp.LastDequeueTime().After(constructed.Add(50*time.Millisecond)),
		"Start() must re-seed LastDequeueTime, so staleness accrued before the consumer existed is not attributed to it")
}

// TestLastDequeueTime_StopsAdvancingWhileConsumerParked is the test required
// by issue #1429: it parks the SubtreeProcessor's single consumer goroutine
// via a real, already-shipped production code path (the lengthCh plumbing
// behind GetCurrentLength - SubtreeProcessor.go's `case lengthCh :=
// <-stp.lengthCh` branch) and asserts LastDequeueTime stops advancing while
// the queue is non-empty, then resumes advancing once unparked.
//
// This is a genuine stall of the real select loop, not a faked clock or a
// direct field write: the loop is blocked exactly the way a slow reorg,
// move-forward-block, reset, or get* handler would block it (the
// default: branch of SubtreeProcessor.Start's select loop only runs when
// no other case is ready), because sending into the unbuffered lengthCh
// response channel without reading it holds the loop inside that case's
// body until the test reads the response.
func TestLastDequeueTime_StopsAdvancingWhileConsumerParked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)

	stp.Start(ctx)

	// respCh is exactly what GetCurrentLength() would hand to lengthCh, but
	// we deliberately never read it up front, so the loop blocks trying to
	// send the response - parking the consumer using real plumbing.
	respCh := make(chan int)

	// t.Cleanup runs LIFO, so register the cancel+Stop cleanup FIRST and the
	// respCh drain SECOND: the drain then executes first, unblocking any
	// goroutine still wedged on the parked send, before cancel+Stop runs -
	// otherwise Stop() would sit waiting (up to its own 5s timeout) on a
	// goroutine that can never see ctx.Done() while blocked on that send.
	t.Cleanup(func() {
		cancel()
		stp.Stop(ctx)
	})
	t.Cleanup(func() {
		select {
		case <-respCh:
		default:
		}
	})

	// Confirm the consumer is alive and advancing on an empty queue before
	// we park it - otherwise a frozen LastDequeueTime later would prove
	// nothing.
	require.Eventually(t, func() bool {
		return time.Since(stp.LastDequeueTime()) < time.Second
	}, 2*time.Second, 5*time.Millisecond,
		"LastDequeueTime must advance every loop iteration, even with an empty queue")

	go func() {
		stp.lengthCh <- respCh
	}()

	// Give the parking send time to land inside the select loop.
	time.Sleep(100 * time.Millisecond)

	// Enqueue directly into the queue (bypassing the gRPC ingest path,
	// which is out of scope here) so the queue is genuinely non-empty for
	// the whole duration of the stall - the case this test exists for is
	// "non-empty queue, no dequeue activity", not just "no dequeue
	// activity".
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: chainhash.HashH([]byte("parked-tx")), Fee: 1, SizeInBytes: 100}},
		[]*subtreepkg.TxInpoints{{}},
	)
	require.Equal(t, int64(1), stp.queue.length(), "precondition: queue is non-empty during the park")

	stalledAt := stp.LastDequeueTime()
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, stalledAt, stp.LastDequeueTime(),
		"LastDequeueTime must not advance while the consumer is parked outside the default: branch, "+
			"even though the queue is non-empty - this is the exact condition Server.go's updater now warns on")
	require.Equal(t, int64(1), stp.queue.length(), "queue must not drain while the consumer is parked")

	// Unpark: read the response the loop has been blocked trying to send.
	<-respCh

	require.Eventually(t, func() bool {
		return stp.LastDequeueTime().After(stalledAt)
	}, 2*time.Second, 5*time.Millisecond, "LastDequeueTime must advance again once the consumer is unparked")

	require.Eventually(t, func() bool {
		return stp.queue.length() == 0
	}, 2*time.Second, 5*time.Millisecond, "queue must drain once the consumer resumes")
}

// newTestSubtreeProcessorForDequeueStaleness builds a minimal real
// SubtreeProcessor (blob_memory store, mock UTXO store, mock blockchain
// client) suitable for exercising the Start() select loop directly. No UTXO
// or blockchain methods are expected to be called by these tests: batches
// are pushed straight into stp.queue and only ever reach the in-memory
// subtree-building path, not decoration or persistence.
func newTestSubtreeProcessorForDequeueStaleness(t *testing.T, ctx context.Context) *SubtreeProcessor {
	t.Helper()

	logger := ulogger.TestLogger{}
	blobStore := blob_memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	mockUtxoStore := new(utxo.MockUtxostore)
	mockBlockchainClient := new(blockchain.Mock)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, blobStore, mockBlockchainClient, mockUtxoStore, newSubtreeChan)
	require.NoError(t, err)

	return stp
}
