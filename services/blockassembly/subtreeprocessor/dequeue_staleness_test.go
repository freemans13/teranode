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

	require.False(t, stp.LastDequeueTime().IsZero(),
		"LastDequeueTime must never read as the zero time - that would misreport a fresh processor as stalled since the epoch")
	require.False(t, stp.LastDequeueTime().Before(before.Add(-time.Second)),
		"LastDequeueTime must be seeded to approximately now at Start(), not left at whatever zero-ish value it had before")
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
// move-forward-block, reset, or get* handler would block it (see
// SubtreeProcessor.go ~954-973 - the default: dequeue branch only runs when
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
