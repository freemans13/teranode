package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestConsumerMessageHandler_UnparseableHashReportsPeer pins that a block message whose
// hash does not parse is committed and reported against the peer, rather than panicking
// on the nil hash and being committed by the consumer's panic barrier with no report.
func TestConsumerMessageHandler_UnparseableHashReportsPeer(t *testing.T) {
	ctx := context.Background()

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, (*chainhash.Hash)(nil), "peerA", "block", mock.Anything).Return(nil).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}

	value, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{Hash: "not-a-hash", URL: "http://peer", PeerId: "peerA"})
	require.NoError(t, err)

	require.NoError(t, server.consumerMessageHandler(ctx)(&kafka.KafkaMessage{Value: value}))
	chain.AssertExpectations(t)
}

func TestIsRecoverableBlockMessageError(t *testing.T) {
	require.True(t, isRecoverableBlockMessageError(errors.NewServiceError("x")))
	require.True(t, isRecoverableBlockMessageError(errors.NewStorageError("x")))
	require.True(t, isRecoverableBlockMessageError(errors.NewProcessingError("wrapped", errors.NewExternalError("x"))))
	require.False(t, isRecoverableBlockMessageError(errors.NewProcessingError("x")))
}

// TestBlockMessageRetries_Claim pins the dedupe and the cap on pending retries.
func TestBlockMessageRetries_Claim(t *testing.T) {
	var r blockMessageRetries

	require.True(t, r.claim("a"))
	require.False(t, r.claim("a"), "a hash already pending must not be queued twice")

	r.release("a")
	require.True(t, r.claim("a"), "a released hash can be queued again")

	for i := 1; i < blockMessageRetryMaxPending; i++ {
		require.True(t, r.claim(chainhash.HashH([]byte{byte(i), byte(i >> 8)}).String()))
	}

	require.False(t, r.claim("overflow"), "the queue must refuse once full")
}

// TestRetryBlockMessage_StopsOnShutdown pins that a pending retry exits and frees its
// slot when the context ends, without running the handler.
func TestRetryBlockMessage_StopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{logger: ulogger.TestLogger{}}

	msg := &kafkamessage.KafkaBlockTopicMessage{Hash: "h"}
	server.scheduleBlockMessageRetry(ctx, msg)
	server.scheduleBlockMessageRetry(ctx, msg) // already pending: refused, no second goroutine
	cancel()

	require.Eventually(t, func() bool { return server.blockMessageRetries.claim("h") }, time.Second, 10*time.Millisecond)
}

// shortenBlockMessageRetryDelays makes the retry loop run in milliseconds for one test.
func shortenBlockMessageRetryDelays(t *testing.T) {
	initial, maxDelay := blockMessageRetryInitialDelay, blockMessageRetryMaxDelay
	blockMessageRetryInitialDelay, blockMessageRetryMaxDelay = time.Millisecond, 2*time.Millisecond

	t.Cleanup(func() {
		blockMessageRetryInitialDelay, blockMessageRetryMaxDelay = initial, maxDelay
	})
}

// alwaysFailingExistsChain fails every GetBlockExists with a recoverable error and counts
// the calls, so a test can count the handler attempts.
type alwaysFailingExistsChain struct {
	blockchain.ClientI

	mu    sync.Mutex
	calls int
}

func (c *alwaysFailingExistsChain) GetBlockExists(context.Context, *chainhash.Hash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++

	return false, errors.NewServiceError("injected persistent blockchain failure")
}

func (c *alwaysFailingExistsChain) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// TestRetryBlockMessage_GivesUpAfterMaxAttempts pins that a fault that never clears is
// retried exactly blockMessageRetryMaxAttempts times, is not blamed on the peer, and
// frees its slot afterwards.
func TestRetryBlockMessage_GivesUpAfterMaxAttempts(t *testing.T) {
	shortenBlockMessageRetryDelays(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &alwaysFailingExistsChain{ClientI: s.blockchainClient}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	// ReportPeerFailure is not expected: a mock call without an expectation panics, and
	// the panic guard would then free the slot early with fewer attempts counted.
	s.blockchainClient = &blockchain.Mock{}

	hash := block.Hash().String()
	s.scheduleBlockMessageRetry(ctx, &kafkamessage.KafkaBlockTopicMessage{Hash: hash, URL: "http://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return s.blockMessageRetries.claim(hash) }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, blockMessageRetryMaxAttempts, chain.count())
}

// TestRetryBlockMessage_UnrecoverableReportsPeer pins that a retry hitting an
// unrecoverable error blames the announcing peer, as the first attempt would have.
func TestRetryBlockMessage_UnrecoverableReportsPeer(t *testing.T) {
	shortenBlockMessageRetryDelays(t)

	hash := chainhash.HashH([]byte("retry-unrecoverable"))

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, &hash, "peerA", "block", mock.Anything).Return(nil).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}

	// A non-http URL fails blockHandler with an unrecoverable processing error.
	server.scheduleBlockMessageRetry(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "ftp://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return server.blockMessageRetries.claim(hash.String()) }, time.Second, 5*time.Millisecond)
	chain.AssertExpectations(t)
}

// TestRetryBlockMessage_RecoversFromPanic pins that a panic in the retry goroutine is
// contained and frees the slot. Without the guard the panic kills the test binary.
func TestRetryBlockMessage_RecoversFromPanic(t *testing.T) {
	shortenBlockMessageRetryDelays(t)

	hash := chainhash.HashH([]byte("retry-panic"))

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { panic("injected panic") }).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}
	server.scheduleBlockMessageRetry(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "ftp://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return server.blockMessageRetries.claim(hash.String()) }, time.Second, 5*time.Millisecond)
	chain.AssertExpectations(t)
}

// TestRunBlockHandler_PanicBecomesServiceError pins that a panic in blockHandler, which
// runs in its own goroutine, is returned as a recoverable service error. Without the
// guard the panic kills the test binary.
func TestRunBlockHandler_PanicBecomesServiceError(t *testing.T) {
	// A zero BlockValidation has a nil exists cache, so GetBlockExists panics.
	server := &Server{logger: ulogger.TestLogger{}, blockValidation: &BlockValidation{}}

	hash := chainhash.HashH([]byte("handler-panic"))
	err := server.runBlockHandler(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "http://peer", PeerId: "peerA"})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
	require.True(t, isRecoverableBlockMessageError(err), "a panic is a local fault and must be retried, not blamed on the peer")
}

// TestConsumerMessageHandler_HandlerPanicIsRetried pins the end state for a panic on
// the first attempt: the message is committed, a retry is pending, and the peer is not
// reported.
func TestConsumerMessageHandler_HandlerPanicIsRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No ReportPeerFailure expectation: a report would make the mock panic.
	chain := &blockchain.Mock{}
	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain, blockValidation: &BlockValidation{}}

	hash := chainhash.HashH([]byte("consumer-handler-panic"))
	value, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "http://peer", PeerId: "peerA"})
	require.NoError(t, err)

	require.NoError(t, server.consumerMessageHandler(ctx)(&kafka.KafkaMessage{Value: value}))
	require.False(t, server.blockMessageRetries.claim(hash.String()), "a retry must be pending for the block")
	chain.AssertNotCalled(t, "ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
