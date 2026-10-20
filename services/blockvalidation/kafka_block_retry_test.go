package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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
	cancel()

	require.Eventually(t, func() bool { return server.blockMessageRetries.claim("h") }, time.Second, 10*time.Millisecond)
}
