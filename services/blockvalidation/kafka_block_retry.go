package blockvalidation

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
)

// Retry policy for Kafka block messages whose handling hit a recoverable (local or
// transient) fault. The consumer commits the highest marked offset per partition, so
// returning the error to Kafka cannot get the message redelivered: the next success on
// the same partition commits past it. The message is committed instead and retried here.
//
// The retries live in memory, so a restart drops any still pending. That is recovered
// elsewhere: the next block that builds on the dropped one has a missing parent and goes
// to catchup, and the P2P sync coordinator syncs from any peer ahead of us.
const (
	blockMessageRetryMaxAttempts = 8
	blockMessageRetryMaxPending  = 1000
)

// The retry delays are variables only so tests can shorten them.
var (
	blockMessageRetryInitialDelay = 500 * time.Millisecond
	blockMessageRetryMaxDelay     = 30 * time.Second
)

// blockMessageRetries tracks block hashes with a retry in flight. A hash is retried at
// most once at a time; announcements of the same block from other peers while it is
// pending are not queued again. The zero value is ready to use.
type blockMessageRetries struct {
	mu      sync.Mutex
	pending map[string]struct{}
}

// claim reserves a retry for hash. It returns false if one is already pending or the
// queue is full.
func (r *blockMessageRetries) claim(hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending == nil {
		r.pending = make(map[string]struct{})
	}

	if _, ok := r.pending[hash]; ok || len(r.pending) >= blockMessageRetryMaxPending {
		return false
	}

	r.pending[hash] = struct{}{}

	return true
}

func (r *blockMessageRetries) release(hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.pending, hash)
}

// isRecoverableBlockMessageError reports whether a block message failed for a local or
// transient reason, and so should be retried rather than blamed on the announcing peer.
func isRecoverableBlockMessageError(err error) bool {
	return errors.Is(err, errors.ErrServiceError) ||
		errors.Is(err, errors.ErrStorageError) ||
		errors.Is(err, errors.ErrThresholdExceeded) ||
		errors.Is(err, errors.ErrContextCanceled) ||
		errors.Is(err, errors.ErrExternal)
}

// runBlockHandler runs blockHandler, returning ctx.Err() if ctx ends first.
func (u *Server) runBlockHandler(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- u.blockHandler(kafkaMsg)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reportBlockMessagePeerFailure reports an unrecoverable block message failure against
// the announcing peer. A hash that does not parse is still reported; the client sends a
// zero hash in its place.
func (u *Server) reportBlockMessagePeerFailure(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage, cause error) {
	blockHash, _ := chainhash.NewHashFromStr(kafkaMsg.Hash)
	if err := u.blockchainClient.ReportPeerFailure(ctx, blockHash, kafkaMsg.GetPeerId(), "block", cause.Error()); err != nil {
		u.logger.Errorf("Failed to report peer failure: %v", err)
	}
}

// scheduleBlockMessageRetry retries a block message in the background after a
// recoverable failure.
func (u *Server) scheduleBlockMessageRetry(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage) {
	if !u.blockMessageRetries.claim(kafkaMsg.Hash) {
		u.logger.Warnf("[blockMessageRetry][%s] retry already pending or retry queue full (%d), not queueing", kafkaMsg.Hash, blockMessageRetryMaxPending)
		return
	}

	go u.retryBlockMessage(ctx, kafkaMsg)
}

func (u *Server) retryBlockMessage(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage) {
	defer u.blockMessageRetries.release(kafkaMsg.Hash)

	// This runs outside the Kafka consumer's panic barrier, so it needs its own.
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[blockMessageRetry][%s] PANIC during retry: %v", kafkaMsg.Hash, r)
		}
	}()

	delay := blockMessageRetryInitialDelay

	for attempt := 1; attempt <= blockMessageRetryMaxAttempts; attempt++ {
		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		err := u.runBlockHandler(ctx, kafkaMsg)
		if err == nil {
			u.logger.Infof("[blockMessageRetry][%s] succeeded on retry %d", kafkaMsg.Hash, attempt)
			return
		}

		if ctx.Err() != nil {
			return
		}

		if !isRecoverableBlockMessageError(err) {
			u.logger.Errorf("[blockMessageRetry][%s] unrecoverable error on retry %d: %v", kafkaMsg.Hash, attempt, err)
			u.reportBlockMessagePeerFailure(ctx, kafkaMsg, err)

			return
		}

		u.logger.Warnf("[blockMessageRetry][%s] retry %d/%d failed: %v", kafkaMsg.Hash, attempt, blockMessageRetryMaxAttempts, err)

		delay = min(delay*2, blockMessageRetryMaxDelay)
	}

	u.logger.Errorf("[blockMessageRetry][%s] giving up after %d retries; the block will be fetched by catchup or sync when a later block or peer references it", kafkaMsg.Hash, blockMessageRetryMaxAttempts)
}
