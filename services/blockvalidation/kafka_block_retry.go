package blockvalidation

import (
	"context"
	"runtime/debug"
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
	blockMessageRetryMaxAttempts  = 8
	blockMessageRetryInitialDelay = 500 * time.Millisecond
	blockMessageRetryMaxDelay     = 30 * time.Second
	blockMessageRetryMaxPending   = 1000

	// blockMessageRetryMaxPendingPerPeer stops one peer's announcements from holding
	// the whole queue during an outage, when every well-formed hash takes the retry
	// path. It does not stop many peer IDs acting together; catchup covers that.
	blockMessageRetryMaxPendingPerPeer = 32

	// blockMessageRetryMaxSources bounds the other announcers kept for a pending block.
	blockMessageRetryMaxSources = 4
)

// pendingBlockRetry is one block with a retry in flight.
type pendingBlockRetry struct {
	// peerID is the announcer that claimed the slot, charged against its per-peer cap.
	peerID string

	// sources are later announcements of the same block. Outside the retry window each
	// would reach the priority queue as an alternative source, so they are handed off
	// once the retried announcement has been handled.
	sources []*kafkamessage.KafkaBlockTopicMessage
}

// blockMessageRetries tracks block hashes with a retry in flight. A hash is retried at
// most once at a time. The zero value is ready to use.
type blockMessageRetries struct {
	mu      sync.Mutex
	pending map[string]*pendingBlockRetry
	perPeer map[string]int

	// initialDelay and maxDelay override the retry delays when set. Only tests set them.
	initialDelay, maxDelay time.Duration
}

func (r *blockMessageRetries) delays() (initial, maxDelay time.Duration) {
	initial, maxDelay = blockMessageRetryInitialDelay, blockMessageRetryMaxDelay
	if r.initialDelay > 0 {
		initial = r.initialDelay
	}

	if r.maxDelay > 0 {
		maxDelay = r.maxDelay
	}

	return initial, maxDelay
}

// claim reserves a retry for msg's block. If one is already pending, msg is kept as
// another source for it and claim returns false. It also returns false if the queue,
// or the announcing peer's share of it, is full.
func (r *blockMessageRetries) claim(msg *kafkamessage.KafkaBlockTopicMessage) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending == nil {
		r.pending = make(map[string]*pendingBlockRetry)
		r.perPeer = make(map[string]int)
	}

	peerID := msg.GetPeerId()

	if p, ok := r.pending[msg.Hash]; ok {
		if peerID != p.peerID && len(p.sources) < blockMessageRetryMaxSources && !hasSourcePeer(p.sources, peerID) {
			p.sources = append(p.sources, msg)
		}

		return false
	}

	if len(r.pending) >= blockMessageRetryMaxPending || r.perPeer[peerID] >= blockMessageRetryMaxPendingPerPeer {
		return false
	}

	r.pending[msg.Hash] = &pendingBlockRetry{peerID: peerID}
	r.perPeer[peerID]++

	return true
}

func hasSourcePeer(sources []*kafkamessage.KafkaBlockTopicMessage, peerID string) bool {
	for _, s := range sources {
		if s.GetPeerId() == peerID {
			return true
		}
	}

	return false
}

// nextSource hands back the next kept source for hash. With none left it releases the
// slot under the same lock, so a source added meanwhile is never lost.
func (r *blockMessageRetries) nextSource(hash string) (*kafkamessage.KafkaBlockTopicMessage, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.pending[hash]
	if !ok {
		return nil, false
	}

	if len(p.sources) == 0 {
		r.releaseLocked(hash, p)
		return nil, false
	}

	src := p.sources[0]
	p.sources = p.sources[1:]

	return src, true
}

// release frees the slot for hash and drops any kept sources. It is safe to call
// more than once.
func (r *blockMessageRetries) release(hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if p, ok := r.pending[hash]; ok {
		r.releaseLocked(hash, p)
	}
}

func (r *blockMessageRetries) releaseLocked(hash string, p *pendingBlockRetry) {
	delete(r.pending, hash)

	if r.perPeer[p.peerID]--; r.perPeer[p.peerID] <= 0 {
		delete(r.perPeer, p.peerID)
	}
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

// runBlockHandler runs blockHandler in the caller's goroutine, so when it returns the
// handler has finished too. A panic is returned as a service error: it is a local
// fault, so it is retried and the announcing peer is not blamed.
func (u *Server) runBlockHandler(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[blockHandler][%s] PANIC: %v\n%s", kafkaMsg.Hash, r, debug.Stack())
			err = errors.NewServiceError("[blockHandler][%s] panic: %v", kafkaMsg.Hash, r)
		}
	}()

	return u.blockHandler(ctx, kafkaMsg)
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
	if !u.blockMessageRetries.claim(kafkaMsg) {
		u.logger.Warnf("[blockMessageRetry][%s] retry already pending, or retry queue full (%d) or full for peer %s (%d), not queueing", kafkaMsg.Hash, blockMessageRetryMaxPending, kafkaMsg.GetPeerId(), blockMessageRetryMaxPendingPerPeer)
		return
	}

	go u.retryBlockMessage(ctx, kafkaMsg)
}

func (u *Server) retryBlockMessage(ctx context.Context, kafkaMsg *kafkamessage.KafkaBlockTopicMessage) {
	defer u.blockMessageRetries.release(kafkaMsg.Hash)

	// This runs outside the Kafka consumer's panic barrier, so it needs its own.
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[blockMessageRetry][%s] PANIC during retry: %v\n%s", kafkaMsg.Hash, r, debug.Stack())
		}
	}()

	delay, maxDelay := u.blockMessageRetries.delays()

	for attempt := 1; attempt <= blockMessageRetryMaxAttempts; attempt++ {
		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		err := u.runBlockHandler(ctx, kafkaMsg)
		if ctx.Err() != nil {
			return
		}

		if err == nil {
			u.logger.Infof("[blockMessageRetry][%s] succeeded on retry %d", kafkaMsg.Hash, attempt)
			u.handOffBlockMessageSources(ctx, kafkaMsg.Hash)

			return
		}

		if !isRecoverableBlockMessageError(err) {
			u.logger.Errorf("[blockMessageRetry][%s] unrecoverable error on retry %d: %v", kafkaMsg.Hash, attempt, err)
			u.reportBlockMessagePeerFailure(ctx, kafkaMsg, err)
			// The fault was this announcer's, so the others still get their turn.
			u.handOffBlockMessageSources(ctx, kafkaMsg.Hash)

			return
		}

		u.logger.Warnf("[blockMessageRetry][%s] retry %d/%d failed: %v", kafkaMsg.Hash, attempt, blockMessageRetryMaxAttempts, err)

		delay = min(delay*2, maxDelay)
	}

	u.logger.Errorf("[blockMessageRetry][%s] giving up after %d retries; the block will be fetched by catchup or sync when a later block or peer references it", kafkaMsg.Hash, blockMessageRetryMaxAttempts)
}

// handOffBlockMessageSources runs each other announcer of hash through the handler once,
// as the first attempt would have, so they reach the priority queue as alternative
// sources. A local fault on one is logged and that source dropped: the block itself has
// already been handled.
func (u *Server) handOffBlockMessageSources(ctx context.Context, hash string) {
	for ctx.Err() == nil {
		src, ok := u.blockMessageRetries.nextSource(hash)
		if !ok {
			return
		}

		err := u.runBlockHandler(ctx, src)

		switch {
		case err == nil, ctx.Err() != nil:
		case isRecoverableBlockMessageError(err):
			u.logger.Warnf("[blockMessageRetry][%s] dropping alternative source %s after local error: %v", hash, src.GetPeerId(), err)
		default:
			u.reportBlockMessagePeerFailure(ctx, src, err)
		}
	}
}
