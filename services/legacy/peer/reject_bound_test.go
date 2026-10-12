package peer_test

import (
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The reject for a refused block body was waited for without a bound before the association
// was dropped. The connection has no write deadline, so a sender that had stopped reading held
// the wait, and the disconnect, for as long as it kept the socket open (review of 2026-10-12).
// Here the sender's read loop is stuck in a listener when the body is refused; the receiver must
// still drop it, within the bound.
func TestARejectedBodyDropsAPeerThatStoppedReading(t *testing.T) {
	refusingSink := func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		return false, terrors.NewBlockInvalidError("merkle root does not match header's", terrors.ErrBlockBodyMismatch)
	}

	sender, receiver, pings := blockStreamingPairWithSink(t, func(chainhash.Hash, *wire.BlockHeader, uint64) error { return nil },
		refusingSink, peer.MessageListeners{})

	// The ping listener hands each ping to a channel that holds one, so the second ping leaves
	// the sender's read loop stuck in the listener: it reads nothing more until the test drains
	// the channel. Registered after the pair's cleanup, so it runs first and unsticks the loop.
	t.Cleanup(func() {
		for range 2 {
			select {
			case <-pings:
			case <-time.After(time.Second):
			}
		}
	})

	for nonce := uint64(1); nonce <= 2; nonce++ {
		written := make(chan struct{}, 1)
		receiver.QueueMessage(wire.NewMsgPing(nonce), written)

		select {
		case <-written:
		case <-time.After(2 * time.Second):
			t.Fatal("the sender never read the ping")
		}
	}

	sender.QueueMessage(streamingTestBlock(), nil)

	done := make(chan struct{})

	go func() {
		receiver.WaitForDisconnect()
		close(done)
	}()

	start := time.Now()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the receiver waited on a reject its sender will never read, and never dropped it")
	}

	require.Less(t, time.Since(start), 10*time.Second)
}
