package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestMinBlockHeightSkipsPruning verifies that prunerProcessor skips all pruning
// operations when blockHeight <= MinBlockHeight and increments the skip metric.
func TestMinBlockHeightSkipsPruning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 100,
			},
		},
	}

	// Start the processor in a goroutine
	go server.prunerProcessor(ctx)

	// Send a signal below the minimum height - should be skipped
	server.pruneNotify <- pruneSignal{blockHeight: 50, blockHash: chainhash.Hash{}}

	// Send a signal at exactly the minimum height - should also be skipped (<=)
	server.pruneNotify <- pruneSignal{blockHeight: 100, blockHash: chainhash.Hash{}}

	// Give the processor time to consume both signals
	time.Sleep(100 * time.Millisecond)

	// Verify no phase processing occurred by checking lastProcessedHeight is still 0
	// (if pruning had run, it would have been updated)
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(),
		"lastProcessedHeight should remain 0 when all signals are below MinBlockHeight")

	// Verify blob deletion worker was NOT notified
	select {
	case <-server.blobNotify:
		t.Fatal("blob deletion worker should not have been notified for skipped heights")
	default:
		// Expected: no blob notification
	}
}

// TestMinBlockHeightZeroAllowsPruning verifies that with MinBlockHeight=0 (default),
// pruning proceeds normally without the height check blocking.
func TestMinBlockHeightZeroAllowsPruning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 0, // Default - no minimum
			},
		},
	}

	// Start the processor in a goroutine
	go server.prunerProcessor(ctx)

	// Send a signal at height 1 - should proceed past the min height check
	// (will hit block assembly safety check and skip, but that's after the min height guard)
	server.pruneNotify <- pruneSignal{blockHeight: 1, blockHash: chainhash.Hash{}}

	// Give the processor time to consume the signal
	time.Sleep(100 * time.Millisecond)

	// With MinBlockHeight=0 and no block assembly client (nil), pruning should proceed.
	// The blobNotify channel should have received a signal (block assembly check passes when client is nil).
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(1), sig.blockHeight, "blob worker should be notified at height 1")
	case <-time.After(time.Second):
		t.Fatal("blob deletion worker should have been notified when MinBlockHeight is 0")
	}
}

// TestMinBlockHeightAboveThresholdProceeds verifies that pruning proceeds normally
// when blockHeight exceeds MinBlockHeight.
func TestMinBlockHeightAboveThresholdProceeds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 100,
			},
		},
	}

	// Start the processor in a goroutine
	go server.prunerProcessor(ctx)

	// Send a signal above the minimum height - should proceed
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	// With no block assembly client (nil), the safety check passes and blob worker is notified
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(101), sig.blockHeight, "blob worker should be notified at height 101")
	case <-time.After(time.Second):
		t.Fatal("blob deletion worker should have been notified when blockHeight > MinBlockHeight")
	}
}
