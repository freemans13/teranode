package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestFallbackTickerFiresWithoutNotifications verifies that with
// pruner_fallbackTickerSeconds set, notificationWorker re-fires a pruneSignal
// carrying the last known persisted height even when zero notifications are
// delivered on the subscription channel. This guards against a stalled block
// persister (or a transient early-exit skip in prunerProcessor) stranding
// deletable rows with no notification ever arriving to retry them.
func TestFallbackTickerFiresWithoutNotifications(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 1,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)

	// The fallback ticker must not fire until a real block hash has been
	// captured from a notification (see the doc comment on fireFallbackTick).
	// Seed one here so this test isolates ticker-firing behaviour from that
	// separate guard, which is covered by TestFallbackTickerSkipsWithoutHash.
	seedHash := chainhash.Hash{0xAA}
	s.lastBlockHash.Store(&seedHash)

	subscriptionCh := make(chan *blockchain_api.Notification) // never written to

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	select {
	case sig := <-s.pruneNotify:
		require.Equal(t, uint32(100), sig.blockHeight)
		require.Equal(t, seedHash, sig.blockHash)
	case <-ctx.Done():
		t.Fatal("fallback ticker never fired")
	}
}

// TestFallbackTickerSkipsWithoutHash verifies that the fallback ticker does
// not fire at all until a real block hash has been observed from a
// notification, even when a target height is already known. This matters in
// both trigger modes: prunerProcessor's waitForBlockMinedStatus runs whenever
// blockAssemblyClient is configured (which it always is in production),
// regardless of pruner_block_trigger. A synthetic zero hash would make that
// wait retry against a hash matching no real block for up to
// BlockAssemblyWaitTimeout instead of safely skipping the tick.
func TestFallbackTickerSkipsWithoutHash(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 1,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)
	// No hash seeded — s.lastBlockHash is left at its zero value (nil).

	subscriptionCh := make(chan *blockchain_api.Notification)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	select {
	case sig := <-s.pruneNotify:
		t.Fatalf("fallback ticker fired without a captured hash: %+v", sig)
	case <-ctx.Done():
		// Expected: no signal fires without a captured hash.
	}
}

// TestFallbackTickerDisabledWhenZero verifies that FallbackTickerSeconds=0
// (the "disabled" contract value) never fires, even with a known height and
// hash, and even though notificationWorker itself must still exit cleanly on
// context cancellation.
func TestFallbackTickerDisabledWhenZero(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 0,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)
	seedHash := chainhash.Hash{0xBB}
	s.lastBlockHash.Store(&seedHash)

	subscriptionCh := make(chan *blockchain_api.Notification)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.notificationWorker(ctx, subscriptionCh)
		close(done)
	}()

	select {
	case sig := <-s.pruneNotify:
		t.Fatalf("fallback ticker fired while disabled (FallbackTickerSeconds=0): %+v", sig)
	case <-ctx.Done():
		// Expected: no signal while disabled.
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notificationWorker did not exit after context cancellation")
	}
}
