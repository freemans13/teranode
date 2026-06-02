package netsync

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestHeaderHeights_SetGetDeleteReset verifies the in-memory hash→height lookup
// that lets the quick-validation pipeline derive a block's height without a
// GetBlockHeader(prevBlock) round-trip. Set registers a height, headerHeight
// retrieves it, deleteHeaderHeight prunes a single entry, and resetHeaderHeights
// clears everything.
func TestHeaderHeights_SetGetDeleteReset(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	h1 := chainhash.Hash{0x01}
	h2 := chainhash.Hash{0x02}

	// Unknown hash before anything is set.
	_, ok := sm.headerHeight(h1)
	require.False(t, ok, "no height before set")

	sm.setHeaderHeight(h1, 100)
	sm.setHeaderHeight(h2, 101)

	got, ok := sm.headerHeight(h1)
	require.True(t, ok)
	require.Equal(t, int32(100), got)

	got, ok = sm.headerHeight(h2)
	require.True(t, ok)
	require.Equal(t, int32(101), got)

	// Pruning one entry leaves the other.
	sm.deleteHeaderHeight(h1)
	_, ok = sm.headerHeight(h1)
	require.False(t, ok, "h1 pruned")
	_, ok = sm.headerHeight(h2)
	require.True(t, ok, "h2 retained")

	// Reset clears all.
	sm.setHeaderHeight(h1, 100)
	sm.resetHeaderHeights()
	_, ok = sm.headerHeight(h1)
	require.False(t, ok, "cleared by reset")
	_, ok = sm.headerHeight(h2)
	require.False(t, ok, "cleared by reset")
}

// TestHeaderHeights_ConcurrentSetGet exercises the lock: handleHeadersMsg writes
// from its own goroutine while the blockQueue consumer reads, so concurrent
// access must be race-free.
func TestHeaderHeights_ConcurrentSetGet(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		hash := chainhash.Hash{byte(i)}
		go func(h chainhash.Hash, height int32) {
			defer wg.Done()
			sm.setHeaderHeight(h, height)
		}(hash, int32(i))
		go func(h chainhash.Hash) {
			defer wg.Done()
			_, _ = sm.headerHeight(h)
		}(hash)
	}
	wg.Wait()

	got, ok := sm.headerHeight(chainhash.Hash{byte(10)})
	require.True(t, ok)
	require.Equal(t, int32(10), got)
}
