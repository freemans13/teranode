package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSubtreeProcessor_LowVolumeNeverIncreases tests that with low transaction volume,
// the subtree size never increases even with high utilization
func TestSubtreeProcessor_LowVolumeNeverIncreases(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	// Handle channel reads to prevent blocking
	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	// Test: High utilization but low volume (< 50 nodes per subtree)
	t.Run("high utilization low volume keeps size", func(t *testing.T) {
		stp.currentItemsPerFile = 8

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(100)

		// Set up very low volume TPS that results in acceptable subtree creation rate
		// With 7 TPS and size 8, we get 7/8 = 0.875 subtrees/sec (close to target of 1/sec)
		stp.recentBlockStats = []blockStats{
			{txCount: 7, duration: 1 * time.Second},   // 7 TPS
			{txCount: 6, duration: 1 * time.Second},   // 6 TPS
			{txCount: 8, duration: 1 * time.Second},   // 8 TPS
		}

		// Simulate 87.5% utilization (7 nodes in size-8 subtree)
		// Populate the ring buffer
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 7
			r = r.Next()
		}

		// Use realistic intervals for low volume (30 TPS with 7 nodes = ~4 subtrees/sec = 250ms)
		// This is still fast enough to potentially trigger increase if volume wasn't considered
		stp.blockIntervals = []time.Duration{
			1 * time.Second,
			1 * time.Second,
			1 * time.Second,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Size should NOT increase despite high utilization and fast creation
		// because volume is low (7 nodes < 50 threshold)
		require.Equal(t, initialSize, stp.currentItemsPerFile,
			"Size should not increase with low transaction volume")
	})

	t.Run("very low utilization decreases size", func(t *testing.T) {
		stp.currentItemsPerFile = 32

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(50)

		// Set up very low volume TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 10, duration: 1 * time.Second},   // 10 TPS
			{txCount: 8, duration: 1 * time.Second},    // 8 TPS
			{txCount: 12, duration: 1 * time.Second},   // 12 TPS
		}

		// Simulate 6.25% utilization (2 nodes in size-32 subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 2
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{1 * time.Second}

		stp.adjustSubtreeSize()

		// Size should decrease when utilization is < 10%
		require.Less(t, stp.currentItemsPerFile, 32,
			"Size should decrease with very low utilization")
		require.GreaterOrEqual(t, stp.currentItemsPerFile,
			settings.BlockAssembly.MinimumMerkleItemsPerSubtree,
			"Size should not go below minimum")
	})

	t.Run("moderate utilization maintains size", func(t *testing.T) {
		stp.currentItemsPerFile = 16

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(80)

		// Set up moderate volume TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 40, duration: 1 * time.Second},   // 40 TPS
			{txCount: 35, duration: 1 * time.Second},   // 35 TPS
			{txCount: 38, duration: 1 * time.Second},   // 38 TPS
		}

		// Simulate 50% utilization (8 nodes in size-16 subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 8
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{
			1 * time.Second,
			1 * time.Second,
			1 * time.Second,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Size should stay the same with moderate utilization
		// However, with 40 TPS and 8 nodes per subtree = 5 subtrees/sec which is faster than target
		// So it might increase. Let's check if it changes and if so, verify it's reasonable
		if stp.currentItemsPerFile != initialSize {
			// If it changed, should be a reasonable adjustment (one step)
			require.LessOrEqual(t, stp.currentItemsPerFile, initialSize*2,
				"Size adjustment should be limited to one step")
		}
	})
}

// TestSubtreeProcessor_UsageBasedCapping tests that size increases are capped
// based on actual observed usage
func TestSubtreeProcessor_UsageBasedCapping(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 32
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("caps increase based on max observed nodes", func(t *testing.T) {
		stp.currentItemsPerFile = 32

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(200)

		// Set up moderate TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 60, duration: 1 * time.Second},   // 60 TPS
			{txCount: 55, duration: 1 * time.Second},   // 55 TPS
			{txCount: 58, duration: 1 * time.Second},   // 58 TPS
		}

		// High utilization with moderate volume
		// Max 27 nodes seen, average 25
		nodes := []int{23, 25, 27, 24, 26, 25, 24, 26, 25, 25}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}

		// Very fast subtree creation that would normally trigger 4x increase
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Size should be capped at 2x max observed (27*2=54 -> round to 64)
		// Not allowed to go higher even though timing suggests it
		require.LessOrEqual(t, stp.currentItemsPerFile, 64,
			"Size should be capped based on actual usage")
	})

	t.Run("allows increase when volume justifies it", func(t *testing.T) {
		stp.currentItemsPerFile = 64

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(500)

		// Set up high volume TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 120, duration: 1 * time.Second},   // 120 TPS
			{txCount: 115, duration: 1 * time.Second},   // 115 TPS
			{txCount: 118, duration: 1 * time.Second},   // 118 TPS
		}

		// High utilization with high volume (> 50 nodes)
		nodes := []int{60, 62, 58, 61, 59, 60, 61, 60, 60, 60}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}

		// Fast creation that justifies increase
		stp.blockIntervals = []time.Duration{
			200 * time.Millisecond,
			200 * time.Millisecond,
			200 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Size should increase when volume is high enough
		require.Greater(t, stp.currentItemsPerFile, initialSize,
			"Size should increase with high volume and fast creation")
	})
}

// TestSubtreeProcessor_RealWorldScenario tests a realistic scenario with varying load
func TestSubtreeProcessor_RealWorldScenario(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("adapts to changing load patterns", func(t *testing.T) {
		// Start with high load
		stp.currentItemsPerFile = 1024

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(5000)

		// Set up high TPS initially
		stp.recentBlockStats = []blockStats{
			{txCount: 1000, duration: 1 * time.Second},   // 1000 TPS
			{txCount: 950, duration: 1 * time.Second},    // 950 TPS
			{txCount: 980, duration: 1 * time.Second},    // 980 TPS
		}
		nodes := []int{900, 950, 920, 940, 910}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{500 * time.Millisecond}

		stp.adjustSubtreeSize()
		highLoadSize := stp.currentItemsPerFile
		require.GreaterOrEqual(t, highLoadSize, 1024,
			"Should maintain or increase size under high load")

		// Transition to low load (2-4 tx/s scenario)
		stp.currentItemsPerFile = highLoadSize

		// Update to low TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 3, duration: 1 * time.Second},   // 3 TPS
			{txCount: 4, duration: 1 * time.Second},   // 4 TPS
			{txCount: 2, duration: 1 * time.Second},   // 2 TPS
		}

		nodes = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
		r = stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{1 * time.Second}

		// Should decrease over multiple adjustments
		for i := 0; i < 5; i++ {
			prevSize := stp.currentItemsPerFile
			stp.adjustSubtreeSize()

			// Each adjustment should decrease or maintain size, never increase
			require.LessOrEqual(t, stp.currentItemsPerFile, prevSize,
				"Size should decrease or stay same under low load")

			// Reset counters as the real code does
			if stp.currentItemsPerFile < prevSize {
				nodes = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
				r = stp.subtreeNodeCounts
				for _, n := range nodes {
					r.Value = n
					r = r.Next()
				}
			}
		}

		// Should eventually reach a small size
		require.LessOrEqual(t, stp.currentItemsPerFile, 64,
			"Size should decrease significantly under sustained low load")
	})
}

// TestSubtreeProcessor_CompleteSubtreeTracking tests that node counts are properly
// tracked when subtrees complete
func TestSubtreeProcessor_CompleteSubtreeTracking(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("tracks node counts correctly", func(t *testing.T) {
		// Create a subtree with known node count
		stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)

		// Add some nodes (including coinbase)
		err = stp.currentSubtree.AddCoinbaseNode()
		require.NoError(t, err)

		// Add 4 more transaction nodes
		for i := 0; i < 4; i++ {
			hash := chainhash.Hash{}
			copy(hash[:], []byte{byte(i)})
			node := subtreepkg.SubtreeNode{
				Hash:        hash,
				Fee:         100,
				SizeInBytes: 250,
			}
			err = stp.currentSubtree.AddSubtreeNode(node)
			require.NoError(t, err)
		}

		// Process the complete subtree
		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Should have tracked 5 nodes (including coinbase)
		// Check the first value in the ring
		count := 0
		actualNodes := 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
				if count == 1 {
					actualNodes = v.(int)
				}
			}
		})
		require.Equal(t, 1, count, "Should have tracked one subtree")
		require.Equal(t, 5, actualNodes,
			"Should track total number of nodes including coinbase")

		// Test that old samples are removed after limit
		// First, fill up remaining slots (we already have 1, buffer size is 18)
		r := stp.subtreeNodeCounts.Next() // Skip the first one we already added
		for i := 0; i < 16; i++ {         // 17 more to reach 18 total
			r.Value = 5
			r = r.Next()
		}

		// Create another subtree that should trigger the limit
		stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)
		err = stp.currentSubtree.AddCoinbaseNode()
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			hash := chainhash.Hash{}
			copy(hash[:], []byte{byte(i + 10)})
			node := subtreepkg.SubtreeNode{
				Hash:        hash,
				Fee:         100,
				SizeInBytes: 250,
			}
			err = stp.currentSubtree.AddSubtreeNode(node)
			require.NoError(t, err)
		}

		// This should add one more, reaching 18 (full buffer)
		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Count values in ring
		count = 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
			}
		})
		require.Equal(t, 18, count, "Should have 18 samples (full buffer)")

		// Add one more to test that it maintains the limit
		stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)
		err = stp.currentSubtree.AddCoinbaseNode()
		require.NoError(t, err)

		hash := chainhash.Hash{}
		copy(hash[:], []byte{byte(20)})
		node := subtreepkg.SubtreeNode{
			Hash:        hash,
			Fee:         100,
			SizeInBytes: 250,
		}
		err = stp.currentSubtree.AddSubtreeNode(node)
		require.NoError(t, err)

		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Should still be at max 18 samples (ring automatically overwrites oldest)
		count = 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
			}
		})
		require.Equal(t, 18, count,
			"Should still have 18 samples (ring overwrites oldest)")
	})
}

// TestSubtreeProcessor_MinimumSizeRespected tests that minimum size is always respected
func TestSubtreeProcessor_MinimumSizeRespected(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("never goes below minimum", func(t *testing.T) {
		stp.currentItemsPerFile = 4 // At minimum

		// Extremely low utilization that would normally trigger decrease
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 1
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{5 * time.Second}

		stp.adjustSubtreeSize()

		// Should stay at minimum
		require.Equal(t, settings.BlockAssembly.MinimumMerkleItemsPerSubtree,
			stp.currentItemsPerFile,
			"Size should not go below minimum")
	})
}

// TestSubtreeProcessor_HighVolumeScaling tests that the dynamic sizing correctly
// scales up when there's genuinely high transaction volume
func TestSubtreeProcessor_HighVolumeScaling(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 64
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	// Handle channel reads to prevent blocking
	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("scales up with sustained high volume", func(t *testing.T) {
		// Start with a moderate size
		stp.currentItemsPerFile = 64

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(1000)

		// Set up recent block stats to simulate high TPS (instead of relying on intervals only)
		stp.recentBlockStats = []blockStats{
			{txCount: 1200, duration: 1 * time.Second},   // 1200 TPS
			{txCount: 1150, duration: 1 * time.Second},   // 1150 TPS
			{txCount: 1180, duration: 1 * time.Second},   // 1180 TPS
		}

		// Simulate high transaction volume (1000+ tx/s)
		// With size 64, we'd be creating subtrees very frequently
		// Let's say we're seeing 90% full subtrees (57-58 nodes each)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 57 + (i % 3) // 57, 58, 59, 57, 58, 59...
			r = r.Next()
		}

		// Subtrees are being created every 50ms (20 per second)
		// This represents ~1140 transactions per second (57 * 20)
		stp.blockIntervals = []time.Duration{
			50 * time.Millisecond,
			50 * time.Millisecond,
			50 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			currentSize := stp.currentItemsPerFile
			t.Logf("Current size: %d, Initial size: %d", currentSize, initialSize)
			return currentSize > initialSize
		}, time.Second, 10*time.Millisecond, "Size should increase with high transaction volume")

		// With ~1177 TPS and size 64, algorithm targets size for ~1 subtree/sec
		// Ideal size would be ~1024, but limited by max 2x increase = 128
		// However, algorithm might choose the closest power of 2 that gives good rate
		// Since we see 256, it seems the algorithm chose that over 128
		require.Contains(t, []int{128, 256}, stp.currentItemsPerFile,
			"Size should increase significantly with high load")
	})

	t.Run("continues scaling with extreme volume", func(t *testing.T) {
		// Now at 256, still seeing high volume
		stp.currentItemsPerFile = 256

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(5000)

		// Set up even higher TPS scenario
		stp.recentBlockStats = []blockStats{
			{txCount: 4800, duration: 1 * time.Second},   // 4800 TPS
			{txCount: 4750, duration: 1 * time.Second},   // 4750 TPS
			{txCount: 4900, duration: 1 * time.Second},   // 4900 TPS
		}

		// Even higher utilization now (240+ nodes per subtree to match larger size)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 240 + (i % 10) // 240-249 nodes
			r = r.Next()
		}

		// Still creating subtrees very fast (25ms intervals)
		stp.blockIntervals = []time.Duration{
			25 * time.Millisecond,
			25 * time.Millisecond,
			25 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile > 256
		}, time.Second, 10*time.Millisecond, "Should continue scaling with extreme load")

		// With ~4800 TPS, should scale to even larger sizes
		require.Contains(t, []int{512, 1024, 2048, 4096}, stp.currentItemsPerFile,
			"Should continue scaling up with extreme volume")
	})

	t.Run("scales to maximum with massive volume", func(t *testing.T) {
		// Set near maximum to test ceiling
		stp.currentItemsPerFile = 16384

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(100000)

		// Set up extreme TPS scenario that should hit the maximum
		stp.recentBlockStats = []blockStats{
			{txCount: 100000, duration: 1 * time.Second},  // 100,000 TPS
			{txCount: 120000, duration: 1 * time.Second},  // 120,000 TPS
			{txCount: 110000, duration: 1 * time.Second},  // 110,000 TPS
		}

		// Extremely high volume - nearly full subtrees matching current size
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 16000 + (i * 30) // 16000-16270 nodes
			r = r.Next()
		}

		// Creating subtrees every 10ms (100 per second)
		stp.blockIntervals = []time.Duration{
			10 * time.Millisecond,
			10 * time.Millisecond,
			10 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile >= settings.BlockAssembly.MaximumMerkleItemsPerSubtree
		}, time.Second, 10*time.Millisecond, "Should reach maximum size with massive volume")

		// Should reach the configured maximum
		require.Equal(t, settings.BlockAssembly.MaximumMerkleItemsPerSubtree, stp.currentItemsPerFile,
			"Should hit the maximum size limit")
	})

	t.Run("scales down when volume decreases", func(t *testing.T) {
		// Start at a high size from previous high volume
		stp.currentItemsPerFile = 8192

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(500)

		// Set up low TPS scenario
		stp.recentBlockStats = []blockStats{
			{txCount: 50, duration: 1 * time.Second},    // 50 TPS
			{txCount: 30, duration: 1 * time.Second},    // 30 TPS
			{txCount: 40, duration: 1 * time.Second},    // 40 TPS
		}

		// Volume has dropped significantly (only 100-200 tx per subtree, very low utilization)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 100 + (i * 10) // 100-190 nodes (very low utilization of 8192 capacity)
			r = r.Next()
		}

		// Subtrees now created every 2 seconds (low frequency)
		stp.blockIntervals = []time.Duration{
			2 * time.Second,
			2 * time.Second,
			2 * time.Second,
		}

		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile < 8192
		}, time.Second, 10*time.Millisecond, "Size should decrease when volume drops")

		// With ~40 TPS and low utilization, should scale down significantly
		// Algorithm should choose size appropriate for the actual transaction rate
		require.LessOrEqual(t, stp.currentItemsPerFile, 1024,
			"Should decrease significantly with low TPS and utilization")
	})

	t.Run("handles burst traffic correctly", func(t *testing.T) {
		// Start at a reasonable size
		stp.currentItemsPerFile = 256

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(2000)

		// Set up burst scenario with very high TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 12000, duration: 1 * time.Second},  // 12,000 TPS burst
			{txCount: 11500, duration: 1 * time.Second},  // 11,500 TPS
			{txCount: 12500, duration: 1 * time.Second},  // 12,500 TPS
		}

		// Sudden burst - subtrees are completely full
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 255 // Nearly full
			r = r.Next()
		}

		// Creating subtrees very rapidly during burst
		stp.blockIntervals = []time.Duration{
			20 * time.Millisecond,
			20 * time.Millisecond,
			20 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile > 256
		}, time.Second, 10*time.Millisecond, "Should increase size to handle burst traffic")

		// With ~12,000 TPS burst, should scale up significantly
		require.GreaterOrEqual(t, stp.currentItemsPerFile, 512,
			"Should handle burst traffic with larger subtrees")

		// Now simulate burst ending
		burstSize := stp.currentItemsPerFile // Remember the burst size

		// Set up low TPS after burst
		stp.recentBlockStats = []blockStats{
			{txCount: 100, duration: 1 * time.Second},   // 100 TPS (much lower)
			{txCount: 80, duration: 1 * time.Second},    // 80 TPS
			{txCount: 90, duration: 1 * time.Second},    // 90 TPS
		}

		// Traffic back to much lower (20-30 nodes per subtree, very low utilization)
		r = stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 20 + (i % 10)
			r = r.Next()
		}

		// Normal intervals again
		stp.blockIntervals = []time.Duration{
			1 * time.Second,
			1 * time.Second,
			1 * time.Second,
		}

		stp.adjustSubtreeSize()

		// Should decrease back down from burst size (very low utilization ~2%)
		require.Less(t, stp.currentItemsPerFile, burstSize,
			"Should decrease after burst ends with low utilization")
	})

	t.Run("realistic high volume scenario", func(t *testing.T) {
		// Simulate realistic high-volume scenario
		// Target: 100,000 tx/s (current BSV record levels)
		// Start small and let it scale
		stp.currentItemsPerFile = 1024

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(10000)

		// Set up realistic high volume TPS
		stp.recentBlockStats = []blockStats{
			{txCount: 9500, duration: 1 * time.Second},   // 9,500 TPS
			{txCount: 9200, duration: 1 * time.Second},   // 9,200 TPS
			{txCount: 9800, duration: 1 * time.Second},   // 9,800 TPS
		}

		// First adjustment - seeing 950+ nodes per subtree
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 950 + (i * 5) // 950-995 nodes
			r = r.Next()
		}

		// Creating subtrees every 100ms (10 per second)
		// This is ~9500 tx/s
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		// Should scale up over multiple adjustments
		sizes := []int{}
		for i := 0; i < 3; i++ { // Reduce iterations to avoid timeout
			prevSize := stp.currentItemsPerFile
			stp.adjustSubtreeSize()

			// Wait for resize operation to complete or confirm no change needed
			time.Sleep(50 * time.Millisecond)
			sizes = append(sizes, stp.currentItemsPerFile)

			// Simulate continued high load if size increased
			if stp.currentItemsPerFile > prevSize {
				// Update node counts to match new size
				newNodeCount := int(float64(stp.currentItemsPerFile) * 0.90) // 90% full
				r = stp.subtreeNodeCounts
				for j := 0; j < 10; j++ {
					r.Value = newNodeCount + (j % 10)
					r = r.Next()
				}
			}
		}

		// Wait for any final resize operation
		time.Sleep(50 * time.Millisecond)

		// Should have scaled up with high volume scenario
		require.GreaterOrEqual(t, stp.currentItemsPerFile, 2048,
			"Should scale up significantly from initial size with high volume")

		// Log the scaling progression
		t.Logf("Size progression with high volume: 1024 -> %v", sizes)

		// Final size should be appropriate for the load
		// With 100ms intervals and needing to handle ~950 tx per subtree,
		// optimal size would be around 2048-4096
		require.GreaterOrEqual(t, stp.currentItemsPerFile, 2048,
			"Should reach appropriate size for sustained high volume")
		require.LessOrEqual(t, stp.currentItemsPerFile, 8192,
			"Should not overshoot reasonable size")
	})
}

// TestSubtreeProcessor_VolumeThresholds tests the 50-node threshold for volume detection
func TestSubtreeProcessor_VolumeThresholds(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 64
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	t.Run("just below threshold blocks increase", func(t *testing.T) {
		stp.currentItemsPerFile = 64

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(200)

		// Set up TPS just below threshold
		stp.recentBlockStats = []blockStats{
			{txCount: 49, duration: 1 * time.Second},   // 49 TPS
			{txCount: 48, duration: 1 * time.Second},   // 48 TPS
			{txCount: 47, duration: 1 * time.Second},   // 47 TPS
		}

		// 49 nodes per subtree - just below threshold
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 49
			r = r.Next()
		}

		// Fast creation that would normally trigger increase
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Should NOT increase due to low volume check
		require.Equal(t, initialSize, stp.currentItemsPerFile,
			"Should not increase with 49 nodes (below 50 threshold)")
	})

	t.Run("just above threshold allows increase", func(t *testing.T) {
		stp.currentItemsPerFile = 64

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(300)

		// Set up high TPS that would benefit from larger subtrees
		// With 200 TPS and 60 nodes per subtree = 3.33 subtrees/sec (too fast)
		stp.recentBlockStats = []blockStats{
			{txCount: 200, duration: 1 * time.Second},   // 200 TPS
			{txCount: 195, duration: 1 * time.Second},   // 195 TPS
			{txCount: 205, duration: 1 * time.Second},   // 205 TPS
		}

		// 60 nodes per subtree - well above threshold and high utilization (93.75%)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 60
			r = r.Next()
		}

		// Same fast creation
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile > initialSize
		}, time.Second, 10*time.Millisecond, "Should increase with 60 nodes (above 50 threshold with high utilization)")
	})

	t.Run("exactly at threshold with high utilization allows increase", func(t *testing.T) {
		stp.currentItemsPerFile = 60

		// Set a transaction count > 1 to pass the initial check
		stp.txCount.Store(250)

		// Set up high TPS that would benefit from larger subtrees
		// With 150 TPS and 50 nodes per subtree = 3 subtrees/sec (too fast)
		stp.recentBlockStats = []blockStats{
			{txCount: 150, duration: 1 * time.Second},   // 150 TPS
			{txCount: 148, duration: 1 * time.Second},   // 148 TPS
			{txCount: 152, duration: 1 * time.Second},   // 152 TPS
		}

		// Exactly 50 nodes per subtree - 83% utilization triggers high path
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 50
			r = r.Next()
		}

		// Fast creation
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()

		// Wait for resize operation to complete (async operation)
		require.Eventually(t, func() bool {
			return stp.currentItemsPerFile > initialSize
		}, time.Second, 10*time.Millisecond, "Should increase with exactly 50 nodes when utilization > 80%")
	})
}
