package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	blob_memory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestSubtreeProcessor_LowVolumeNeverIncreases tests that with low transaction volume,
// the subtree size never increases even with high utilization
func TestSubtreeProcessor_LowVolumeNeverIncreases(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings()
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
		
		// Simulate 87.5% utilization (7 nodes in size-8 subtree)
		stp.subtreeNodeCounts = []int{7, 7, 7, 7, 7}
		
		// Even with fast subtree creation (200ms intervals)
		stp.blockIntervals = []time.Duration{
			200 * time.Millisecond,
			200 * time.Millisecond,
			200 * time.Millisecond,
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
		
		// Simulate 6.25% utilization (2 nodes in size-32 subtree)
		stp.subtreeNodeCounts = []int{2, 2, 2, 2, 2}
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
		
		// Simulate 50% utilization (8 nodes in size-16 subtree)
		stp.subtreeNodeCounts = []int{8, 8, 8, 8, 8}
		stp.blockIntervals = []time.Duration{1 * time.Second}
		
		initialSize := stp.currentItemsPerFile
		stp.adjustSubtreeSize()
		
		// Size should stay the same with moderate utilization
		require.Equal(t, initialSize, stp.currentItemsPerFile,
			"Size should remain stable with moderate utilization")
	})
}

// TestSubtreeProcessor_UsageBasedCapping tests that size increases are capped
// based on actual observed usage
func TestSubtreeProcessor_UsageBasedCapping(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings()
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
		
		// High utilization with moderate volume
		// Max 27 nodes seen, average 25
		stp.subtreeNodeCounts = []int{23, 25, 27, 24, 26, 25, 24, 26, 25, 25}
		
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
		
		// High utilization with high volume (> 50 nodes)
		stp.subtreeNodeCounts = []int{60, 62, 58, 61, 59, 60, 61, 60, 60, 60}
		
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
	settings := test.CreateBaseTestSettings()
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
		stp.subtreeNodeCounts = []int{900, 950, 920, 940, 910}
		stp.blockIntervals = []time.Duration{500 * time.Millisecond}
		
		stp.adjustSubtreeSize()
		highLoadSize := stp.currentItemsPerFile
		require.GreaterOrEqual(t, highLoadSize, 1024,
			"Should maintain or increase size under high load")
		
		// Transition to low load (2-4 tx/s scenario)
		stp.currentItemsPerFile = highLoadSize
		stp.subtreeNodeCounts = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
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
				stp.subtreeNodeCounts = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
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
	settings := test.CreateBaseTestSettings()
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
		
		// Should have tracked 4 nodes (excluding coinbase)
		require.Len(t, stp.subtreeNodeCounts, 1)
		require.Equal(t, 4, stp.subtreeNodeCounts[0],
			"Should track correct number of non-coinbase nodes")
		
		// Test that old samples are removed after limit
		// First, fill up to 99 samples (we already have 1)
		for i := 0; i < 98; i++ {
			stp.subtreeNodeCounts = append(stp.subtreeNodeCounts, 5)
		}
		require.Len(t, stp.subtreeNodeCounts, 99)
		
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
		
		// This should add one more, reaching 100
		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)
		require.Len(t, stp.subtreeNodeCounts, 100)
		
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
		
		// Should still be at max 100 samples (oldest one removed)
		require.Len(t, stp.subtreeNodeCounts, 100,
			"Should limit tracked samples to 100")
	})
}

// TestSubtreeProcessor_MinimumSizeRespected tests that minimum size is always respected
func TestSubtreeProcessor_MinimumSizeRespected(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings()
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
		stp.subtreeNodeCounts = []int{1, 1, 1, 1, 1}
		stp.blockIntervals = []time.Duration{5 * time.Second}
		
		stp.adjustSubtreeSize()
		
		// Should stay at minimum
		require.Equal(t, settings.BlockAssembly.MinimumMerkleItemsPerSubtree,
			stp.currentItemsPerFile,
			"Size should not go below minimum")
	})
}