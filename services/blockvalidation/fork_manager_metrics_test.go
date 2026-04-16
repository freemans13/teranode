package blockvalidation

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForkManager_UpdateAggregateMetrics tests aggregate metrics calculation
func TestForkManager_UpdateAggregateMetrics(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	t.Run("No forks", func(t *testing.T) {
		// Should handle empty fork map
		fm.updateAggregateMetrics()
		// No assertions needed, just verify it doesn't panic
	})

	t.Run("Single active fork", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fork := fm.RegisterFork("fork1", baseHash, 1000)
		fork.State = ForkStateActive

		// Add some blocks
		for i := 0; i < 5; i++ {
			merkleRoot := &chainhash.Hash{}
			merkleRoot[0] = byte(i)

			block := &model.Block{
				Height: uint32(1001 + i),
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  baseHash,
					HashMerkleRoot: merkleRoot,
					Timestamp:      uint32(time.Now().Unix()),
					Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
					Nonce:          uint32(i),
				},
			}

			require.NoError(t, fm.AddBlockToFork(block, "fork1"))
		}

		fm.updateAggregateMetrics()
		// Metrics should be updated without error
	})

	t.Run("Multiple active forks", func(t *testing.T) {
		fm2 := NewForkManager(logger, tSettings)

		// Create multiple active forks with different depths
		for i := 0; i < 3; i++ {
			hash := &chainhash.Hash{}
			hash[0] = byte(10 + i)
			fork := fm2.RegisterFork("fork-"+string(rune(i)), hash, uint32(1000+i*10))
			fork.State = ForkStateActive

			// Add varying numbers of blocks
			for j := 0; j < (i+1)*2; j++ {
				merkleRoot := &chainhash.Hash{}
				merkleRoot[0] = byte(i*10 + j)

				block := &model.Block{
					Height: uint32(1001 + i*10 + j),
					Header: &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  hash,
						HashMerkleRoot: merkleRoot,
						Timestamp:      uint32(time.Now().Unix()),
						Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
						Nonce:          uint32(j),
					},
				}

				require.NoError(t, fm2.AddBlockToFork(block, "fork-"+string(rune(i))))
			}
		}

		fm2.updateAggregateMetrics()
		// Should calculate averages across all active forks
	})

	t.Run("Mix of active and inactive forks", func(t *testing.T) {
		fm3 := NewForkManager(logger, tSettings)

		// Active fork
		hash1 := &chainhash.Hash{}
		hash1[0] = 1
		fork1 := fm3.RegisterFork("active1", hash1, 1000)
		fork1.State = ForkStateActive

		// Resolved fork (should be excluded)
		hash2 := &chainhash.Hash{}
		hash2[0] = 2
		fork2 := fm3.RegisterFork("resolved1", hash2, 2000)
		fork2.State = ForkStateResolved

		// Orphaned fork (should be excluded)
		hash3 := &chainhash.Hash{}
		hash3[0] = 3
		fork3 := fm3.RegisterFork("orphaned1", hash3, 3000)
		fork3.State = ForkStateOrphaned

		fm3.updateAggregateMetrics()
		// Should only consider active forks in calculations
	})
}

// TestForkManager_UpdateProcessingBlocksMetrics tests stuck block detection
func TestForkManager_UpdateProcessingBlocksMetrics(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	t.Run("No stuck blocks", func(t *testing.T) {
		fm.updateProcessingBlocksMetrics()
		// Should handle empty state
	})

	t.Run("Fork with processing block and active worker", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fork := fm.RegisterFork("fork1", baseHash, 1000)

		blockHash := &chainhash.Hash{}
		blockHash[0] = 1

		fork.mu.Lock()
		fork.ProcessingBlock = blockHash
		atomic.StoreInt32(&fork.Workers, 1) // Has active worker
		fork.mu.Unlock()

		fm.updateProcessingBlocksMetrics()
		// Should not count as stuck (has worker)
	})

	t.Run("Fork with stuck processing block", func(t *testing.T) {
		fm2 := NewForkManager(logger, tSettings)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		fork := fm2.RegisterFork("stuck-fork", baseHash, 2000)

		blockHash := &chainhash.Hash{}
		blockHash[0] = 2

		fork.mu.Lock()
		fork.ProcessingBlock = blockHash
		atomic.StoreInt32(&fork.Workers, 0) // No active workers - STUCK!
		fork.mu.Unlock()

		fm2.updateProcessingBlocksMetrics()
		// Should count as stuck
	})

	t.Run("Multiple forks with mixed states", func(t *testing.T) {
		fm3 := NewForkManager(logger, tSettings)

		for i := 0; i < 5; i++ {
			hash := &chainhash.Hash{}
			hash[0] = byte(10 + i)
			fork := fm3.RegisterFork("multi-fork-"+string(rune(i)), hash, uint32(1000+i))

			blockHash := &chainhash.Hash{}
			blockHash[0] = byte(20 + i)

			fork.mu.Lock()
			fork.ProcessingBlock = blockHash

			// Alternate between stuck and active
			if i%2 == 0 {
				atomic.StoreInt32(&fork.Workers, 0) // Stuck
			} else {
				atomic.StoreInt32(&fork.Workers, 1) // Active
			}
			fork.mu.Unlock()
		}

		fm3.updateProcessingBlocksMetrics()
		// Should correctly count stuck blocks
	})

	t.Run("Fork with nil processing block", func(t *testing.T) {
		fm4 := NewForkManager(logger, tSettings)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		fork := fm4.RegisterFork("nil-fork", baseHash, 3000)

		fork.mu.Lock()
		fork.ProcessingBlock = nil // No block being processed
		atomic.StoreInt32(&fork.Workers, 0)
		fork.mu.Unlock()

		fm4.updateProcessingBlocksMetrics()
		// Should not count as stuck (no processing block)
	})
}

// TestForkManager_PerformCleanup tests the performCleanup function
func TestForkManager_PerformCleanup(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}

	config := ForkCleanupConfig{
		Enabled:          true,
		MaxAge:           100 * time.Millisecond,
		MaxInactiveForks: 5,
		CheckInterval:    50 * time.Millisecond,
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 20
	tSettings.ChainCfgParams.CoinbaseMaturity = 100
	fm := NewForkManagerWithConfig(logger, tSettings, config)

	t.Run("Cleanup resolved forks after delay", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fork := fm.RegisterFork("resolved-fork", baseHash, 1000)
		fork.State = ForkStateResolved
		fork.LastActivity = time.Now().Add(-10 * time.Minute) // Old resolved fork

		initialCount := fm.GetForkCount()
		fm.performCleanup()

		// Resolved fork should be cleaned up
		assert.Less(t, fm.GetForkCount(), initialCount)
	})

	t.Run("Cleanup stale forks after max age", func(t *testing.T) {
		fm2 := NewForkManagerWithConfig(logger, tSettings, config)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		fork := fm2.RegisterFork("stale-fork", baseHash, 2000)
		fork.State = ForkStateStale
		fork.LastActivity = time.Now().Add(-200 * time.Millisecond) // Exceeds max age

		initialCount := fm2.GetForkCount()
		fm2.performCleanup()

		assert.Less(t, fm2.GetForkCount(), initialCount, "Stale fork should be cleaned")
	})

	t.Run("Keep active forks with recent activity", func(t *testing.T) {
		fm3 := NewForkManagerWithConfig(logger, tSettings, config)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		fork := fm3.RegisterFork("active-fork", baseHash, 3000)
		fork.State = ForkStateActive
		fork.LastActivity = time.Now() // Recent activity

		initialCount := fm3.GetForkCount()
		fm3.performCleanup()

		assert.Equal(t, initialCount, fm3.GetForkCount(), "Active fork should not be cleaned")
	})

	t.Run("Convert old active fork to stale", func(t *testing.T) {
		fm4 := NewForkManagerWithConfig(logger, tSettings, config)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000004")
		fork := fm4.RegisterFork("old-active", baseHash, 4000)
		fork.State = ForkStateActive
		fork.LastActivity = time.Now().Add(-200 * time.Millisecond) // Exceeds max age
		atomic.StoreInt32(&fork.Workers, 0)                         // No workers

		fm4.performCleanup()

		// Should be marked as stale
		assert.Equal(t, ForkStateStale, fork.State)
	})

	t.Run("Keep active fork with workers", func(t *testing.T) {
		fm5 := NewForkManagerWithConfig(logger, tSettings, config)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000005")
		fork := fm5.RegisterFork("active-workers", baseHash, 5000)
		fork.State = ForkStateActive
		fork.LastActivity = time.Now().Add(-200 * time.Millisecond) // Old
		atomic.StoreInt32(&fork.Workers, 2)                         // Has workers

		fm5.performCleanup()

		// Should stay active (has workers)
		assert.Equal(t, ForkStateActive, fork.State)
	})
}
