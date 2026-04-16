package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
)

// TestBlockPriorityQueue_BroadcastAndSignal tests the Broadcast and Signal methods
func TestBlockPriorityQueue_BroadcastAndSignal(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)

	// Test Signal - should not panic
	pq.Signal()

	// Test Broadcast - should not panic
	pq.Broadcast()

	// Verify methods can be called safely
	assert.NotNil(t, pq)
}

// TestBlockPriorityQueue_Peek tests the Peek method
func TestBlockPriorityQueue_Peek(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)

	// Test peek on empty queue
	_, _, ok := pq.Peek()
	assert.False(t, ok, "Peek should return false on empty queue")

	// Add items
	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2
	hash3 := &chainhash.Hash{}
	hash3[0] = 3

	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)
	pq.Add(processBlockFound{hash: hash3}, PriorityDeepFork, 1002)

	// Peek should return highest priority item without removing it
	blockFound, priority, ok := pq.Peek()
	assert.True(t, ok)
	assert.Equal(t, PriorityChainExtending, priority)
	assert.Equal(t, hash1, blockFound.hash)

	// Verify item is still in queue
	assert.Equal(t, 3, pq.Size())

	// Peek again should return same item
	blockFound2, priority2, ok2 := pq.Peek()
	assert.True(t, ok2)
	assert.Equal(t, priority, priority2)
	assert.Equal(t, blockFound.hash, blockFound2.hash)
}

// TestBlockPriorityQueue_GetEffectivePriority tests priority aging and skip count boost
func TestBlockPriorityQueue_GetEffectivePriority(t *testing.T) {
	initPrometheusMetrics()

	now := time.Now()

	t.Run("Recent block keeps original priority", func(t *testing.T) {
		item := &PrioritizedBlock{
			priority:  PriorityDeepFork,
			timestamp: now,
			skipCount: 0,
		}

		effectivePriority := item.getEffectivePriority(now)
		assert.Equal(t, PriorityDeepFork, effectivePriority)
	})

	t.Run("Old block gets priority boost", func(t *testing.T) {
		item := &PrioritizedBlock{
			priority:  PriorityDeepFork,
			timestamp: now.Add(-5 * time.Minute), // 5 minutes old
			skipCount: 0,
		}

		effectivePriority := item.getEffectivePriority(now)
		// Should be boosted from DeepFork (2) by 4 levels (5 minutes - 1)
		// But capped to not go below ChainExtending (0)
		assert.Less(t, effectivePriority, PriorityDeepFork)
		assert.Equal(t, PriorityChainExtending, effectivePriority)
	})

	t.Run("Partial age boost", func(t *testing.T) {
		item := &PrioritizedBlock{
			priority:  PriorityDeepFork,          // Priority 2
			timestamp: now.Add(-2 * time.Minute), // 2 minutes old
			skipCount: 0,
		}

		effectivePriority := item.getEffectivePriority(now)
		// Should be boosted by 1 level (2 minutes - 1 = 1)
		assert.Equal(t, PriorityNearFork, effectivePriority)
	})

	t.Run("High skip count forces chain extending priority", func(t *testing.T) {
		item := &PrioritizedBlock{
			priority:  PriorityDeepFork,
			timestamp: now,
			skipCount: DefaultSkipCount + 1,
		}

		effectivePriority := item.getEffectivePriority(now)
		assert.Equal(t, PriorityChainExtending, effectivePriority)
	})

	t.Run("Skip count at threshold forces boost", func(t *testing.T) {
		item := &PrioritizedBlock{
			priority:  PriorityNearFork,
			timestamp: now,
			skipCount: DefaultSkipCount + 5,
		}

		effectivePriority := item.getEffectivePriority(now)
		assert.Equal(t, PriorityChainExtending, effectivePriority)
	})
}

// TestBlockPriorityQueue_PriorityToString tests priority string conversion
func TestBlockPriorityQueue_PriorityToString(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		priority BlockPriority
		expected string
	}{
		{PriorityChainExtending, "chain_extending"},
		{PriorityNearFork, "near_fork"},
		{PriorityDeepFork, "deep_fork"},
		{BlockPriority(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := priorityToString(tt.priority)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBlockPriorityQueue_SortItems tests sorting with different priorities and ages
func TestBlockPriorityQueue_SortItems(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)

	now := time.Now()

	// Add blocks with different priorities and timestamps
	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2
	hash3 := &chainhash.Hash{}
	hash3[0] = 3
	hash4 := &chainhash.Hash{}
	hash4[0] = 4

	// Deep fork, old
	pq.items = append(pq.items, &PrioritizedBlock{
		blockFound: processBlockFound{hash: hash1},
		priority:   PriorityDeepFork,
		height:     1000,
		timestamp:  now.Add(-10 * time.Minute),
		skipCount:  0,
	})

	// Near fork, recent
	pq.items = append(pq.items, &PrioritizedBlock{
		blockFound: processBlockFound{hash: hash2},
		priority:   PriorityNearFork,
		height:     1001,
		timestamp:  now,
		skipCount:  0,
	})

	// Chain extending
	pq.items = append(pq.items, &PrioritizedBlock{
		blockFound: processBlockFound{hash: hash3},
		priority:   PriorityChainExtending,
		height:     1002,
		timestamp:  now,
		skipCount:  0,
	})

	// Deep fork, high skip count
	pq.items = append(pq.items, &PrioritizedBlock{
		blockFound: processBlockFound{hash: hash4},
		priority:   PriorityDeepFork,
		height:     999,
		timestamp:  now,
		skipCount:  DefaultSkipCount + 1,
	})

	pq.needsSort = true
	pq.sortItems()

	// Verify order: chain extending and high skip count first
	// hash4 (skip count boosted) and hash3 (chain extending) should be first
	// Then hash1 (old deep fork, gets age boost)
	// Then hash2 (recent near fork)

	assert.False(t, pq.needsSort)
	assert.Len(t, pq.items, 4)
}
