package pruner

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	return h
}

func TestPrunedTxSet_AddAndContains(t *testing.T) {
	set := NewPrunedTxSet(16)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)
	h3 := makeHash(0x03)

	set.Add(h1)
	set.Add(h2)

	assert.True(t, set.Contains(h1))
	assert.True(t, set.Contains(h2))
	assert.False(t, set.Contains(h3))
}

func TestPrunedTxSet_CheckAndRemove(t *testing.T) {
	set := NewPrunedTxSet(16)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)

	set.Add(h1)
	set.Add(h2)

	// CheckAndRemove returns true and removes
	assert.True(t, set.CheckAndRemove(h1))
	// Second call returns false — already removed
	assert.False(t, set.CheckAndRemove(h1))
	assert.False(t, set.Contains(h1))

	// h2 still present
	assert.True(t, set.Contains(h2))
}

func TestPrunedTxSet_Len(t *testing.T) {
	set := NewPrunedTxSet(16)

	assert.Equal(t, 0, set.Len())

	set.Add(makeHash(0x01))
	set.Add(makeHash(0x02))
	assert.Equal(t, 2, set.Len())

	set.CheckAndRemove(makeHash(0x01))
	assert.Equal(t, 1, set.Len())
}

func TestPrunedTxSet_ConcurrentAccess(t *testing.T) {
	set := NewPrunedTxSet(256)
	const numGoroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Half the goroutines add entries
	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.Add(h)
			}
		}(g)
	}

	// Other half check and remove
	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.CheckAndRemove(h) // may or may not find it — just must not panic
			}
		}(g)
	}

	wg.Wait()
	// No assertion on final count — just verifying no data races or panics
}

func TestPrunedTxSet_ShardDistribution(t *testing.T) {
	set := NewPrunedTxSet(256)

	// Add hashes with different first bytes to verify they go to different shards
	for i := 0; i < 256; i++ {
		set.Add(makeHash(byte(i)))
	}

	assert.Equal(t, 256, set.Len())

	// Remove all
	for i := 0; i < 256; i++ {
		require.True(t, set.CheckAndRemove(makeHash(byte(i))))
	}

	assert.Equal(t, 0, set.Len())
}

func TestPrunedTxSet_SimulateChainPruning(t *testing.T) {
	// Simulate a tight chain: A -> B -> C -> D
	// All four TXs are in the same block and will be pruned
	// When processing B, A should be found in the set (skip parent update)
	// When processing C, B should be found (skip parent update)
	// etc.

	set := NewPrunedTxSet(16)

	txA := makeHash(0x0A)
	txB := makeHash(0x0B)
	txC := makeHash(0x0C)
	txD := makeHash(0x0D)

	// Stage 1 (reader) registers all TXIDs before processing starts
	set.Add(txA)
	set.Add(txB)
	set.Add(txC)
	set.Add(txD)

	assert.Equal(t, 4, set.Len())

	// Stage 2 (processor) processes B — parent is A
	assert.True(t, set.CheckAndRemove(txA), "parent A should be found and removed")

	// Stage 2 processes C — parent is B
	assert.True(t, set.CheckAndRemove(txB), "parent B should be found and removed")

	// Stage 2 processes D — parent is C
	assert.True(t, set.CheckAndRemove(txC), "parent C should be found and removed")

	// D has no child in this block — stays in set as dangling
	assert.True(t, set.Contains(txD))
	assert.Equal(t, 1, set.Len())
}

func TestPrunedTxSet_ParentNotInBlock(t *testing.T) {
	// TX_child's parent is NOT in this block — should not be found
	set := NewPrunedTxSet(16)

	txChild := makeHash(0x01)
	txParent := makeHash(0xFF) // parent from a previous block

	set.Add(txChild)

	// Parent not in set — must not skip update
	assert.False(t, set.CheckAndRemove(txParent))
}
