package utxo

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockStateHolderSnapshot pins issue 1443: a reader must never observe a
// pair that no single writer published. The writer publishes pairs with the
// fixed relation MedianTime == Height + 1000 via SetPair; concurrent readers
// assert every snapshot satisfies the relation. On the old two-independent-
// atomics layout the equivalent loop tears within a few thousand iterations.
func TestBlockStateHolderSnapshot(t *testing.T) {
	var holder BlockStateHolder

	holder.SetPair(1, 1001)

	const iterations = 200_000

	var wg sync.WaitGroup

	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				got := holder.Load()
				require.Equal(t, got.Height+1000, got.MedianTime, "torn snapshot: height %d paired with median time %d", got.Height, got.MedianTime)
			}
		}()
	}

	for i := uint32(2); i <= iterations; i++ {
		holder.SetPair(i, i+1000)
	}

	close(stop)
	wg.Wait()
}

// TestBlockStateHolderSingleFieldSetters pins the carry-forward semantics of
// the single-field setters: each preserves the other field, and the zero
// holder loads as the zero BlockState.
func TestBlockStateHolderSingleFieldSetters(t *testing.T) {
	var holder BlockStateHolder

	require.Equal(t, BlockState{}, holder.Load())

	holder.SetHeight(7)
	require.Equal(t, BlockState{Height: 7}, holder.Load())

	holder.SetMedianTime(99)
	require.Equal(t, BlockState{Height: 7, MedianTime: 99}, holder.Load())

	holder.SetHeight(8)
	require.Equal(t, BlockState{Height: 8, MedianTime: 99}, holder.Load())

	holder.SetPair(10, 200)
	require.Equal(t, BlockState{Height: 10, MedianTime: 200}, holder.Load())
}

// TestBlockStateHolderConcurrentSingleFieldSetters pins the CAS carry-forward:
// two goroutines each hammering ONE field must not erase each other's final
// value.
func TestBlockStateHolderConcurrentSingleFieldSetters(t *testing.T) {
	var holder BlockStateHolder

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := uint32(1); i <= 50_000; i++ {
			holder.SetHeight(i)
		}
	}()

	go func() {
		defer wg.Done()

		for i := uint32(1); i <= 50_000; i++ {
			holder.SetMedianTime(i)
		}
	}()

	wg.Wait()

	got := holder.Load()
	require.Equal(t, uint32(50_000), got.Height)
	require.Equal(t, uint32(50_000), got.MedianTime)
}
