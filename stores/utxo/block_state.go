package utxo

import "sync/atomic"

// BlockStateHolder keeps the chain-tip pair — block height and median block
// time — behind a single atomic pointer, so a GetBlockState reader receives a
// genuine snapshot: both fields written together, never a new height paired
// with a stale median time (issue 1443). Before this, stores read two
// independent atomics one after the other, and a reader landing between the
// two setter calls saw a torn pair that could drive one finality decision
// with values from two different chain tips.
//
// SetPair is the consistent write path (both values from the same tip, as the
// blockchain notification listener supplies them). SetHeight and
// SetMedianTime exist for callers that legitimately update one field alone
// (tests, height-only bootstraps); they carry the other field forward with a
// compare-and-swap loop so concurrent single-field writers cannot erase each
// other's update — but a pair produced that way is only as consistent as the
// caller's sequencing, which is why production code uses SetPair.
type BlockStateHolder struct {
	pair atomic.Pointer[BlockState]
}

// Load returns the current snapshot; the zero BlockState before any write.
func (h *BlockStateHolder) Load() BlockState {
	if p := h.pair.Load(); p != nil {
		return *p
	}

	return BlockState{}
}

// SetPair atomically publishes both fields as one snapshot.
func (h *BlockStateHolder) SetPair(height, medianTime uint32) {
	h.pair.Store(&BlockState{Height: height, MedianTime: medianTime})
}

// SetHeight publishes a new height, carrying the latest median time forward.
func (h *BlockStateHolder) SetHeight(height uint32) {
	for {
		old := h.pair.Load()

		next := &BlockState{Height: height}
		if old != nil {
			next.MedianTime = old.MedianTime
		}

		if h.pair.CompareAndSwap(old, next) {
			return
		}
	}
}

// SetMedianTime publishes a new median time, carrying the latest height forward.
func (h *BlockStateHolder) SetMedianTime(medianTime uint32) {
	for {
		old := h.pair.Load()

		next := &BlockState{MedianTime: medianTime}
		if old != nil {
			next.Height = old.Height
		}

		if h.pair.CompareAndSwap(old, next) {
			return
		}
	}
}
