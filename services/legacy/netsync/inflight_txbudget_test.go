// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCalculateMaxInFlightBlocks_TxBudget exercises the transaction-WORK
// in-flight fetch bound (with the byte-safety clamp) that replaces the old
// fixed block-count step function. Each sub-case pins a distinct regime:
//
//	(a) tiny blocks           → high in-flight (near the cap, keeps peer busy)
//	(b) fat blocks            → low in-flight (few, bounds memory)
//	(c) few-but-huge txs      → byte clamp binds (in-flight bytes stay ≤ budget)
//	(d) no samples            → safe default, no divide-by-zero
func TestCalculateMaxInFlightBlocks_TxBudget(t *testing.T) {
	const (
		MB = int64(1024 * 1024)
		GB = 1024 * MB
	)

	t.Run("(a) tiny blocks stream many in flight, capped at maxInFlightBlocksCap", func(t *testing.T) {
		// 50k tx budget, tiny 1-tx blocks → txBound = 50000, but the hard cap
		// (1024) must bind so we don't request an absurd number. Byte budget
		// generous (4 GB / 256 B tiny blocks) so the byte clamp does not bind.
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 4*GB)
		for i := 0; i < 10; i++ {
			bst.addBlockStats(256, 1) // 256-byte, 1-tx blocks
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, maxInFlightBlocksCap, got,
			"tiny blocks must stream up to the hard cap to keep the peer busy")
	})

	t.Run("(a2) small-ish blocks: tx budget divides to a mid count", func(t *testing.T) {
		// 50k tx budget, 100-tx blocks → txBound = 500. Byte budget generous.
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 4*GB)
		for i := 0; i < 10; i++ {
			bst.addBlockStats(50*1024, 100) // 50 KB, 100-tx blocks
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, 500, got, "50000/100 = 500 in flight")
	})

	t.Run("(b) fat blocks stay few in flight", func(t *testing.T) {
		// 50k tx budget, ~2500-tx fat blocks → txBound = 20. Byte budget is
		// large enough (4 GB / ~1 MB blocks) not to bind, so the tx budget wins
		// and we get a small handful in flight — NOT the old flat 20-for-small,
		// this is 20 derived from WORK.
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 4*GB)
		for i := 0; i < 10; i++ {
			bst.addBlockStats(1*MB, 2500) // ~1 MB, 2500-tx blocks
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, 20, got, "50000/2500 = 20 in flight for fat blocks")
	})

	t.Run("(b2) very fat blocks clamp toward the admit-one floor", func(t *testing.T) {
		// 50k tx budget, 60k-tx monster blocks → txBound = 0 → floor to 1.
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 8*GB)
		for i := 0; i < 5; i++ {
			bst.addBlockStats(32*MB, 60000)
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, 1, got, "monster blocks must floor to at least 1 in flight")
	})

	t.Run("(c) byte-safety clamp binds when txs are few but huge", func(t *testing.T) {
		// Few transactions (10) with a huge tx budget would allow thousands in
		// flight (50000/10 = 5000, capped at 1024). But each block is 1.5 GB of
		// a handful of huge txs. With a 3 GB byte budget the byte clamp must bind
		// at 2 (3 GB / 1.5 GB), keeping in-flight bytes ≤ the budget even though
		// the tx count is misleadingly small.
		const (
			byteBudget = 3 * GB
			avgSize    = 1536 * MB // 1.5 GB
		)

		bst := newBlockSizeTrackerWithBudgets(10, 50000, byteBudget)
		for i := 0; i < 4; i++ {
			bst.addBlockStats(avgSize, 10) // 1.5 GB blocks, only 10 huge txs each
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, 2, got, "byte clamp must bind at 2 despite the tx budget allowing more")

		// The clamp's whole purpose: in-flight bytes must stay within budget.
		require.LessOrEqual(t, int64(got)*avgSize, byteBudget,
			"in-flight bytes must not exceed the byte budget")
	})

	t.Run("(d) no samples → safe default, no divide-by-zero", func(t *testing.T) {
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 4*GB)
		// No addBlockStats call → avgTxCount == 0.

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, noSampleInFlightDefault, got,
			"no samples must fall back to the safe default without dividing by zero")
	})

	t.Run("result is never below 1", func(t *testing.T) {
		// Tiny byte budget smaller than one block → byteBound = 0 → floor to 1.
		bst := newBlockSizeTrackerWithBudgets(10, 50000, 1*MB)
		for i := 0; i < 3; i++ {
			bst.addBlockStats(1*GB, 2500) // block bigger than the byte budget
		}

		got := bst.calculateMaxInFlightBlocks()
		require.Equal(t, 1, got, "must always keep at least 1 in flight")
	})
}

// TestBlockSizeTracker_RollingAverages verifies addBlockStats maintains both
// rolling averages and honours the maxSamples window.
func TestBlockSizeTracker_RollingAverages(t *testing.T) {
	bst := newBlockSizeTracker(3) // window of 3

	bst.addBlockStats(100, 10)
	bst.addBlockStats(200, 20)
	bst.addBlockStats(300, 30)

	require.Equal(t, int64(200), bst.getAverageSize(), "avg of 100/200/300")
	require.Equal(t, int64(20), bst.getAverageTxCount(), "avg of 10/20/30")

	// Fourth sample evicts the first from both windows.
	bst.addBlockStats(600, 60)
	require.Equal(t, int64((200+300+600)/3), bst.getAverageSize())
	require.Equal(t, int64((20+30+60)/3), bst.getAverageTxCount())
}
