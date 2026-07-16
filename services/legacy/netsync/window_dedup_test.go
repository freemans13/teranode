// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Regression test for the create-vs-create Postgres deadlock (40P01) root cause:
// under refetch/park churn the SAME block could be added to the window accumulator
// more than once, and drainJob (which did not dedupe) then committed it twice, so
// ProcessBlockWindow created that block's txs concurrently and collided on the txs
// unique index. drainJob now de-duplicates by height (the window is below-checkpoint,
// one block per height), eliminating the duplicate concurrent creates at the source.
import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func TestWindowDrainJob_DedupesDuplicateBlocks(t *testing.T) {
	wa := newWindowAccumulator(1<<30, 0) // huge budget, no count cap

	// Add blocks out of order, with height 11 and 12 each delivered TWICE (the
	// re-delivery-while-still-queued case that churn produces).
	wa.add(&model.Block{Height: 12, SizeInBytes: 10})
	wa.add(&model.Block{Height: 10, SizeInBytes: 10})
	wa.add(&model.Block{Height: 11, SizeInBytes: 10})
	wa.add(&model.Block{Height: 11, SizeInBytes: 10}) // duplicate
	wa.add(&model.Block{Height: 12, SizeInBytes: 10}) // duplicate

	job, ok := wa.drainJob()
	require.True(t, ok)

	heights := make([]uint32, 0, len(job.blocks))
	for _, b := range job.blocks {
		heights = append(heights, b.Height)
	}

	require.Equal(t, []uint32{10, 11, 12}, heights,
		"drainJob must dedupe repeated heights and return a single, strictly-ascending run")
}

func TestWindowDrainJob_NoDuplicatesUnchanged(t *testing.T) {
	wa := newWindowAccumulator(1<<30, 0)
	wa.add(&model.Block{Height: 7, SizeInBytes: 10})
	wa.add(&model.Block{Height: 5, SizeInBytes: 10})
	wa.add(&model.Block{Height: 6, SizeInBytes: 10})

	job, ok := wa.drainJob()
	require.True(t, ok)
	require.Len(t, job.blocks, 3, "a clean window is unchanged")
	require.Equal(t, uint32(5), job.blocks[0].Height)
	require.Equal(t, uint32(7), job.blocks[2].Height)
}
