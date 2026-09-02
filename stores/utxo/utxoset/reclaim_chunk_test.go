package utxoset

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// journalLeafExists reports whether the journal partition covering a height is still attached.
//
// The reclaim tests below need this so they cannot pass for the wrong reason. An assertion that
// a parent survived is worthless if the partition naming its spend never retired in the first
// place, because then the reclaimer was never asked the question.
func journalLeafExists(t *testing.T, s *Store, ctx context.Context, height uint32) bool {
	t.Helper()

	name := fmt.Sprintf("spend_journal_%d", height/SpendJournalPartitionBlocks)

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_class c
		  JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE i.inhparent = 'spend_journal'::regclass AND c.relname = $1`, name).Scan(&n))

	return n > 0
}

// spendOneOutput builds a transaction taking one of parent's outputs and applies the spend at
// height, leaving the spender unmined.
func spendOneOutput(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout uint32,
	height uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	_, err := s.Create(ctx, child, height)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, child, height)
	require.NoError(t, err)

	return child
}

// settledPair creates a parent with one output, spends it, and mines both deep enough that
// the reclaimer will delete the parent.
func settledPair(t *testing.T, s *Store, ctx context.Context, height uint32, blockID uint32) *bt.Tx {
	t.Helper()

	// Satoshis vary per call so every seeded parent is a distinct transaction; mkTx is
	// otherwise deterministic and the second Create would be refused as a duplicate.
	parent := mkTx(t, 1, uint64(5_000+blockID))
	_, err := s.Create(ctx, parent, height)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{
		BlockID: blockID, BlockHeight: height, OnLongestChain: true})
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, height)

	_, err = s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{
		BlockID: blockID + 1, BlockHeight: height + 10, OnLongestChain: true})
	require.NoError(t, err)

	return parent
}

// TestReclaimProcessesAPartitionInBoundedChunks.
//
// A journal table covers 48 blocks. At this project's own fat-band figures, 27,423
// transactions per block at 1.016 inputs each, that is about 1.34 million spend records in one
// table. The loop used to read every one of them into memory before asking a single question,
// which is roughly half a gigabyte of transient allocation against a 5 GiB heap ceiling, once
// per table, bounded by nothing.
//
// It is the same shape as two out-of-memory failures this codebase has already had: work that
// is fine at test volumes and fatal at chain volumes. So the bound is the behaviour under test,
// not the throughput.
func TestReclaimProcessesAPartitionInBoundedChunks(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96
	s.reclaimChunkParents = 3

	const at = uint32(100)

	parents := make([]*bt.Tx, 0, 10)
	for i := 0; i < 10; i++ {
		parents = append(parents, settledPair(t, s, ctx, at, uint32(100+i*2)))
	}

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	reclaimed, chunks, err := s.reclaimFromPartition(ctx, partition, 500, -1)
	require.NoError(t, err)

	require.Equal(t, 10, reclaimed, "every parent is fully spent by a settled spender")
	require.GreaterOrEqual(t, chunks, 4, "ten parents at three per chunk cannot be one pass")

	for _, p := range parents {
		require.False(t, identExists(t, s, ctx, p), "fully spent and settled, nothing can need it")
	}
}

// TestReclaimChunkingNeverSplitsAParentsSpenders.
//
// Chunking has to cut on a parent boundary, never on a row boundary. A parent is judged on
// whether EVERY transaction that spent it is buried deep, so a chunk holding only some of a
// parent's spenders would judge it on half the evidence and delete a record that is still
// needed. That is a smaller copy of the bug the reclaimer already has across tables.
//
// Here one parent has three outputs, two taken by settled spenders and one by a transaction
// still in the mempool. With a chunk size of one parent it cannot be split by construction, so
// the test sets the chunk size below the number of SPENDERS to catch an implementation that
// counts rows instead of parents.
func TestReclaimChunkingNeverSplitsAParentsSpenders(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96
	s.reclaimChunkParents = 2

	const at = uint32(100)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, at)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: at, OnLongestChain: true})
	require.NoError(t, err)

	// Two settled spenders.
	for vout, id := range []uint32{2, 3} {
		child := spendOneOutput(t, s, ctx, parent, uint32(vout), at)
		_, err = s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{
			BlockID: id, BlockHeight: at + 10, OnLongestChain: true})
		require.NoError(t, err)
	}

	// One spender that is still waiting, in the same table.
	spendOneOutput(t, s, ctx, parent, 2, at)

	// Company, so there is more than one parent to chunk over.
	other := settledPair(t, s, ctx, at, 40)

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	_, _, err = s.reclaimFromPartition(ctx, partition, 500, -1)
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent),
		"one of this parent's spenders is still in the mempool, so its record cannot go")
	require.False(t, identExists(t, s, ctx, other),
		"the unrelated parent is finished and should still be reclaimed")
}

// TestReadingEveryHeightReachesTheSameVerdictsAsOneWholeRead is the property the change rests on.
//
// A pruner run now reads ONE of a partition's 48 block heights rather than all of it. That is
// only safe if the 48 heights together reach exactly the verdicts one whole read would have
// reached. Anything else means spreading the work changed what gets deleted, which is a
// consensus-relevant difference rather than a scheduling one.
func TestReadingEveryHeightReachesTheSameVerdictsAsOneWholeRead(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	const at = uint32(100)

	// Twenty finished parents, spread across several heights of one partition.
	parents := make([]*bt.Tx, 0, 20)
	for i := 0; i < 20; i++ {
		parents = append(parents, settledPair(t, s, ctx, at+uint32(i%8), uint32(200+i*2)))
	}

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	base := int64(at/SpendJournalPartitionBlocks) * SpendJournalPartitionBlocks

	total := 0

	for off := int64(0); off < SpendJournalPartitionBlocks; off++ {
		n, _, rerr := s.reclaimFromPartition(ctx, partition, 500, base+off)
		require.NoError(t, rerr)

		total += n
	}

	require.Equal(t, 20, total,
		"every height together must reclaim exactly what one whole read would have")

	for _, p := range parents {
		require.False(t, identExists(t, s, ctx, p),
			"a parent missed by every height is a row nothing can ever reclaim")
	}
}

// TestOneHeightDoesNotDoTheWholePartition guards against the predicate silently matching
// everything, which would make the test above pass while nothing was spread at all.
func TestOneHeightDoesNotDoTheWholePartition(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	const at = uint32(100)

	for i := 0; i < 20; i++ {
		settledPair(t, s, ctx, at+uint32(i%8), uint32(300+i*2))
	}

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)
	base := int64(at/SpendJournalPartitionBlocks) * SpendJournalPartitionBlocks

	n, _, err := s.reclaimFromPartition(ctx, partition, 500, base+int64(at%SpendJournalPartitionBlocks))
	require.NoError(t, err)
	require.Less(t, n, 20, "one height must not reclaim every parent, or nothing is being spread")
}

// TestHeightOffsetsCoverHeightsThePrunerSkipped.
//
// The pruner deduplicates to the newest notification when it falls behind, so heights are
// skipped. A skipped height is one of a partition's own heights that no run would ever read, and
// a partition is dropped once its heights are due whether or not they were read, so a missed one
// is identity rows nothing can find again. The previous height is remembered in memory purely to
// close that gap, and losing it on a restart falls back to one height rather than to something
// wrong.
func TestHeightOffsetsCoverHeightsThePrunerSkipped(t *testing.T) {
	s, _ := newTestStore(t)

	require.Equal(t, []uint32{1000 % SpendJournalPartitionBlocks}, s.heightOffsetsFor(1000))
	require.Equal(t, []uint32{1001 % SpendJournalPartitionBlocks}, s.heightOffsetsFor(1001))

	require.Equal(t, []uint32{
		1002 % SpendJournalPartitionBlocks, 1003 % SpendJournalPartitionBlocks,
		1004 % SpendJournalPartitionBlocks, 1005 % SpendJournalPartitionBlocks,
		1006 % SpendJournalPartitionBlocks,
	}, s.heightOffsetsFor(1006), "every skipped height must still be read")

	require.Len(t, s.heightOffsetsFor(1006+SpendJournalPartitionBlocks+5),
		int(SpendJournalPartitionBlocks), "a gap of a whole partition means every height is due")

	require.Len(t, s.heightOffsetsFor(5), 1, "a height going backwards must not produce a wrapped range")
}

// TestOverduePartitionIsReadWholeBeforeItIsDropped is the backlog case, and it is the one
// that turns spreading the work into a row leak if it is got wrong.
//
// Spreading only works on the normal schedule, where a partition is read across the heights
// leading up to its due date. A partition that is ALREADY past its due date has no window left.
// Handing it one slice and then dropping it would destroy the work list with forty-seven
// forty-eighths of it unread, and the identity rows those slices would have reclaimed could
// never be found again, because the partition naming them is gone.
//
// This is not a hypothetical: the mainnet box is two thousand partitions behind, so every one
// of them is overdue.
func TestOverduePartitionIsReadWholeBeforeItIsDropped(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2

	seen := make(map[int64]int)

	// A height far past this partition's due date, which is what a backlog looks like, and an
	// offsets argument naming one height, which is what a normal run passes.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 100_000, []uint32{7},
		func(_ context.Context, _ string, at int64) error {
			seen[at]++
			return nil
		})
	require.NoError(t, err)

	require.Equal(t, 1, dropped, "an overdue partition must still be dropped")
	require.Equal(t, map[int64]int{-1: 1}, seen,
		"an overdue partition must be read WHOLE in one pass, not sliced: it has no window left to "+
			"spread across, and 48 passes shrink the array the decision queries get from 20,000 to 400")
}

// TestPartitionInsideItsWindowGetsOnlyThisRunsHeight is the other half of the pair above.
//
// If an overdue partition takes all its slices, the obvious mistake is to give every partition
// all its slices, which puts the work straight back into one run and undoes the change.
func TestPartitionInsideItsWindowGetsOnlyThisRunsHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2

	seen := make(map[int64]int)

	// Inside leaf 2's work window: read, not yet due.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 2*SpendJournalPartitionBlocks,
		[]uint32{7}, func(_ context.Context, _ string, at int64) error {
			seen[at]++
			return nil
		})
	require.NoError(t, err)

	require.Zero(t, dropped, "a partition inside its work window is not due yet")
	require.Equal(t, map[int64]int{2*SpendJournalPartitionBlocks + 7: 1}, seen,
		"a partition on the normal schedule reads one of its own heights a run, and offset 7 of "+
			"leaf 2 is absolute height 103")
}
