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

	reclaimed, chunks, err := s.reclaimFromPartition(ctx, partition, 500, 0)
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

	_, _, err = s.reclaimFromPartition(ctx, partition, 500, 0)
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent),
		"one of this parent's spenders is still in the mempool, so its record cannot go")
	require.False(t, identExists(t, s, ctx, other),
		"the unrelated parent is finished and should still be reclaimed")
}

// TestSlicingReachesTheSameVerdictsAsOneWholeRead is the property the whole change rests on.
//
// A pruner run now reads a fraction of a journal partition rather than all of it, choosing the
// fraction from a byte of each parent's transaction id. That is only safe if the slices together
// reach exactly the verdicts one unsliced read would have reached. Anything else means the
// spreading changed what gets deleted, which is a consensus-relevant difference and not a
// scheduling one.
func TestSlicingReachesTheSameVerdictsAsOneWholeRead(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	const at = uint32(100)

	// Twenty finished parents, which at 48 slices spread across many different slices.
	parents := make([]*bt.Tx, 0, 20)
	for i := 0; i < 20; i++ {
		parents = append(parents, settledPair(t, s, ctx, at, uint32(200+i*2)))
	}

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	// Production slicing on.
	s.journalSlices = SpendJournalSlices

	// One slice alone must NOT finish the job, or this test would pass without slicing working.
	reclaimedOne, _, err := s.reclaimFromPartition(ctx, partition, 500, 0)
	require.NoError(t, err)
	require.Less(t, reclaimedOne, 20,
		"one slice of 48 must not reclaim every parent, or the predicate is not slicing anything")

	total := reclaimedOne

	for slice := int16(1); slice < SpendJournalSlices; slice++ {
		n, _, rerr := s.reclaimFromPartition(ctx, partition, 500, slice)
		require.NoError(t, rerr)

		total += n
	}

	require.Equal(t, 20, total,
		"every slice together must reclaim exactly what one unsliced read would have")

	for _, p := range parents {
		require.False(t, identExists(t, s, ctx, p),
			"a parent missed by every slice is a row nothing can ever reclaim")
	}
}

// TestSliceNeverSplitsAParentAcrossBlocks is why the slice is chosen from the transaction id
// rather than from the block a spend landed in.
//
// A parent's outputs can be taken in different blocks of the same 48-block window, measured at
// 16.2% of parents on a real mainnet partition. Slicing on the block height would then show one
// run only some of who spent that parent, and a parent judged on part of its spenders can be
// called finished when it is not. Slicing on the parent's own id cannot do that, because every
// record naming a parent carries the same id.
func TestSliceNeverSplitsAParentAcrossBlocks(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96
	s.journalSlices = SpendJournalSlices

	const at = uint32(100)

	// A parent with two outputs, taken in two DIFFERENT blocks of the same window.
	parent := mkTx(t, 2, 7_000)
	_, err := s.Create(ctx, parent, at)
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, at)
	spendOneOutput(t, s, ctx, parent, 1, at+10)

	partition := fmt.Sprintf("spend_journal_%d", at/SpendJournalPartitionBlocks)

	// Whichever slice this parent belongs to, that one run must see BOTH of its spenders. Run
	// every slice and count how many of them found any record naming this parent at all.
	hash := hashBytes(parent)

	var slicesNamingParent, recordsInThatSlice int

	for slice := int16(0); slice < SpendJournalSlices; slice++ {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE txid = $1 AND mod(get_byte(txid,31), %d) = $2`,
				partition, SpendJournalSlices), hash, slice).Scan(&n))

		if n > 0 {
			slicesNamingParent++
			recordsInThatSlice = n
		}
	}

	require.Equal(t, 1, slicesNamingParent,
		"a parent must belong to exactly one slice, whatever blocks its outputs were taken in")
	require.Equal(t, 2, recordsInThatSlice,
		"and that one slice must carry BOTH of its spend records, or it judges on half the evidence")
}

// TestSlicesForCoversHeightsThePrunerSkipped.
//
// The pruner deduplicates to the newest notification when it falls behind, so heights are
// skipped. A skipped height is a slice no run would ever do, and a partition is dropped after
// its slices are due whether or not they ran, so a missed slice is identity rows nothing can
// reclaim afterwards. The previous height is remembered in memory purely to close that gap, and
// losing it on a restart falls back to doing one slice rather than to doing something wrong.
func TestSlicesForCoversHeightsThePrunerSkipped(t *testing.T) {
	s, _ := newTestStore(t)
	s.journalSlices = SpendJournalSlices

	// Nothing remembered yet: one slice, not all of them. Doing all of them on a large backlog
	// is the unbounded session this change exists to remove.
	require.Equal(t, []int16{int16(1000 % SpendJournalSlices)}, s.slicesFor(1000))

	// The next block along: one slice again.
	require.Equal(t, []int16{int16(1001 % SpendJournalSlices)}, s.slicesFor(1001))

	// Four heights skipped: all five slices, so none is lost.
	got := s.slicesFor(1006)
	require.Equal(t, []int16{
		int16(1002 % SpendJournalSlices), int16(1003 % SpendJournalSlices),
		int16(1004 % SpendJournalSlices), int16(1005 % SpendJournalSlices),
		int16(1006 % SpendJournalSlices),
	}, got, "every skipped height's slice must still be done")

	// A gap of a whole partition width or more: every slice is due, so return them all rather
	// than walking a range that wraps.
	require.Len(t, s.slicesFor(1006+SpendJournalSlices+5), int(SpendJournalSlices))

	// A height going backwards must not produce a wrapped range.
	require.Len(t, s.slicesFor(5), 1)
}

// TestPartitionIsWorkedBeforeItIsDueAndDroppedOnTime pins the schedule, and the point of the
// test is the SECOND half.
//
// The work is spread across the SpendJournalSlices heights LEADING UP TO a partition's due date,
// not the ones after it. Doing it after would mean holding every partition an extra 48 blocks
// beyond DefaultSpendJournalRetentionBlocks purely to have somewhere to put the work, which is
// more retained undo history for no gain. So a partition must be readable before it is
// droppable, and must still be dropped at exactly the height it would have been dropped at
// before any of this.
func TestPartitionIsWorkedBeforeItIsDueAndDroppedOnTime(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalSlices = SpendJournalSlices

	// One partition, leaf 2, covering block heights 96 to 143.
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100))

	const leaf = uint32(2)

	// The height argument here is already retention-adjusted by the caller, so it is the
	// partition's own coordinates. Leaf 2 is worked from 96 and dropped at 144.
	worked := make(map[string]int)

	countWork := func(_ context.Context, partition string, _ int16) error {
		worked[partition]++
		return nil
	}

	// Just before the work window: nothing read, nothing dropped.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, leaf*SpendJournalPartitionBlocks-1,
		[]int16{0}, countWork)
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Empty(t, worked, "a partition must not be read before its work window opens")

	// Inside the work window: read, but NOT dropped.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx, leaf*SpendJournalPartitionBlocks,
		[]int16{0}, countWork)
	require.NoError(t, err)
	require.Zero(t, dropped, "the work window must not drop the work list out from under itself")
	require.Equal(t, 1, worked["spend_journal_2"], "the partition must be readable before it is due")

	// The last height of the work window: still read, still not dropped.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx,
		leaf*SpendJournalPartitionBlocks+SpendJournalSlices-1, []int16{0}, countWork)
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Equal(t, 2, worked["spend_journal_2"])

	// Due: dropped, at exactly the height the single-cutoff version would have dropped it.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx,
		(leaf+1)*SpendJournalPartitionBlocks, []int16{0}, countWork)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "retention must be unchanged: due means dropped, not deferred")
}

// TestOverduePartitionGetsEverySliceBeforeItIsDropped is the backlog case, and it is the one
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
func TestOverduePartitionGetsEverySliceBeforeItIsDropped(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalSlices = SpendJournalSlices

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2

	seen := make(map[int16]int)

	// A height far past this partition's due date, which is what a backlog looks like, and a
	// slices argument naming only one slice, which is what a normal run passes.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 100_000, []int16{7},
		func(_ context.Context, _ string, slice int16) error {
			seen[slice]++
			return nil
		})
	require.NoError(t, err)

	require.Equal(t, 1, dropped, "an overdue partition must still be dropped")
	require.Len(t, seen, int(SpendJournalSlices),
		"every slice must be read before an overdue partition goes, not just the one this run was due to do")
}

// TestPartitionInsideItsWindowGetsOnlyThisRunsSlice is the other half of the pair above.
//
// If an overdue partition takes all its slices, the obvious mistake is to give every partition
// all its slices, which puts the work straight back into one run and undoes the change.
func TestPartitionInsideItsWindowGetsOnlyThisRunsSlice(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalSlices = SpendJournalSlices

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2

	seen := make(map[int16]int)

	// Inside leaf 2's work window: read, not yet due.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 2*SpendJournalPartitionBlocks,
		[]int16{7}, func(_ context.Context, _ string, slice int16) error {
			seen[slice]++
			return nil
		})
	require.NoError(t, err)

	require.Zero(t, dropped, "a partition inside its work window is not due yet")
	require.Equal(t, map[int16]int{7: 1}, seen,
		"a partition on the normal schedule takes one slice a run, or the work is not spread at all")
}
