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

// recordReads is a `before` callback that records which heights were read from which partition.
// A read of -1 is a whole-partition read.
func recordReads(seen map[string][]int64) func(context.Context, string, int64) error {
	return func(_ context.Context, partition string, at int64) error {
		seen[partition] = append(seen[partition], at)
		return nil
	}
}

// TestNormalScheduleReadsEachHeightOnceAndNeverTheWholePartition drives one partition through its
// entire work window, one run per height, and then through the run where it becomes due.
//
// This is the test that caught the bug the verifiers found. An earlier version defaulted every
// due partition to a whole read, so a partition on the normal schedule was read 48 times a
// height at a time and then a 49th time in full before it was dropped. Spreading the work then
// cost more than not spreading it, and the peak run it was meant to remove was unchanged.
func TestNormalScheduleReadsEachHeightOnceAndNeverTheWholePartition(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: heights 96..143

	seen := map[string][]int64{}

	// The 48 runs whose cutoff lies inside leaf 2, each knowing the previous cutoff.
	for cutoff := uint32(96); cutoff <= 143; cutoff++ {
		dropped, err := s.dropSpendJournalPartitionsBelow(ctx, cutoff, cutoff-1, recordReads(seen))
		require.NoError(t, err)
		require.Zero(t, dropped, "leaf 2 is inside its work window at cutoff %d", cutoff)
	}

	counts := map[int64]int{}
	for _, at := range seen["spend_journal_2"] {
		counts[at]++
	}

	for h := int64(96); h <= 143; h++ {
		require.Equal(t, 1, counts[h], "height %d must be read exactly once during the window", h)
	}

	require.Zero(t, counts[-1], "no whole read inside the window")

	// The run where leaf 2 becomes due.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 144, 143, recordReads(seen))
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "leaf 2 must be dropped on time")

	counts = map[int64]int{}
	for _, at := range seen["spend_journal_2"] {
		counts[at]++
	}

	require.Zero(t, counts[-1],
		"a leaf whose 48 heights were all read on the preceding 48 runs must not be read whole again before it is dropped")
	require.Len(t, seen["spend_journal_2"], 48, "exactly 48 reads in total, one per height, and nothing on the drop run")
}

// TestPartitionIsWorkedBeforeItIsDueAndDroppedOnTime pins the schedule, and the point is the
// SECOND half.
//
// The work is spread across the 48 heights LEADING UP TO a partition's due date, not the ones
// after it. Doing it after would mean holding every partition an extra 48 blocks beyond
// DefaultSpendJournalRetentionBlocks purely to have somewhere to put the work. So a partition
// must be readable before it is droppable, and must be dropped at exactly the height it would
// have been dropped at before any of this. This test was deleted by mistake in an earlier
// rewrite, which is how the whole-read-on-due bug got through.
func TestPartitionIsWorkedBeforeItIsDueAndDroppedOnTime(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: heights 96..143

	seen := map[string][]int64{}

	// Just before the window: nothing read, nothing dropped.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 95, 94, recordReads(seen))
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Empty(t, seen, "a partition must not be read before its work window opens")

	// First height of the window: read, not dropped.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx, 96, 95, recordReads(seen))
	require.NoError(t, err)
	require.Zero(t, dropped, "the work window must not drop the work list out from under itself")
	require.Equal(t, []int64{96}, seen["spend_journal_2"])

	// Last height of the window: read, still not dropped.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx, 143, 142, recordReads(seen))
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Equal(t, []int64{96, 143}, seen["spend_journal_2"])

	// Due: dropped, at exactly the height the single-cutoff code dropped it, with no read.
	dropped, err = s.dropSpendJournalPartitionsBelow(ctx, 144, 143, recordReads(seen))
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "retention must be unchanged: due means dropped, not deferred")
	require.Equal(t, []int64{96, 143}, seen["spend_journal_2"], "and the drop run reads nothing")
}

// TestSkippedHeightsAcrossAPartitionBoundaryGoToTheRightPartition.
//
// The pruner deduplicates to the newest notification when it falls behind, so a run can follow
// several skipped heights. Offsets computed as "tip mod 48" were blind to which partition they
// belonged to, so a gap straddling a boundary rebased the older partition's tail heights onto the
// newer one: the newer partition was read early at the wrong heights, and the older one's tail
// was never read at all. Absolute heights cannot do that, because each partition takes only the
// heights inside its own range.
func TestSkippedHeightsAcrossAPartitionBoundaryGoToTheRightPartition(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: 96..143
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 150)) // leaf 3: 144..191

	seen := map[string][]int64{}

	// Previous run cleaned up to 141. This run cleans up to 150. The nine heights in between
	// straddle the boundary between leaf 2 and leaf 3.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 150, 141, recordReads(seen))
	require.NoError(t, err)

	require.Equal(t, []int64{142, 143}, seen["spend_journal_2"],
		"leaf 2 gets only its own remaining heights")
	require.Equal(t, []int64{144, 145, 146, 147, 148, 149, 150}, seen["spend_journal_3"],
		"leaf 3 gets only its own heights, and nothing rebased from leaf 2")
	require.Equal(t, 1, dropped, "leaf 2 became due inside this gap and its tail was read, so it goes")
}

// TestFreshProcessCannotVouchSoItReadsWhatItCannotAccountFor.
//
// After a restart nothing is remembered about earlier runs. A partition already due might have
// had its window under the previous process, or might not, and there is no way to tell, so it
// is read whole. A partition still in its window is read from its first height up to the
// current cutoff, which may repeat reads the previous process did; a repeated read is harmless
// and a skipped one is a permanent leak of identity rows.
func TestFreshProcessCannotVouchSoItReadsWhatItCannotAccountFor(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 50))  // leaf 1: 48..95, will be due
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: 96..143, in window

	seen := map[string][]int64{}

	// prevCutoff 0 means unknown. Cutoff 120: leaf 1 is due, leaf 2 is halfway through.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 120, 0, recordReads(seen))
	require.NoError(t, err)

	require.Equal(t, []int64{-1}, seen["spend_journal_1"], "a due partition nobody can vouch for is read whole")
	require.Equal(t, 1, dropped)

	want := make([]int64, 0, 25)
	for h := int64(96); h <= 120; h++ {
		want = append(want, h)
	}

	require.Equal(t, want, seen["spend_journal_2"],
		"an in-window partition is read from its first height up to the cutoff, and never whole")
}

// TestAFailedRunIsRedoneNotSkipped.
//
// The pruner worker records a height as processed even when Prune returns an error, so nothing
// upstream comes back for it. The store therefore must not advance its own memory of the
// previous cutoff until the run has succeeded, or the heights of a failed run are never read.
func TestAFailedRunIsRedoneNotSkipped(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: 96..143

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	// A successful run at tip 192, cutoff 96, reads height 96 and remembers cutoff 96.
	_, err = svc.Prune(ctx, 192, "x")
	require.NoError(t, err)
	require.Equal(t, uint32(96), s.lastPruneCutoff.Load())

	// A run that fails part way. A cancelled context makes the first database call fail, which
	// is the shape of a real failure: some work may have happened, the call returned an error.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = svc.Prune(cancelled, 193, "x")
	require.Error(t, err, "the run must fail loudly rather than pretend")
	require.Equal(t, uint32(96), s.lastPruneCutoff.Load(),
		"a failed run must not advance the remembered cutoff, or its heights are never read")

	// The next good run at the same height must do height 97, not skip past it.
	seen := map[string][]int64{}
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 97, s.lastPruneCutoff.Load(), recordReads(seen))
	require.NoError(t, err)
	require.Equal(t, []int64{97}, seen["spend_journal_2"],
		"the height the failed run was meant to read must be read by the run that follows it")
}

// TestOverduePartitionIsReadWholeBeforeItIsDropped is the backlog case, and it is the one that
// turns spreading the work into a row leak if it is got wrong.
//
// A partition that was already past its due date at the previous run has no window left, and
// this process cannot vouch for whatever happened before. Handing it one height and then dropping
// it would destroy the work list with most of it unread, and the identity rows those heights would
// have reclaimed could never be found again. This is not hypothetical: the mainnet box was two
// thousand partitions behind, so every one of them was overdue.
func TestOverduePartitionIsReadWholeBeforeItIsDropped(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2: 96..143

	seen := map[string][]int64{}

	// Both cutoffs far past leaf 2, so it was already due last run and is still here.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 100_000, 99_999, recordReads(seen))
	require.NoError(t, err)

	require.Equal(t, 1, dropped, "an overdue partition must still be dropped")
	require.Equal(t, []int64{-1}, seen["spend_journal_2"],
		"an overdue partition must be read WHOLE in one pass, not a height at a time")
}
