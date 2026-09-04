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

	reclaimed, chunks, err := s.reclaimFromPartition(ctx, partition, 500)
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

	_, _, err = s.reclaimFromPartition(ctx, partition, 500)
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, parent),
		"one of this parent's spenders is still in the mempool, so its record cannot go")
	require.False(t, identExists(t, s, ctx, other),
		"the unrelated parent is finished and should still be reclaimed")
}
