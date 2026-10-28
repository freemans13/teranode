package sql

import (
	"database/sql"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// forEachBestTipEngine runs fn on a fresh SQLite store and, when a container is available, a
// fresh PostgreSQL store. The sequence number is written by SQL both engines must accept.
func forEachBestTipEngine(t *testing.T, fn func(t *testing.T, s *SQL)) {
	t.Run("sqlite", func(t *testing.T) {
		fn(t, setupTestStore(t))
	})

	t.Run("postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("postgres container test")
		}

		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		fn(t, s)
	})
}

// bestTipSeqOf reads a block's best_tip_seq; ok is false when it is NULL.
func bestTipSeqOf(t *testing.T, s *SQL, hash *chainhash.Hash) (seq int64, ok bool) {
	t.Helper()

	var v sql.NullInt64

	require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT best_tip_seq FROM blocks WHERE hash = $1`, hash.CloneBytes()).Scan(&v))

	return v.Int64, v.Valid
}

// startSeq returns the number the store holds before a test writes anything: genesis is
// stored as the first best tip.
func startSeq(t *testing.T, s *SQL) uint64 {
	t.Helper()

	seq, err := s.GetBestTipSeq(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq, "genesis is the first best tip")

	return seq
}

func requireBestTipSeq(t *testing.T, s *SQL, block *model.Block, want uint64) {
	t.Helper()

	seq, ok := bestTipSeqOf(t, s, block.Hash())
	require.True(t, ok, "block %s has no best_tip_seq", block.Hash())
	require.Equal(t, int64(want), seq, "best_tip_seq of %s", block.Hash())
}

func requireNoBestTipSeq(t *testing.T, s *SQL, block *model.Block) {
	t.Helper()

	seq, ok := bestTipSeqOf(t, s, block.Hash())
	require.False(t, ok, "block %s has best_tip_seq %d, want NULL", block.Hash(), seq)
}

// requireTipChanges asserts GetBestTipChanges after afterSeq returns exactly these blocks with
// these numbers, in order.
func requireTipChanges(t *testing.T, s *SQL, afterSeq uint64, minHeight, limit uint32, want []*model.Block, wantSeqs []uint64) {
	t.Helper()

	headers, metas, seqs, err := s.GetBestTipChanges(t.Context(), afterSeq, minHeight, limit)
	require.NoError(t, err)
	require.Len(t, headers, len(want))
	require.Len(t, metas, len(want))
	require.Equal(t, wantSeqs, seqs)

	for i, b := range want {
		require.Equal(t, b.Hash().String(), headers[i].Hash().String(), "entry %d", i)
	}
}

func TestBestTipSeq_WrittenByEveryTipChange(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()
		base := startSeq(t, s)
		genesis := s.chainParams.GenesisHash

		// Two extensions take the fast path and are numbered by their INSERT.
		a1 := createTestBlock(t, 101, genesis)
		a2 := createTestBlock(t, 102, a1.Hash())

		_, _, err := s.StoreBlock(ctx, a1, "a")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(ctx, a2, "a")
		require.NoError(t, err)

		requireBestTipSeq(t, s, a1, base+1)
		requireBestTipSeq(t, s, a2, base+2)

		// b2 ties a2 on work and loses the tiebreak (a2 has the lower id): no tip change.
		b2 := createTestBlock(t, 202, a1.Hash())
		_, _, err = s.StoreBlock(ctx, b2, "a")
		require.NoError(t, err)
		requireNoBestTipSeq(t, s, b2)

		// b3 gives branch b more work: the fork path numbers it.
		b3 := createTestBlock(t, 203, b2.Hash())
		_, _, err = s.StoreBlock(ctx, b3, "a")
		require.NoError(t, err)
		requireBestTipSeq(t, s, b3, base+3)
		requireNoBestTipSeq(t, s, b2)

		// A block stored invalid never is the best tip.
		bad := createTestBlock(t, 204, b3.Hash())
		_, _, err = s.StoreBlock(ctx, bad, "a", options.WithInvalid(true))
		require.NoError(t, err)
		requireNoBestTipSeq(t, s, bad)

		// Invalidating b3 returns the best to a2, which is numbered again.
		_, err = s.InvalidateBlock(ctx, b3.Hash())
		require.NoError(t, err)
		requireBestTipSeq(t, s, a2, base+4)

		// Revalidating b3 makes it the best again.
		require.NoError(t, s.RevalidateBlock(ctx, b3.Hash()))
		requireBestTipSeq(t, s, b3, base+5)

		seq, err := s.GetBestTipSeq(ctx)
		require.NoError(t, err)
		require.Equal(t, base+5, seq)

		// Each block keeps the number of the last time it became the best.
		requireTipChanges(t, s, base, 0, 100, []*model.Block{a1, a2, b3}, []uint64{base + 1, base + 4, base + 5})
	})
}

func TestBestTipSeq_LosingForkStaysUnnumbered(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()
		base := startSeq(t, s)

		a1 := createTestBlock(t, 301, s.chainParams.GenesisHash)
		a2 := createTestBlock(t, 302, a1.Hash())
		b2 := createTestBlock(t, 402, a1.Hash())

		for _, b := range []*model.Block{a1, a2, b2} {
			_, _, err := s.StoreBlock(ctx, b, "a")
			require.NoError(t, err)
		}

		requireNoBestTipSeq(t, s, b2)

		// A tie re-checked by an unrelated write on the fork path does not number anything.
		a3 := createTestBlock(t, 303, a2.Hash())
		_, _, err := s.StoreBlock(ctx, a3, "a")
		require.NoError(t, err)
		requireBestTipSeq(t, s, a3, base+3)
		requireNoBestTipSeq(t, s, b2)
	})
}

func TestBestTipSeq_DeleteBestNumbersTheNewBest(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()
		base := startSeq(t, s)

		a1 := createTestBlock(t, 501, s.chainParams.GenesisHash)
		a2 := createTestBlock(t, 502, a1.Hash())

		for _, b := range []*model.Block{a1, a2} {
			_, _, err := s.StoreBlock(ctx, b, "a")
			require.NoError(t, err)
		}

		require.NoError(t, s.DeleteBlock(ctx, a2.Hash()))
		requireBestTipSeq(t, s, a1, base+3)
	})
}

func TestGetBestTipChanges_PositionFloorAndLimit(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()
		base := startSeq(t, s)

		chain := make([]*model.Block, 0, 5)
		parent := s.chainParams.GenesisHash

		for i := uint32(0); i < 5; i++ {
			b := createTestBlock(t, 600+i, parent)
			_, _, err := s.StoreBlock(ctx, b, "a")
			require.NoError(t, err)

			chain = append(chain, b)
			parent = b.Hash()
		}

		// After the position, in order.
		requireTipChanges(t, s, base+2, 0, 100, chain[2:], []uint64{base + 3, base + 4, base + 5})

		// The height floor drops the entries below it, genesis included.
		requireTipChanges(t, s, 0, 4, 100, chain[3:], []uint64{base + 4, base + 5})

		// The limit pages from the position.
		requireTipChanges(t, s, base, 0, 2, chain[:2], []uint64{base + 1, base + 2})
		requireTipChanges(t, s, base+2, 0, 2, chain[2:4], []uint64{base + 3, base + 4})

		// Nothing after the latest number.
		requireTipChanges(t, s, base+5, 0, 100, []*model.Block{}, []uint64{})

		// The flags are read now, so a block invalidated since reports it.
		_, err := s.InvalidateBlock(ctx, chain[4].Hash())
		require.NoError(t, err)

		_, metas, seqs, err := s.GetBestTipChanges(ctx, base+4, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []uint64{base + 5, base + 6}, seqs)
		require.True(t, metas[0].Invalid)
		require.False(t, metas[1].Invalid)

		_, wantMeta, err := s.GetBlockHeader(ctx, chain[4].Hash())
		require.NoError(t, err)
		require.Equal(t, wantMeta.Invalid, metas[0].Invalid)
		require.Equal(t, wantMeta.MinedSet, metas[0].MinedSet)
		require.Equal(t, wantMeta.Height, metas[0].Height)
	})
}
