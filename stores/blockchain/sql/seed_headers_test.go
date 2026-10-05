package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// seedTestChain builds count headers on top of genesis, with timestamps that are not monotonic
// so the median time past differs from the parent's timestamp.
func seedTestChain(t *testing.T, s *SQL, count int) []*model.Block {
	t.Helper()

	genesis, _, err := s.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	prev := genesis.Hash()
	blocks := make([]*model.Block, 0, count)

	for i := 1; i <= count; i++ {
		ts := uint32(1_600_000_000 + i*600)
		if i%3 == 0 {
			ts -= 1500
		}

		b := makeMTPTestBlock(t, uint32(i), prev, ts) //nolint:gosec // test heights fit uint32
		blocks = append(blocks, b)
		prev = b.Hash()
	}

	return blocks
}

func seedOptions() []options.StoreBlockOption {
	return []options.StoreBlockOption{options.WithMinedSet(true), options.WithSubtreesSet(true), options.WithPersistedAt()}
}

// blockRows reads every column of every blocks row in id order. The two insert-time stamps
// are reduced to whether they are set, since two stores never write the same wall clock.
func blockRows(t *testing.T, s *SQL) []map[string]any {
	t.Helper()

	rows, err := s.db.QueryContext(context.Background(), `SELECT * FROM blocks ORDER BY id`)
	require.NoError(t, err)

	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	var out []map[string]any

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		require.NoError(t, rows.Scan(ptrs...))

		row := make(map[string]any, len(cols))

		for i, c := range cols {
			switch c {
			case "inserted_at", "persisted_at":
				row[c] = vals[i] != nil
			default:
				row[c] = vals[i]
			}
		}

		out = append(out, row)
	}

	require.NoError(t, rows.Err())

	return out
}

// StoreSeedHeaders writes exactly the rows StoreBlock writes for the same headers, one at a
// time, including across batch boundaries and once the median time past starts to apply.
func TestStoreSeedHeadersWritesWhatStoreBlockWrites(t *testing.T) {
	ctx := context.Background()

	single := newMTPTestStore(t)
	batched := newMTPTestStore(t)

	blocks := seedTestChain(t, single, 40)

	for _, b := range blocks {
		_, _, err := single.StoreBlock(ctx, b, "headers", seedOptions()...)
		require.NoError(t, err)
	}

	for start := 0; start < len(blocks); start += 15 {
		end := min(start+15, len(blocks))
		stored, err := batched.StoreSeedHeaders(ctx, blocks[start:end], "headers", seedOptions()...)
		require.NoError(t, err)
		require.True(t, stored)
	}

	require.Equal(t, blockRows(t, single), blockRows(t, batched))

	want, _, err := single.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	got, meta, err := batched.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, want.Hash(), got.Hash())
	require.Equal(t, uint32(len(blocks)), meta.Height) //nolint:gosec // test sizes fit uint32
}

// A run that does not extend the best block, or is not itself a chain, is refused with
// nothing written, so the caller can fall back to StoreBlock.
func TestStoreSeedHeadersRefusesARunThatDoesNotExtendTheBest(t *testing.T) {
	ctx := context.Background()
	s := newMTPTestStore(t)

	blocks := seedTestChain(t, s, 6)

	stored, err := s.StoreSeedHeaders(ctx, blocks[1:], "headers", seedOptions()...)
	require.NoError(t, err)
	require.False(t, stored, "a run that starts above the best block")

	gapped := []*model.Block{blocks[0], blocks[2]}
	stored, err = s.StoreSeedHeaders(ctx, gapped, "headers", seedOptions()...)
	require.NoError(t, err)
	require.False(t, stored, "a run with a gap")

	var n int
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT count(*) FROM blocks`).Scan(&n))
	require.Equal(t, 1, n, "only genesis")
}
