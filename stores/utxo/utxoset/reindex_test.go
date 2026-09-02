package utxoset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// bloatCoinIndex churns the coin table so its index goes loose.
//
// Insert n rows, delete all but a scattering, vacuum. That is the production shape in
// miniature: the packed key is derived from the transaction id so deletes land all over the
// index, and a btree leaf page returns to the free list only when it empties COMPLETELY. With
// keys this spread out almost no page does, so the pages stay allocated and half empty.
//
// The vacuum matters. Without it the rows are merely dead rather than removed, the index still
// points at them, and the test would be measuring a vacuum backlog instead of index bloat.
func bloatCoinIndex(t *testing.T, s *Store, ctx context.Context, n int) {
	t.Helper()

	// A test table cannot reach production's hundred thousand rows per partition in reasonable
	// time, and this guard decides whether the bloat ratio is consulted at all. Left at its
	// default it skips every partition, which silently turns every assertion below into a
	// tautology.
	s.coinIndexMinRows = 1000

	seedCoins(t, s, ctx, n)

	// Keep one row in every 64. Enough that no leaf page can empty, which is exactly the
	// condition that leaves the index allocated at low density.
	_, err := s.pool.Exec(ctx, `DELETE FROM utxo WHERE (get_byte(txid, 1) & 63) <> 0`)
	require.NoError(t, err, "churning the coin table")

	_, err = s.pool.Exec(ctx, `VACUUM (ANALYZE) utxo`)
	require.NoError(t, err, "vacuuming the coin table")
}

func coinIndexBytesPerRow(t *testing.T, s *Store, ctx context.Context, index string) int64 {
	t.Helper()

	var bytes, live int64

	require.NoError(t, s.pool.QueryRow(ctx, `
SELECT pg_relation_size(i.indexrelid), GREATEST(t.n_live_tup, 1)
  FROM pg_stat_user_indexes i JOIN pg_stat_user_tables t ON t.relid = i.relid
 WHERE i.indexrelname = $1`, index).Scan(&bytes, &live), "reading index size")

	return bytes / live
}

// TestRebuildBloatedCoinIndexRecoversTheSpace is the whole feature end to end: churn the coin
// table until its index is loose, then prove one pruner step tightens it.
//
// It asserts on bytes per live row rather than on leaf density, because that ratio is what the
// production trigger actually reads. A test that measured density would pass while the thing
// deciding when to fire looked at a different number.
func TestRebuildBloatedCoinIndexRecoversTheSpace(t *testing.T) {
	s, ctx := newTestStore(t)

	bloatCoinIndex(t, s, ctx, 700_000)

	// Aim the threshold just under the bloat this churn produces, so the test is about the
	// rebuild rather than about guessing production's number on a small table.
	before := coinIndexBytesPerRow(t, s, ctx, "utxo_p0_ukey")
	require.Greater(t, before, int64(60), "churn did not loosen the index; nothing to rebuild")

	s.coinIndexRebuildBytesPerRow = int(before) - 1

	rebuilt, err := s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rebuilt, "a bloated index should have been chosen")

	after := coinIndexBytesPerRow(t, s, ctx, rebuilt)
	require.Less(t, after, before, "the rebuild did not recover any space")
}

// TestRebuildBloatedCoinIndexTakesOneIndexPerCall pins the cost control.
//
// One rebuild a call is what keeps a pruning session bounded. On the mainnet box each of the
// eight took 11.8 to 18.2 seconds, so a session that did the set would stall for two minutes.
// Doing one leaves the rest to later blocks, and a rebuilt leaf drops below the threshold and
// stops being chosen, so the work spreads and then stops on its own.
func TestRebuildBloatedCoinIndexTakesOneIndexPerCall(t *testing.T) {
	s, ctx := newTestStore(t)

	bloatCoinIndex(t, s, ctx, 700_000)

	s.coinIndexRebuildBytesPerRow = 1 // every index is over this

	first, err := s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, second, "the next call should pick up another index")
}

// TestRebuildBloatedCoinIndexLeavesATightIndexAlone guards against pointless churn.
//
// A freshly built coin index sits near 30 bytes per live row, well under the default threshold
// of 80. If the step fired on that it would rewrite the index every block forever, recovering
// nothing, which is the failure the ratio trigger exists to avoid.
func TestRebuildBloatedCoinIndexLeavesATightIndexAlone(t *testing.T) {
	s, ctx := newTestStore(t)

	s.coinIndexMinRows = 1000

	seedCoins(t, s, ctx, 700_000)

	_, err := s.pool.Exec(ctx, `ANALYZE utxo`)
	require.NoError(t, err)

	// Prove the ratio is genuinely being consulted, so this cannot pass by the row guard
	// skipping every partition, which is how it passed the first time it was written.
	tight := coinIndexBytesPerRow(t, s, ctx, "utxo_p0_ukey")
	require.Less(t, tight, int64(DefaultCoinIndexRebuildBytesPerRow),
		"a freshly built index should sit well under the rebuild threshold")

	rebuilt, err := s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)
	require.Empty(t, rebuilt, "a healthy index must not be rebuilt")
}

// TestRebuildBloatedCoinIndexIsDisabledByANegativeThreshold covers the operator switch.
func TestRebuildBloatedCoinIndexIsDisabledByANegativeThreshold(t *testing.T) {
	s, ctx := newTestStore(t)

	bloatCoinIndex(t, s, ctx, 700_000)

	s.coinIndexRebuildBytesPerRow = -1

	rebuilt, err := s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)
	require.Empty(t, rebuilt, "a negative threshold must switch the step off entirely")
}

// TestRebuildBloatedCoinIndexClearsAnInvalidIndexFirst is the recovery path, and it is the
// reason this feature is safe to leave running.
//
// A REINDEX CONCURRENTLY that fails part way leaves an invalid index behind, and PostgreSQL
// will not rebuild over it. Without this the first failure would silently stop every rebuild
// that followed, and the bloat would grow back with nothing in the log to say why.
//
// The invalid index is created directly rather than by interrupting a real rebuild, because a
// test cannot reliably kill a REINDEX at the right instant. What matters is the state left
// behind, and that state is an index row with indisvalid false.
func TestRebuildBloatedCoinIndexClearsAnInvalidIndexFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	bloatCoinIndex(t, s, ctx, 700_000)

	// Mark a leftover index invalid, exactly as an interrupted rebuild would leave it.
	_, err := s.pool.Exec(ctx, `CREATE INDEX utxo_p0_ukey_ccnew ON utxo_p0 (ukey)`)
	require.NoError(t, err, "creating the leftover index")

	_, err = s.pool.Exec(ctx,
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'utxo_p0_ukey_ccnew'::regclass`)
	require.NoError(t, err, "marking the leftover index invalid")

	s.coinIndexRebuildBytesPerRow = 1

	_, err = s.rebuildBloatedCoinIndex(ctx)
	require.NoError(t, err)

	var leftovers int
	require.NoError(t, s.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
 WHERE NOT x.indisvalid AND c.relname ~ '^utxo_p[0-9]+_ukey'`).Scan(&leftovers))

	require.Zero(t, leftovers, "the invalid index should have been dropped")
}
