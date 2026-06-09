package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// leafExists reports whether a relation with the given name exists.
func leafExists(t *testing.T, store *Store, name string) bool {
	t.Helper()
	var exists bool
	err := store.pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// TestDropEmptyAgedBucketLeaves verifies the pruner's empty-aged-leaf
// reclamation: aged EMPTY bucket leaf pairs are detached+dropped, aged
// NON-EMPTY leaves are skipped, the EnsureBucket cache is invalidated for
// dropped buckets (so they can be recreated), and the cutoff gate suppresses
// repeat catalog sweeps at the same cutoff.
func TestDropEmptyAgedBucketLeaves(t *testing.T) {
	store, ctx := setupTestStore(t)

	// setupTestStore created the bucket-0 leaves (EnsureBucket(0) at height 0).
	// Create a tx at height 100 → bucket 1 leaves get created and one of them
	// holds a row.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	require.True(t, leafExists(t, store, "txs_p00_b0000"))
	require.True(t, leafExists(t, store, "spends_p00_b0000"))

	// Pick a prune height whose cutoff bucket is 3: cutoff =
	// bucketFor(pruneHeight - 2*retention) = bucketFor(3*bucketHeights) = 3.
	// Bucket 0 (upper bound 1 < 3) is aged AND empty → dropped on all
	// partitions. Bucket 1 (upper bound 2 < 3) is aged but holds the tx on one
	// partition → that partition's pair must survive.
	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.Positive(t, retention)

	pruneHeight := 3*bucketHeights + 2*retention
	dropped, err := store.dropEmptyAgedBucketLeaves(ctx, pruneHeight)
	require.NoError(t, err)

	// All numPartitions bucket-0 pairs dropped, plus the EMPTY bucket-1 pairs
	// on the partitions the tx does NOT hash into (numPartitions-1 of them).
	require.Equal(t, 2*numPartitions-1, dropped)

	for p := 0; p < numPartitions; p++ {
		require.False(t, leafExists(t, store, leafName("txs", p, 0)), "txs bucket-0 leaf p%02d should be dropped", p)
		require.False(t, leafExists(t, store, leafName("spends", p, 0)), "spends bucket-0 leaf p%02d should be dropped", p)
	}

	// Exactly one bucket-1 pair survives (the one holding the tx), and the row
	// is still readable through the partitioned parent.
	survivors := 0
	for p := 0; p < numPartitions; p++ {
		txsThere := leafExists(t, store, leafName("txs", p, 1))
		spendsThere := leafExists(t, store, leafName("spends", p, 1))
		require.Equal(t, txsThere, spendsThere, "txs/spends bucket-1 leaves of p%02d must be dropped as a pair", p)
		if txsThere {
			survivors++
		}
	}
	require.Equal(t, 1, survivors)

	var liveRows int
	require.NoError(t, store.pool.QueryRow(ctx, `SELECT count(*) FROM txs`).Scan(&liveRows))
	require.Equal(t, 1, liveRows)

	// Cache coherence: the dropped buckets were removed from the EnsureBucket
	// cache, so EnsureBucket recreates the leaves rather than trusting a stale
	// cache hit.
	_, cached := store.buckets.created.Load(int32(0))
	require.False(t, cached, "bucket 0 must be evicted from the EnsureBucket cache")
	require.NoError(t, store.EnsureBucket(ctx, 0))
	require.True(t, leafExists(t, store, "txs_p00_b0000"))
	require.True(t, leafExists(t, store, "spends_p00_b0000"))

	// Cutoff gate: a second pass at the same height is a no-op (single atomic
	// load), even though the recreated bucket-0 leaves are aged and empty again.
	dropped, err = store.dropEmptyAgedBucketLeaves(ctx, pruneHeight)
	require.NoError(t, err)
	require.Equal(t, 0, dropped)
	require.True(t, leafExists(t, store, "txs_p00_b0000"))

	// Once the cutoff advances (next bucket boundary), the sweep runs again and
	// reclaims the recreated empty pair.
	dropped, err = store.dropEmptyAgedBucketLeaves(ctx, pruneHeight+bucketHeights)
	require.NoError(t, err)
	require.Equal(t, numPartitions, dropped)
	require.False(t, leafExists(t, store, "txs_p00_b0000"))
	require.False(t, leafExists(t, store, "spends_p00_b0000"))
}

// TestDropEmptyAgedBucketLeaves_NoopBelowHorizon verifies the underflow guard:
// at heights at or below 2*retention nothing is enumerated or dropped.
func TestDropEmptyAgedBucketLeaves_NoopBelowHorizon(t *testing.T) {
	store, ctx := setupTestStore(t)

	dropped, err := store.dropEmptyAgedBucketLeaves(ctx, 2*store.settings.GetUtxoStoreBlockHeightRetention())
	require.NoError(t, err)
	require.Equal(t, 0, dropped)
	require.True(t, leafExists(t, store, "txs_p00_b0000"))
}

// leafName builds a bucket leaf relation name, mirroring createBucketLeaves.
func leafName(table string, partIdx int, bucket int32) string {
	return fmt.Sprintf("%s_p%02d_b%04d", table, partIdx, bucket)
}
