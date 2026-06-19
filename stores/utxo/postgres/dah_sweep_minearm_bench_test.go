package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkSweepRangeMineArm measures the cost the now-active mine-arm adds to a
// sweep. It seeds M mined-but-UNSPENT parents: with mined_at_height tagged (the
// Create change), the mine-arm enumerates all M, evaluates allSpent, and rejects
// them (not fully spent). With mined_at_height NULL (baseline) the mine-arm
// matches nothing, so the sweep is near-empty. The before/after delta is the
// pure overhead of enumerating + rejecting mined-not-spent candidates.
func BenchmarkSweepRangeMineArm(b *testing.B) {
	const M = 2000
	const minedHeight = uint32(100)
	const tipHeight = uint32(200)

	store, ctx := setupBenchStore(b)
	require.NoError(b, store.SetBlockHeight(tipHeight))

	for i := 0; i < M; i++ {
		_ = benchNewMinedTx(b, store, ctx, minedHeight) // mined, NOT spent
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := store.pool.Exec(ctx, `UPDATE dah_watermark SET last_swept_height = 0 WHERE id = 1`)
		require.NoError(b, err)

		n, _, _, err := store.sweepDAHRange(ctx, 0, int64(tipHeight), M*2)
		require.NoError(b, err)
		if n < 0 {
			b.Fatal("unexpected negative count")
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(M), "candidates/iter")
}
