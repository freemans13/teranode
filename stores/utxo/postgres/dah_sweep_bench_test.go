package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// setupBenchStore creates a fresh Store for benchmarks. Mirrors setupTestStore
// but accepts *testing.B. The schema is reset on each call so benches start
// with a clean slate.
func setupBenchStore(b *testing.B) (*Store, context.Context) {
	b.Helper()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		b.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		b.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, dah_watermark,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(b, err)
	storeURL.Scheme = "postgres"

	bSettings := test.CreateBaseTestSettings(b)
	bSettings.UtxoStore.DBTimeout = 30 * time.Second

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, bSettings, storeURL)
	require.NoError(b, err)

	b.Cleanup(func() {
		store.Stop()
	})

	return store, ctx
}

// benchNewMinedTx creates a unique mined tx with 2 outputs at the given height.
// It uses randomised satoshis so every call produces a distinct txid.
func benchNewMinedTx(b *testing.B, store *Store, ctx context.Context, height uint32) *bt.Tx {
	b.Helper()
	tx := bt.NewTx()
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        height,
		BlockHeight:    height,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := store.Create(ctx, tx, height, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(b, err)
	return tx
}

// benchSpendAllOutputs builds a child tx spending all outputs of parent, creates
// and spends it in the store at spendHeight.
func benchSpendAllOutputs(b *testing.B, store *Store, ctx context.Context, parent *bt.Tx, spendHeight uint32) {
	b.Helper()
	child := bt.NewTx()
	for i, out := range parent.Outputs {
		if out == nil {
			continue
		}
		err := child.From(
			parent.TxIDChainHash().String(),
			uint32(i),
			out.LockingScript.String(),
			out.Satoshis,
		)
		require.NoError(b, err)
	}
	//nolint:gosec
	_ = child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1000+1)
	for _, inp := range child.Inputs {
		if inp.UnlockingScript == nil || len(*inp.UnlockingScript) == 0 {
			inp.UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		}
	}
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(b, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(b, err)
}

// BenchmarkSpendInsertBRINCost empirically confirms that the BRIN index on
// spends.spent_at_height adds negligible overhead to spend inserts.
//
// Two sub-benchmarks run identical work (Create parent + Spend child) with the
// BRIN index present (with_brin) and absent (without_brin). The delta should be
// marginal because BRIN maintains only one summary tuple per heap range
// (pages_per_range=32), not a per-row index entry.
//
// Both sub-benchmarks create parents outside the timed loop, then measure the
// Spend path only (which is what generates rows in the spends table).
func BenchmarkSpendInsertBRINCost(b *testing.B) {
	const brinIndexName = "spends_p00_spent_at_height_brin"
	const spendHeight = uint32(200)

	b.Run("with_brin", func(b *testing.B) {
		store, ctx := setupBenchStore(b)
		require.NoError(b, store.SetBlockHeight(spendHeight))

		// Pre-create b.N parent txs outside the timed section.
		parents := make([]*bt.Tx, b.N)
		for i := range parents {
			parents[i] = benchNewMinedTx(b, store, ctx, 100)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchSpendAllOutputs(b, store, ctx, parents[i], spendHeight)
		}
		b.StopTimer()
	})

	b.Run("without_brin", func(b *testing.B) {
		store, ctx := setupBenchStore(b)
		require.NoError(b, store.SetBlockHeight(spendHeight))

		// Drop the BRIN index before the timed section.
		_, err := store.pool.Exec(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", brinIndexName))
		require.NoError(b, err)

		// Pre-create b.N parent txs outside the timed section.
		parents := make([]*bt.Tx, b.N)
		for i := range parents {
			parents[i] = benchNewMinedTx(b, store, ctx, 100)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchSpendAllOutputs(b, store, ctx, parents[i], spendHeight)
		}
		b.StopTimer()

		// Recreate the index so the schema is left consistent.
		_, _ = store.pool.Exec(ctx, fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %s ON spends_p00 USING brin (spent_at_height) WITH (pages_per_range = 32, autosummarize = on)",
			brinIndexName,
		))
	})
}

// BenchmarkSweepRange measures Worker 2's sweepDAHRange throughput
// (candidates/sec) over a pre-populated set of fully-spent mined parents, to
// size the rate budget for the continuous cursor.
//
// Arrangement: M fully-spent mined parents at heights [1, M]. The sweep tip
// is set above all of them. Each benchmark iteration resets the watermark to 0
// and calls sweepDAHRange across the full range so the same M candidates are
// re-processed every iter. The ns/op figure thus represents the per-sweep cost
// for M candidates; divide by M to get ns/candidate.
func BenchmarkSweepRange(b *testing.B) {
	const M = 2000 // candidate pool size
	const minedHeight = uint32(100)
	const spendHeight = uint32(101)
	const tipHeight = uint32(200)

	store, ctx := setupBenchStore(b)
	require.NoError(b, store.SetBlockHeight(tipHeight))

	// Pre-populate M fully-spent mined parents.
	for i := 0; i < M; i++ {
		parent := benchNewMinedTx(b, store, ctx, minedHeight)
		benchSpendAllOutputs(b, store, ctx, parent, spendHeight)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset watermark so the sweep re-processes all M candidates.
		_, err := store.pool.Exec(ctx, `UPDATE dah_watermark SET last_swept_height = 0 WHERE id = 1`)
		require.NoError(b, err)

		n, _, err := store.sweepDAHRange(ctx, 0, int64(tipHeight), M*2)
		require.NoError(b, err)
		// Ensure the compiler doesn't optimise away the call.
		if n < 0 {
			b.Fatal("unexpected negative count")
		}
	}
	b.StopTimer()

	// Report derived metric: ns per candidate.
	b.ReportMetric(float64(M), "candidates/iter")
}
