package utxoset

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestBenchNettedVsPerTx applies real mainnet blocks below the checkpoint the two ways quick
// validation has done it: per transaction (SpendAndCreate for each, level by level, with the
// old create fan-out of 3,200 callers) and as one netted list (SpendAndCreateMulti). Each method
// gets a fresh store, seeded with the coins the blocks spend from before the range, with the
// mainnet batcher settings. Only the block applies are timed.
//
// BENCH_BLOCKS_DIR holds <height>.bin raw blocks; BENCH_METHODS is a comma list, default
// "pertx,netted". Skipped when BENCH_BLOCKS_DIR is not set.
func TestBenchNettedVsPerTx(t *testing.T) {
	dir := os.Getenv("BENCH_BLOCKS_DIR")
	if dir == "" {
		t.Skip("BENCH_BLOCKS_DIR not set")
	}

	methods := strings.Split(envOr("BENCH_METHODS", "pertx,netted"), ",")

	blocks := loadBenchBlocks(t, dir)
	external, nTx := externalParents(t, blocks)

	t.Logf("loaded %d blocks (%d to %d), %d transactions, %d external parent transactions",
		len(blocks), blocks[0].height, blocks[len(blocks)-1].height, nTx, len(external))

	for _, method := range methods {
		runBenchMethod(t, method, blocks, external, nTx)
	}
}

// benchBlock is one block file. Its transactions are parsed when it is applied and dropped after,
// as the node holds one block at a time, so a set of large blocks fits in memory.
type benchBlock struct {
	height   uint32
	path     string
	coinbase *bt.Tx
	txs      []*bt.Tx
}

func (b benchBlock) load(t *testing.T) benchBlock {
	t.Helper()

	raw, err := os.ReadFile(b.path)
	require.NoError(t, err)

	var msg wire.MsgBlock
	require.NoError(t, msg.Deserialize(bytes.NewReader(raw)))

	out := benchBlock{height: b.height, path: b.path}

	for i, wtx := range msg.Transactions {
		var buf bytes.Buffer
		require.NoError(t, wtx.Serialize(&buf))

		tx, err := bt.NewTxFromBytes(buf.Bytes())
		require.NoError(t, err)

		if i == 0 {
			out.coinbase = tx
			continue
		}

		out.txs = append(out.txs, tx)
	}

	return out
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return d
}

func loadBenchBlocks(t *testing.T, dir string) []benchBlock {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var blocks []benchBlock

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".bin") {
			continue
		}

		h, err := strconv.Atoi(strings.TrimSuffix(name, ".bin"))
		require.NoError(t, err)

		b := benchBlock{height: uint32(h), path: filepath.Join(dir, name)} //nolint:gosec // block heights

		blocks = append(blocks, b)
	}

	sort.Slice(blocks, func(i, j int) bool { return blocks[i].height < blocks[j].height })

	return blocks
}

// externalParents returns, for each transaction the blocks spend from but do not create, the
// output indexes they spend.
func externalParents(t *testing.T, files []benchBlock) (map[chainhash.Hash]map[uint32]struct{}, int) {
	t.Helper()

	created := map[chainhash.Hash]struct{}{}
	external := map[chainhash.Hash]map[uint32]struct{}{}
	nTx := 0

	for _, f := range files {
		b := f.load(t)
		nTx += len(b.txs)
		created[*b.coinbase.TxIDChainHash()] = struct{}{}

		for _, tx := range b.txs {
			for _, in := range tx.Inputs {
				p := *in.PreviousTxIDChainHash()
				if _, ok := created[p]; ok {
					continue
				}

				if external[p] == nil {
					external[p] = map[uint32]struct{}{}
				}

				external[p][in.PreviousTxOutIndex] = struct{}{}
			}

			created[*tx.TxIDChainHash()] = struct{}{}
		}
	}

	return external, nTx
}

func mainnetBatcherTune(ts *settings.Settings) {
	ts.UtxoStore.StoreBatcherSize = 50
	ts.UtxoStore.StoreBatcherDurationMillis = 5
	ts.UtxoStore.StoreBatcherGreedyAccumulate = true
	ts.UtxoStore.SpendBatcherSize = 500
	ts.UtxoStore.SpendBatcherDurationMillis = 1
	ts.UtxoStore.SpendBatcherConcurrency = 32
	ts.UtxoStore.BatcherMaxConcurrent = 64
	ts.UtxoStore.GetBatcherSize = 500
	ts.UtxoStore.GetBatcherDurationMillis = 1
	// Mainnet runs with utxostore_skipTxBodyBelowCheckpoint=true, so below the checkpoint no
	// tx_body row is written. Without it the bench pays ~29 us a row that mainnet never pays.
	ts.UtxoStore.SkipTxBodyBelowCheckpoint = true
}

func runBenchMethod(t *testing.T, method string, blocks []benchBlock, external map[chainhash.Hash]map[uint32]struct{}, nTx int) {
	t.Helper()

	dsn := testDSN(t)
	if !strings.Contains(dsn, "pool_max_conns") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}

		dsn += sep + "pool_max_conns=300"
	}

	t.Setenv("UTXOSET_TEST_DSN", dsn)

	// BENCH_ABOVE=1 runs the store's above-checkpoint route: a network with no checkpoints, so
	// every height is above the checkpoint, and lists carry no mined block info, as the catch-up
	// and legacy batch path sends them. The inputs are not extended (the bench blocks carry no
	// previous outputs), so the UTXO hash check is skipped there too.
	above := os.Getenv("BENCH_ABOVE") == "1"

	tune := mainnetBatcherTune
	if above {
		tune = func(ts *settings.Settings) {
			mainnetBatcherTune(ts)
			withCheckpoints(ts, nil)
		}
	}

	s, ctx := newTestStoreWith(t, tune)

	seedStart := time.Now()
	seedExternalParents(t, ctx, s, external, blocks[0].height-1)
	t.Logf("[%s] seeded %d external parents in %s", method, len(external), time.Since(seedStart).Round(time.Second))

	perBlock := make([]time.Duration, 0, len(blocks))

	// Postgres's per-table insert and delete counters before and after the timed applies, so the
	// run reports the net work it did. Partitions are summed under their parent's name. An idle
	// backend flushes its counters within about a second, hence the pause before each read.
	countRows := func() map[string]int64 {
		time.Sleep(3 * time.Second)

		rows, err := s.pool.Query(ctx, `
SELECT p, sum(n_tup_ins)::bigint, sum(n_tup_del)::bigint FROM (
  SELECT CASE WHEN relname LIKE 'tx_mined%' AND relname NOT LIKE 'tx_mined_floor%' AND relname NOT LIKE 'tx_mined_stamped%' THEN 'tx_mined'
              WHEN relname LIKE 'tx_body%' THEN 'tx_body'
              WHEN relname LIKE 'spend_journal%' THEN 'spend_journal'
              WHEN relname LIKE 'tx_ident%' THEN 'tx_ident'
              WHEN relname = 'utxo' OR relname LIKE 'utxo\_p%' THEN 'utxo' END AS p, n_tup_ins, n_tup_del
  FROM pg_stat_user_tables) x WHERE p IS NOT NULL GROUP BY p`)
		require.NoError(t, err)
		defer rows.Close()

		out := map[string]int64{}
		for rows.Next() {
			var name string
			var ins, del int64
			require.NoError(t, rows.Scan(&name, &ins, &del))
			out[name+".ins"] = ins
			out[name+".del"] = del
		}
		require.NoError(t, rows.Err())

		return out
	}

	rowsBefore := countRows()

	// Write-ahead log volume over the timed applies only: the seed above is excluded.
	var walStart string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&walStart))

	var applyTotal time.Duration

	for _, file := range blocks {
		b := file.load(t)

		cbOpts := []utxo.CreateOption{
			utxo.WithCreateOnly(),
			utxo.WithSetCoinbase(true),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: b.height, BlockHeight: b.height}),
		}

		_, _, err := s.SpendAndCreate(ctx, b.coinbase, b.height, cbOpts...)
		require.NoError(t, err, "coinbase at %d", b.height)

		// Quick validation hands the store one range of benchRange transaction positions at a time
		// (position 0 is the coinbase), so the bench does the same, for every method.
		const benchRange = 65536

		start := time.Now()

		for lo := 0; lo < len(b.txs); {
			hi := lo
			for hi < len(b.txs) && (hi+1)/benchRange == (lo+1)/benchRange {
				hi++
			}

			txs := b.txs[lo:hi]

			idxs := make([]int, len(txs))
			for k := range idxs {
				idxs[k] = (lo + k + 1) / 4096
			}

			opts := []utxo.CreateOption{
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: b.height, BlockHeight: b.height}),
				utxo.WithSubtreeIdxs(idxs),
				utxo.WithLocked(false),
				utxo.WithIgnoreLocked(true),
				utxo.WithSkipExtendedInputs(true),
				utxo.WithSkipUTXOHashCheck(true),
			}

			if above {
				opts = []utxo.CreateOption{
					utxo.WithIgnoreLocked(true),
					utxo.WithSkipExtendedInputs(true),
					utxo.WithSkipUTXOHashCheck(true),
				}
			}

			var results []utxo.SpendAndCreateMultiResult

			switch method {
			case "pertx":
				results, err = utxo.DefaultSpendAndCreateMulti(ctx, s, 3200, txs, b.height, opts...)
			case "netted":
				results, err = s.SpendAndCreateMulti(ctx, txs, b.height, opts...)
			case "twophase":
				results, err = benchTwoPhase(ctx, s, benchBlock{height: b.height, txs: txs}, idxs)
			default:
				t.Fatalf("unknown method %q", method)
			}

			require.NoError(t, err, "[%s] block %d range at %d", method, b.height, lo)

			for k, r := range results {
				if r.Status != utxo.MultiTxCreated {
					t.Fatalf("[%s] block %d tx %d status %d: %v", method, b.height, lo+k, r.Status, r.Err)
				}
			}

			lo = hi
		}

		d := time.Since(start)

		perBlock = append(perBlock, d)
		applyTotal += d
	}

	var walBytes int64
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint`, walStart).Scan(&walBytes))

	var walEnd string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&walEnd))

	t.Logf("[%s] WAL bytes=%d bytes_per_block=%d bytes_per_tx=%d", method, walBytes, walBytes/int64(len(blocks)), walBytes/int64(nTx))
	t.Logf("[%s] WALRANGE %s %s", method, walStart, walEnd)

	rowsAfter := countRows()
	d := func(k string) int64 { return rowsAfter[k] - rowsBefore[k] }
	netRows := d("tx_mined.ins") + d("utxo.ins") + d("utxo.del") + d("tx_body.ins") + d("spend_journal.ins") + d("tx_ident.ins")

	t.Logf("[%s] NETWORK txs=%d mined_records=%d coin_inserts=%d coin_deletes=%d bodies=%d journal=%d ident=%d net_rows=%d us_per_tx=%.1f us_per_net_row=%.1f",
		method, nTx, d("tx_mined.ins"), d("utxo.ins"), d("utxo.del"), d("tx_body.ins"), d("spend_journal.ins"), d("tx_ident.ins"), netRows,
		float64(applyTotal.Microseconds())/float64(nTx), float64(applyTotal.Microseconds())/float64(max(netRows, 1)))

	sorted := append([]time.Duration(nil), perBlock...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	t.Logf("[%s] RESULT blocks=%d txs=%d apply_total=%s ms_per_block=%.1f tx_per_s=%.0f p50=%s p95=%s max=%s",
		method, len(blocks), nTx, applyTotal.Round(time.Millisecond),
		float64(applyTotal.Milliseconds())/float64(len(blocks)),
		float64(nTx)/applyTotal.Seconds(),
		sorted[len(sorted)/2].Round(time.Millisecond), sorted[len(sorted)*95/100].Round(time.Millisecond),
		sorted[len(sorted)-1].Round(time.Millisecond))

	fmt.Fprintf(os.Stderr, "RESULT %s %.1f ms/block %.0f tx/s\n", method,
		float64(applyTotal.Milliseconds())/float64(len(blocks)), float64(nTx)/applyTotal.Seconds())
}

// benchTwoPhase applies a block the way quick validation on main does: every transaction created
// at once (create-only, 3,200 callers, as StoreBatcherSize 50 x BatcherMaxConcurrent 64 allowed),
// then every transaction's inputs spent at once (spend-only, SpendBatcherSize 500 x
// SpendBatcherConcurrency 32 x 2 callers). No dependency order at all.
func benchTwoPhase(ctx context.Context, s *Store, b benchBlock, idxs []int) ([]utxo.SpendAndCreateMultiResult, error) {
	results := make([]utxo.SpendAndCreateMultiResult, len(b.txs))

	run := func(limit int, call func(i int) error) error {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(limit)

		for i := range b.txs {
			g.Go(func() error {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				return call(i)
			})
		}

		return g.Wait()
	}

	err := run(3200, func(i int) error {
		_, _, err := s.SpendAndCreate(ctx, b.txs[i], b.height, utxo.WithCreateOnly(),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: b.height, BlockHeight: b.height, SubtreeIdx: idxs[i]}),
			utxo.WithLocked(false), utxo.WithSkipExtendedInputs(true))

		return err
	})
	if err != nil {
		return nil, err
	}

	err = run(500*32*2, func(i int) error {
		_, _, err := s.SpendAndCreate(ctx, b.txs[i], b.height, utxo.WithSpendOnly(),
			utxo.WithIgnoreLocked(true), utxo.WithSkipUTXOHashCheck(true))

		return err
	})
	if err != nil {
		return nil, err
	}

	for i := range results {
		results[i].Status = utxo.MultiTxCreated
	}

	return results, nil
}

// seedExternalParents writes, for each parent transaction outside the range, one coin at each
// output the blocks spend, as the seeder writes coins: an output-only transaction under the
// parent's txid. Outpoint-only spends below the checkpoint check only that the coin exists.
func seedExternalParents(t *testing.T, ctx context.Context, s *Store, external map[chainhash.Hash]map[uint32]struct{}, height uint32) {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	type job struct {
		txid chainhash.Hash
		outs map[uint32]struct{}
	}

	jobs := make(chan job, 1024)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	for w := 0; w < 256; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := range jobs {
				var maxIdx uint32
				for i := range j.outs {
					maxIdx = max(maxIdx, i)
				}

				tx := &bt.Tx{Outputs: make([]*bt.Output, int(maxIdx)+1)}
				for i := range j.outs {
					tx.Outputs[i] = &bt.Output{Satoshis: 1000, LockingScript: script}
				}

				txid := j.txid

				_, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly(), utxo.WithTXID(&txid),
					utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 0, BlockHeight: height}))
				if err != nil && !errors.Is(err, errors.ErrTxExists) {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}

	for txid, outs := range external {
		jobs <- job{txid, outs}
	}

	close(jobs)
	wg.Wait()

	require.NoError(t, firstErr)
}
