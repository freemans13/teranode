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
	external := externalParents(blocks)

	var nTx int
	for _, b := range blocks {
		nTx += len(b.txs)
	}

	t.Logf("loaded %d blocks (%d to %d), %d transactions, %d external parent transactions",
		len(blocks), blocks[0].height, blocks[len(blocks)-1].height, nTx, len(external))

	for _, method := range methods {
		runBenchMethod(t, method, blocks, external, nTx)
	}
}

type benchBlock struct {
	height   uint32
	coinbase *bt.Tx
	txs      []*bt.Tx
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

		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)

		var msg wire.MsgBlock
		require.NoError(t, msg.Deserialize(bytes.NewReader(raw)))

		b := benchBlock{height: uint32(h)} //nolint:gosec // block heights

		for i, wtx := range msg.Transactions {
			var buf bytes.Buffer
			require.NoError(t, wtx.Serialize(&buf))

			tx, err := bt.NewTxFromBytes(buf.Bytes())
			require.NoError(t, err)

			if i == 0 {
				b.coinbase = tx
				continue
			}

			b.txs = append(b.txs, tx)
		}

		blocks = append(blocks, b)
	}

	sort.Slice(blocks, func(i, j int) bool { return blocks[i].height < blocks[j].height })

	return blocks
}

// externalParents returns, for each transaction the blocks spend from but do not create, the
// output indexes they spend.
func externalParents(blocks []benchBlock) map[chainhash.Hash]map[uint32]struct{} {
	created := map[chainhash.Hash]struct{}{}
	external := map[chainhash.Hash]map[uint32]struct{}{}

	for _, b := range blocks {
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

	return external
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

	s, ctx := newTestStoreWith(t, mainnetBatcherTune)

	seedStart := time.Now()
	seedExternalParents(t, ctx, s, external, blocks[0].height-1)
	t.Logf("[%s] seeded %d external parents in %s", method, len(external), time.Since(seedStart).Round(time.Second))

	perBlock := make([]time.Duration, 0, len(blocks))

	var applyTotal time.Duration

	for _, b := range blocks {
		cbOpts := []utxo.CreateOption{
			utxo.WithCreateOnly(),
			utxo.WithSetCoinbase(true),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: b.height, BlockHeight: b.height}),
		}

		_, _, err := s.SpendAndCreate(ctx, b.coinbase, b.height, cbOpts...)
		require.NoError(t, err, "coinbase at %d", b.height)

		idxs := make([]int, len(b.txs))
		for i := range idxs {
			idxs[i] = (i + 1) / 4096
		}

		opts := []utxo.CreateOption{
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: b.height, BlockHeight: b.height}),
			utxo.WithSubtreeIdxs(idxs),
			utxo.WithLocked(false),
			utxo.WithIgnoreLocked(true),
			utxo.WithSkipExtendedInputs(true),
			utxo.WithSkipUTXOHashCheck(true),
		}

		start := time.Now()

		var results []utxo.SpendAndCreateMultiResult

		switch method {
		case "pertx":
			results, err = utxo.DefaultSpendAndCreateMulti(ctx, s, 3200, b.txs, b.height, opts...)
		case "netted":
			results, err = s.SpendAndCreateMulti(ctx, b.txs, b.height, opts...)
		case "twophase":
			results, err = benchTwoPhase(ctx, s, b, idxs)
		default:
			t.Fatalf("unknown method %q", method)
		}

		d := time.Since(start)

		require.NoError(t, err, "[%s] block %d", method, b.height)

		for i, r := range results {
			if r.Status != utxo.MultiTxCreated {
				t.Fatalf("[%s] block %d tx %d status %d: %v", method, b.height, i, r.Status, r.Err)
			}
		}

		perBlock = append(perBlock, d)
		applyTotal += d
	}

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
