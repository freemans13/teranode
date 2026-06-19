package utxo_test

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Pruning-enabled throughput harness
// ---------------------------------------------------------------------------
//
// The Tier-3b sustained run showed the store holds ~100K TPS while the working
// set fits shared_buffers, then decays as the UNPRUNED tables grow past cache
// (104K -> ~43K over 10 min, table -> 41GB). That decay is an artifact of the
// stable harness pinning block height so nothing is ever pruned — it is NOT how
// a real node behaves, where fully-spent txs are reclaimed continuously and the
// table stays bounded to roughly the active UTXO set.
//
// This harness closes that gap WITHOUT standing up block validation / block
// persister: it drives pruning directly from the test. The validator hot path
// is unchanged (Get+Spend+Create+SetLocked, identical to TestThroughput_*Stable
// so the number is comparable). Background goroutines play the role of block
// assembly + the pruner:
//
//   - HEIGHT goroutine: advances the shared block height on a fast ticker so
//     each height-cohort of new txs is small (independent of prune speed).
//   - MINER goroutines: drain a channel of freshly-created tx hashes and mine
//     them in batches via the real Store.SetMinedMulti (a tx must be mined +
//     fully-spent before the DAH sweep will stamp it for deletion).
//   - PRUNER goroutine: continuously calls the real pruner.Service.Prune(height),
//     which runs the DAH sweep and cascade-deletes (spends -> outputs -> txs)
//     every tx whose delete_at_height (= completion + 1 + retention) is reached.
//
// The hash hand-off channel is BOUNDED and the worker BLOCKS on it: when the
// miner/pruner pipeline cannot keep up, creation back-pressures to the rate at
// which rows can also be reclaimed. That is the point — it measures the
// SUSTAINABLE balanced throughput and keeps the table bounded, instead of
// letting creation outrun reclamation and grow unbounded.
//
// Run with, e.g.:
//
//	THROUGHPUT_WORKERS=10000 THROUGHPUT_VERBOSE=1 \
//	  go test ./stores/utxo/ -run TestThroughput_QueueStorePruned -v -timeout 30m

const (
	// prunedRetention: a fully-spent tx is reclaimed ~this many height-ticks
	// after it completes. Comfortably larger than the number of ticks a worker
	// can fall behind between iterations, so a worker's immediate parent is
	// never pruned out from under it.
	prunedRetention = 20
	// prunedHeightTickMS: how often the shared block height advances. Faster =>
	// smaller per-height cohorts => tighter table bound.
	prunedHeightTickMS = 250
	// prunedMiners: concurrent miner goroutines draining the hash channel.
	// Production drives SetMinedMulti through an errgroup with
	// MaxMinedRoutines=128 concurrent batches and never gates the per-tx
	// validator path on it; 3 serial drainers turned the channel hand-off into
	// the measured bottleneck (3 × 4000 / ~200ms call ≈ the observed plateau).
	prunedMiners = 12
	// prunedMineBatch: hashes per SetMinedMulti call.
	prunedMineBatch = 4000
	// prunedMineChanCap: bounded in-flight unmined hashes; workers block when
	// full, throttling creation to the sustainable mine+prune rate. Sized tighter
	// (50K) so creation back-pressures BEFORE the table can outgrow shared_buffers,
	// keeping the working set cache-resident and the throughput non-decaying.
	prunedMineChanCap = 50000
	// prunedTableCapRows: hard bound on live txs rows. The mine channel bounds
	// create→mine lag but NOTHING bounds create→reclaim lag: when the reclaim
	// pipeline (stamp+delete) falls behind, the table grows unboundedly, the
	// working set leaves cache, reclaim degrades further, and the run lands in a
	// self-reinforcing slow regime (observed bimodal 88K vs ~65K medians on
	// identical code). Gating creation on live-table size converts that spiral
	// into honest back-pressure: the reported TPS becomes "balanced throughput
	// with the table bounded at <= cap", which is the claim that matters.
	prunedTableCapRows = 1_500_000
)

// newPrunedQueueStore builds a fresh postgres queue store on a clean DB with a
// short DAH retention, and returns the concrete *Store (so the pruner service is
// reachable) plus an explicit stop func.
func newPrunedQueueStore(t *testing.T) (*pgstore.Store, func()) {
	t.Helper()
	cleanDB(t) // t.Skipf's the whole test if postgres is unreachable
	ctx := context.Background()

	storeURL, _ := url.Parse(throughputDSN)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	// Short retention so pruning actually reclaims rows during the run.
	tSettings.GlobalBlockHeightRetention = prunedRetention
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	// Isolate the DAH sweep CALLs + pruner deletes onto a dedicated pool so the
	// validator's batchers cannot starve reclaim under high worker counts (the cause
	// of the bimodal collapse to 0 TPS), and sweep all 8 partitions in parallel on
	// this cache-resident box so stamping keeps pace with a ~100K/s create rate.
	tSettings.UtxoStore.PostgresMaintenancePoolConns = 64
	tSettings.UtxoStore.PostgresDAHSweepConcurrency = 8

	s, err := pgstore.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	if err != nil {
		t.Fatalf("pruned queue store: %v", err)
	}
	s.Start(ctx)
	return s, func() { s.Stop() }
}

// prunedBenchStore is the slice of the store surface the pruned harness needs:
// the full utxo.Store hot path plus access to the pruner service. Satisfied by
// *pgstore.Store and by the 2-shard router in throughput_sharded_test.go.
type prunedBenchStore interface {
	utxo.Store
	GetPrunerService() (pruner.Service, error)
}

// runPrunedValidator mirrors runStableValidator's hot path exactly but runs it
// against a continuously-pruned store: a shared advancing block height plus
// background miner + pruner goroutines. Returns per-rep TPS.
func runPrunedValidator(t *testing.T, store prunedBenchStore, numWorkers int, cfg stableCfg, statPool *pgxpool.Pool) []float64 {
	t.Helper()
	ctx := context.Background()
	const startHeight = int64(200)

	var curH atomic.Int64
	curH.Store(startHeight)
	_ = store.SetBlockHeight(uint32(startHeight))

	svc, err := store.GetPrunerService()
	if err != nil {
		t.Fatalf("pruner service: %v", err)
	}
	svc.Start(ctx) // Worker 2 DAH cursor — realistic background stamping load.

	mineCh := make(chan chainhash.Hash, prunedMineChanCap)
	driverCtx, cancel := context.WithCancel(ctx)
	var driverWG sync.WaitGroup
	var totalMined, totalDeleted atomic.Int64

	// TABLE-SIZE GATE: poll the live-row estimate and close the gate while the
	// table exceeds prunedTableCapRows (see const). gateClosed is read by every
	// worker each iteration; gatedNs accumulates total worker wait for telemetry.
	var gateClosed atomic.Bool
	var gatedPauses atomic.Int64
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				var live int64
				if err := statPool.QueryRow(driverCtx, `SELECT COALESCE(sum(n_live_tup),0)
					FROM pg_stat_user_tables WHERE relname LIKE 'txs_p%'`).Scan(&live); err == nil {
					gateClosed.Store(live > prunedTableCapRows)
				}
			}
		}
	}()

	// HEIGHT: advance the chain independently of prune speed.
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		tk := time.NewTicker(prunedHeightTickMS * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				_ = store.SetBlockHeight(uint32(curH.Add(1)))
			}
		}
	}()

	// MINERS: batch-mine freshly created txs so the DAH sweep can stamp them.
	for m := 0; m < prunedMiners; m++ {
		driverWG.Add(1)
		go func() {
			defer driverWG.Done()
			buf := make([]*chainhash.Hash, 0, prunedMineBatch)
			flush := func() {
				if len(buf) == 0 {
					return
				}
				if _, mErr := store.SetMinedMulti(driverCtx, buf, utxo.MinedBlockInfo{
					BlockID: 1, BlockHeight: uint32(curH.Load()), OnLongestChain: true,
				}); mErr == nil {
					totalMined.Add(int64(len(buf)))
				} else if driverCtx.Err() == nil {
					t.Logf("[prune] SetMinedMulti(%d): %v", len(buf), mErr)
				}
				buf = buf[:0]
			}
			tk := time.NewTicker(100 * time.Millisecond) // flush partial batches
			defer tk.Stop()
			for {
				select {
				case <-driverCtx.Done():
					return
				case h := <-mineCh:
					hh := h
					buf = append(buf, &hh)
					if len(buf) >= prunedMineBatch {
						flush()
					}
				case <-tk.C:
					flush()
				}
			}
		}()
	}

	// PRUNER: continuously sweep + cascade-delete reached-DAH txs. Runs
	// back-to-back while there is a backlog and backs off only briefly when a
	// sweep found nothing to delete — so reclaim drains continuously rather than
	// in fixed 25ms slices (a real node prunes continuously too).
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		for {
			if driverCtx.Err() != nil {
				return
			}
			d, pErr := svc.Prune(driverCtx, uint32(curH.Load()), "bench")
			if pErr == nil {
				totalDeleted.Add(d)
			} else if driverCtx.Err() == nil {
				t.Logf("[prune] Prune: %v", pErr)
			}
			if d == 0 {
				select {
				case <-driverCtx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
	}()

	// Genesis tx per worker (untimed), created at the start height.
	parents := make([]*bt.Tx, numWorkers)
	for i := 0; i < numWorkers; i++ {
		g := makeGenesisTx(i)
		if _, cErr := store.Create(ctx, g, uint32(startHeight)); cErr != nil {
			t.Fatalf("genesis create worker %d: %v", i, cErr)
		}
		parents[i] = g
	}

	runPhase := func(dur time.Duration) int64 {
		var ops atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		deadline := time.Now().Add(dur)

		for w := 0; w < numWorkers; w++ {
			w := w
			go func() {
				defer wg.Done()
				parent := parents[w]
				defer func() { parents[w] = parent }()

				for time.Now().Before(deadline) {
					// Honour the table-size gate: pause creation while the live
					// table exceeds the cap so reclaim can catch up. The pause
					// counts against measured TPS — that is the point.
					for gateClosed.Load() {
						gatedPauses.Add(1)
						select {
						case <-driverCtx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}
						if !time.Now().Before(deadline) {
							return
						}
					}
					h := uint32(curH.Load())
					child := makeChildTx(parent)
					parentHash := parent.TxIDChainHash()

					if _, err := store.Get(ctx, parentHash, fields.BlockIDs, fields.BlockHeights); err != nil {
						return
					}
					if _, err := store.Spend(ctx, child, h); err != nil {
						return
					}
					if _, err := store.Create(ctx, child, h, utxo.WithLocked(true)); err != nil {
						return
					}
					if err := store.SetLocked(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false); err != nil {
						return
					}

					parent = child
					ops.Add(1)
					// Hand the new tx to the miners. Blocks when the pipeline is
					// saturated — back-pressuring creation to the sustainable rate.
					select {
					case mineCh <- *child.TxIDChainHash():
					case <-driverCtx.Done():
						return
					}
				}
			}()
		}
		wg.Wait()
		return ops.Load()
	}

	// Warmup (discarded): also lets the prune loop reach steady-state so the
	// table is already bounded before the clock starts.
	if cfg.warmup > 0 {
		_ = runPhase(cfg.warmup)
	}

	samples := make([]float64, 0, cfg.reps)
	for r := 0; r < cfg.reps; r++ {
		start := time.Now()
		ops := runPhase(cfg.measure)
		elapsed := time.Since(start)
		if elapsed <= 0 {
			continue
		}
		tps := float64(ops) / elapsed.Seconds()
		samples = append(samples, tps)
		if cfg.verbose {
			rows, dead, stamped := prunedTableStats(ctx, statPool)
			t.Logf("[rep] %s workers=%d rep=%d/%d tps=%.0f txs_rows=%d txs_dead=%d stamped=%d height=%d mined=%d deleted=%d mineCh=%d/%d gatedPauses=%d",
				time.Now().Format("15:04:05"), numWorkers, r+1, cfg.reps, tps, rows, dead, stamped, curH.Load(),
				totalMined.Load(), totalDeleted.Load(), len(mineCh), cap(mineCh), gatedPauses.Load())
		}
	}

	cancel()
	driverWG.Wait()
	t.Logf("[prune] totals: mined=%d deleted=%d finalHeight=%d", totalMined.Load(), totalDeleted.Load(), curH.Load())
	return samples
}

// prunedTableStats reports live + dead tuple estimates for the txs partitions,
// plus the count of rows currently STAMPED for deletion (delete_at_height set) —
// the latter distinguishes a stamping (DAH sweep) bottleneck from a delete
// bottleneck: if stamped stays ~0 while rows grow, the sweep isn't keeping up.
func prunedTableStats(ctx context.Context, pool *pgxpool.Pool) (rows, dead, stamped int64) {
	_ = pool.QueryRow(ctx, `SELECT COALESCE(sum(n_live_tup),0), COALESCE(sum(n_dead_tup),0)
		FROM pg_stat_user_tables WHERE relname LIKE 'txs%'`).Scan(&rows, &dead)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM txs WHERE delete_at_height IS NOT NULL`).Scan(&stamped)
	return rows, dead, stamped
}

func TestThroughput_QueueStorePruned(t *testing.T) {
	terminateOtherConnections(t)
	cfg := defaultStableCfg()

	statPool, err := pgxpool.New(context.Background(), throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer statPool.Close()

	t.Logf("[Pruned Queue Store] retention=%d heightTick=%dms miners=%d reps=%d warmup=%s measure=%s workers=%v",
		prunedRetention, prunedHeightTickMS, prunedMiners, cfg.reps, cfg.warmup, cfg.measure, cfg.workers)

	for _, w := range cfg.workers {
		store, stop := newPrunedQueueStore(t)
		samples := runPrunedValidator(t, store, w, cfg, statPool)
		stop()

		st := summarize(samples)
		t.Logf("[Pruned Queue Store] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
			w, st.median, st.mean, st.cv, st.min, st.max, st.n, unstableFlag(st.cv, cfg.unstableCV))
	}
}

// TestThroughput_PruneDrainCapacity measures the pruner's DELETE throughput in
// ISOLATION (no concurrent create load during the timed drain), which both
// avoids the macOS shared-memory pressure that the combined create+parallel-
// delete load provokes on a dev box, and cleanly measures raw reclaim capacity.
//
// It builds a backlog of fully-spent, mined, DAH-eligible txs, records the
// create throughput while doing so, then times a single Prune() draining the
// whole backlog. The goal: deleteTPS >= 1.5x createTPS (prune can reclaim faster
// than the chain is created, so it keeps the table bounded with headroom).
//
//	THROUGHPUT_VERBOSE=1 PRUNE_DRAIN_TXS=1000000 PRUNE_DRAIN_WORKERS=200 \
//	  go test ./stores/utxo/ -run TestThroughput_PruneDrainCapacity -v -timeout 12m
func TestThroughput_PruneDrainCapacity(t *testing.T) {
	store, stop := newPrunedQueueStore(t)
	defer stop()
	ctx := context.Background()
	const startHeight = uint32(200)
	_ = store.SetBlockHeight(startHeight)

	target := envInt("PRUNE_DRAIN_TXS", 1000000)
	nw := envInt("PRUNE_DRAIN_WORKERS", 200)
	perWorker := target / nw

	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer pool.Close()

	// --- Populate: build chains (create+spend) at startHeight, timing creates. ---
	parents := make([]*bt.Tx, nw)
	for i := 0; i < nw; i++ {
		g := makeGenesisTx(i)
		if _, cErr := store.Create(ctx, g, startHeight); cErr != nil {
			t.Fatalf("genesis %d: %v", i, cErr)
		}
		parents[i] = g
	}

	var created atomic.Int64
	var wg sync.WaitGroup
	wg.Add(nw)
	start := time.Now()
	for w := 0; w < nw; w++ {
		w := w
		go func() {
			defer wg.Done()
			parent := parents[w]
			for j := 0; j < perWorker; j++ {
				child := makeChildTx(parent)
				if _, err := store.Spend(ctx, child, startHeight); err != nil {
					return
				}
				if _, err := store.Create(ctx, child, startHeight); err != nil {
					return
				}
				parent = child
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	createTPS := float64(created.Load()) / time.Since(start).Seconds()

	// --- Mine every tx AT startHeight so mined_at_height stays low (so the DAH
	// (completion+retention) is reached once we advance the height below). ---
	mineRows, err := pool.Query(ctx, `SELECT hash FROM txs WHERE block_ids IS NULL`)
	if err != nil {
		t.Fatalf("query unmined: %v", err)
	}
	var hashes []*chainhash.Hash
	for mineRows.Next() {
		var b []byte
		if mineRows.Scan(&b) == nil {
			if ch, hErr := chainhash.NewHash(b); hErr == nil {
				hashes = append(hashes, ch)
			}
		}
	}
	mineRows.Close()
	const mineChunk = 5000
	for i := 0; i < len(hashes); i += mineChunk {
		end := i + mineChunk
		if end > len(hashes) {
			end = len(hashes)
		}
		if _, mErr := store.SetMinedMulti(ctx, hashes[i:end], utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: startHeight, OnLongestChain: true,
		}); mErr != nil {
			t.Fatalf("mine: %v", mErr)
		}
	}

	pruneHeight := startHeight + uint32(prunedRetention) + 5
	_ = store.SetBlockHeight(pruneHeight)

	// Stamp the ENTIRE fully-spent+mined backlog for deletion directly, so the
	// drain measures the cascade-DELETE path's true throughput rather than the
	// catch-up sweep's per-call LIMIT. (Equivalent to what Worker 2 + repeated
	// sweeps stamp over time; done in one statement here so the timed drain is a
	// clean measurement of reclaim capacity.) Every non-tip chain tx is mined and
	// has exactly one spend, so this matches the sweep's "fully spent" predicate.
	if _, sErr := pool.Exec(ctx, `UPDATE txs t SET delete_at_height = 1
		WHERE block_ids IS NOT NULL
		  AND EXISTS (SELECT 1 FROM spends s WHERE s.prev_tx_hash = t.hash)`); sErr != nil {
		t.Fatalf("stamp backlog: %v", sErr)
	}
	var eligible int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM txs WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1`, int64(pruneHeight)).Scan(&eligible)

	svc, err := store.GetPrunerService()
	if err != nil {
		t.Fatalf("pruner service: %v", err)
	}

	// --- Time the drain: Prune() cascade-deletes the whole eligible backlog,
	// with NO concurrent create load (isolated reclaim capacity). ---
	drainStart := time.Now()
	deleted, pErr := svc.Prune(ctx, pruneHeight, "drain")
	drainElapsed := time.Since(drainStart)
	if pErr != nil {
		t.Fatalf("prune drain: %v", pErr)
	}
	deleteTPS := float64(deleted) / drainElapsed.Seconds()

	// The goal: prune reclaim >= 1.5x the create throughput. Compare against the
	// 60K-TPS acceptance floor (need >=90K) and the measured create rate.
	const tpsFloor = 60000.0
	t.Logf("[prune-drain] eligible=%d deleted=%d in %s | deleteTPS=%.0f | createTPS(populate@%dw)=%.0f",
		eligible, deleted, drainElapsed.Round(time.Millisecond), deleteTPS, nw, createTPS)
	t.Logf("[prune-drain] reclaim vs 60K floor: %.2fx (need >=1.5x => >=90K/s) | vs populate-create: %.2fx",
		deleteTPS/tpsFloor, deleteTPS/createTPS)
}
