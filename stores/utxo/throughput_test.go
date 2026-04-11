package utxo_test

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/queue"
	sqlstore "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

const throughputDSN = "postgres://teranode:teranode@localhost:5432/teranode_test"

func cleanDB(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, `SELECT cron.unschedule('materialize')`)
	_, _ = pool.Exec(ctx, `
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP TABLE IF EXISTS batch_notifications, conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs,
			create_queue, input_queue, output_queue, spend_queue, mined_queue CASCADE;
	`)
}

func p2pkhScript() *bscript.Script {
	s := bscript.Script([]byte{0x76, 0xa9, 0x14,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x88, 0xac})
	return &s
}

// makeGenesisTx creates a unique genesis tx for a worker.
func makeGenesisTx(workerID int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	var h [32]byte
	h[0] = byte(workerID)
	h[1] = byte(workerID >> 8)
	h[2] = byte(workerID >> 16)
	h[3] = byte(workerID >> 24)
	h[4] = 0xFF
	prev, _ := chainhash.NewHash(h[:])
	_ = tx.From(prev.String(), 0, p2pkhScript().String(), 100_000_000)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50_000_000)
	return tx
}

// makeChildTx creates a tx that spends output 0 of the parent.
func makeChildTx(parent *bt.Tx) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	_ = tx.From(
		parent.TxIDChainHash().String(), 0,
		parent.Outputs[0].LockingScript.String(),
		parent.Outputs[0].Satoshis,
	)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	outVal := parent.Outputs[0].Satoshis / 2
	if outVal == 0 {
		outVal = 1
	}
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", outVal)
	return tx
}

func runValidatorThroughputOffset(t *testing.T, store utxo.Store, opsPerWorker int, numWorkers int, workerOffset int) float64 {
	return doRunValidatorThroughput(t, store, opsPerWorker, numWorkers, workerOffset)
}

// runValidatorThroughput simulates the validator hot path per tx:
//  1. Get parent tx (verify it exists)
//  2. Spend parent outputs
//  3. Create child tx (with WithLocked)
//  4. SetLocked(false) to unlock
//
// Each worker builds a chain: genesis → tx1 → tx2 → ...
func runValidatorThroughput(t *testing.T, store utxo.Store, opsPerWorker int, numWorkers int) float64 {
	return doRunValidatorThroughput(t, store, opsPerWorker, numWorkers, 0)
}

func doRunValidatorThroughput(t *testing.T, store utxo.Store, opsPerWorker int, numWorkers int, workerOffset int) float64 {
	ctx := context.Background()
	blockHeight := uint32(200)
	_ = store.SetBlockHeight(blockHeight)

	// Pre-create genesis txs sequentially (these are the "coinbases")
	genesisTxs := make([]*bt.Tx, numWorkers)
	for i := 0; i < numWorkers; i++ {
		genesisTxs[i] = makeGenesisTx(i + workerOffset)
		_, err := store.Create(ctx, genesisTxs[i], blockHeight)
		if err != nil {
			t.Fatalf("genesis create worker %d: %v", i, err)
		}
	}

	// Collect all tx hashes for SetMinedMulti at the end
	allHashes := make([][]*chainhash.Hash, numWorkers)

	var totalOps atomic.Int64
	var errCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		w := w
		go func() {
			defer wg.Done()
			parent := genesisTxs[w]
			var workerHashes []*chainhash.Hash
			// Include genesis tx
			h := genesisTxs[w].TxIDChainHash()
			workerHashes = append(workerHashes, h)

			for i := 0; i < opsPerWorker; i++ {
				child := makeChildTx(parent)
				parentHash := parent.TxIDChainHash()

				// 1. GET parent tx — verify it exists (same fields as validator)
				_, err := store.Get(ctx, parentHash, fields.BlockIDs, fields.BlockHeights)
				if err != nil {
					if errCount.Add(1) <= 5 {
						t.Logf("worker %d iter %d (of %d): GET: %v (parent=%s)", w, i, opsPerWorker, err, parentHash)
					}
					return
				}

				// 2. SPEND parent outputs via child tx
				spends, err := store.Spend(ctx, child, blockHeight)
				if err != nil {
					if errCount.Add(1) <= 5 {
						spendDetail := ""
						for _, sp := range spends {
							if sp.Err != nil {
								spendDetail = fmt.Sprintf(" [%s:%d err=%v]", sp.TxID, sp.Vout, sp.Err)
								break
							}
						}
						t.Logf("worker %d iter %d: SPEND: %v%s", w, i, err, spendDetail)
					}
					return
				}

				// 3. CREATE child tx (locked)
				_, err = store.Create(ctx, child, blockHeight, utxo.WithLocked(true))
				if err != nil {
					if errCount.Add(1) <= 3 {
						t.Logf("worker %d iter %d: CREATE: %v", w, i, err)
					}
					return
				}

				// 4. UNLOCK child tx
				err = store.SetLocked(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false)
				if err != nil {
					if errCount.Add(1) <= 3 {
						t.Logf("worker %d iter %d: UNLOCK: %v", w, i, err)
					}
					return
				}

				childHash := child.TxIDChainHash()
				workerHashes = append(workerHashes, childHash)
				totalOps.Add(1)
				parent = child
			}
			allHashes[w] = workerHashes
		}()
	}

	wg.Wait()
	validatorElapsed := time.Since(start)
	ops := totalOps.Load()
	errs := errCount.Load()
	validatorTPS := float64(ops) / validatorElapsed.Seconds()

	if errs > 0 {
		t.Logf("  (%d workers errored out)", errs)
	}

	// --- SetMinedMulti: simulate a block being mined with all txs ---
	allTxHashes := make([]*chainhash.Hash, 0, len(allHashes)*opsPerWorker)
	for _, wh := range allHashes {
		allTxHashes = append(allTxHashes, wh...)
	}

	if len(allTxHashes) > 0 {
		minedInfo := utxo.MinedBlockInfo{
			BlockID:        1,
			BlockHeight:    blockHeight + 1,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}
		minedStart := time.Now()
		_, err := store.SetMinedMulti(ctx, allTxHashes, minedInfo)
		minedElapsed := time.Since(minedStart)

		if err != nil {
			t.Logf("  SetMinedMulti failed: %v", err)
		} else {
			minedTPS := float64(len(allTxHashes)) / minedElapsed.Seconds()
			t.Logf("  SetMinedMulti: %d txs in %v = %.0f TPS", len(allTxHashes), minedElapsed, minedTPS)
		}
	}

	return validatorTPS
}

func newSQLStoreForBench(t *testing.T) utxo.Store {
	t.Helper()
	cleanDB(t)
	ctx := context.Background()
	storeURL, _ := url.Parse(throughputDSN)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 10
	tSettings.UtxoStore.StoreBatcherDurationMillis = 10
	tSettings.UtxoStore.GetBatcherSize = 100
	tSettings.Postgres.MaxOpenConns = 40
	logger := ulogger.TestLogger{}
	s, err := sqlstore.New(ctx, logger, tSettings, storeURL)
	if err != nil {
		t.Fatalf("sql store: %v", err)
	}
	return s
}

func newQueueStoreForBench(t *testing.T) utxo.Store {
	t.Helper()
	cleanDB(t)
	ctx := context.Background()
	storeURL, _ := url.Parse(throughputDSN)
	storeURL.Scheme = "postgresqueue"
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 10
	tSettings.UtxoStore.StoreBatcherDurationMillis = 10
	logger := ulogger.TestLogger{}
	s, err := queue.New(ctx, logger, tSettings, storeURL)
	if err != nil {
		t.Fatalf("queue store: %v", err)
	}
	s.Start(ctx)
	// Materializer runs via pg_cron (scheduled during schema creation).
	t.Cleanup(func() { s.Stop() })
	return s
}

func terminateOtherConnections(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		return
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid != pg_backend_pid()`)
	time.Sleep(500 * time.Millisecond)
}

func TestThroughput_SQLStore(t *testing.T) {
	s := newSQLStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1, 10, 100, 500, 1000, 10000} {
		opsPerWorker := 20
		if workers <= 10 {
			opsPerWorker = 100
		} else if workers <= 100 {
			opsPerWorker = 50
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("SQL Store: %d workers × %d ops = %.0f TPS (Get+Spend+Create+Unlock per tx)",
				workers, opsPerWorker, opsPerSec)
		})
	}
}

func TestThroughput_QueueStore(t *testing.T) {
	terminateOtherConnections(t)
	s := newQueueStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1, 10, 100, 500, 1000, 10000} {
		opsPerWorker := 20
		if workers <= 10 {
			opsPerWorker = 50
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("Queue Store: %d workers × %d ops = %.0f TPS (Get+Spend+Create+Unlock per tx)",
				workers, opsPerWorker, opsPerSec)
		})
	}
}
