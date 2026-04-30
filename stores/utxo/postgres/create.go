package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Batch types
// ---------------------------------------------------------------------------

// batchCreateItem represents a single Create() request queued into the batcher.
type batchCreateItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan batchCreateResult
}

// batchCreateResult holds the result routed back to a Create() caller.
type batchCreateResult struct {
	Data *meta.Data
	Err  error
}

// ---------------------------------------------------------------------------
// Array builders — pack transaction data into parallel arrays for UNNEST
// ---------------------------------------------------------------------------

// outputArrayParams holds parallel arrays for UNNEST output insertion.
type outputArrayParams struct {
	idx                    []int64
	lockingScript          [][]byte
	satoshis               []int64
	frozen                 []bool
	utxoHash               [][]byte
	coinbaseSpendingHeight []int64
}

// buildOutputArrays packs transaction outputs into parallel arrays for UNNEST.
func buildOutputArrays(txHash *chainhash.Hash, btTx *bt.Tx, isCoinbase bool, blockHeight uint32, coinbaseMaturity int) (outputArrayParams, error) {
	count := countNonNilOutputs(btTx)
	if count == 0 {
		return outputArrayParams{}, nil
	}

	var coinbaseSpendingHeight int64
	if isCoinbase {
		coinbaseSpendingHeight = int64(blockHeight) + int64(coinbaseMaturity)
	}

	p := outputArrayParams{
		idx:                    make([]int64, 0, count),
		lockingScript:          make([][]byte, 0, count),
		satoshis:               make([]int64, 0, count),
		frozen:                 make([]bool, 0, count),
		utxoHash:               make([][]byte, 0, count),
		coinbaseSpendingHeight: make([]int64, 0, count),
	}
	for i, output := range btTx.Outputs {
		if output == nil {
			continue
		}
		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return outputArrayParams{}, err
		}
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return outputArrayParams{}, err
		}
		p.idx = append(p.idx, int64(i))
		if output.LockingScript != nil {
			p.lockingScript = append(p.lockingScript, []byte(*output.LockingScript))
		} else {
			p.lockingScript = append(p.lockingScript, nil)
		}
		p.satoshis = append(p.satoshis, int64(output.Satoshis))
		p.frozen = append(p.frozen, false)
		p.utxoHash = append(p.utxoHash, utxoHash[:])
		p.coinbaseSpendingHeight = append(p.coinbaseSpendingHeight, coinbaseSpendingHeight)
	}
	return p, nil
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash       *chainhash.Hash
	txMeta       *meta.Data
	isCoinbase   bool
	unminedSince interface{}
	rawTx        []byte
	blockIDs     []int32
	blockHeights []int32
	subtreeIdxs  []int32
	outArrs      outputArrayParams
}

// ---------------------------------------------------------------------------
// Create — public API
// ---------------------------------------------------------------------------

// Create stores a new transaction's outputs as UTXOs. When a createBatcher is
// active (after Start()), requests are enqueued for batch processing. Otherwise,
// the INSERT is executed directly for single-item operation (used in tests).
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	startTime := time.Now()
	defer func() {
		if prometheusDirectCreate != nil {
			prometheusDirectCreate.Inc()
		}
		if prometheusDirectCreateDuration != nil {
			prometheusDirectCreateDuration.Observe(time.Since(startTime).Seconds())
		}
	}()

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// If batcher is active, enqueue and wait.
	if s.createBatcher != nil {
		return s.createBatched(ctx, tx, blockHeight, options)
	}

	// No batcher — execute INSERT directly (single-item path).
	return s.createDirect(ctx, tx, blockHeight, options)
}

// createBatched enqueues a Create request into the batcher for bulk processing.
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	done := make(chan batchCreateResult, 1)
	s.createBatcher.Put(&batchCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		done:        done,
	})

	select {
	case result := <-done:
		return result.Data, result.Err
	case <-ctx.Done():
		s.logger.Warnf("[createBatched] context cancelled while waiting for batcher result")
		return nil, ctx.Err()
	}
}

// createDirect executes the INSERT directly for a single transaction.
func (s *Store) createDirect(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	if options.Conflicting {
		txMeta.Conflicting = true
	}
	if options.Locked {
		txMeta.Locked = true
	}

	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	var unminedSince interface{}
	if len(options.MinedBlockInfos) == 0 {
		unminedSince = int64(blockHeight)
	}

	rawTx := tx.ExtendedBytes()

	// Build block_id arrays.
	var blockIDs, blkHeights, subtreeIdxs []int32
	if len(options.MinedBlockInfos) > 0 {
		blockIDs = make([]int32, len(options.MinedBlockInfos))
		blkHeights = make([]int32, len(options.MinedBlockInfos))
		subtreeIdxs = make([]int32, len(options.MinedBlockInfos))
		for i, info := range options.MinedBlockInfos {
			blockIDs[i] = int32(info.BlockID)
			blkHeights[i] = int32(info.BlockHeight)
			subtreeIdxs[i] = int32(info.SubtreeIdx)
		}
	}

	outArrs, err := buildOutputArrays(txHash, tx, isCoinbase, blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
	if err != nil {
		return nil, err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire connection", err)
	}
	defer conn.Release()

	// Direct partition addressing: compute the partition for this tx_hash on
	// the Go side and target the child relation directly (txs_pK / outputs_pK).
	// This skips the parent's planner-side partition fanout and lets ON
	// CONFLICT (hash) work since each child has its own primary key.
	rk := Route(txHash)
	txsTable := "txs" + PartitionSuffix(rk.Partition)
	outputsTable := "outputs" + PartitionSuffix(rk.Partition)

	var insertedHash []byte
	err = conn.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since,
			block_ids, block_heights, subtree_idxs, conflicting_children)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`, txsTable),
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase, rawTx,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		blockIDs, blkHeights, subtreeIdxs, [][]byte(nil),
	).Scan(&insertedHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
		}
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}

	// Insert outputs via unnest into the same partition.
	if len(outArrs.idx) > 0 {
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
			SELECT $1, u.idx, u.locking_script, u.satoshis, u.frozen, u.utxo_hash, u.csh
			FROM UNNEST($2::bigint[], $3::bytea[], $4::bigint[],
						$5::boolean[], $6::bytea[], $7::bigint[])
				AS u(idx, locking_script, satoshis, frozen, utxo_hash, csh)
			ON CONFLICT (tx_hash, idx) DO NOTHING`, outputsTable),
			txHash[:],
			outArrs.idx, outArrs.lockingScript, outArrs.satoshis,
			outArrs.frozen, outArrs.utxoHash, outArrs.coinbaseSpendingHeight,
		)
		if err != nil {
			return nil, errors.NewStorageError("failed to insert outputs", err)
		}
	}

	// Handle conflicting children (rare path).
	if txMeta.Conflicting {
		if err = s.insertConflictingChildrenDirect(ctx, conn, txHash, tx); err != nil {
			return nil, err
		}
	}

	result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
	s.cache.Add(*txHash, result)
	return result, nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	// Single-item fast path — direct INSERT into the partition.
	if len(batch) == 1 {
		item := batch[0]
		result, err := s.createDirect(context.Background(), item.tx, item.blockHeight, item.options)
		item.done <- batchCreateResult{Data: result, Err: err}
		return
	}

	ctx := context.Background()

	// Hot path: when no item carries MinedBlockInfos or Conflicting flags, use
	// the partition-affinity UNNEST path. The validator hot path (Get →
	// Spend → Create → Unlock) always lands here because it never sets those
	// options on Create.
	useUNNEST := true
	for _, item := range batch {
		if len(item.options.MinedBlockInfos) > 0 || item.options.Conflicting {
			useUNNEST = false
			break
		}
	}
	if useUNNEST {
		s.sendCreateBatchUNNEST(ctx, batch)
		return
	}

	// Fallback: items with MinedBlockInfos / Conflicting (rare path during
	// initial sync of already-mined txs) — fall back to per-item createDirect.
	// Slower than the old COPY+staging path but always correct against the
	// list-partitioned schema, and not on the validator hot path.
	for _, item := range batch {
		result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
		item.done <- batchCreateResult{Data: result, Err: err}
	}
}

// ---------------------------------------------------------------------------
// sendCreateBatchUNNEST — partition-affinity UNNEST hot path.
//
// Buckets the batch by RouteKey.Partition and dispatches one goroutine per
// non-empty partition. Each goroutine acquires its own connection, BEGINs a
// transaction, and runs two UNNEST'd INSERTs against `txs_pK` and
// `outputs_pK` directly (no parent-table planning, no partition fanout). All
// goroutines run concurrently — multiple Postgres backends commit to
// different relfilenodes in parallel.
// ---------------------------------------------------------------------------
func (s *Store) sendCreateBatchUNNEST(ctx context.Context, batch []*batchCreateItem) {
	prepared := make([]preparedCreate, len(batch))
	valid := make([]bool, len(batch))
	// Bucket of original-batch indices per partition.
	buckets := make([][]int, NumPartitions)

	for i, item := range batch {
		txMeta, err := util.TxMetaDataFromTx(item.tx)
		if err != nil {
			item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
			continue
		}
		if item.options.Locked {
			txMeta.Locked = true
		}

		var txHash *chainhash.Hash
		if item.options.TxID != nil {
			txHash = item.options.TxID
		} else {
			txHash = item.tx.TxIDChainHash()
		}

		isCoinbase := item.tx.IsCoinbase()
		if item.options.IsCoinbase != nil {
			isCoinbase = *item.options.IsCoinbase
		}

		outArrs, err := buildOutputArrays(txHash, item.tx, isCoinbase, item.blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}

		prepared[i] = preparedCreate{
			txHash:     txHash,
			txMeta:     txMeta,
			isCoinbase: isCoinbase,
			outArrs:    outArrs,
		}
		valid[i] = true
		rk := Route(txHash)
		buckets[rk.Partition] = append(buckets[rk.Partition], i)
	}

	// Dispatch one goroutine per non-empty partition.
	var wg sync.WaitGroup
	for partition := 0; partition < NumPartitions; partition++ {
		idxs := buckets[partition]
		if len(idxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(partition int, idxs []int) {
			defer wg.Done()
			s.sendCreateBatchUNNESTPartition(ctx, partition, batch, prepared, idxs)
		}(partition, idxs)
	}
	wg.Wait()
}

// sendCreateBatchUNNESTPartition runs the UNNEST INSERT pair (txs + outputs)
// for one partition's worth of items, on its own connection inside one
// transaction. All txs in `idxs` route to `partition`.
func (s *Store) sendCreateBatchUNNESTPartition(ctx context.Context, partition int, batch []*batchCreateItem, prepared []preparedCreate, idxs []int) {
	txsTable := "txs" + PartitionSuffix(partition)
	outputsTable := "outputs" + PartitionSuffix(partition)

	n := len(idxs)
	hashes := make([][]byte, 0, n)
	versions := make([]int64, 0, n)
	lockTimes := make([]int64, 0, n)
	fees := make([]int64, 0, n)
	sizes := make([]int64, 0, n)
	coinbases := make([]bool, 0, n)
	rawTxs := make([][]byte, 0, n)
	lockeds := make([]bool, 0, n)
	conflictings := make([]bool, 0, n)
	frozens := make([]bool, 0, n)
	unminedSinces := make([]int64, 0, n)

	var outTxHashes [][]byte
	var outIdxs []int64
	var outLockingScripts [][]byte
	var outSatoshis []int64
	var outFrozens []bool
	var outUtxoHashes [][]byte
	var outCoinbaseSpendingHeights []int64

	for _, i := range idxs {
		p := &prepared[i]
		item := batch[i]
		hashes = append(hashes, p.txHash[:])
		versions = append(versions, int64(item.tx.Version))
		lockTimes = append(lockTimes, int64(item.tx.LockTime))
		fees = append(fees, int64(p.txMeta.Fee))
		sizes = append(sizes, int64(p.txMeta.SizeInBytes))
		coinbases = append(coinbases, p.isCoinbase)
		rawTxs = append(rawTxs, item.tx.ExtendedBytes())
		lockeds = append(lockeds, item.options.Locked)
		conflictings = append(conflictings, false) // fast path excludes conflicting items
		frozens = append(frozens, item.options.Frozen)
		unminedSinces = append(unminedSinces, int64(item.blockHeight))

		for j := range p.outArrs.idx {
			outTxHashes = append(outTxHashes, p.txHash[:])
			outIdxs = append(outIdxs, p.outArrs.idx[j])
			outLockingScripts = append(outLockingScripts, p.outArrs.lockingScript[j])
			outSatoshis = append(outSatoshis, p.outArrs.satoshis[j])
			outFrozens = append(outFrozens, p.outArrs.frozen[j])
			outUtxoHashes = append(outUtxoHashes, p.outArrs.utxoHash[j])
			outCoinbaseSpendingHeights = append(outCoinbaseSpendingHeights, p.outArrs.coinbaseSpendingHeight[j])
		}
	}

	failAll := func(err error) {
		for _, i := range idxs {
			batch[i].done <- batchCreateResult{Err: err}
		}
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		failAll(errors.NewStorageError("failed to acquire connection", err))
		return
	}
	defer conn.Release()

	pgxTx, err := conn.Begin(ctx)
	if err != nil {
		failAll(errors.NewStorageError("failed to begin transaction", err))
		return
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	rows, err := pgxTx.Query(ctx, fmt.Sprintf(`
		INSERT INTO %s (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since)
		SELECT u.hash, u.version, u.lock_time, u.fee, u.size_in_bytes, u.coinbase, u.raw_tx,
		       u.locked, u.conflicting, u.frozen, u.unmined_since
		FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[],
		            $6::boolean[], $7::bytea[], $8::boolean[], $9::boolean[], $10::boolean[],
		            $11::bigint[])
		     AS u(hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
		          locked, conflicting, frozen, unmined_since)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`, txsTable),
		hashes, versions, lockTimes, fees, sizes, coinbases, rawTxs,
		lockeds, conflictings, frozens, unminedSinces,
	)
	if err != nil {
		failAll(errors.NewStorageError("failed to INSERT txs via UNNEST", err))
		return
	}

	newHashSet := make(map[chainhash.Hash]struct{})
	for rows.Next() {
		var hashBytes []byte
		if scanErr := rows.Scan(&hashBytes); scanErr != nil {
			rows.Close()
			failAll(errors.NewStorageError("failed to scan inserted hash", scanErr))
			return
		}
		var h chainhash.Hash
		copy(h[:], hashBytes)
		newHashSet[h] = struct{}{}
	}
	rows.Close()

	if len(outTxHashes) > 0 {
		_, err = pgxTx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
			SELECT u.tx_hash, u.idx, u.locking_script, u.satoshis, u.frozen, u.utxo_hash, u.csh
			FROM UNNEST($1::bytea[], $2::bigint[], $3::bytea[], $4::bigint[],
			            $5::boolean[], $6::bytea[], $7::bigint[])
			     AS u(tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, csh)
			ON CONFLICT (tx_hash, idx) DO NOTHING`, outputsTable),
			outTxHashes, outIdxs, outLockingScripts, outSatoshis,
			outFrozens, outUtxoHashes, outCoinbaseSpendingHeights,
		)
		if err != nil {
			failAll(errors.NewStorageError("failed to INSERT outputs via UNNEST", err))
			return
		}
	}

	if err := pgxTx.Commit(ctx); err != nil {
		failAll(errors.NewStorageError("failed to commit create batch", err))
		return
	}

	for _, i := range idxs {
		p := &prepared[i]
		item := batch[i]
		if _, wasNew := newHashSet[*p.txHash]; wasNew {
			result := s.buildCreateMeta(p.txMeta, item.options, p.isCoinbase, item.blockHeight)
			s.cache.Add(*p.txHash, result)
			item.done <- batchCreateResult{Data: result}
		} else {
			item.done <- batchCreateResult{
				Err: errors.NewTxExistsError("transaction already exists (coinbase=%v):", p.isCoinbase),
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildCreateMeta populates the meta.Data from computed values.
func (s *Store) buildCreateMeta(txMeta *meta.Data, options *utxo.CreateOptions, isCoinbase bool, blockHeight uint32) *meta.Data {
	txMeta.IsCoinbase = isCoinbase
	txMeta.Frozen = options.Frozen
	if len(options.MinedBlockInfos) == 0 {
		txMeta.UnminedSince = blockHeight
	}
	if len(options.MinedBlockInfos) > 0 {
		txMeta.BlockIDs = make([]uint32, len(options.MinedBlockInfos))
		txMeta.BlockHeights = make([]uint32, len(options.MinedBlockInfos))
		txMeta.SubtreeIdxs = make([]int, len(options.MinedBlockInfos))
		for i, info := range options.MinedBlockInfos {
			txMeta.BlockIDs[i] = info.BlockID
			txMeta.BlockHeights[i] = info.BlockHeight
			txMeta.SubtreeIdxs[i] = info.SubtreeIdx
		}
	}
	return txMeta
}

// insertConflictingChildrenDirect updates each parent tx row to append this
// child to its conflicting_children array. Targets the parent's partition
// directly so the UPDATE only touches one relation per parent.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, conn *pgxpool.Conn, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}

		rk := Route(&parentHash)
		_, err := conn.Exec(ctx, fmt.Sprintf(`
			UPDATE txs%s SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
			WHERE hash = $1`, PartitionSuffix(rk.Partition)),
			parentHash[:], [][]byte{childTxHash[:]},
		)
		if err != nil {
			return errors.NewStorageError("failed to insert conflicting_children", err)
		}
	}

	return nil
}

// countNonNilOutputs counts non-nil outputs in a transaction.
func countNonNilOutputs(tx *bt.Tx) int {
	count := 0
	for _, o := range tx.Outputs {
		if o != nil {
			count++
		}
	}
	return count
}

// buildINClauseLocal generates a SQL IN clause placeholder string and args.
// startIdx is the 1-based parameter index ($startIdx, $startIdx+1, ...).
func buildINClauseLocal(hashes [][]byte, startIdx int) (string, []interface{}) {
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
		args[i] = h
	}
	return "(" + strings.Join(placeholders, ",") + ")", args
}
