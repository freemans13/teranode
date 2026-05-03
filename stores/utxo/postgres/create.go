package postgres

import (
	"context"
	"fmt"
	"strings"
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
// SQL strings — `% NumPartitions` substituted from the Go constant so a
// bump to NumPartitions automatically updates every partition_key derivation.
// ---------------------------------------------------------------------------

var (
	insertTxSQL = fmt.Sprintf(`
		INSERT INTO txs (hash, partition_key, version, lock_time, fee, size_in_bytes, coinbase,
			locked, conflicting, frozen, unmined_since)
		VALUES ($1, get_byte($1, 1) %% %d, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (hash, partition_key) DO NOTHING
		RETURNING hash`, NumPartitions)

	insertTxRawSingleSQL = fmt.Sprintf(`INSERT INTO txs_raw (hash, partition_key, raw_tx)
		 VALUES ($1, get_byte($1, 1) %% %d, $2)
		 ON CONFLICT (hash, partition_key) DO NOTHING`, NumPartitions)

	insertTxBlocksSingleSQL = fmt.Sprintf(`INSERT INTO txs_blocks (hash, partition_key, block_ids, block_heights, subtree_idxs)
			 VALUES ($1, get_byte($1, 1) %% %d, $2, $3, $4)
			 ON CONFLICT (hash, partition_key) DO NOTHING`, NumPartitions)

	insertOutputsSingleSQL = fmt.Sprintf(`
			INSERT INTO outputs (tx_hash, partition_key, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
			SELECT $1, get_byte($1, 1) %% %d, u.idx, u.locking_script, u.satoshis, u.frozen, u.utxo_hash, u.csh
			FROM UNNEST($2::bigint[], $3::bytea[], $4::bigint[],
						$5::boolean[], $6::bytea[], $7::bigint[])
				AS u(idx, locking_script, satoshis, frozen, utxo_hash, csh)
			ON CONFLICT (tx_hash, idx, partition_key) DO NOTHING`, NumPartitions)

	insertTxsBulkSQL = fmt.Sprintf(`
		INSERT INTO txs (hash, partition_key, version, lock_time, fee, size_in_bytes, coinbase,
			locked, conflicting, frozen, unmined_since)
		SELECT u.hash, get_byte(u.hash, 1) %% %d, u.version, u.lock_time, u.fee, u.size_in_bytes, u.coinbase,
		       u.locked, u.conflicting, u.frozen, u.unmined_since
		FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[],
		            $6::boolean[], $7::boolean[], $8::boolean[], $9::boolean[],
		            $10::bigint[])
		     AS u(hash, version, lock_time, fee, size_in_bytes, coinbase,
		          locked, conflicting, frozen, unmined_since)
		ON CONFLICT (hash, partition_key) DO NOTHING
		RETURNING hash`, NumPartitions)

	insertTxsRawBulkSQL = fmt.Sprintf(`
		INSERT INTO txs_raw (hash, partition_key, raw_tx)
		SELECT u.hash, get_byte(u.hash, 1) %% %d, u.raw_tx
		FROM UNNEST($1::bytea[], $2::bytea[]) AS u(hash, raw_tx)
		ON CONFLICT (hash, partition_key) DO NOTHING`, NumPartitions)

	insertOutputsBulkSQL = fmt.Sprintf(`
		INSERT INTO outputs (tx_hash, partition_key, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
		SELECT u.tx_hash, get_byte(u.tx_hash, 1) %% %d, u.idx, u.locking_script, u.satoshis, u.frozen, u.utxo_hash, u.csh
		FROM UNNEST($1::bytea[], $2::bigint[], $3::bytea[], $4::bigint[],
		            $5::boolean[], $6::bytea[], $7::bigint[])
		     AS u(tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, csh)
		ON CONFLICT (tx_hash, idx, partition_key) DO NOTHING`, NumPartitions)

	insertConflictingChildrenSQL = fmt.Sprintf(`
			INSERT INTO txs_conflicts (hash, partition_key, conflicting_children)
			VALUES ($1, get_byte($1, 1) %% %d, $2::bytea[])
			ON CONFLICT (hash, partition_key) DO UPDATE SET
			    conflicting_children = COALESCE(txs_conflicts.conflicting_children, '{}') || EXCLUDED.conflicting_children`, NumPartitions)
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

	// If workers are active, route the item to its partition's create worker.
	if s.workersStarted() {
		return s.createBatched(ctx, tx, blockHeight, options)
	}

	// No workers — execute INSERT directly (single-item path).
	return s.createDirect(ctx, tx, blockHeight, options)
}

// createBatched routes a Create request to the worker for the tx's partition.
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	done := make(chan batchCreateResult, 1)
	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}
	rk := Route(txHash)
	s.createSlots[rk.Shard].input <- &batchCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		done:        done,
	}

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

	// Insert against parent tables. Postgres routes the row to the right
	// partition itself via the schema's PARTITION BY LIST declaration on
	// partition_key. ON CONFLICT (hash, partition_key) silently skips a
	// duplicate hash (returns no row); RETURNING hash returns the new row
	// when inserted. ErrNoRows on a non-error scan means duplicate.
	var insertedHash []byte
	err = conn.QueryRow(ctx, insertTxSQL,
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
	).Scan(&insertedHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
		}
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}

	// Insert raw_tx into the side table. We always have non-empty raw_tx
	// here (Create's caller always passes the full tx). ON CONFLICT covers
	// retry / test paths where the txs row already existed.
	if _, err = conn.Exec(ctx, insertTxRawSingleSQL,
		txHash[:], rawTx,
	); err != nil {
		return nil, errors.NewStorageError("failed to insert raw_tx", err)
	}

	// Insert block arrays into the txs_blocks side table only when this tx
	// already has mining info (rare initial-sync path). Validator hot path
	// never sets MinedBlockInfos.
	if len(blockIDs) > 0 {
		_, err = conn.Exec(ctx, insertTxBlocksSingleSQL,
			txHash[:], blockIDs, blkHeights, subtreeIdxs,
		)
		if err != nil {
			return nil, errors.NewStorageError("failed to insert txs_blocks", err)
		}
	}

	if len(outArrs.idx) > 0 {
		_, err = conn.Exec(ctx, insertOutputsSingleSQL,
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
// runCreateBatch — per-shard Create worker callback.
//
// All items in `batch` were routed by Route(tx_hash).Shard. The worker
// holds the connection for life. Items without MinedBlockInfos /
// Conflicting use the UNNEST hot path (single transaction wrapping
// INSERT INTO txs + INSERT INTO outputs on the parent tables; postgres
// routes to the right child via PARTITION BY LIST). Items with those rare
// options fall back to per-item createDirect.
// ---------------------------------------------------------------------------
func (s *Store) runCreateBatch(conn *pgxpool.Conn, batch []*batchCreateItem) {
	ctx := context.Background()

	// If any item carries MinedBlockInfos or Conflicting, the whole batch
	// falls back to per-item createDirect (rare initial-sync path). Cheap
	// short-circuit because the validator hot path never sets these.
	for _, item := range batch {
		if len(item.options.MinedBlockInfos) > 0 || item.options.Conflicting {
			for _, it := range batch {
				result, err := s.createDirect(ctx, it.tx, it.blockHeight, it.options)
				it.done <- batchCreateResult{Data: result, Err: err}
			}
			return
		}
	}

	prepared := make([]preparedCreate, len(batch))
	valid := make([]bool, len(batch))

	n := len(batch)
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

		hashes = append(hashes, txHash[:])
		versions = append(versions, int64(item.tx.Version))
		lockTimes = append(lockTimes, int64(item.tx.LockTime))
		fees = append(fees, int64(txMeta.Fee))
		sizes = append(sizes, int64(txMeta.SizeInBytes))
		coinbases = append(coinbases, isCoinbase)
		rawTxs = append(rawTxs, item.tx.ExtendedBytes())
		lockeds = append(lockeds, item.options.Locked)
		conflictings = append(conflictings, false)
		frozens = append(frozens, item.options.Frozen)
		unminedSinces = append(unminedSinces, int64(item.blockHeight))

		for j := range outArrs.idx {
			outTxHashes = append(outTxHashes, txHash[:])
			outIdxs = append(outIdxs, outArrs.idx[j])
			outLockingScripts = append(outLockingScripts, outArrs.lockingScript[j])
			outSatoshis = append(outSatoshis, outArrs.satoshis[j])
			outFrozens = append(outFrozens, outArrs.frozen[j])
			outUtxoHashes = append(outUtxoHashes, outArrs.utxoHash[j])
			outCoinbaseSpendingHeights = append(outCoinbaseSpendingHeights, outArrs.coinbaseSpendingHeight[j])
		}
	}

	if len(hashes) == 0 {
		return
	}

	failAll := func(err error) {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: err}
			}
		}
	}

	// Pipeline BEGIN + INSERT txs + INSERT outputs + COMMIT into ONE
	// network round-trip via pgx.Batch. The previous implementation made
	// these 4 sequential round-trips per Create batch, which (per runtime
	// trace 2026-04-29) accounted for 61% of all bench-worker blocking
	// time. SendBatch sends all 4 statements in one TCP write; postgres
	// processes them in order; we read the 4 responses in one TCP read.
	// Bulk INSERTs are package-level vars `insertTxsBulkSQL`,
	// `insertTxsRawBulkSQL`, `insertOutputsBulkSQL` — each substitutes
	// NumPartitions into `% NumPartitions` so a bump to NumPartitions
	// flows through automatically. ON CONFLICT (hash, partition_key) DO
	// NOTHING silently skips duplicate hashes; the RETURNING hash set
	// tells us which were actually inserted.

	pgxBatch := &pgx.Batch{}
	pgxBatch.Queue("BEGIN")
	pgxBatch.Queue(insertTxsBulkSQL,
		hashes, versions, lockTimes, fees, sizes, coinbases,
		lockeds, conflictings, frozens, unminedSinces,
	)
	pgxBatch.Queue(insertTxsRawBulkSQL, hashes, rawTxs)
	if len(outTxHashes) > 0 {
		pgxBatch.Queue(insertOutputsBulkSQL,
			outTxHashes, outIdxs, outLockingScripts, outSatoshis,
			outFrozens, outUtxoHashes, outCoinbaseSpendingHeights,
		)
	}
	pgxBatch.Queue("COMMIT")

	br := conn.SendBatch(ctx, pgxBatch)
	// Drain BEGIN.
	if _, err := br.Exec(); err != nil {
		br.Close()
		failAll(errors.NewStorageError("failed to BEGIN: %v", err))
		return
	}
	// Read INSERT txs RETURNING rows.
	rows, err := br.Query()
	if err != nil {
		br.Close()
		failAll(errors.NewStorageError("failed to INSERT txs via UNNEST: %v", err))
		return
	}
	newHashSet := make(map[chainhash.Hash]struct{})
	for rows.Next() {
		var hashBytes []byte
		if scanErr := rows.Scan(&hashBytes); scanErr != nil {
			rows.Close()
			br.Close()
			failAll(errors.NewStorageError("failed to scan inserted hash", scanErr))
			return
		}
		var h chainhash.Hash
		copy(h[:], hashBytes)
		newHashSet[h] = struct{}{}
	}
	rows.Close()
	// Drain INSERT txs_raw.
	if _, err := br.Exec(); err != nil {
		br.Close()
		failAll(errors.NewStorageError("failed to INSERT txs_raw via UNNEST: %v", err))
		return
	}
	// Drain INSERT outputs (if queued).
	if len(outTxHashes) > 0 {
		if _, err := br.Exec(); err != nil {
			br.Close()
			failAll(errors.NewStorageError("failed to INSERT outputs via UNNEST: %v", err))
			return
		}
	}
	// Drain COMMIT.
	if _, err := br.Exec(); err != nil {
		br.Close()
		failAll(errors.NewStorageError("failed to commit create batch: %v", err))
		return
	}
	if err := br.Close(); err != nil {
		failAll(errors.NewStorageError("failed to close create batch: %v", err))
		return
	}

	for i, item := range batch {
		if !valid[i] {
			continue
		}
		p := &prepared[i]
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

// insertConflictingChildrenDirect upserts each parent's row in txs_conflicts
// to append this child to its conflicting_children array.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, conn *pgxpool.Conn, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}

		_, err := conn.Exec(ctx, insertConflictingChildrenSQL,
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
