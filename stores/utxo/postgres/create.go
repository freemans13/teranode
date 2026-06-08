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

// outputArrayParams holds parallel arrays for txs array-column population.
// locking_script and satoshis are recovered from raw_tx at read time (see
// batchDecorateOutputs), so they are not stored separately here.
type outputArrayParams struct {
	idx                    []int64
	frozen                 []bool
	utxoHash               [][]byte
	coinbaseSpendingHeight []int64
	spendable              []bool
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
		frozen:                 make([]bool, 0, count),
		utxoHash:               make([][]byte, 0, count),
		coinbaseSpendingHeight: make([]int64, 0, count),
		spendable:              make([]bool, 0, count),
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
		p.frozen = append(p.frozen, false)
		p.utxoHash = append(p.utxoHash, utxoHash[:])
		p.coinbaseSpendingHeight = append(p.coinbaseSpendingHeight, coinbaseSpendingHeight)
		// A zero-value OP_RETURN / data-carrier output can never be spent, so it
		// must not count toward the "fully spent" pruning check. Mirror the
		// aerospike store's ShouldStoreOutputAsUTXO gate. (nil script → no deref,
		// treated as spendable; non-nil zero-value scripts are inspected.)
		spendable := output.LockingScript == nil || utxo.ShouldStoreOutputAsUTXO(isCoinbase, output, blockHeight)
		p.spendable = append(p.spendable, spendable)
	}
	return p, nil
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash     *chainhash.Hash
	txMeta     *meta.Data
	isCoinbase bool
	outArrs    outputArrayParams
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
	// minedAtHeight feeds Worker 2's deferred-DAH mine-arm: a tx created already
	// mined (legacy catch-up) must record the height it was mined at, exactly as
	// SetMinedMulti does, otherwise the sweep can never enumerate the
	// fully-spent-then-mined case and it falls to the keyspace backstop. Use the
	// lowest mined block height (earliest mining).
	var minedAtHeight *int64
	if len(options.MinedBlockInfos) > 0 {
		blockIDs = make([]int32, len(options.MinedBlockInfos))
		blkHeights = make([]int32, len(options.MinedBlockInfos))
		subtreeIdxs = make([]int32, len(options.MinedBlockInfos))
		minH := int64(options.MinedBlockInfos[0].BlockHeight)
		for i, info := range options.MinedBlockInfos {
			blockIDs[i] = int32(info.BlockID)
			blkHeights[i] = int32(info.BlockHeight)
			subtreeIdxs[i] = int32(info.SubtreeIdx)
			if int64(info.BlockHeight) < minH {
				minH = int64(info.BlockHeight)
			}
		}
		minedAtHeight = &minH
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

	// Build per-output txs arrays (parallel to outArrs; empty slice → NULL column).
	// These are indexed 0-based in Go; Postgres 1-based subscripts are [i+1].
	var txsUtxoHashes [][]byte
	var txsOutSpendables []bool
	var txsOutFrozens []bool
	var txsCoinbaseSpendingHeights []int64
	var txsSpendableIns []*int32 // pointer so nil elements become SQL NULL
	if len(outArrs.idx) > 0 {
		cnt := len(outArrs.idx)
		txsUtxoHashes = make([][]byte, cnt)
		txsOutSpendables = make([]bool, cnt)
		txsOutFrozens = make([]bool, cnt)
		txsCoinbaseSpendingHeights = make([]int64, cnt)
		txsSpendableIns = make([]*int32, cnt)
		for j := range outArrs.idx {
			txsUtxoHashes[j] = outArrs.utxoHash[j]
			txsOutSpendables[j] = outArrs.spendable[j]
			txsOutFrozens[j] = outArrs.frozen[j]
			txsCoinbaseSpendingHeights[j] = outArrs.coinbaseSpendingHeight[j]
			// spendable_in is always 0 on initial create (ReAssignUTXO sets it later).
			// Keep as nil (NULL element) — 0 would mean "spendable from genesis".
		}
	}

	// Insert into txs (metadata + state + raw_tx + output arrays).
	var insertedHash []byte
	err = conn.QueryRow(ctx, `
		INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since,
			block_ids, block_heights, subtree_idxs, conflicting_children, mined_at_height,
			utxo_hashes, out_spendables, out_frozens, coinbase_spending_heights, spendable_ins)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
		        $17, $18, $19, $20, $21)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`,
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase, rawTx,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		blockIDs, blkHeights, subtreeIdxs, [][]byte(nil), minedAtHeight,
		txsUtxoHashes, txsOutSpendables, txsOutFrozens, txsCoinbaseSpendingHeights, txsSpendableIns,
	).Scan(&insertedHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
		}
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}

	// Handle conflicting children (rare path).
	if txMeta.Conflicting {
		if err = s.insertConflictingChildrenDirect(ctx, conn, txHash, tx); err != nil {
			return nil, err
		}
	}

	result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
	return result, nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	s.batchStats.createItems.Add(int64(len(batch)))
	s.batchStats.createBatches.Add(1)
	ctx := context.Background()

	// Single-item: the direct INSERT path handles every option (mined,
	// conflicting, locked, frozen) in a single round-trip.
	if len(batch) == 1 {
		item := batch[0]
		result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
		item.done <- batchCreateResult{Data: result, Err: err}
		return
	}

	// Bulk path: a single UNNEST'd INSERT handles the common cases — plain
	// validator-path creates and single-block mined creates (legacy catch-up).
	// The two rare exceptions fall back to the proven per-item createDirect:
	//   - Conflicting txs need a separate conflicting_children insert.
	//   - A tx mined in >1 block at once needs per-row variable-length array
	//     columns, which a flat UNNEST cannot express.
	// (UNNEST benchmarked faster than the old COPY+staging+merge path across
	// the entire configured batch-size range, so there is no large-batch case
	// where staging would win — see BenchmarkCreatePaths.)
	bulk := make([]*batchCreateItem, 0, len(batch))
	for _, item := range batch {
		if item.options.Conflicting || len(item.options.MinedBlockInfos) > 1 {
			result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
			item.done <- batchCreateResult{Data: result, Err: err}
			continue
		}
		bulk = append(bulk, item)
	}
	if len(bulk) > 0 {
		s.sendCreateBatchUNNEST(ctx, bulk)
	}
}

// ---------------------------------------------------------------------------
// sendCreateBatchUNNEST — direct INSERT…SELECT FROM UNNEST. The single bulk
// create path: no staging tables, no COPY protocol, no merge step. Handles
// plain validator-path creates and single-block mined creates (block_ids etc.
// populated when the item carries exactly one MinedBlockInfo). Conflicting and
// multi-block items are filtered out by sendCreateBatch and never reach here.
// Atomicity is preserved by wrapping both INSERTs in a transaction.
// ---------------------------------------------------------------------------
func (s *Store) sendCreateBatchUNNEST(ctx context.Context, batch []*batchCreateItem) {
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
	// Mined-block columns. mined[i] discriminates: when true the row is mined in
	// exactly one block and block_ids/heights/subtree_idxs are written as a
	// single-element array with unmined_since NULL; when false the tx is unmined
	// (block columns NULL, unmined_since = blockHeight). Matches createDirect.
	mineds := make([]bool, 0, n)
	blockIDs := make([]int64, 0, n)
	blockHeights := make([]int64, 0, n)
	subtreeIdxs := make([]int64, 0, n)

	prepared := make([]preparedCreate, n)
	valid := make([]bool, n)

	var outIdxs []int64
	var outFrozens []bool
	var outUtxoHashes [][]byte
	var outCoinbaseSpendingHeights []int64
	var outSpendables []bool
	// outBatchIdx is the 1-based position of the tx in the hashes/new_txs slice.
	// Used by the out_arr aggregation CTE to join per-tx output arrays back to the
	// tx row via UNNEST WITH ORDINALITY (ord = 1-based position).
	var outBatchIdx []int32

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
		conflictings = append(conflictings, false) // conflicting items routed to createDirect
		frozens = append(frozens, item.options.Frozen)
		unminedSinces = append(unminedSinces, int64(item.blockHeight))

		// sendCreateBatch guarantees len(MinedBlockInfos) <= 1 here.
		mined := len(item.options.MinedBlockInfos) == 1
		mineds = append(mineds, mined)
		var bID, bHeight, sIdx int64
		if mined {
			info := item.options.MinedBlockInfos[0]
			bID, bHeight, sIdx = int64(info.BlockID), int64(info.BlockHeight), int64(info.SubtreeIdx)
		}
		blockIDs = append(blockIDs, bID)
		blockHeights = append(blockHeights, bHeight)
		subtreeIdxs = append(subtreeIdxs, sIdx)

		// batchOrd is the 1-based ordinal position of this tx in the hashes slice.
		// It is set AFTER the append above so it matches the UNNEST WITH ORDINALITY
		// position (which is 1-based). We compute it here from len(hashes) after append.
		batchOrd := int32(len(hashes)) // len after append = 1-based ordinal of this tx

		for j := range outArrs.idx {
			outIdxs = append(outIdxs, outArrs.idx[j])
			outFrozens = append(outFrozens, outArrs.frozen[j])
			outUtxoHashes = append(outUtxoHashes, outArrs.utxoHash[j])
			outCoinbaseSpendingHeights = append(outCoinbaseSpendingHeights, outArrs.coinbaseSpendingHeight[j])
			outSpendables = append(outSpendables, outArrs.spendable[j])
			outBatchIdx = append(outBatchIdx, batchOrd)
		}
	}

	if len(hashes) == 0 {
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("failed to acquire connection", err)}
			}
		}
		return
	}
	defer conn.Release()

	// Single autocommit CTE:
	//   out_arr  — aggregates flat output rows back into per-tx parallel arrays
	//              using the batch-index (outBatchIdx = 1-based ordinal of the tx
	//              in the UNNEST); empty output arrays produce zero rows → harmless.
	//   final INSERT into txs — metadata + state + raw_tx + output arrays.
	rows, err := conn.Query(ctx, `
		WITH out_arr AS (
			SELECT bidx,
			       array_agg(uh  ORDER BY oidx) AS utxo_hashes,
			       array_agg(sp  ORDER BY oidx) AS out_spendables,
			       array_agg(fr  ORDER BY oidx) AS out_frozens,
			       array_agg(csh ORDER BY oidx) AS coinbase_spending_heights,
			       array_agg(si  ORDER BY oidx) AS spendable_ins
			FROM UNNEST($16::int[], $17::bigint[], $18::bytea[],
			            $19::boolean[], $20::boolean[], $21::bigint[], $22::int[])
			     AS o(bidx, oidx, uh, sp, fr, csh, si)
			GROUP BY bidx
		)
		INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since, block_ids, block_heights, subtree_idxs, mined_at_height,
			utxo_hashes, out_spendables, out_frozens, coinbase_spending_heights, spendable_ins)
		SELECT u.hash, u.version, u.lock_time, u.fee, u.size_in_bytes, u.coinbase, u.raw_tx,
		       u.locked, u.conflicting, u.frozen,
		       CASE WHEN u.mined THEN NULL::bigint ELSE u.unmined_since END,
		       CASE WHEN u.mined THEN ARRAY[u.block_id]::int[] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN ARRAY[u.block_height]::int[] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN ARRAY[u.subtree_idx]::int[] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN u.block_height::bigint ELSE NULL::bigint END,
		       oa.utxo_hashes, oa.out_spendables, oa.out_frozens, oa.coinbase_spending_heights, oa.spendable_ins
		FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[],
		            $6::boolean[], $7::bytea[], $8::boolean[], $9::boolean[], $10::boolean[],
		            $11::bigint[], $12::boolean[], $13::bigint[], $14::bigint[], $15::bigint[])
		     WITH ORDINALITY AS u(hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
		          locked, conflicting, frozen, unmined_since, mined, block_id, block_height, subtree_idx, ord)
		LEFT JOIN out_arr oa ON oa.bidx = u.ord
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`,
		hashes, versions, lockTimes, fees, sizes, coinbases, rawTxs,
		lockeds, conflictings, frozens, unminedSinces,
		mineds, blockIDs, blockHeights, subtreeIdxs,
		// out_arr params ($16..$22): bidx, oidx, uh, sp, fr, csh, si
		// spendable_ins are all NULL on initial create (set by ReAssignUTXO later).
		// A nil-element []*int32 slice causes array_agg to preserve NULL elements.
		outBatchIdx, outIdxs, outUtxoHashes, outSpendables, outFrozens, outCoinbaseSpendingHeights, make([]*int32, len(outBatchIdx)),
	)
	if err != nil {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("failed to INSERT create batch via CTE", err)}
			}
		}
		return
	}

	newHashSet := make(map[chainhash.Hash]struct{})
	for rows.Next() {
		var hashBytes []byte
		if scanErr := rows.Scan(&hashBytes); scanErr != nil {
			rows.Close()
			for i, item := range batch {
				if valid[i] {
					item.done <- batchCreateResult{Err: errors.NewStorageError("failed to scan inserted hash", scanErr)}
				}
			}
			return
		}
		var h chainhash.Hash
		copy(h[:], hashBytes)
		newHashSet[h] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("create batch rows error", err)}
			}
		}
		return
	}
	rows.Close()

	for i, item := range batch {
		if !valid[i] {
			continue
		}
		p := &prepared[i]
		if _, wasNew := newHashSet[*p.txHash]; wasNew {
			// Consume the hash so only the first item with this hash in the
			// batch is treated as newly created. Concurrent duplicate creates
			// of the same tx coalesce into a single bulk INSERT whose ON CONFLICT
			// DO NOTHING inserts exactly one row (RETURNING it once); without
			// consuming it here, every same-hash item would be marked created and
			// each caller would notify block assembly. Subsequent same-hash items
			// must return TxExists, matching createDirect's ON CONFLICT semantics.
			delete(newHashSet, *p.txHash)
			result := s.buildCreateMeta(p.txMeta, item.options, p.isCoinbase, item.blockHeight)
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

// insertConflictingChildrenDirect updates the parent txs rows to append this child
// to their conflicting_children array.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, conn *pgxpool.Conn, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}

		_, err := conn.Exec(ctx, `
			UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
			WHERE hash = $1
			  AND NOT (COALESCE(conflicting_children, '{}') @> $2::bytea[])`,
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
