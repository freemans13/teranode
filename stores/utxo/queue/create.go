package queue

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

// megaTxThreshold is the number of inputs or outputs above which CopyFrom is
// used instead of unnest for bulk insertion.
const megaTxThreshold = 100

// ---------------------------------------------------------------------------
// Create CTE SQL — single statement that inserts a tx + all its inputs,
// outputs, tx_state, and block_ids in one round-trip.
// ---------------------------------------------------------------------------

// createCTESQL combines the 4 separate INSERTs into a single CTE statement.
// Parameters: $1=hash, $2=version, $3=lock_time, $4=fee, $5=size_in_bytes, $6=coinbase,
// $7=locked, $8=conflicting, $9=frozen, $10=unmined_since,
// $11-$17=input arrays, $18-$24=output arrays, $25-$27=block_id arrays.
const createCTESQL = `
WITH new_tx AS (
    INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, coinbase)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (hash) DO NOTHING
    RETURNING hash
),
new_tx_state AS (
    INSERT INTO tx_state (tx_hash, locked, conflicting, frozen, unmined_since)
    SELECT $1, $7, $8, $9, $10
    WHERE EXISTS (SELECT 1 FROM new_tx)
    ON CONFLICT (tx_hash) DO NOTHING
),
new_inputs AS (
    INSERT INTO inputs (tx_hash, idx, previous_transaction_hash, previous_tx_idx,
                        previous_tx_satoshis, previous_tx_script, unlocking_script, sequence_number)
    SELECT $1, u.idx, u.prev_hash, u.prev_idx, u.prev_satoshis, u.prev_script, u.unlock_script, u.seq_num
    FROM new_tx, UNNEST($11::bigint[], $12::bytea[], $13::bigint[],
                        $14::bigint[], $15::bytea[], $16::bytea[], $17::bigint[])
        AS u(idx, prev_hash, prev_idx, prev_satoshis, prev_script, unlock_script, seq_num)
    ON CONFLICT (tx_hash, idx) DO NOTHING
),
new_outputs AS (
    INSERT INTO outputs (tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
    SELECT $1, u.idx, u.locking_script, u.satoshis, u.frozen, u.utxo_hash, u.csh
    FROM new_tx, UNNEST($18::bigint[], $19::bytea[], $20::bigint[],
                        $21::boolean[], $22::bytea[], $23::bigint[])
        AS u(idx, locking_script, satoshis, frozen, utxo_hash, csh)
    ON CONFLICT (tx_hash, idx) DO NOTHING
),
new_blocks AS (
    INSERT INTO block_ids (tx_hash, block_id, block_height, subtree_idx)
    SELECT $1, u.block_id, u.block_height, u.subtree_idx
    FROM new_tx, UNNEST($24::bigint[], $25::bigint[], $26::bigint[])
        AS u(block_id, block_height, subtree_idx)
    ON CONFLICT (tx_hash, block_id) DO NOTHING
)
SELECT EXISTS (SELECT 1 FROM new_tx)
`

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

// inputArrayParams holds parallel arrays for UNNEST input insertion.
type inputArrayParams struct {
	idx          []int64
	prevHash     [][]byte
	prevIdx      []int64
	prevSatoshis []int64
	prevScript   [][]byte
	unlockScript [][]byte
	seqNum       []int64
}

// buildInputArrays packs transaction inputs into parallel arrays for UNNEST.
func buildInputArrays(btTx *bt.Tx) inputArrayParams {
	n := len(btTx.Inputs)
	if n == 0 {
		return inputArrayParams{}
	}
	p := inputArrayParams{
		idx:          make([]int64, n),
		prevHash:     make([][]byte, n),
		prevIdx:      make([]int64, n),
		prevSatoshis: make([]int64, n),
		prevScript:   make([][]byte, n),
		unlockScript: make([][]byte, n),
		seqNum:       make([]int64, n),
	}
	for i, input := range btTx.Inputs {
		p.idx[i] = int64(i)
		p.prevHash[i] = input.PreviousTxIDChainHash()[:]
		p.prevIdx[i] = int64(input.PreviousTxOutIndex)
		p.prevSatoshis[i] = int64(input.PreviousTxSatoshis)
		if input.PreviousTxScript != nil {
			p.prevScript[i] = []byte(*input.PreviousTxScript)
		}
		if input.UnlockingScript != nil {
			p.unlockScript[i] = []byte(*input.UnlockingScript)
		}
		p.seqNum[i] = int64(input.SequenceNumber)
	}
	return p
}

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

// blockIDArrayParams holds parallel arrays for UNNEST block_id insertion.
type blockIDArrayParams struct {
	blockID     []int64
	blockHeight []int64
	subtreeIdx  []int64
}

// buildBlockIDArrays packs block info into parallel arrays for UNNEST.
func buildBlockIDArrays(blockInfos []utxo.MinedBlockInfo) blockIDArrayParams {
	n := len(blockInfos)
	if n == 0 {
		return blockIDArrayParams{}
	}
	p := blockIDArrayParams{
		blockID:     make([]int64, n),
		blockHeight: make([]int64, n),
		subtreeIdx:  make([]int64, n),
	}
	for i, info := range blockInfos {
		p.blockID[i] = int64(info.BlockID)
		p.blockHeight[i] = int64(info.BlockHeight)
		p.subtreeIdx[i] = int64(info.SubtreeIdx)
	}
	return p
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash       *chainhash.Hash
	txMeta       *meta.Data
	isCoinbase   bool
	unminedSince interface{}
	inpArrs      inputArrayParams
	outArrs      outputArrayParams
	blkArrs      blockIDArrayParams
}

// ---------------------------------------------------------------------------
// Create — public API
// ---------------------------------------------------------------------------

// Create stores a new transaction's outputs as UTXOs. When a createBatcher is
// active (after Start()), requests are enqueued for batch processing. Otherwise,
// the CTE is executed directly for single-item operation (used in tests).
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

	// No batcher — execute CTE directly (single-item path).
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

// createDirect executes the CTE directly for a single transaction.
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

	inpArrs := buildInputArrays(tx)
	outArrs, err := buildOutputArrays(txHash, tx, isCoinbase, blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
	if err != nil {
		return nil, err
	}
	blkArrs := buildBlockIDArrays(options.MinedBlockInfos)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire connection", err)
	}
	defer conn.Release()

	var inserted bool
	err = conn.QueryRow(ctx, createCTESQL,
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		inpArrs.idx, inpArrs.prevHash, inpArrs.prevIdx,
		inpArrs.prevSatoshis, inpArrs.prevScript,
		inpArrs.unlockScript, inpArrs.seqNum,
		outArrs.idx, outArrs.lockingScript, outArrs.satoshis,
		outArrs.frozen, outArrs.utxoHash, outArrs.coinbaseSpendingHeight,
		blkArrs.blockID, blkArrs.blockHeight, blkArrs.subtreeIdx,
	).Scan(&inserted)
	if err != nil {
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}
	if !inserted {
		return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
	}

	// Handle conflicting children (rare path)
	if txMeta.Conflicting {
		if err = s.insertConflictingChildrenDirect(ctx, conn, txHash, tx); err != nil {
			return nil, err
		}
	}

	return s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight), nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	ctx := context.Background()

	// Phase 1: Pre-compute all array parameters (CPU only, no DB)
	prepared := make([]preparedCreate, len(batch))
	for i, item := range batch {
		txMeta, err := util.TxMetaDataFromTx(item.tx)
		if err != nil {
			item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
			continue
		}

		if item.options.Conflicting {
			txMeta.Conflicting = true
		}
		if item.options.Locked {
			txMeta.Locked = true
		}

		var unminedSince interface{}
		if len(item.options.MinedBlockInfos) == 0 {
			unminedSince = int64(item.blockHeight)
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

		inpArrs := buildInputArrays(item.tx)
		outArrs, err := buildOutputArrays(txHash, item.tx, isCoinbase, item.blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}
		blkArrs := buildBlockIDArrays(item.options.MinedBlockInfos)

		prepared[i] = preparedCreate{
			txHash:       txHash,
			txMeta:       txMeta,
			isCoinbase:   isCoinbase,
			unminedSince: unminedSince,
			inpArrs:      inpArrs,
			outArrs:      outArrs,
			blkArrs:      blkArrs,
		}
	}

	// Collect valid items (those without prep errors).
	validIndices := make([]int, 0, len(batch))
	for i := range batch {
		if prepared[i].txHash != nil {
			validIndices = append(validIndices, i)
		}
	}
	if len(validIndices) == 0 {
		return
	}

	// Phase 2: Acquire one pgx connection, queue all valid CTEs into SendBatch.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, idx := range validIndices {
			batch[idx].done <- batchCreateResult{Err: errors.NewStorageError("failed to acquire connection", err)}
		}
		return
	}
	defer conn.Release()

	pgxBatch := &pgx.Batch{}
	for _, idx := range validIndices {
		p := &prepared[idx]
		item := batch[idx]
		pgxBatch.Queue(createCTESQL,
			// $1-$6: transaction scalars
			p.txHash[:], int64(item.tx.Version), int64(item.tx.LockTime),
			int64(p.txMeta.Fee), int64(p.txMeta.SizeInBytes), p.isCoinbase,
			// $7-$10: tx_state scalars
			item.options.Locked, item.options.Conflicting,
			item.options.Frozen, p.unminedSince,
			// $11-$17: input arrays
			p.inpArrs.idx, p.inpArrs.prevHash, p.inpArrs.prevIdx,
			p.inpArrs.prevSatoshis, p.inpArrs.prevScript,
			p.inpArrs.unlockScript, p.inpArrs.seqNum,
			// $18-$23: output arrays
			p.outArrs.idx, p.outArrs.lockingScript, p.outArrs.satoshis,
			p.outArrs.frozen, p.outArrs.utxoHash, p.outArrs.coinbaseSpendingHeight,
			// $24-$26: block_id arrays
			p.blkArrs.blockID, p.blkArrs.blockHeight, p.blkArrs.subtreeIdx,
		)
	}

	br := conn.SendBatch(ctx, pgxBatch)

	// Read results — collect but don't send to callers yet.
	// We must call br.Close() before signalling callers, because
	// pipelined auto-committed statements may not be fully visible
	// to other connections until the batch reader is closed.
	type batchResult struct {
		idx    int
		result batchCreateResult
	}
	results := make([]batchResult, 0, len(validIndices))

	for _, idx := range validIndices {
		p := &prepared[idx]
		rows, queryErr := br.Query()
		var inserted bool
		if queryErr == nil {
			if rows.Next() {
				if scanErr := rows.Scan(&inserted); scanErr != nil {
					queryErr = scanErr
				}
			}
			if err := rows.Err(); err != nil {
				queryErr = err
			}
			rows.Close()
		}
		if queryErr != nil {
			results = append(results, batchResult{idx: idx, result: batchCreateResult{
				Err: errors.NewStorageError("failed to create UTXO", queryErr),
			}})
		} else if !inserted {
			results = append(results, batchResult{idx: idx, result: batchCreateResult{
				Err: errors.NewTxExistsError("transaction already exists (coinbase=%v):", p.isCoinbase),
			}})
		} else {
			results = append(results, batchResult{idx: idx, result: batchCreateResult{
				Data: s.buildCreateMeta(p.txMeta, batch[idx].options, p.isCoinbase, batch[idx].blockHeight),
			}})
		}
	}

	// Close the batch reader — ensures all pipelined commits are finalized.
	if closeErr := br.Close(); closeErr != nil {
		s.logger.Warnf("[sendCreateBatch] error closing batch results: %v", closeErr)
	}

	// Signal callers.
	for _, r := range results {
		batch[r.idx].done <- r.result
	}

	// Phase 3: Handle conflicting children (rare path — separate round-trips only when needed).
	for _, idx := range validIndices {
		p := &prepared[idx]
		if p.txMeta != nil && p.txMeta.Conflicting {
			if conflictErr := s.insertConflictingChildrenDirect(ctx, conn, p.txHash, batch[idx].tx); conflictErr != nil {
				s.logger.Warnf("[sendCreateBatch] failed to insert conflicting children for %x: %v", p.txHash[:], conflictErr)
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

// insertConflictingChildrenDirect inserts conflicting children using an acquired connection.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, conn *pgxpool.Conn, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))
	parentHashes := make([][]byte, 0, len(tx.Inputs))
	childHashes := make([][]byte, 0, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}
		parentHashes = append(parentHashes, parentHash[:])
		childHashes = append(childHashes, childTxHash[:])
	}

	if len(parentHashes) == 0 {
		return nil
	}

	_, err := conn.Exec(ctx, `
		INSERT INTO conflicting_children (tx_hash, child_tx_hash)
		SELECT * FROM unnest($1::bytea[], $2::bytea[])
		ON CONFLICT DO NOTHING`,
		parentHashes, childHashes,
	)
	if err != nil {
		return errors.NewStorageError("failed to insert conflicting_children", err)
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
