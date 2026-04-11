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
// Array builders — pack transaction data into parallel arrays for the utxos table
// ---------------------------------------------------------------------------

// outputArrays holds parallel arrays for insertion into the utxos table.
type outputArrays struct {
	utxoHashes      [][]byte
	lockingScripts  [][]byte
	satoshis        []int64
	spendingData    [][]byte // nil elements = unspent
	coinbaseHeights []int64
	frozenOutputs   []bool
	spendableIn     []*int32 // nil = not set
}

// buildOutputArraysV6 packs transaction outputs into parallel arrays for the utxos table.
func buildOutputArraysV6(txHash *chainhash.Hash, btTx *bt.Tx, isCoinbase bool, blockHeight uint32, coinbaseMaturity int) (outputArrays, error) {
	numOutputs := len(btTx.Outputs)
	if numOutputs == 0 {
		return outputArrays{}, nil
	}

	var coinbaseSpendingHeight int64
	if isCoinbase {
		coinbaseSpendingHeight = int64(blockHeight) + int64(coinbaseMaturity)
	}

	p := outputArrays{
		utxoHashes:      make([][]byte, numOutputs),
		lockingScripts:  make([][]byte, numOutputs),
		satoshis:        make([]int64, numOutputs),
		spendingData:    make([][]byte, numOutputs), // all nil = unspent
		coinbaseHeights: make([]int64, numOutputs),
		frozenOutputs:   make([]bool, numOutputs),
		spendableIn:     make([]*int32, numOutputs),
	}
	for i, output := range btTx.Outputs {
		if output == nil {
			continue
		}
		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return outputArrays{}, err
		}
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return outputArrays{}, err
		}
		p.utxoHashes[i] = utxoHash[:]
		if output.LockingScript != nil {
			p.lockingScripts[i] = []byte(*output.LockingScript)
		}
		p.satoshis[i] = int64(output.Satoshis)
		p.coinbaseHeights[i] = coinbaseSpendingHeight
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
	outArrs      outputArrays
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

	outArrs, err := buildOutputArraysV6(txHash, tx, isCoinbase, blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
	if err != nil {
		return nil, err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire connection", err)
	}
	defer conn.Release()

	// Single INSERT into utxos table.
	var insertedHash []byte
	err = conn.QueryRow(ctx, `
		INSERT INTO utxos (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			utxo_hashes, locking_scripts, satoshis, spending_data, coinbase_heights, frozen_outputs, spendable_in,
			locked, conflicting, frozen, unmined_since,
			block_ids, block_heights, subtree_idxs, conflicting_children)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`,
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase, rawTx,
		outArrs.utxoHashes, outArrs.lockingScripts, outArrs.satoshis,
		outArrs.spendingData, outArrs.coinbaseHeights, outArrs.frozenOutputs, outArrs.spendableIn,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		blockIDs, blkHeights, subtreeIdxs, [][]byte(nil),
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
	s.cache.Add(*txHash, result)
	return result, nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	ctx := context.Background()

	// Phase 1: Pre-compute all parameters (CPU only, no DB).
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

		rawTx := item.tx.ExtendedBytes()

		var blockIDs, blkHeights, subtreeIdxs []int32
		if len(item.options.MinedBlockInfos) > 0 {
			blockIDs = make([]int32, len(item.options.MinedBlockInfos))
			blkHeights = make([]int32, len(item.options.MinedBlockInfos))
			subtreeIdxs = make([]int32, len(item.options.MinedBlockInfos))
			for j, info := range item.options.MinedBlockInfos {
				blockIDs[j] = int32(info.BlockID)
				blkHeights[j] = int32(info.BlockHeight)
				subtreeIdxs[j] = int32(info.SubtreeIdx)
			}
		}

		outArrs, err := buildOutputArraysV6(txHash, item.tx, isCoinbase, item.blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}

		prepared[i] = preparedCreate{
			txHash:       txHash,
			txMeta:       txMeta,
			isCoinbase:   isCoinbase,
			unminedSince: unminedSince,
			rawTx:        rawTx,
			blockIDs:     blockIDs,
			blockHeights: blkHeights,
			subtreeIdxs:  subtreeIdxs,
			outArrs:      outArrs,
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

	// Phase 2: Acquire one pgx connection and pipeline INSERTs via SendBatch.
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
		pgxBatch.Queue(`
			INSERT INTO utxos (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
				utxo_hashes, locking_scripts, satoshis, spending_data, coinbase_heights, frozen_outputs, spendable_in,
				locked, conflicting, frozen, unmined_since,
				block_ids, block_heights, subtree_idxs, conflicting_children)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
			ON CONFLICT (hash) DO NOTHING
			RETURNING hash`,
			p.txHash[:], int64(item.tx.Version), int64(item.tx.LockTime),
			int64(p.txMeta.Fee), int64(p.txMeta.SizeInBytes), p.isCoinbase, p.rawTx,
			p.outArrs.utxoHashes, p.outArrs.lockingScripts, p.outArrs.satoshis,
			p.outArrs.spendingData, p.outArrs.coinbaseHeights, p.outArrs.frozenOutputs, p.outArrs.spendableIn,
			item.options.Locked, item.options.Conflicting, item.options.Frozen, p.unminedSince,
			p.blockIDs, p.blockHeights, p.subtreeIdxs, [][]byte(nil),
		)
	}

	br := conn.SendBatch(ctx, pgxBatch)

	// Read results for each queued INSERT.
	newHashSet := make(map[chainhash.Hash]struct{}, len(validIndices))
	for _, idx := range validIndices {
		var hashBytes []byte
		err := br.QueryRow().Scan(&hashBytes)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// ON CONFLICT DO NOTHING — tx already exists.
				continue
			}
			// Real error — signal this item.
			batch[idx].done <- batchCreateResult{Err: errors.NewStorageError("failed to INSERT into utxos", err)}
			continue
		}
		var h chainhash.Hash
		copy(h[:], hashBytes)
		newHashSet[h] = struct{}{}
	}
	br.Close()

	// Signal callers.
	for _, idx := range validIndices {
		p := &prepared[idx]
		if p.txMeta == nil {
			continue // already errored during prep
		}
		_, wasNew := newHashSet[*p.txHash]
		if wasNew {
			result := s.buildCreateMeta(p.txMeta, batch[idx].options, p.isCoinbase, batch[idx].blockHeight)
			s.cache.Add(*p.txHash, result)
			batch[idx].done <- batchCreateResult{Data: result}
		} else {
			batch[idx].done <- batchCreateResult{
				Err: errors.NewTxExistsError("transaction already exists (coinbase=%v):", p.isCoinbase),
			}
		}
	}

	// Phase 3: Handle conflicting children (rare path — separate round-trips only when needed).
	for _, idx := range validIndices {
		p := &prepared[idx]
		if p.txMeta != nil && p.txMeta.Conflicting {
			if _, wasNew := newHashSet[*p.txHash]; wasNew {
				if conflictErr := s.insertConflictingChildrenDirect(ctx, conn, p.txHash, batch[idx].tx); conflictErr != nil {
					s.logger.Warnf("[sendCreateBatch] failed to insert conflicting children for %x: %v", p.txHash[:], conflictErr)
				}
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

// insertConflictingChildrenDirect updates the parent utxos rows to append this child
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
			UPDATE utxos SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
			WHERE hash = $1`,
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
