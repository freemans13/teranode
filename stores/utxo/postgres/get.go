package postgres

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// maxINClauseSize limits the number of hashes per IN clause to avoid exceeding
// Postgres parameter limits.
const maxINClauseSize = 400

// minDecorateChunkSize is the floor on the per-chunk hash count when
// BatchPreviousOutputsDecorate splits work across concurrent queries. Chunks
// smaller than this add round-trip/planning overhead that outweighs the extra
// parallelism, so very small blocks stay in fewer chunks.
const minDecorateChunkSize = 50

// batchGetItem represents a single Get request queued into the batcher.
type batchGetItem struct {
	hash *chainhash.Hash
	bins []fields.FieldName
	done chan batchGetResult
}

type batchGetResult struct {
	Data *meta.Data
	Err  error
}

// Get retrieves UTXO metadata for a given transaction hash.
// When getBatcher is active, simple gets are batched for throughput.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Use batcher for simple metadata-only gets (BlockIDs, BlockHeights — no Tx body).
	if s.getBatcher != nil && !contains(bins, fields.Tx) && !contains(bins, fields.Outputs) &&
		!contains(bins, fields.Utxos) && !contains(bins, fields.TxInpoints) && !contains(bins, fields.Inputs) {
		done := make(chan batchGetResult, 1)
		s.getBatcher.Put(&batchGetItem{hash: hash, bins: bins, done: done})
		select {
		case result := <-done:
			return result.Data, result.Err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return s.getInternal(ctx, hash, bins)
}

// sendGetBatch pipelines N SELECT queries via SendBatch.
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	s.batchStats.getItems.Add(int64(len(batch)))
	s.batchStats.getBatches.Add(1)
	ctx := context.Background()

	// Single-item fast path: direct query, no SendBatch overhead.
	if len(batch) == 1 {
		result, err := s.getInternal(ctx, batch[0].hash, batch[0].bins)
		batch[0].done <- batchGetResult{Data: result, Err: err}
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.done <- batchGetResult{Err: errors.NewStorageError("[Get] acquire: %v", err)}
		}
		return
	}
	defer conn.Release()

	pgxBatch := &pgx.Batch{}
	for _, item := range batch {
		pgxBatch.Queue(`
			SELECT version, lock_time, fee, size_in_bytes, coinbase,
			       locked, conflicting, frozen, unmined_since,
			       block_ids, block_heights, subtree_idxs
			FROM txs WHERE hash = $1`,
			item.hash[:],
		)
	}

	br := conn.SendBatch(ctx, pgxBatch)

	for _, item := range batch {
		data := &meta.Data{}
		var version, lockTime int64
		var unminedSince *int64
		var blockIDs, blockHeights, subtreeIdxs []int32

		err := br.QueryRow().Scan(&version, &lockTime, &data.Fee, &data.SizeInBytes,
			&data.IsCoinbase, &data.Locked, &data.Conflicting, &data.Frozen,
			&unminedSince, &blockIDs, &blockHeights, &subtreeIdxs)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				item.done <- batchGetResult{Err: errors.NewTxNotFoundError("transaction %s not found", item.hash)}
			} else {
				item.done <- batchGetResult{Err: err}
			}
			continue
		}

		if unminedSince != nil {
			data.UnminedSince = uint32(*unminedSince)
		}
		if len(blockIDs) > 0 {
			data.BlockIDs = make([]uint32, len(blockIDs))
			data.BlockHeights = make([]uint32, len(blockHeights))
			data.SubtreeIdxs = make([]int, len(subtreeIdxs))
			for i := range blockIDs {
				data.BlockIDs[i] = uint32(blockIDs[i])
				if i < len(blockHeights) {
					data.BlockHeights[i] = uint32(blockHeights[i])
				}
				if i < len(subtreeIdxs) {
					data.SubtreeIdxs[i] = int(subtreeIdxs[i])
				}
			}
		}

		item.done <- batchGetResult{Data: data}
	}

	br.Close()
}

// GetMeta retrieves only the metadata for a transaction (no Tx body).
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := s.getInternal(ctx, hash, utxo.MetaFields)
	if err != nil {
		return err
	}
	if result != nil {
		*data = *result
	}
	return nil
}

// getInternal is the shared get implementation used by Get and GetMeta.
func (s *Store) getInternal(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	data := &meta.Data{}

	// Single SELECT from txs — all metadata, state, raw_tx, and arrays.
	var (
		version             int64
		lockTime            int64
		unminedSince        *int64
		rawTx               []byte
		blockIDs            []int32
		blockHeights        []int32
		subtreeIdxs         []int32
		conflictingChildren [][]byte
	)

	err := s.pool.QueryRow(ctx, `
		SELECT version, lock_time, fee, size_in_bytes, coinbase,
		       locked, conflicting, frozen, unmined_since, raw_tx,
		       block_ids, block_heights, subtree_idxs, conflicting_children
		FROM txs WHERE hash = $1`,
		hash[:],
	).Scan(&version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase,
		&data.Locked, &data.Conflicting, &data.Frozen, &unminedSince, &rawTx,
		&blockIDs, &blockHeights, &subtreeIdxs, &conflictingChildren)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("transaction %s not found", hash, err)
		}
		return nil, err
	}

	if unminedSince != nil {
		data.UnminedSince = uint32(*unminedSince)
	}

	// Deserialize raw_tx for Tx/Inputs/TxInpoints fields.
	var tx *bt.Tx
	if rawTx != nil && (contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos)) {
		tx, err = bt.NewTxFromBytes(rawTx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to deserialize raw_tx", err)
		}
	} else {
		// Create a minimal Tx with version/locktime for output assembly.
		tx = &bt.Tx{
			Version:  uint32(version),
			LockTime: uint32(lockTime),
		}
	}

	// Fetch outputs for Tx reconstruction (locking_script, satoshis).
	if contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos) {
		rows, err := s.pool.Query(ctx, `
			SELECT idx, locking_script, satoshis
			FROM outputs
			WHERE tx_hash = $1
			ORDER BY idx`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		// Replace the outputs from raw_tx with the authoritative outputs table data.
		tx.Outputs = nil
		for rows.Next() {
			var idx int64
			output := &bt.Output{}
			if err := rows.Scan(&idx, &output.LockingScript, &output.Satoshis); err != nil {
				return nil, err
			}
			tx.Outputs = append(tx.Outputs, output)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Fetch block_ids from arrays.
	if contains(bins, fields.BlockIDs) && len(blockIDs) > 0 {
		data.BlockIDs = make([]uint32, len(blockIDs))
		data.BlockHeights = make([]uint32, len(blockHeights))
		data.SubtreeIdxs = make([]int, len(subtreeIdxs))
		for i := range blockIDs {
			data.BlockIDs[i] = uint32(blockIDs[i])
			if i < len(blockHeights) {
				data.BlockHeights[i] = uint32(blockHeights[i])
			}
			if i < len(subtreeIdxs) {
				data.SubtreeIdxs[i] = int(subtreeIdxs[i])
			}
		}
	}

	// Fetch conflicting children from array.
	if contains(bins, fields.ConflictingChildren) && len(conflictingChildren) > 0 {
		data.ConflictingChildren = make([]chainhash.Hash, 0, len(conflictingChildren))
		for _, childBytes := range conflictingChildren {
			if len(childBytes) == chainhash.HashSize {
				data.ConflictingChildren = append(data.ConflictingChildren, chainhash.Hash(childBytes))
			}
		}
	}

	// Fetch UTXOs with spend status from spends table.
	if contains(bins, fields.Utxos) {
		rows, err := s.pool.Query(ctx, `
			SELECT o.idx, sp.spending_data, o.frozen
			FROM outputs o
			LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
			WHERE o.tx_hash = $1
			ORDER BY o.idx`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		data.SpendingDatas = make([]*spendpkg.SpendingData, len(tx.Outputs))
		for rows.Next() {
			var (
				idx               int
				spendingDataBytes []byte
				frozen            bool
			)
			if err := rows.Scan(&idx, &spendingDataBytes, &frozen); err != nil {
				return nil, err
			}

			if data.Frozen || frozen {
				data.SpendingDatas[idx] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, idx)
			} else if spendingDataBytes != nil {
				sd, err := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
				if err != nil {
					return nil, errors.NewProcessingError("failed to parse spending data", err)
				}
				data.SpendingDatas[idx] = sd
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Attach Tx to meta if requested.
	if contains(bins, fields.Tx) {
		data.Tx = tx
	}

	// Build TxInpoints from inputs.
	if contains(bins, fields.TxInpoints) {
		var err error
		data.TxInpoints, err = subtree.NewTxInpointsFromInputs(tx.Inputs)
		if err != nil {
			return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
		}
	}

	return data, nil
}

// GetSpend retrieves the spend status for a specific UTXO.
// It validates UTXO state by JOINing outputs + txs + spends.
func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	var (
		utxoHashBytes          []byte
		coinbaseSpendingHeight uint32
		spendingDataBytes      []byte
		frozen                 bool
		spendableIn            *uint32
		conflicting            bool
		locked                 bool
	)

	err := s.pool.QueryRow(ctx, `
		SELECT o.utxo_hash, o.coinbase_spending_height, sp.spending_data,
		       o.frozen OR t.frozen, o.spendable_in, t.conflicting, t.locked
		FROM outputs o
		JOIN txs t ON t.hash = o.tx_hash
		LEFT JOIN spends sp ON sp.prev_tx_hash = o.tx_hash AND sp.prev_output_idx = o.idx
		WHERE o.tx_hash = $1 AND o.idx = $2`,
		spend.TxID[:], spend.Vout,
	).Scan(&utxoHashBytes, &coinbaseSpendingHeight, &spendingDataBytes, &frozen, &spendableIn, &conflicting, &locked)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &utxo.SpendResponse{
				Status: int(utxo.Status_NOT_FOUND),
			}, nil
		}
		return nil, err
	}

	// Validate UTXO hash matches.
	if !bytes.Equal(utxoHashBytes, spend.UTXOHash[:]) {
		return nil, errors.NewUtxoHashMismatchError("utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	var spendingData *spendpkg.SpendingData
	if len(spendingDataBytes) > 0 {
		spendingData, err = spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
		if err != nil {
			return nil, err
		}
	}

	utxoStatus := utxo.CalculateUtxoStatus(spendingData, coinbaseSpendingHeight, s.blockHeight.Load())

	if frozen {
		utxoStatus = utxo.Status_FROZEN
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(spend.Vout))
	}
	if conflicting {
		utxoStatus = utxo.Status_CONFLICTING
	}
	if locked {
		utxoStatus = utxo.Status_LOCKED
	}
	if spendableIn != nil && s.GetBlockHeight() < *spendableIn {
		utxoStatus = utxo.Status_IMMATURE
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
		LockTime:     coinbaseSpendingHeight,
	}, nil
}

// BatchDecorate efficiently fetches metadata for multiple transactions.
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, requestedFields ...fields.FieldName) error {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Filter out nil entries.
	items := make([]*utxo.UnresolvedMetaData, 0, len(unresolvedMetaDataSlice))
	for _, item := range unresolvedMetaDataSlice {
		if item != nil {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return nil
	}

	// Process in chunks.
	for i := 0; i < len(items); i += maxINClauseSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + maxINClauseSize
		if end > len(items) {
			end = len(items)
		}

		if err := s.batchDecorateChunk(ctx, items[i:end], bins); err != nil {
			return err
		}
	}

	return nil
}

// batchDecorateChunk fetches metadata for a chunk of transactions using bulk queries.
func (s *Store) batchDecorateChunk(ctx context.Context, items []*utxo.UnresolvedMetaData, bins []fields.FieldName) error {
	// Build hash list and mapping.
	hashes := make([][]byte, len(items))
	hashToItems := make(map[chainhash.Hash][]*utxo.UnresolvedMetaData, len(items))
	for i, item := range items {
		hashes[i] = item.Hash[:]
		hashToItems[item.Hash] = append(hashToItems[item.Hash], item)
	}

	// Query 1: Bulk fetch from txs (single table, no JOIN).
	inClause, inArgs := buildINClauseLocal(hashes, 1)

	q := `SELECT hash, version, lock_time, fee, size_in_bytes, coinbase,
	             locked, conflicting, frozen, unmined_since, raw_tx,
	             block_ids, block_heights, subtree_idxs
	      FROM txs
	      WHERE hash IN ` + inClause

	rows, err := s.pool.Query(ctx, q, inArgs...)
	if err != nil {
		return err
	}

	hashToTx := make(map[chainhash.Hash]*txRow, len(items))

	for rows.Next() {
		var (
			hashBytes    []byte
			unminedSince *int64
			version      int64
			lockTime     int64
			rawTx        []byte
			blockIDs     []int32
			blkHeights   []int32
			subIdxs      []int32
		)
		row := &txRow{data: &meta.Data{}}
		if err := rows.Scan(&hashBytes, &version, &lockTime, &row.data.Fee, &row.data.SizeInBytes, &row.data.IsCoinbase,
			&row.data.Locked, &row.data.Conflicting, &row.data.Frozen, &unminedSince, &rawTx,
			&blockIDs, &blkHeights, &subIdxs); err != nil {
			rows.Close()
			return err
		}
		row.version = uint32(version)
		row.lockTime = uint32(lockTime)
		row.rawTx = rawTx
		copy(row.hash[:], hashBytes)
		if unminedSince != nil {
			row.data.UnminedSince = uint32(*unminedSince)
		}
		// Store block_ids on the data object.
		if len(blockIDs) > 0 {
			row.data.BlockIDs = make([]uint32, len(blockIDs))
			row.data.BlockHeights = make([]uint32, len(blkHeights))
			row.data.SubtreeIdxs = make([]int, len(subIdxs))
			for i := range blockIDs {
				row.data.BlockIDs[i] = uint32(blockIDs[i])
				if i < len(blkHeights) {
					row.data.BlockHeights[i] = uint32(blkHeights[i])
				}
				if i < len(subIdxs) {
					row.data.SubtreeIdxs[i] = int(subIdxs[i])
				}
			}
		}
		hashToTx[row.hash] = row
	}
	rows.Close()

	// Mark not-found transactions.
	for _, item := range items {
		if _, found := hashToTx[item.Hash]; !found {
			item.Err = errors.NewTxNotFoundError("transaction %s not found", &item.Hash)
		}
	}

	if len(hashToTx) == 0 {
		return nil
	}

	// Collect tx hashes for subsequent queries.
	txHashes := make([][]byte, 0, len(hashToTx))
	for h := range hashToTx {
		hCopy := h
		txHashes = append(txHashes, hCopy[:])
	}

	needInputs := contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos)
	needOutputs := contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos)

	// Query 2: Deserialize raw_tx for inputs.
	if needInputs {
		for _, row := range hashToTx {
			if row.rawTx != nil {
				parsedTx, parseErr := bt.NewTxFromBytes(row.rawTx)
				if parseErr == nil {
					if row.data.Tx == nil {
						row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
					}
					row.data.Tx.Inputs = parsedTx.Inputs
				}
			}
		}
	}

	// Query 3: Bulk fetch outputs.
	if needOutputs {
		if err := s.batchDecorateOutputs(ctx, txHashes, hashToTx); err != nil {
			return err
		}
	}

	// Assemble results.
	for hash, matchedItems := range hashToItems {
		row, found := hashToTx[hash]
		if !found {
			continue
		}

		var tx *bt.Tx
		if contains(bins, fields.Tx) || contains(bins, fields.TxInpoints) {
			tx = &bt.Tx{
				Version:  row.version,
				LockTime: row.lockTime,
			}
			if needInputs && row.data.Tx != nil {
				tx.Inputs = row.data.Tx.Inputs
			}
			if needOutputs && row.data.Tx != nil {
				tx.Outputs = row.data.Tx.Outputs
			}
		}

		if contains(bins, fields.TxInpoints) && row.data.Tx != nil && len(row.data.Tx.Inputs) > 0 {
			row.data.TxInpoints, _ = subtree.NewTxInpointsFromInputs(row.data.Tx.Inputs)
		}

		if contains(bins, fields.Tx) || needInputs || needOutputs {
			row.data.Tx = tx
		} else {
			row.data.Tx = nil
		}

		for _, item := range matchedItems {
			item.Data = row.data
		}
	}

	return nil
}

// batchDecorateOutputs bulk-fetches outputs for multiple transactions keyed by tx_hash.
func (s *Store) batchDecorateOutputs(ctx context.Context, txHashes [][]byte, hashToTx map[chainhash.Hash]*txRow) error {
	inClause, args := buildINClauseLocal(txHashes, 1)

	q := `SELECT tx_hash, locking_script, satoshis
	      FROM outputs WHERE tx_hash IN ` + inClause + ` ORDER BY tx_hash, idx`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var txHashBytes []byte
		output := &bt.Output{}
		if err := rows.Scan(&txHashBytes, &output.LockingScript, &output.Satoshis); err != nil {
			return err
		}

		var h chainhash.Hash
		copy(h[:], txHashBytes)
		row := hashToTx[h]
		if row == nil {
			continue
		}
		if row.data.Tx == nil {
			row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
		}
		row.data.Tx.Outputs = append(row.data.Tx.Outputs, output)
	}
	return rows.Err()
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs
// across multiple transactions in bulk.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	if len(txs) == 0 {
		return nil
	}

	// Collect all (parentTxHash, outputIdx) pairs that need decoration.
	type inputRef struct {
		txIdx    int
		inputIdx int
		outIdx   uint32
	}
	needsByParent := make(map[chainhash.Hash][]inputRef)

	for txIdx, tx := range txs {
		if tx == nil {
			continue
		}
		for inputIdx, input := range tx.Inputs {
			if input == nil || input.PreviousTxScript != nil {
				continue // already decorated or nil
			}
			parentHash := *input.PreviousTxIDChainHash()
			needsByParent[parentHash] = append(needsByParent[parentHash], inputRef{
				txIdx:    txIdx,
				inputIdx: inputIdx,
				outIdx:   input.PreviousTxOutIndex,
			})
		}
	}

	if len(needsByParent) == 0 {
		return nil
	}

	// Collect unique parent hashes for chunked IN queries.
	parentHashes := make([][]byte, 0, len(needsByParent))
	for h := range needsByParent {
		hCopy := h
		parentHashes = append(parentHashes, hCopy[:])
	}

	// Parallelise chunk queries when configured. Each chunk fetches a disjoint
	// set of parent hashes, and the input slots a chunk writes to are disjoint
	// across chunks by construction (different parents → different inputRefs →
	// different tx.Inputs[] elements), so workers write directly without shared
	// state. needsByParent is read-only after construction. missingInputs is the
	// only cross-worker counter and uses atomic.Int64.
	//
	// During legacy IBD this chunk loop is the dominant per-block cost: ~3 s of
	// serial single-backend lookups into the 84M-row outputs heap at disk QD~1.
	// Running chunks concurrently overlaps the per-query round-trips and lifts
	// the SSD's queue depth, which is the actual IBD bottleneck on modest HW.
	concurrency := s.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	// Size chunks so the work splits into ~concurrency parallel queries (raising
	// DB/disk queue depth), bounded by the IN-clause cap and a sane minimum.
	// Serial behaviour (concurrency=1) keeps the full IN-clause cap per query.
	chunkSize := maxINClauseSize
	if concurrency > 1 && len(parentHashes) > 0 {
		chunkSize = (len(parentHashes) + concurrency - 1) / concurrency
		if chunkSize < minDecorateChunkSize {
			chunkSize = minDecorateChunkSize
		}
		if chunkSize > maxINClauseSize {
			chunkSize = maxINClauseSize
		}
	}

	var missingInputs atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	for chunkStart := 0; chunkStart < len(parentHashes); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(parentHashes) {
			chunkEnd = len(parentHashes)
		}
		chunk := parentHashes[chunkStart:chunkEnd]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			inClause, args := buildINClauseLocal(chunk, 1)
			q := `SELECT tx_hash, idx, locking_script, satoshis FROM outputs WHERE tx_hash IN ` + inClause

			rows, err := s.pool.Query(gCtx, q, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			// Track which (parentHash, outIdx) pairs this chunk resolved so we
			// can count the unresolved ones afterwards.
			type foundKey struct {
				hash chainhash.Hash
				idx  uint32
			}
			found := make(map[foundKey]struct{}, len(chunk))

			for rows.Next() {
				var hashBytes []byte
				var idx uint32
				var lockingScript []byte
				var satoshis uint64
				if err := rows.Scan(&hashBytes, &idx, &lockingScript, &satoshis); err != nil {
					return err
				}
				var h chainhash.Hash
				copy(h[:], hashBytes)
				// Dispatch this output to every input that needs (h, idx). The
				// refs for h live only in this chunk (disjoint hashes), so these
				// writes never race another worker.
				for _, ref := range needsByParent[h] {
					if ref.outIdx == idx {
						txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxScript = bscript.NewFromBytes(lockingScript)
						txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxSatoshis = satoshis
					}
				}
				found[foundKey{hash: h, idx: idx}] = struct{}{}
			}
			if err := rows.Err(); err != nil {
				return err
			}

			// Count input slots in this chunk left unresolved.
			var localMissing int64
			for _, hb := range chunk {
				var h chainhash.Hash
				copy(h[:], hb)
				for _, ref := range needsByParent[h] {
					if _, ok := found[foundKey{hash: h, idx: ref.outIdx}]; !ok {
						localMissing++
					}
				}
			}
			if localMissing > 0 {
				missingInputs.Add(localMissing)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if m := missingInputs.Load(); m > 0 {
		return errors.NewProcessingError("failed to decorate previous outputs: %d missing", m)
	}

	return nil
}

// contains checks if a slice of FieldName contains a specific item.
func contains(slice []fields.FieldName, item fields.FieldName) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// txRow holds intermediate results for a single transaction during bulk fetch.
type txRow struct {
	data     *meta.Data
	version  uint32
	lockTime uint32
	hash     chainhash.Hash
	rawTx    []byte
}
