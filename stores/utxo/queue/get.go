package queue

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// maxINClauseSize limits the number of hashes per IN clause to avoid exceeding
// Postgres parameter limits.
const maxINClauseSize = 400

// Get retrieves UTXO metadata for a given transaction hash.
// Checks in-process cache first (populated by Create).
// The requested fields control which additional queries are executed.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	return s.getInternal(ctx, hash, bins)
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

	// Single SELECT from txs — all metadata, state, raw_tx, arrays, and output arrays.
	var (
		version             int64
		lockTime            int64
		unminedSince        *int64
		rawTx               []byte
		blockIDs            []int32
		blockHeights        []int32
		subtreeIdxs         []int32
		conflictingChildren [][]byte
		lockingScripts      [][]byte
		satoshisArr         []int64
		frozenOutputs       []bool
	)

	err := s.pool.QueryRow(ctx, `
		SELECT version, lock_time, fee, size_in_bytes, coinbase,
		       locked, conflicting, frozen, unmined_since, raw_tx,
		       block_ids, block_heights, subtree_idxs, conflicting_children,
		       locking_scripts, satoshis_arr, frozen_outputs
		FROM txs WHERE hash = $1`,
		hash[:],
	).Scan(&version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase,
		&data.Locked, &data.Conflicting, &data.Frozen, &unminedSince, &rawTx,
		&blockIDs, &blockHeights, &subtreeIdxs, &conflictingChildren,
		&lockingScripts, &satoshisArr, &frozenOutputs)
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

	// Reconstruct outputs from txs arrays (v7: no separate outputs table).
	if contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos) {
		tx.Outputs = make([]*bt.Output, len(lockingScripts))
		for i := range lockingScripts {
			output := &bt.Output{}
			if lockingScripts[i] != nil {
				ls := bscript.Script(lockingScripts[i])
				output.LockingScript = &ls
			}
			if i < len(satoshisArr) {
				output.Satoshis = uint64(satoshisArr[i])
			}
			tx.Outputs[i] = output
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

	// Build UTXOs with spend status from spends table (v7: no outputs table JOIN).
	if contains(bins, fields.Utxos) {
		numOutputs := len(lockingScripts)
		data.SpendingDatas = make([]*spendpkg.SpendingData, numOutputs)

		// Query spends for this tx.
		rows, err := s.pool.Query(ctx, `
			SELECT prev_output_idx, spending_data
			FROM spends
			WHERE prev_tx_hash = $1
			ORDER BY prev_output_idx`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		spendMap := make(map[int][]byte)
		for rows.Next() {
			var idx int
			var spendingDataBytes []byte
			if err := rows.Scan(&idx, &spendingDataBytes); err != nil {
				return nil, err
			}
			spendMap[idx] = spendingDataBytes
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		for i := 0; i < numOutputs; i++ {
			outputFrozen := i < len(frozenOutputs) && frozenOutputs[i]
			if data.Frozen || outputFrozen {
				data.SpendingDatas[i] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, i)
			} else if spendBytes, ok := spendMap[i]; ok && spendBytes != nil {
				sd, err := spendpkg.NewSpendingDataFromBytes(spendBytes)
				if err != nil {
					return nil, errors.NewProcessingError("failed to parse spending data", err)
				}
				data.SpendingDatas[i] = sd
			}
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
// v7: reads output data from txs arrays, JOINs only txs + spends.
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
		SELECT t.utxo_hashes[$2+1],
		       COALESCE(t.coinbase_heights[$2+1], 0)::int,
		       sp.spending_data,
		       t.frozen OR COALESCE(t.frozen_outputs[$2+1], false),
		       t.spendable_in_arr[$2+1],
		       t.conflicting, t.locked
		FROM txs t
		LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
		WHERE t.hash = $1 AND t.utxo_hashes[$2+1] IS NOT NULL`,
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
	// v7: also fetch output arrays for output reconstruction.
	inClause, inArgs := buildINClauseLocal(hashes, 1)

	q := `SELECT hash, version, lock_time, fee, size_in_bytes, coinbase,
	             locked, conflicting, frozen, unmined_since, raw_tx,
	             block_ids, block_heights, subtree_idxs,
	             locking_scripts, satoshis_arr
	      FROM txs
	      WHERE hash IN ` + inClause

	rows, err := s.pool.Query(ctx, q, inArgs...)
	if err != nil {
		return err
	}

	hashToTx := make(map[chainhash.Hash]*txRow, len(items))

	for rows.Next() {
		var (
			hashBytes      []byte
			unminedSince   *int64
			version        int64
			lockTime       int64
			rawTx          []byte
			blockIDs       []int32
			blkHeights     []int32
			subIdxs        []int32
			lockingScripts [][]byte
			satoshisArr    []int64
		)
		row := &txRow{data: &meta.Data{}}
		if err := rows.Scan(&hashBytes, &version, &lockTime, &row.data.Fee, &row.data.SizeInBytes, &row.data.IsCoinbase,
			&row.data.Locked, &row.data.Conflicting, &row.data.Frozen, &unminedSince, &rawTx,
			&blockIDs, &blkHeights, &subIdxs,
			&lockingScripts, &satoshisArr); err != nil {
			rows.Close()
			return err
		}
		// Store output array data for later assembly.
		row.lockingScripts = lockingScripts
		row.satoshisArr = satoshisArr
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

	// v7: Reconstruct outputs from arrays already fetched in Query 1.
	if needOutputs {
		for _, row := range hashToTx {
			if len(row.lockingScripts) > 0 {
				if row.data.Tx == nil {
					row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
				}
				row.data.Tx.Outputs = make([]*bt.Output, len(row.lockingScripts))
				for i := range row.lockingScripts {
					output := &bt.Output{}
					if row.lockingScripts[i] != nil {
						ls := bscript.Script(row.lockingScripts[i])
						output.LockingScript = &ls
					}
					if i < len(row.satoshisArr) {
						output.Satoshis = uint64(row.satoshisArr[i])
					}
					row.data.Tx.Outputs[i] = output
				}
			}
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

	// Collect unique parent hashes for chunked IN query.
	parentHashes := make([][]byte, 0, len(needsByParent))
	for h := range needsByParent {
		hCopy := h
		parentHashes = append(parentHashes, hCopy[:])
	}

	// v7: Bulk fetch locking_scripts and satoshis_arr from txs arrays.
	type txOutputArrays struct {
		lockingScripts [][]byte
		satoshisArr    []int64
	}
	parentOutputs := make(map[chainhash.Hash]*txOutputArrays)

	for chunkStart := 0; chunkStart < len(parentHashes); chunkStart += maxINClauseSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkEnd := chunkStart + maxINClauseSize
		if chunkEnd > len(parentHashes) {
			chunkEnd = len(parentHashes)
		}
		chunk := parentHashes[chunkStart:chunkEnd]

		inClause, args := buildINClauseLocal(chunk, 1)
		q := `SELECT hash, locking_scripts, satoshis_arr FROM txs WHERE hash IN ` + inClause

		rows, err := s.pool.Query(ctx, q, args...)
		if err != nil {
			return err
		}

		for rows.Next() {
			var hashBytes []byte
			var lockingScripts [][]byte
			var satoshisArr []int64
			if err := rows.Scan(&hashBytes, &lockingScripts, &satoshisArr); err != nil {
				rows.Close()
				return err
			}
			var h chainhash.Hash
			copy(h[:], hashBytes)
			parentOutputs[h] = &txOutputArrays{
				lockingScripts: lockingScripts,
				satoshisArr:    satoshisArr,
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	// Map results back to inputs and track missing.
	var missingInputs []string
	for parentHash, refs := range needsByParent {
		po, found := parentOutputs[parentHash]
		for _, ref := range refs {
			if found && int(ref.outIdx) < len(po.lockingScripts) {
				txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxScript = bscript.NewFromBytes(po.lockingScripts[ref.outIdx])
				if int(ref.outIdx) < len(po.satoshisArr) {
					txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxSatoshis = uint64(po.satoshisArr[ref.outIdx])
				}
			} else {
				missingInputs = append(missingInputs, fmt.Sprintf("tx[%d].input[%d] parent=%x vout=%d",
					ref.txIdx, ref.inputIdx, parentHash[:], ref.outIdx))
			}
		}
	}

	if len(missingInputs) > 0 {
		s.logger.Warnf("[BatchPreviousOutputsDecorate] missing parent outputs: %s", strings.Join(missingInputs, ", "))
		return errors.NewProcessingError("failed to decorate previous outputs: %d missing", len(missingInputs))
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
	data           *meta.Data
	version        uint32
	lockTime       uint32
	hash           chainhash.Hash
	rawTx          []byte
	lockingScripts [][]byte
	satoshisArr    []int64
}
