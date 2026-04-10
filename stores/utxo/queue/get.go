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
// Postgres parameter limits. v3 uses 400; we match that.
const maxINClauseSize = 400

// Get retrieves UTXO metadata for a given transaction hash.
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

	// Always fetch transaction metadata + tx_state in one JOIN query.
	var (
		version      int64
		lockTime     int64
		unminedSince *int64
	)

	err := s.pool.QueryRow(ctx, `
		SELECT t.version, t.lock_time, t.fee, t.size_in_bytes, t.coinbase,
		       ts.locked, ts.conflicting, ts.frozen, ts.unmined_since
		FROM transactions t
		JOIN tx_state ts ON ts.tx_hash = t.hash
		WHERE t.hash = $1`,
		hash[:],
	).Scan(&version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase,
		&data.Locked, &data.Conflicting, &data.Frozen, &unminedSince)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("transaction %s not found", hash, err)
		}
		return nil, err
	}

	if unminedSince != nil {
		data.UnminedSince = uint32(*unminedSince)
	}

	tx := bt.Tx{
		Version:  uint32(version),
		LockTime: uint32(lockTime),
	}

	// Fetch inputs if needed (for Tx, Inputs, TxInpoints, or Utxos fields).
	if contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos) {
		rows, err := s.pool.Query(ctx, `
			SELECT previous_transaction_hash, previous_tx_idx, previous_tx_satoshis,
			       previous_tx_script, unlocking_script, sequence_number
			FROM inputs
			WHERE tx_hash = $1
			ORDER BY idx`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			input := &bt.Input{}
			var prevTxHashBytes []byte
			var prevIdx int64

			if err := rows.Scan(&prevTxHashBytes, &prevIdx, &input.PreviousTxSatoshis,
				&input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
				return nil, err
			}
			input.PreviousTxOutIndex = uint32(prevIdx)

			prevHash, err := chainhash.NewHash(prevTxHashBytes)
			if err != nil {
				return nil, err
			}
			if err := input.PreviousTxIDAdd(prevHash); err != nil {
				return nil, err
			}

			tx.Inputs = append(tx.Inputs, input)
		}
		if err := rows.Err(); err != nil {
			return nil, err
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

	// Fetch block_ids.
	if contains(bins, fields.BlockIDs) {
		rows, err := s.pool.Query(ctx, `
			SELECT block_id, block_height, subtree_idx
			FROM block_ids
			WHERE tx_hash = $1
			ORDER BY block_id`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				blockID     uint32
				blockHeight uint32
				subtreeIdx  int
			)
			if err := rows.Scan(&blockID, &blockHeight, &subtreeIdx); err != nil {
				return nil, err
			}
			data.BlockIDs = append(data.BlockIDs, blockID)
			data.BlockHeights = append(data.BlockHeights, blockHeight)
			data.SubtreeIdxs = append(data.SubtreeIdxs, subtreeIdx)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Fetch conflicting children.
	if contains(bins, fields.ConflictingChildren) {
		rows, err := s.pool.Query(ctx, `
			SELECT child_tx_hash
			FROM conflicting_children
			WHERE tx_hash = $1`,
			hash[:],
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		data.ConflictingChildren = make([]chainhash.Hash, 0, 16)
		for rows.Next() {
			var childHashBytes []byte
			if err := rows.Scan(&childHashBytes); err != nil {
				return nil, err
			}
			data.ConflictingChildren = append(data.ConflictingChildren, chainhash.Hash(childHashBytes))
		}
		if err := rows.Err(); err != nil {
			return nil, err
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
		data.Tx = &tx
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
// It validates UTXO state by JOINing outputs + tx_state + spends.
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
		       o.frozen OR ts.frozen, o.spendable_in, ts.conflicting, ts.locked
		FROM outputs o
		JOIN tx_state ts ON ts.tx_hash = o.tx_hash
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
// Delegates to Get for each item (could be optimized with bulk queries later).
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, requestedFields ...fields.FieldName) error {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Filter out nil entries
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

	// Query 1: Bulk fetch transactions + tx_state
	inClause, inArgs := buildINClauseLocal(hashes, 1)

	q := `SELECT t.hash, t.version, t.lock_time, t.fee, t.size_in_bytes, t.coinbase,
	             ts.locked, ts.conflicting, ts.frozen, ts.unmined_since
	      FROM transactions t
	      JOIN tx_state ts ON ts.tx_hash = t.hash
	      WHERE t.hash IN ` + inClause

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
		)
		row := &txRow{data: &meta.Data{}}
		if err := rows.Scan(&hashBytes, &version, &lockTime, &row.data.Fee, &row.data.SizeInBytes, &row.data.IsCoinbase,
			&row.data.Locked, &row.data.Conflicting, &row.data.Frozen, &unminedSince); err != nil {
			rows.Close()
			return err
		}
		row.version = uint32(version)
		row.lockTime = uint32(lockTime)
		copy(row.hash[:], hashBytes)
		if unminedSince != nil {
			row.data.UnminedSince = uint32(*unminedSince)
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
	needBlockIDs := contains(bins, fields.BlockIDs)

	// Query 2: Bulk fetch inputs
	if needInputs {
		if err := s.batchDecorateInputs(ctx, txHashes, hashToTx); err != nil {
			return err
		}
	}

	// Query 3: Bulk fetch outputs
	if needOutputs {
		if err := s.batchDecorateOutputs(ctx, txHashes, hashToTx); err != nil {
			return err
		}
	}

	// Query 4: Bulk fetch block_ids
	if needBlockIDs {
		if err := s.batchDecorateBlockIDs(ctx, txHashes, hashToTx); err != nil {
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

// batchDecorateInputs bulk-fetches inputs for multiple transactions keyed by tx_hash.
func (s *Store) batchDecorateInputs(ctx context.Context, txHashes [][]byte, hashToTx map[chainhash.Hash]*txRow) error {
	inClause, args := buildINClauseLocal(txHashes, 1)

	q := `SELECT tx_hash, previous_transaction_hash, previous_tx_idx, previous_tx_satoshis,
	             previous_tx_script, unlocking_script, sequence_number
	      FROM inputs WHERE tx_hash IN ` + inClause + ` ORDER BY tx_hash, idx`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			txHashBytes     []byte
			prevTxHashBytes []byte
			prevIdx         int64
		)
		input := &bt.Input{}
		if err := rows.Scan(&txHashBytes, &prevTxHashBytes, &prevIdx, &input.PreviousTxSatoshis,
			&input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
			return err
		}
		input.PreviousTxOutIndex = uint32(prevIdx)

		prevHash, err := chainhash.NewHash(prevTxHashBytes)
		if err != nil {
			return err
		}
		if err := input.PreviousTxIDAdd(prevHash); err != nil {
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
		row.data.Tx.Inputs = append(row.data.Tx.Inputs, input)
	}
	return rows.Err()
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

// batchDecorateBlockIDs bulk-fetches block_ids for multiple transactions keyed by tx_hash.
func (s *Store) batchDecorateBlockIDs(ctx context.Context, txHashes [][]byte, hashToTx map[chainhash.Hash]*txRow) error {
	inClause, args := buildINClauseLocal(txHashes, 1)

	q := `SELECT tx_hash, block_id, block_height, subtree_idx
	      FROM block_ids WHERE tx_hash IN ` + inClause + ` ORDER BY tx_hash, block_id`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			txHashBytes []byte
			blockID     uint32
			blockHeight uint32
			subtreeIdx  int
		)
		if err := rows.Scan(&txHashBytes, &blockID, &blockHeight, &subtreeIdx); err != nil {
			return err
		}

		var h chainhash.Hash
		copy(h[:], txHashBytes)
		row := hashToTx[h]
		if row == nil {
			continue
		}
		row.data.BlockIDs = append(row.data.BlockIDs, blockID)
		row.data.BlockHeights = append(row.data.BlockHeights, blockHeight)
		row.data.SubtreeIdxs = append(row.data.SubtreeIdxs, subtreeIdx)
	}
	return rows.Err()
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs
// across multiple transactions in bulk. Chunks parent hashes into IN clauses
// (max 400 per query). Reads only from the outputs table.
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

	// Query in chunks and build results map.
	type outputKey struct {
		hash chainhash.Hash
		idx  uint32
	}
	type outputInfo struct {
		lockingScript []byte
		satoshis      uint64
	}
	results := make(map[outputKey]*outputInfo)

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
		q := `SELECT tx_hash, idx, locking_script, satoshis FROM outputs WHERE tx_hash IN ` + inClause

		rows, err := s.pool.Query(ctx, q, args...)
		if err != nil {
			return err
		}

		for rows.Next() {
			var hashBytes []byte
			var idx uint32
			var lockingScript []byte
			var satoshis uint64
			if err := rows.Scan(&hashBytes, &idx, &lockingScript, &satoshis); err != nil {
				rows.Close()
				return err
			}
			var h chainhash.Hash
			copy(h[:], hashBytes)
			results[outputKey{hash: h, idx: idx}] = &outputInfo{
				lockingScript: lockingScript,
				satoshis:      satoshis,
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
		for _, ref := range refs {
			key := outputKey{hash: parentHash, idx: ref.outIdx}
			if info, ok := results[key]; ok {
				txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxScript = bscript.NewFromBytes(info.lockingScript)
				txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxSatoshis = info.satoshis
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
// Used by batchDecorateChunk and its helper methods.
type txRow struct {
	data     *meta.Data
	version  uint32
	lockTime uint32
	hash     chainhash.Hash
}
