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
)

// megaTxThreshold is the number of inputs or outputs above which CopyFrom is
// used instead of unnest for bulk insertion.
const megaTxThreshold = 100

// Create stores a new transaction's outputs as UTXOs using direct INSERTs
// into the v4 snapshot tables within a single pgx transaction.
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

	// Compute tx metadata (fee, sizeInBytes, txInpoints, etc.)
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

	// Determine tx hash (with optional override).
	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	// Determine coinbase flag (with optional override).
	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	// unminedSince: nil if mined, blockHeight if unmined.
	var unminedSince *int64
	if len(options.MinedBlockInfos) == 0 {
		v := int64(blockHeight)
		unminedSince = &v
	}

	// coinbaseSpendingHeight: non-zero only for coinbase txs.
	var coinbaseSpendingHeight int64
	if isCoinbase {
		coinbaseSpendingHeight = int64(blockHeight) + int64(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	// Begin a pgx transaction for atomic multi-table insert.
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to begin transaction", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// 1. INSERT INTO transactions ON CONFLICT DO NOTHING
	tag, err := pgxTx.Exec(ctx, `
		INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, coinbase)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (hash) DO NOTHING`,
		txHash[:], int64(tx.Version), int64(tx.LockTime), int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase,
	)
	if err != nil {
		return nil, errors.NewStorageError("failed to insert transaction", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
	}

	// 2. INSERT INTO tx_state ON CONFLICT DO NOTHING
	if _, err = pgxTx.Exec(ctx, `
		INSERT INTO tx_state (tx_hash, locked, conflicting, frozen, unmined_since)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tx_hash) DO NOTHING`,
		txHash[:], options.Locked, options.Conflicting, options.Frozen, unminedSince,
	); err != nil {
		return nil, errors.NewStorageError("failed to insert tx_state", err)
	}

	// 3. INSERT INTO inputs
	if len(tx.Inputs) > 0 {
		if len(tx.Inputs) > megaTxThreshold {
			if err = s.createInputsCopy(ctx, pgxTx, txHash, tx); err != nil {
				return nil, err
			}
		} else {
			if err = s.createInputsUnnest(ctx, pgxTx, txHash, tx); err != nil {
				return nil, err
			}
		}
	}

	// 4. INSERT INTO outputs
	nonNilOutputs := countNonNilOutputs(tx)
	if nonNilOutputs > 0 {
		if nonNilOutputs > megaTxThreshold {
			if err = s.createOutputsCopy(ctx, pgxTx, txHash, tx, isCoinbase, coinbaseSpendingHeight); err != nil {
				return nil, err
			}
		} else {
			if err = s.createOutputsUnnest(ctx, pgxTx, txHash, tx, isCoinbase, coinbaseSpendingHeight); err != nil {
				return nil, err
			}
		}
	}

	// 5. INSERT INTO block_ids (only if mined)
	if len(options.MinedBlockInfos) > 0 {
		if err = s.createBlockIDs(ctx, pgxTx, txHash, options.MinedBlockInfos); err != nil {
			return nil, err
		}
	}

	// 6. Handle conflicting children (rare path)
	if txMeta.Conflicting {
		if err = s.insertConflictingChildren(ctx, pgxTx, txHash, tx); err != nil {
			return nil, err
		}
	}

	if err = pgxTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("failed to commit create transaction", err)
	}

	// Set metadata fields from what we computed.
	txMeta.IsCoinbase = isCoinbase
	txMeta.Frozen = options.Frozen
	if unminedSince != nil {
		txMeta.UnminedSince = uint32(*unminedSince)
	}

	// Populate block IDs in meta if mined.
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

	return txMeta, nil
}

// createInputsUnnest inserts inputs via unnest arrays for small/medium transactions.
func (s *Store) createInputsUnnest(ctx context.Context, pgxTx pgx.Tx, txHash *chainhash.Hash, tx *bt.Tx) error {
	n := len(tx.Inputs)
	txHashes := make([][]byte, n)
	idxs := make([]int64, n)
	prevHashes := make([][]byte, n)
	prevIdxs := make([]int64, n)
	prevSatoshis := make([]int64, n)
	prevScripts := make([][]byte, n)
	unlockScripts := make([][]byte, n)
	seqNums := make([]int64, n)

	for i, input := range tx.Inputs {
		txHashes[i] = txHash[:]
		idxs[i] = int64(i)
		prevHashes[i] = input.PreviousTxIDChainHash()[:]
		prevIdxs[i] = int64(input.PreviousTxOutIndex)
		prevSatoshis[i] = int64(input.PreviousTxSatoshis)
		if input.PreviousTxScript != nil {
			prevScripts[i] = []byte(*input.PreviousTxScript)
		}
		if input.UnlockingScript != nil {
			unlockScripts[i] = []byte(*input.UnlockingScript)
		}
		seqNums[i] = int64(input.SequenceNumber)
	}

	_, err := pgxTx.Exec(ctx, `
		INSERT INTO inputs (tx_hash, idx, previous_transaction_hash, previous_tx_idx, previous_tx_satoshis, previous_tx_script, unlocking_script, sequence_number)
		SELECT * FROM unnest($1::bytea[], $2::bigint[], $3::bytea[], $4::bigint[], $5::bigint[], $6::bytea[], $7::bytea[], $8::bigint[])
		ON CONFLICT DO NOTHING`,
		txHashes, idxs, prevHashes, prevIdxs, prevSatoshis, prevScripts, unlockScripts, seqNums,
	)
	if err != nil {
		return errors.NewStorageError("failed to insert inputs via unnest", err)
	}
	return nil
}

// createInputsCopy inserts inputs via pgx.CopyFrom for mega transactions.
func (s *Store) createInputsCopy(ctx context.Context, pgxTx pgx.Tx, txHash *chainhash.Hash, tx *bt.Tx) error {
	rows := make([][]interface{}, 0, len(tx.Inputs))
	for i, input := range tx.Inputs {
		var prevScript []byte
		if input.PreviousTxScript != nil {
			prevScript = []byte(*input.PreviousTxScript)
		}
		var unlockScript []byte
		if input.UnlockingScript != nil {
			unlockScript = []byte(*input.UnlockingScript)
		}
		rows = append(rows, []interface{}{
			txHash[:],
			int64(i),
			input.PreviousTxIDChainHash()[:],
			int64(input.PreviousTxOutIndex),
			int64(input.PreviousTxSatoshis),
			prevScript,
			unlockScript,
			int64(input.SequenceNumber),
		})
	}

	_, err := pgxTx.CopyFrom(ctx,
		pgx.Identifier{"inputs"},
		[]string{"tx_hash", "idx", "previous_transaction_hash", "previous_tx_idx", "previous_tx_satoshis", "previous_tx_script", "unlocking_script", "sequence_number"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return errors.NewStorageError("failed to copy inputs", err)
	}
	return nil
}

// createOutputsUnnest inserts outputs via unnest arrays for small/medium transactions.
func (s *Store) createOutputsUnnest(ctx context.Context, pgxTx pgx.Tx, txHash *chainhash.Hash, tx *bt.Tx, isCoinbase bool, coinbaseSpendingHeight int64) error {
	count := countNonNilOutputs(tx)

	txHashes := make([][]byte, 0, count)
	idxs := make([]int64, 0, count)
	lockingScripts := make([][]byte, 0, count)
	satoshis := make([]int64, 0, count)
	frozenSlice := make([]bool, 0, count)
	utxoHashes := make([][]byte, 0, count)
	cshSlice := make([]int64, 0, count)

	for i, output := range tx.Outputs {
		if output == nil {
			continue
		}

		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return err
		}

		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return err
		}

		txHashes = append(txHashes, txHash[:])
		idxs = append(idxs, int64(i))
		if output.LockingScript != nil {
			lockingScripts = append(lockingScripts, []byte(*output.LockingScript))
		} else {
			lockingScripts = append(lockingScripts, nil)
		}
		satoshis = append(satoshis, int64(output.Satoshis))
		frozenSlice = append(frozenSlice, false)
		utxoHashes = append(utxoHashes, utxoHash[:])
		cshSlice = append(cshSlice, coinbaseSpendingHeight)
	}

	_, err := pgxTx.Exec(ctx, `
		INSERT INTO outputs (tx_hash, idx, locking_script, satoshis, frozen, utxo_hash, coinbase_spending_height)
		SELECT * FROM unnest($1::bytea[], $2::bigint[], $3::bytea[], $4::bigint[], $5::boolean[], $6::bytea[], $7::bigint[])
		ON CONFLICT DO NOTHING`,
		txHashes, idxs, lockingScripts, satoshis, frozenSlice, utxoHashes, cshSlice,
	)
	if err != nil {
		return errors.NewStorageError("failed to insert outputs via unnest", err)
	}
	return nil
}

// createOutputsCopy inserts outputs via pgx.CopyFrom for mega transactions.
func (s *Store) createOutputsCopy(ctx context.Context, pgxTx pgx.Tx, txHash *chainhash.Hash, tx *bt.Tx, isCoinbase bool, coinbaseSpendingHeight int64) error {
	rows := make([][]interface{}, 0, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output == nil {
			continue
		}

		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return err
		}

		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return err
		}

		var lockingScript []byte
		if output.LockingScript != nil {
			lockingScript = []byte(*output.LockingScript)
		}

		rows = append(rows, []interface{}{
			txHash[:],
			int64(i),
			lockingScript,
			int64(output.Satoshis),
			false, // frozen
			utxoHash[:],
			coinbaseSpendingHeight,
		})
	}

	_, err := pgxTx.CopyFrom(ctx,
		pgx.Identifier{"outputs"},
		[]string{"tx_hash", "idx", "locking_script", "satoshis", "frozen", "utxo_hash", "coinbase_spending_height"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return errors.NewStorageError("failed to copy outputs", err)
	}
	return nil
}

// createBlockIDs inserts block_ids via unnest.
func (s *Store) createBlockIDs(ctx context.Context, pgxTx pgx.Tx, txHash *chainhash.Hash, blockInfos []utxo.MinedBlockInfo) error {
	n := len(blockInfos)
	txHashes := make([][]byte, n)
	blockIDs := make([]int64, n)
	blockHeights := make([]int64, n)
	subtreeIdxs := make([]int64, n)

	for i, info := range blockInfos {
		txHashes[i] = txHash[:]
		blockIDs[i] = int64(info.BlockID)
		blockHeights[i] = int64(info.BlockHeight)
		subtreeIdxs[i] = int64(info.SubtreeIdx)
	}

	_, err := pgxTx.Exec(ctx, `
		INSERT INTO block_ids (tx_hash, block_id, block_height, subtree_idx)
		SELECT * FROM unnest($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[])
		ON CONFLICT DO NOTHING`,
		txHashes, blockIDs, blockHeights, subtreeIdxs,
	)
	if err != nil {
		return errors.NewStorageError("failed to insert block_ids", err)
	}
	return nil
}

// insertConflictingChildren records that this conflicting transaction is a
// child of each parent it tries to spend from.
func (s *Store) insertConflictingChildren(ctx context.Context, pgxTx pgx.Tx, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	// Collect unique parent hashes from inputs
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

	_, err := pgxTx.Exec(ctx, `
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
