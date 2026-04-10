package queue

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// unminedTxIterator implements utxo.UnminedTxIterator for the v4 queue store.
type unminedTxIterator struct {
	store *Store
	rows  pgx.Rows
	err   error
	done  bool
}

const iteratorBatchSize = 1024

func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	q := `
		SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
		       ts.locked, ts.unmined_since
		FROM transactions t
		JOIN tx_state ts ON ts.tx_hash = t.hash
		WHERE ts.unmined_since IS NOT NULL AND ts.conflicting = false
		ORDER BY t.hash
	`

	rows, err := store.pool.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

func newPrunableUnminedTxIterator(store *Store, cutoffBlockHeight uint32) (*unminedTxIterator, error) {
	q := `
		SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
		       ts.locked, ts.unmined_since
		FROM transactions t
		JOIN tx_state ts ON ts.tx_hash = t.hash
		WHERE ts.unmined_since IS NOT NULL
		  AND ts.unmined_since <= $1
		  AND ts.conflicting = false
		ORDER BY t.hash
	`

	rows, err := store.pool.Query(context.Background(), q, int64(cutoffBlockHeight))
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	batch := make([]*utxo.UnminedTransaction, 0, iteratorBatchSize)
	for i := 0; i < iteratorBatchSize; i++ {
		tx, err := it.readOne(ctx)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			break
		}
		batch = append(batch, tx)
	}

	if len(batch) == 0 {
		return nil, nil
	}

	return batch, nil
}

func (it *unminedTxIterator) readOne(ctx context.Context) (*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	if !it.rows.Next() {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}
		return nil, nil
	}

	var (
		txHashBytes  []byte
		fee          int64
		sizeInBytes  int64
		insertedAt   time.Time
		isCoinbase   bool
		locked       bool
		unminedSince int64
	)

	if err := it.rows.Scan(&txHashBytes, &fee, &sizeInBytes, &insertedAt, &isCoinbase, &locked, &unminedSince); err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}

	txHash, err := chainhash.NewHash(txHashBytes)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}

	if isCoinbase {
		return &utxo.UnminedTransaction{Skip: true}, nil
	}

	// Fetch inputs for this transaction.
	inputRows, err := it.store.pool.Query(ctx, `
		SELECT previous_transaction_hash, previous_tx_idx, previous_tx_satoshis,
		       previous_tx_script, unlocking_script, sequence_number
		FROM inputs
		WHERE tx_hash = $1
		ORDER BY idx
	`, txHashBytes)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}
	defer inputRows.Close()

	tx := bt.Tx{}
	for inputRows.Next() {
		input := &bt.Input{}
		var (
			prevTxHashBytes []byte
			previousTxIdx   int64
		)

		if err = inputRows.Scan(&prevTxHashBytes, &previousTxIdx, &input.PreviousTxSatoshis,
			&input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			return nil, err
		}

		input.PreviousTxOutIndex = uint32(previousTxIdx)

		prevTxHash, hashErr := chainhash.NewHash(prevTxHashBytes)
		if hashErr != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			return nil, hashErr
		}

		if err = input.PreviousTxIDAdd(prevTxHash); err != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			return nil, err
		}

		tx.Inputs = append(tx.Inputs, input)
	}

	if err = inputRows.Err(); err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		return nil, err
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
	}

	// Fetch block_ids for this transaction.
	blockRows, err := it.store.pool.Query(ctx, `
		SELECT block_id FROM block_ids WHERE tx_hash = $1 ORDER BY block_id
	`, txHashBytes)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}
	defer blockRows.Close()

	blockIDs := make([]uint32, 0, 2)
	for blockRows.Next() {
		var blockID int64
		if err = blockRows.Scan(&blockID); err != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			return nil, err
		}
		blockIDs = append(blockIDs, uint32(blockID))
	}

	if err = blockRows.Err(); err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		return nil, err
	}

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *txHash,
			Fee:         uint64(fee),
			SizeInBytes: uint64(sizeInBytes),
		},
		TxInpoints:   &txInpoints,
		CreatedAt:    int(insertedAt.UnixMilli()),
		Locked:       locked,
		BlockIDs:     blockIDs,
		UnminedSince: int(unminedSince),
	}, nil
}

func (it *unminedTxIterator) Err() error {
	return it.err
}

func (it *unminedTxIterator) Close() error {
	it.done = true
	if it.rows != nil {
		it.rows.Close()
	}
	return nil
}

// GetUnminedTxIterator returns an iterator for all unmined, non-conflicting transactions.
func (s *Store) GetUnminedTxIterator(_ bool) (utxo.UnminedTxIterator, error) {
	return newUnminedTxIterator(s)
}

// GetPrunableUnminedTxIterator returns an iterator for unmined transactions
// older than the cutoff height.
func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return newPrunableUnminedTxIterator(s, cutoffBlockHeight)
}

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions
// older than the cutoff height.
func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.hash FROM transactions t
		JOIN tx_state ts ON ts.tx_hash = t.hash
		WHERE ts.unmined_since IS NOT NULL AND ts.unmined_since <= $1
		ORDER BY ts.unmined_since LIMIT 1000
	`, int64(cutoffBlockHeight))
	if err != nil {
		return nil, errors.NewStorageError("failed to query old unmined transactions", err)
	}
	defer rows.Close()

	var txHashes []chainhash.Hash
	for rows.Next() {
		var hashBytes []byte
		if err := rows.Scan(&hashBytes); err != nil {
			s.logger.Errorf("[QueryOldUnminedTransactions] error scanning row: %v", err)
			continue
		}
		var txHash chainhash.Hash
		copy(txHash[:], hashBytes)
		txHashes = append(txHashes, txHash)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating unmined transactions", err)
	}

	return txHashes, nil
}
