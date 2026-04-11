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

// unminedTxIterator implements utxo.UnminedTxIterator for the v6 queue store.
type unminedTxIterator struct {
	store *Store
	rows  pgx.Rows
	err   error
	done  bool
}

const iteratorBatchSize = 1024

func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	q := `
		SELECT hash, fee, size_in_bytes, inserted_at, coinbase,
		       locked, unmined_since, raw_tx, block_ids
		FROM utxos
		WHERE unmined_since IS NOT NULL AND conflicting = false
		ORDER BY hash
	`

	rows, err := store.pool.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

func newPrunableUnminedTxIterator(store *Store, cutoffBlockHeight uint32) (*unminedTxIterator, error) {
	q := `
		SELECT hash, fee, size_in_bytes, inserted_at, coinbase,
		       locked, unmined_since, raw_tx, block_ids
		FROM utxos
		WHERE unmined_since IS NOT NULL
		  AND unmined_since <= $1
		  AND conflicting = false
		ORDER BY hash
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

func (it *unminedTxIterator) readOne(_ context.Context) (*utxo.UnminedTransaction, error) {
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
		rawTx        []byte
		blockIDs     []int32
	)

	if err := it.rows.Scan(&txHashBytes, &fee, &sizeInBytes, &insertedAt, &isCoinbase,
		&locked, &unminedSince, &rawTx, &blockIDs); err != nil {
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

	// Deserialize raw_tx for inputs.
	var inputs []*bt.Input
	if rawTx != nil {
		parsedTx, parseErr := bt.NewTxFromBytes(rawTx)
		if parseErr != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			it.err = parseErr
			return nil, it.err
		}
		inputs = parsedTx.Inputs
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = errors.NewProcessingError("failed to create tx inpoints from inputs", err)
		return nil, it.err
	}

	// Convert block_ids from []int32 to []uint32.
	bidResult := make([]uint32, len(blockIDs))
	for i, bid := range blockIDs {
		bidResult[i] = uint32(bid)
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
		BlockIDs:     bidResult,
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
		SELECT hash FROM utxos
		WHERE unmined_since IS NOT NULL AND unmined_since <= $1
		ORDER BY unmined_since LIMIT 1000
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
