package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// stubIterator is a placeholder iterator that returns nothing.
type stubIterator struct{}

func (it *stubIterator) Next(_ context.Context) ([]*utxo.UnminedTransaction, error) {
	return nil, nil
}

func (it *stubIterator) Err() error {
	return nil
}

func (it *stubIterator) Close() error {
	return nil
}

// GetUnminedTxIterator returns an iterator for all unmined transactions.
// TODO: implement JOIN tx_state in Task 9.
func (s *Store) GetUnminedTxIterator(_ bool) (utxo.UnminedTxIterator, error) {
	return &stubIterator{}, nil
}

// GetPrunableUnminedTxIterator returns an iterator for unmined transactions
// older than the cutoff height.
// TODO: implement JOIN tx_state in Task 9.
func (s *Store) GetPrunableUnminedTxIterator(_ uint32) (utxo.UnminedTxIterator, error) {
	return &stubIterator{}, nil
}

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions
// older than the cutoff height.
// TODO: implement in Task 9.
func (s *Store) QueryOldUnminedTransactions(_ context.Context, _ uint32) ([]chainhash.Hash, error) {
	return nil, errors.NewProcessingError("not implemented")
}
