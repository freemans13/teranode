package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// emptyUnminedIterator is an iterator over nothing.
//
// It is a real value rather than a nil interface on purpose: the block assembly caller
// checks only the error and then uses the iterator (BlockAssembler.go:2629-2636), so
// handing back a nil would move a startup error into a nil dereference, which is a worse
// failure in a worse place.
type emptyUnminedIterator struct{}

func (emptyUnminedIterator) Next(_ context.Context) ([]*utxo.UnminedTransaction, error) {
	return nil, nil
}
func (emptyUnminedIterator) Err() error   { return nil }
func (emptyUnminedIterator) Close() error { return nil }

// GetUnminedTxIterator reports that nothing is unmined.
//
// Empty is the TRUE answer for this store rather than a convenient stub, and the
// distinction matters because a wrong "none" here would silently drop transactions out
// of block assembly. The unmined set is transaction-level state and lives in tx_meta.
// This store has no tx_meta, so it cannot be holding an unmined transaction: there is no
// row anywhere that could represent one. "None" describes it exactly.
//
// That stops being true the moment tx_meta lands, and this must then return real data.
// The conformance suite in stores/utxo/tests is what catches it if it does not.
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return emptyUnminedIterator{}, nil
}

// PendingConflictIntents reports that no conflict resolution was interrupted.
//
// Same reasoning as GetUnminedTxIterator, and additionally: the intent log exists so a
// ProcessConflicting interrupted by a crash can be replayed, and this store cannot begin
// an intent in the first place, since BeginConflictIntent is still unimplemented. A store
// that cannot start one cannot have an unfinished one.
//
// Block assembly replays these at startup (BlockAssembler.go:936) and an error is fatal.
func (s *Store) PendingConflictIntents(_ context.Context) ([]utxo.ConflictIntent, error) {
	return nil, nil
}

// GetPrunableUnminedTxIterator reports that nothing is prunable, for the same reason
// GetUnminedTxIterator reports that nothing is unmined: there is no tx_meta, so no row
// could represent an unmined transaction, prunable or otherwise.
func (s *Store) GetPrunableUnminedTxIterator(_ uint32) (utxo.UnminedTxIterator, error) {
	return emptyUnminedIterator{}, nil
}

// QueryOldUnminedTransactions finds none, same reason.
func (s *Store) QueryOldUnminedTransactions(_ context.Context, _ uint32) ([]chainhash.Hash, error) {
	return nil, nil
}

// PreserveTransactions is meaningless here, not merely unimplemented, and that is the
// stronger claim.
//
// Preservation exists to stop a pruner deleting a parent transaction that a live unmined
// child still needs. This store has no pruner (see pruner.go) and never deletes an
// unspent output: the only DELETE it issues is the one that spends a coin, authorised by
// the spend itself. There is nothing to preserve anything from.
func (s *Store) PreserveTransactions(_ context.Context, _ []chainhash.Hash, _ uint32) error {
	return nil
}

// ProcessExpiredPreservations has nothing to expire, because nothing was preserved.
//
// This runs on a background timer, so an error here is not fatal but is logged on every
// cycle, and a store that errors on a timer teaches everyone to ignore its errors.
func (s *Store) ProcessExpiredPreservations(_ context.Context, _ uint32) error {
	return nil
}
