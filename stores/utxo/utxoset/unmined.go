package utxoset

import (
	"context"

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
