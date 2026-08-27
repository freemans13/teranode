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

// QueryOldUnminedTransactions finds none, same reason.
func (s *Store) QueryOldUnminedTransactions(_ context.Context, _ uint32) ([]chainhash.Hash, error) {
	return nil, nil
}

// PreserveTransactions stays a no-op, but the reason has CHANGED and the old one is no
// longer true.
//
// It used to be that this store had no reclaimer and never deleted an unspent output, so
// there was nothing to preserve anything from. That stopped being true the day tx_ident
// gained a reclaimer.
//
// The new justification is the settled rule, and it is stronger. Preservation exists to stop
// a reclaimer deleting a parent that a live unmined child still needs, and it does that with
// a timer: extend the parent's life by 1,440 blocks and hope the child is mined inside it.
// The reclaimer here does not need the hope, because it consults the spender's status at the
// moment of the decision rather than racing a clock. It deletes a parent only when every
// transaction that spent it is buried past the depth at which the node could un-mine it, so
// there is no window in which preservation would have helped.
//
// If anyone ever weakens that rule, this comment must stop claiming the no-op is safe.
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
