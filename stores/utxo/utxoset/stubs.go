package utxoset

// M1 stubs. Every method here is outside the UTXO-table-only scope and fails loudly rather
// than returning a plausible wrong answer -- a store that quietly answers "not found"
// for a question it cannot answer is how consensus bugs start. Replacing these, guided
// by the store-agnostic suite in stores/utxo/tests, is the M1..M4 worklist.

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

func (s *Store) Delete(_ context.Context, _ *chainhash.Hash) error {
	return errM1("Delete")
}

func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return nil, errM1("ScanInconsistentUnminedTxs")
}

func (s *Store) ReAssignUTXO(_ context.Context, _ *utxo.Spend, _ *utxo.Spend, _ *settings.Settings) error {
	return errM1("ReAssignUTXO")
}

func (s *Store) GetCounterConflicting(_ context.Context, _ chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, errM1("GetCounterConflicting")
}

func (s *Store) GetConflictingChildren(_ context.Context, _ chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, errM1("GetConflictingChildren")
}

func (s *Store) SetConflicting(_ context.Context, _ []chainhash.Hash, _ bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, errM1("SetConflicting")
}

func (s *Store) RemoveFromConflictingChildren(_ context.Context, _ []utxo.ConflictingChildRemoval) error {
	return errM1("RemoveFromConflictingChildren")
}

func (s *Store) RemoveBlockIDs(_ context.Context, _ []utxo.BlockIDsRemoval) error {
	return errM1("RemoveBlockIDs")
}

func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	return nil, errM1("GetConflictingTxIterator")
}

func (s *Store) BeginConflictIntent(_ context.Context, _ utxo.ConflictIntent) error {
	return errM1("BeginConflictIntent")
}

func (s *Store) CompleteConflictIntent(_ context.Context, _ chainhash.Hash) error {
	return errM1("CompleteConflictIntent")
}
