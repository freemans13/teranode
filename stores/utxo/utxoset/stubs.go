package utxoset

// M1 stubs. Every method here is outside the UTXO-table-only scope and fails loudly rather
// than returning a plausible wrong answer -- a store that quietly answers "not found"
// for a question it cannot answer is how consensus bugs start. Replacing these, guided
// by the store-agnostic suite in stores/utxo/tests, is the M1..M4 worklist.

import (
	"context"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

func (s *Store) ReAssignUTXO(_ context.Context, _ *utxo.Spend, _ *utxo.Spend, _ *settings.Settings) error {
	return errM1("ReAssignUTXO")
}
