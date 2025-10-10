package utxovalidator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestValidateUTXOFile(t *testing.T) {
	t.Skip("Skipping long-running test that requires large UTXO file on disk")

	file := "../../746044/00000000000000000c60d122906015547c6ba4c46ed29f62a6a30a73819ae960.utxo-set"

	tSettings := test.CreateBaseTestSettings(t)

	result, err := ValidateUTXOFile(t.Context(), file, ulogger.TestLogger{}, tSettings, false)
	require.NoError(t, err)

	t.Logf("Block Height: %d", result.BlockHeight)
	t.Logf("Block Hash: %s", result.BlockHash.String())
	t.Logf("Previous Hash: %s", result.PreviousHash.String())
	t.Logf("Actual Satoshis: %d", result.ActualSatoshis)
	t.Logf("Expected Satoshis: %d", result.ExpectedSatoshis)
	t.Logf("Is Valid: %t", result.IsValid)
	t.Logf("UTXO Count: %d", result.UTXOCount)
}
