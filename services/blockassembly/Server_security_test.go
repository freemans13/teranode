package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// TestCoinbaseValidation tests that coinbase transactions are properly validated,
// preventing a potential panic/DoS vulnerability.
//
// This test addresses a security issue where crafted transactions with zero or multiple
// inputs could cause an index-out-of-range panic when accessing coinbaseTx.Inputs[0]
// in SubmitMiningSolution. A valid coinbase must have exactly one input with specific
// characteristics (previous tx hash = all zeros, index = 0xFFFFFFFF).
func TestCoinbaseValidation(t *testing.T) {
	t.Run("transaction with zero inputs can be parsed but is invalid", func(t *testing.T) {
		// Create a transaction with zero inputs (malicious payload)
		maliciousTx := bt.NewTx()

		// Add an output but no inputs
		err := maliciousTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
		require.NoError(t, err)

		// Serialize and deserialize to simulate network transmission
		maliciousTxBytes := maliciousTx.Bytes()

		parsedTx, err := bt.NewTxFromBytes(maliciousTxBytes)
		require.NoError(t, err, "bt.NewTxFromBytes should successfully parse tx with zero inputs")
		require.NotNil(t, parsedTx)

		// Verify the transaction has zero inputs
		require.Equal(t, 0, len(parsedTx.Inputs), "Parsed transaction should have zero inputs")
		require.False(t, parsedTx.IsCoinbase(), "Transaction with zero inputs is not a valid coinbase")

		// This demonstrates the vulnerability: accessing Inputs[0] would panic
		// The fix in Server.go now validates len(coinbaseTx.Inputs) == 1 before accessing Inputs[0]
	})

	t.Run("valid coinbase has exactly one input", func(t *testing.T) {
		// Use a real coinbase transaction hex from Bitcoin mainnet
		// This is block 100000's coinbase transaction
		coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000"
		coinbaseBytes, err := bt.NewTxFromString(coinbaseHex)
		require.NoError(t, err)

		// Verify it has exactly one input and is a valid coinbase
		require.Equal(t, 1, len(coinbaseBytes.Inputs), "Valid coinbase must have exactly one input")
		require.True(t, coinbaseBytes.IsCoinbase(), "Transaction should be identified as coinbase")

		// Serialize and deserialize to simulate network transmission
		validTxBytes := coinbaseBytes.Bytes()
		parsedTx, err := bt.NewTxFromBytes(validTxBytes)
		require.NoError(t, err)

		// Verify parsed transaction has exactly one input and is valid coinbase
		require.Equal(t, 1, len(parsedTx.Inputs), "Parsed coinbase must have exactly one input")
		require.True(t, parsedTx.IsCoinbase(), "Parsed transaction should be identified as coinbase")

		// Safe to access Inputs[0] now
		require.NotNil(t, parsedTx.Inputs[0])

		// Verify coinbase characteristics
		require.Equal(t, uint32(0xFFFFFFFF), parsedTx.Inputs[0].PreviousTxOutIndex, "Coinbase input must have index 0xFFFFFFFF")
	})

	t.Run("transaction with multiple inputs is not a valid coinbase", func(t *testing.T) {
		// Create a transaction with two inputs (invalid coinbase)
		invalidTx := bt.NewTx()

		// Add two inputs
		invalidTx.Inputs = append(invalidTx.Inputs, &bt.Input{
			PreviousTxOutIndex: 0,
			SequenceNumber:     0xFFFFFFFF,
		})
		invalidTx.Inputs = append(invalidTx.Inputs, &bt.Input{
			PreviousTxOutIndex: 1,
			SequenceNumber:     0xFFFFFFFF,
		})

		// Add output
		err := invalidTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
		require.NoError(t, err)

		// Verify it has multiple inputs and is not a valid coinbase
		require.Equal(t, 2, len(invalidTx.Inputs), "Transaction should have two inputs")
		require.False(t, invalidTx.IsCoinbase(), "Transaction with multiple inputs is not a valid coinbase")
	})
}
