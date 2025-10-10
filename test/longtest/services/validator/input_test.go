package validator

import (
	"testing"

	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestCheckInputsWithDuplicateInputs(t *testing.T) {
	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tx1 := transactions.Create(t,
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tx2 := transactions.Create(t,
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tSettings := test.CreateBaseTestSettings(t)

	tv := validator.NewTxValidator(
		ulogger.TestLogger{},
		tSettings,
	)

	// Check for duplicate inputs
	err = tv.ValidateTransaction(tx1, 0, nil, &validator.Options{
		SkipPolicyChecks: true,
	})
	require.NoError(t, err)

	// Check for duplicate inputs
	err = tv.ValidateTransaction(tx2, 0, nil, &validator.Options{
		SkipPolicyChecks: true,
	})

	require.Error(t, err, "Expected error due to duplicate inputs")
}
