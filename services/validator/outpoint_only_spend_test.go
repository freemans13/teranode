package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestValidate_OutpointOnlySpend verifies that ValidateWithOptions with
// OutpointOnlySpend=true succeeds on an un-extended child transaction, and
// that the parent output is recorded as spent (a competing spender is rejected).
//
// Exercises:
//   - Site 1: parent Get (block-heights + extend) is skipped entirely
//   - Site 2: Spend is issued with SkipUTXOHashCheck=true
//   - Site 4: TxMetaDataFromTxNoFee is called (SkipUtxoCreation=true path)
//
// The parent is stored without extension (WithSkipExtendedInputs), so any path
// that attempted to extend the child would fail with a nil-satoshis error.
func TestValidate_OutpointOnlySpend(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_spend")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// parentTx: coinbase-style with one P2PKH output (500 sat).
	// Stored without extended inputs so it carries no satoshi metadata —
	// any path that tries to read parent satoshis would fail.
	parentTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = coinbaseInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: coinbaseScript,
	})

	// Store parent with WithSkipExtendedInputs so no fee/extension is required.
	_, err = store.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: spends parentTx output:0.
	// Inputs are NOT extended (no PreviousTxScript, no PreviousTxSatoshis).
	childTx := bt.NewTx()
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	err = childInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.NoError(t, err, "outpoint-only spend on un-extended input must succeed")

	// Verify the parent output is now spent by attempting to spend it with a
	// different competing transaction. The store records childTx's spending_data
	// on the output; any other spender must be rejected with ErrSpent.
	competingTx := bt.NewTx()
	competingInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x01}), // different unlocking script → different txid
	}
	err = competingInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	competingTx.Inputs = append(competingTx.Inputs, competingInput)
	competingTx.Outputs = append(competingTx.Outputs, &bt.Output{
		Satoshis:      300,
		LockingScript: coinbaseScript,
	})

	_, err = store.Spend(ctx, competingTx, 500, utxostore.IgnoreFlags{
		IgnoreLocked:      true,
		SkipUTXOHashCheck: true,
	})
	require.Error(t, err, "competing spender must be rejected after outpoint-only spend committed the first spender")
}

// TestValidate_OutpointOnlySpend_RequiresSkipScriptValidation verifies that
// setting OutpointOnlySpend=true without SkipScriptValidation=true returns a
// clear misconfiguration error before any extend/parent-read work is attempted.
func TestValidate_OutpointOnlySpend_RequiresSkipScriptValidation(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_misconfig")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// Minimal child tx — contents don't matter; the guard fires before any store access.
	childTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = childInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	// OutpointOnlySpend=true but SkipScriptValidation is left false — misconfiguration.
	opts := &Options{
		SkipUtxoCreation:  true,
		OutpointOnlySpend: true,
		// SkipScriptValidation intentionally omitted (false)
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.Error(t, err, "OutpointOnlySpend without SkipScriptValidation must return an error")
	require.Contains(t, err.Error(), "OutpointOnlySpend requires SkipScriptValidation")
}
