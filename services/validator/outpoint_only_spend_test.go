package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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

// TestValidate_OutpointOnlySpend_BIP68HeightSkipped is the TDD regression guard for the bug
// where BIP68 sequence-lock validation ran on the OutpointOnlySpend fast path even though
// validateInternal intentionally left utxoHeights nil — causing "MTP store not loaded" errors
// on V2 txs with height-based relative locks at/above CSVHeight, stalling block validation.
//
// The fix adds `validationOptions.OutpointOnlySpend ||` as the first disjunct in the BIP68
// entry-guard at validateTransaction (~line 1753), causing it to return nil immediately
// without touching utxoHeights. Below-checkpoint BIP68 compliance is already certified by
// the pinned hardcoded checkpoint — same basis as skipping script validation.
//
// RED (before fix): blockHeight=1000 >= CSVHeight=1, blockchainClient != nil, SkipPolicyChecks=true
// → BIP68 guard passes → readMTPsLocked called with empty mtpStore → error returned.
// GREEN (after fix): OutpointOnlySpend=true short-circuits the BIP68 guard → returns nil.
func TestValidate_OutpointOnlySpend_BIP68HeightSkipped(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// CSVHeight=1 ensures BIP68 is active at any practical blockHeight.
	tSettings.ChainCfgParams.CSVHeight = 1

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_bip68height")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(1000))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// parentTx: coinbase-style with one P2PKH output.
	// Stored without extended inputs so no satoshi metadata is present — any path
	// that tries to extend the child would fail.
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

	_, err = store.Create(ctx, parentTx, 990, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: V2 tx with a height-based BIP68 relative lock (SequenceNumber=10).
	// Disable bit (0x80000000) is CLEAR and type bit (0x00400000) is CLEAR → height-based,
	// 10-block relative lock. Inputs are NOT extended (no PreviousTxScript/Satoshis).
	childTx := bt.NewTx()
	childTx.Version = 2
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     10, // height-based relative lock: 10 blocks
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	err = childInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	// blockchainClient is non-nil so the `v.blockchainClient == nil` guard in
	// validateTransaction does NOT short-circuit BIP68. The mock has no expectations
	// set: BIP68 only does a nil-check on blockchainClient; it does not call any
	// client methods — it reads from the pre-loaded mtpStore instead.
	mockClient := &blockchain.Mock{}

	v := &Validator{
		logger:           logger,
		utxoStore:        store,
		settings:         tSettings,
		txValidator:      NewTxValidator(logger, tSettings),
		stats:            gocore.NewStat("validator"),
		blockchainClient: mockClient,
		// mtpStore intentionally left nil/empty: before the fix, BIP68 runs and
		// readMTPsLocked returns "MTP store not loaded up to height 1000". After the
		// fix, OutpointOnlySpend=true short-circuits before readMTPsLocked is reached.
	}

	opts := &Options{
		SkipUtxoCreation:          true,
		SkipScriptValidation:      true,
		SkipPolicyChecks:          true,
		OutpointOnlySpend:         true,
		IgnoreLocked:              true,
		CandidateParentMedianTime: 1700000000, // required when blockHeight >= CSVHeight && SkipPolicyChecks=true
	}

	// RED trigger: blockHeight=1000 >= CSVHeight=1, blockchainClient != nil,
	// SkipPolicyChecks=true → BIP68 runs → readMTPsLocked fails on empty mtpStore.
	// AFTER fix: OutpointOnlySpend=true short-circuits BIP68 → returns nil.
	_, err = v.ValidateWithOptions(ctx, childTx, 1000, opts)
	require.NoError(t, err, "BIP68 sequence-lock must be skipped on OutpointOnlySpend fast path")
}
