package validator

import (
	"context"
	"net/url"
	"sync/atomic"
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

// decorateSpyStore wraps a real utxo.Store and counts every call to
// PreviousOutputsDecorate and BatchPreviousOutputsDecorate. This lets a test
// assert that the below-checkpoint OutpointOnlySpend fast path issues zero
// parent-read calls through extendTransaction.
type decorateSpyStore struct {
	utxostore.Store
	decorateCallCount atomic.Int64
}

func (s *decorateSpyStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	s.decorateCallCount.Add(1)
	return s.Store.PreviousOutputsDecorate(ctx, tx)
}

func (s *decorateSpyStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	s.decorateCallCount.Add(1)
	return s.Store.BatchPreviousOutputsDecorate(ctx, txs)
}

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

// TestValidate_OutpointOnlySpend_NoParentRead is the TDD regression guard for the bug
// where validateTransaction (~line 1677) contained an ungated re-extend block:
//
//	if !tx.IsExtended() {
//	    if err := v.extendTransaction(ctx, tx); err != nil { ... }
//	}
//
// extendTransaction calls PreviousOutputsDecorate, issuing one SELECT raw_tx per
// parent. With OutpointOnlySpend=true the fast path in validateInternal (~line 735)
// deliberately leaves the tx un-extended, but the ungated re-extend in
// validateTransaction immediately undid that — re-issuing exactly the per-parent
// parent reads the fast path exists to eliminate (142,733 per-tx observed on mainnet).
//
// The fix gates the re-extend on !validationOptions.OutpointOnlySpend:
//
//	if !validationOptions.OutpointOnlySpend && !tx.IsExtended() { ... }
//
// This test proves correctness by wrapping the real store in a decorateSpyStore
// and asserting that zero PreviousOutputsDecorate calls are issued when
// OutpointOnlySpend=true. It will FAIL on pre-fix code (count>0) and PASS after.
func TestValidate_OutpointOnlySpend_NoParentRead(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_noparentread")
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, realStore.SetBlockHeight(500))
	require.NoError(t, realStore.SetMedianBlockTime(1700000000))

	// Wrap the real store in a spy so we can count PreviousOutputsDecorate calls.
	spy := &decorateSpyStore{Store: realStore}

	// parentTx: coinbase-style with one P2PKH output.
	// Stored WITHOUT extended inputs (WithSkipExtendedInputs) so it carries no
	// satoshi metadata. Any path that attempts to extend the child via
	// PreviousOutputsDecorate would fail (or at minimum increment the spy counter).
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

	_, err = realStore.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: spends parentTx output:0. Inputs are NOT extended — no
	// PreviousTxScript, no PreviousTxSatoshis — so IsExtended() returns false
	// and any re-extend attempt via PreviousOutputsDecorate would fire.
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
		utxoStore:   spy, // spy wraps the real store — intercepts decorate calls
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

	// THE REGRESSION GUARD: PreviousOutputsDecorate must NEVER be called when
	// OutpointOnlySpend=true. Before the fix, validateTransaction re-extended the tx
	// unconditionally, firing PreviousOutputsDecorate once per transaction. After the
	// fix the guard prevents the re-extend and this counter stays at zero.
	require.Equal(t, int64(0), spy.decorateCallCount.Load(),
		"OutpointOnlySpend must issue zero PreviousOutputsDecorate calls; "+
			"a non-zero count means validateTransaction is re-issuing the per-parent "+
			"reads that the fast path exists to eliminate")
}
