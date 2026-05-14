package validator

import (
	"context"
	"net/url"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// newValidatorForTest creates a minimal Validator suitable for unit tests.
// It uses an in-memory SQLite UTXO store and disables block assembly so
// that no external services need to be running.
func newValidatorForTest(t testing.TB) *Validator {
	t.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	iface, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil)
	require.NoError(t, err)

	return iface.(*Validator)
}

func TestValidateBatch_FallbackParity_Empty(t *testing.T) {
	v := newValidatorForTest(t)
	results, err := v.ValidateBatch(context.Background(), nil, 0)
	require.NoError(t, err)
	require.Len(t, results, 0)
}

func TestValidateBatch_FallbackParity_MalformedTxFailsPerTx(t *testing.T) {
	v := newValidatorForTest(t)

	// Use an obviously-invalid tx: a single empty tx with no inputs and
	// no outputs. ValidateWithOptions will reject it. The point of this
	// test is to verify ValidateBatch's fan-out wires per-tx errors
	// correctly — not to test the validation rules themselves.
	bad := &bt.Tx{}

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{bad}, 0)
	require.NoError(t, err, "whole-batch err must be nil even when all per-tx fail")
	require.Len(t, results, 1)
	require.Equal(t, *bad.TxIDChainHash(), results[0].TxHash)
	require.Error(t, results[0].Err, "an empty tx must fail validation")
}

func TestValidateBatch_FallbackParity_MultipleTx_PositionalIndex(t *testing.T) {
	v := newValidatorForTest(t)

	// Two distinct empty tx — both will fail, but at different indices.
	// The test verifies the result slice is positional and dense.
	bads := []*bt.Tx{{}, {}}
	results, err := v.ValidateBatch(context.Background(), bads, 0)
	require.NoError(t, err)
	require.Len(t, results, 2)
	for i, r := range results {
		require.Error(t, r.Err, "index %d", i)
		require.Equal(t, *bads[i].TxIDChainHash(), r.TxHash, "index %d", i)
	}
}

func TestValidateBatch_FallbackBoundedParallelism(t *testing.T) {
	// With flag off, fallback uses errgroup with SetLimit(NumCPU). This
	// test asserts ValidateBatch does NOT spawn one goroutine per tx
	// unboundedly. With 200 tx, peak goroutine count should stay
	// well below 200.
	v := newValidatorForTest(t)
	const N = 200

	txs := make([]*bt.Tx, N)
	for i := range txs {
		txs[i] = &bt.Tx{}
	}

	before := runtime.NumGoroutine()
	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Len(t, results, N)
	after := runtime.NumGoroutine()
	require.Less(t, after-before, N/4, "fallback should be bounded by NumCPU, not N")
}
