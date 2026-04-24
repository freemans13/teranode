package validator

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// setupValidateBatchFixture wires a Validator backed by a sqlitememory store
// and returns it alongside the store handle and a deterministic private key
// shared by the generated coinbase+child txs. Each test calls
// buildValidateBatchTx(t, fixture, i) to produce pair-wise-independent valid
// txs that spend distinct, pre-seeded coinbase parents.
type validateBatchFixture struct {
	ctx     context.Context
	v       *Validator
	store   *sql.Store
	privKey *bec.PrivateKey
}

func setupValidateBatchFixture(t *testing.T) *validateBatchFixture {
	t.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true // avoid needing the block assembler

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// BlockAssembly is disabled above, so the block-assembly client is not needed.
	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	vI, err := New(ctx, logger, tSettings, store, nil, nil, blockAssemblyClient, nil)
	require.NoError(t, err)

	v, ok := vI.(*Validator)
	require.True(t, ok, "New should return a concrete *Validator for these tests")

	// Advance the store's block-height past coinbase maturity so tests can
	// spend freshly-seeded coinbase UTXOs at spendHeight.
	require.NoError(t, store.SetBlockHeight(spendHeight))

	privKey, _ := bec.PrivateKeyFromBytes([]byte("VALIDATE_BATCH_TEST_DETERMINISTIC_KEY"))

	return &validateBatchFixture{
		ctx:     ctx,
		v:       v,
		store:   store,
		privKey: privKey,
	}
}

// coinbaseSeedHeight is the block-height each fresh test coinbase is stored
// at. Children reference it from spendHeight = coinbaseSeedHeight + 200 so
// that they clear the 100-block coinbase-maturity rule on mainnet-style
// chain params.
const coinbaseSeedHeight uint32 = 1

// spendHeight is the block-height we pass to ValidateBatch for all test txs.
// It's well past coinbase maturity so Spend() accepts the coinbase UTXOs.
const spendHeight uint32 = coinbaseSeedHeight + 200

// buildValidateBatchTx produces a valid child tx that spends a freshly-seeded
// coinbase output. Each call uses a distinct coinbase "seed" nonce inside the
// coinbase arbitrary-data so the coinbase txids and resulting child txids
// are all unique within a test. The coinbase parent is stored at
// coinbaseSeedHeight (not per-seed, because the sqlitememory store tracks
// one block-height). Uniqueness of each coinbase txid comes from the unique
// miner-info string which is mixed into the coinbase's arbitrary-data.
func buildValidateBatchTx(t *testing.T, f *validateBatchFixture, seed uint32) *bt.Tx {
	t.Helper()
	coinbaseTx := transactions.Create(t,
		transactions.WithCoinbaseData(coinbaseSeedHeight, fmt.Sprintf("/ValidateBatch test seed=%d/", seed)),
		transactions.WithP2PKHOutputs(1, 50e8, f.privKey.PubKey()),
	)

	// Seed the coinbase as an existing UTXO so the child can spend it.
	_, err := f.store.Create(f.ctx, coinbaseTx, coinbaseSeedHeight)
	require.NoError(t, err)

	child := transactions.Create(t,
		transactions.WithPrivateKey(f.privKey),
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)
	return child
}

// buildValidateBatchTxWithBrokenScript produces a tx whose unlocking script
// is deliberately invalid (a single OP_0) so Phase-1 script verification
// rejects it.
func buildValidateBatchTxWithBrokenScript(t *testing.T, f *validateBatchFixture, seed uint32) *bt.Tx {
	t.Helper()
	tx := buildValidateBatchTx(t, f, seed)
	// Replace the unlocking script with a single OP_0 byte — guaranteed to
	// fail script verification.
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	return tx
}

// TestValidateBatch_IndexMath verifies the batch API returns slot-aligned
// metas/errs with the correct length and nil errors for a batch of fresh,
// well-formed transactions.
func TestValidateBatch_IndexMath(t *testing.T) {
	f := setupValidateBatchFixture(t)

	const N = 5
	txs := make([]*bt.Tx, N)
	for i := 0; i < N; i++ {
		txs[i] = buildValidateBatchTx(t, f, uint32(i)+1)
	}

	metas, errs := f.v.ValidateBatch(f.ctx, txs, spendHeight, WithSkipPolicyChecks(true))
	require.Len(t, metas, N)
	require.Len(t, errs, N)
	for i := 0; i < N; i++ {
		require.NoError(t, errs[i], "slot %d errored: %v", i, errs[i])
		require.NotNil(t, metas[i], "slot %d has nil meta", i)
	}
}

// TestValidateBatch_PartialFailure verifies per-slot error isolation:
// one tx is pre-existing (duplicate → ErrTxExists during Phase 2), one
// has a bad script (fails in Phase 1), one is valid. All three slots
// must carry the correct individual outcome.
func TestValidateBatch_PartialFailure(t *testing.T) {
	f := setupValidateBatchFixture(t)

	// Slot 0: valid fresh tx.
	goodTx := buildValidateBatchTx(t, f, 100)

	// Slot 1: valid tx, but we pre-create it in the store so CreateBatch
	// sees ErrTxExists. The store's ErrTxExists branch routes back to
	// GetMeta which returns the pre-created meta; the slot resolves to
	// (*meta.Data, nil). (This mirrors the single-tx ErrTxExists → fallback
	// path in validateInternalPostCreate.)
	existingTx := buildValidateBatchTx(t, f, 101)
	_, err := f.store.Create(f.ctx, existingTx, 1)
	require.NoError(t, err)

	// Slot 2: tx with a deliberately broken script — fails in Phase 1.
	brokenScriptTx := buildValidateBatchTxWithBrokenScript(t, f, 102)

	txs := []*bt.Tx{goodTx, existingTx, brokenScriptTx}
	metas, errs := f.v.ValidateBatch(f.ctx, txs, spendHeight, WithSkipPolicyChecks(true))

	require.Len(t, metas, 3)
	require.Len(t, errs, 3)

	// Slot 0: fresh valid → success.
	require.NoError(t, errs[0], "slot 0 (fresh valid tx) must succeed")
	require.NotNil(t, metas[0])

	// Slot 1: duplicate → ErrTxExists fallback path in PostCreate returns
	// (*meta, nil) — the fallback treats the pre-existing tx as already
	// blessed. This mirrors single-tx behaviour at Validator.go:674-682.
	require.NoError(t, errs[1], "slot 1 (duplicate) must resolve to nil err via ErrTxExists fallback")
	require.NotNil(t, metas[1], "slot 1 must carry the pre-existing meta")

	// Slot 2: broken script → Phase-1 failure.
	require.Error(t, errs[2], "slot 2 (broken script) must error")
	require.Nil(t, metas[2], "slot 2 must have nil meta on Phase-1 failure")

	// Verify the two non-failing slots do NOT accidentally carry slot 2's error.
	require.False(t, errors.Is(errs[0], errs[2]))
	require.False(t, errors.Is(errs[1], errs[2]))
}

// TestValidateBatch_PreservesBatcherFlush verifies that after a
// ValidateBatch call, successful txs are immediately queryable in the
// store without a separate batcher flush. The pre-PR sequential path
// called TriggerBatcher() after the loop; the batch path flushes
// internally via CreateBatch. This test gates regressions on that
// invariant.
func TestValidateBatch_PreservesBatcherFlush(t *testing.T) {
	f := setupValidateBatchFixture(t)

	txs := []*bt.Tx{
		buildValidateBatchTx(t, f, 200),
		buildValidateBatchTx(t, f, 201),
	}
	metas, errs := f.v.ValidateBatch(f.ctx, txs, spendHeight, WithSkipPolicyChecks(true))
	require.Len(t, metas, 2)
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	// Immediately after ValidateBatch returns, both txs must be findable
	// in the store without any trigger / flush.
	for i, tx := range txs {
		got := &meta.Data{}
		err := f.store.GetMeta(f.ctx, tx.TxIDChainHash(), got)
		require.NoError(t, err, "tx %d lookup failed immediately after ValidateBatch", i)
		require.NotNil(t, got)
	}
}
