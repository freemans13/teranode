package netsync

// Task 9 — crash-replay idempotency on the fail-closed spend leg.
//
// The T5 fail-closed change dropped the ErrTxConflicting transient tolerance on
// the inline below-checkpoint path. This test proves a below-cp block re-
// delivered after a partial/crashed prior attempt does NOT spuriously hard-fail
// under the flag:
//
//	(a) createUtxos tolerates ErrTxExists on the second Create pass (handle_block.go
//	    swallows ErrTxExists and re-stamps mined info via SetMinedMulti).
//	(b) the same-spender re-spend of an already-spent-by-THIS-tx outpoint returns
//	    idempotent success, NOT ErrSpent. The sql store already handles this: when
//	    the stored spending_data equals the replayed tx's spending_data it treats
//	    the spend as success without an UPDATE (stores/utxo/sql/sql.go ~:2264).
//	    No store code change was needed — this test locks that behaviour in.
//	(c) the block does not hard-fail: the full inline prepareSubtrees pipeline
//	    (create + fail-closed spend) succeeds on the replay.
//
// Parts (d) commitBlock/AddBlock swallowing ErrBlockExists and (e) StoreBlock not
// upserting quick_validated are pinned by TestStoreBlock_NoUpsertQuickValidated
// in stores/blockchain/sql (StoreBlock is a plain INSERT; a duplicate hash raises
// a unique-constraint violation translated to ErrBlockExists and never rewrites
// the row).

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newReplaySyncManager wires an inline (non-unified) fail-closed SyncManager
// backed by a real sqlitememory store so prepareSubtrees does genuine
// Create/Spend work against durable rows that survive between the two delivery
// attempts.
func newReplaySyncManager(t *testing.T, ctx context.Context, store utxo.Store) *SyncManager {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = true
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	tSettings.Legacy.StoreBatcherSize = 2
	tSettings.Legacy.StoreBatcherConcurrency = 2
	tSettings.Legacy.SpendBatcherSize = 2
	tSettings.Legacy.SpendBatcherConcurrency = 2

	// Each delivery attempt calls AssignBlockID at most once (reuseBlockIDFromUTXO
	// short-circuits the second attempt once the tx rows carry a BlockID).
	mockBC := &blockchain.Mock{}
	mockBC.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(101), nil).Maybe()

	return &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		utxoStore:        store,
		blockchainClient: mockBC,
		validationClient: makeSpendValidator(store),
		subtreeStore:     memory.New(),
		ctx:              ctx,
	}
}

// TestLegacyFailClosed_CrashReplay_Idempotent commits a below-cp block through the
// inline fail-closed pipeline, then re-delivers the identical block and asserts
// that the second attempt succeeds (create tolerates ErrTxExists, the same-spender
// re-spend is idempotent, and the block does not hard-fail).
func TestLegacyFailClosed_CrashReplay_Idempotent(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}

	// Build a corpus and use only the first block (height 500) for the replay:
	// coinbase + tx_a + tx_b, where the block also self-spends within itself is
	// not required — we just need real Create+Spend rows to replay against.
	corpus := buildParityCorpus(t)

	// Use blocks 500 and 501 so we exercise both a cross-block spend (tx_c spends
	// tx_a from block 500) and a same-block spend (tx_d spends tx_c in block 501)
	// on replay.
	tSettings, _ := newOutpointOnlySettings(t, true, true, 1000)
	storeURL, err := url.Parse("sqlitememory:///crash_replay")
	require.NoError(t, err)
	store, err := utxosql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	sm := newReplaySyncManager(t, ctx, store)

	replayBlocks := corpus.blocks[:2] // heights 500, 501

	// First delivery: fresh commit.
	for _, block := range replayBlocks {
		_, _, _, prepErr := sm.prepareSubtrees(ctx, block)
		require.NoError(t, prepErr, "first delivery must succeed at height %d", block.Height())
	}

	// Capture spend state after the first delivery so we can prove the replay does
	// not corrupt or double-count it.
	spentHash := corpus.nonCBHashes[0] // tx_a, spent by tx_c in block 501
	before, err := store.Get(ctx, &spentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotEmpty(t, before.SpendingDatas, "tx_a must be spent after first delivery")
	require.NotNil(t, before.SpendingDatas[0], "tx_a spending data must be recorded")
	firstSpender := *before.SpendingDatas[0].TxID

	// Second delivery: identical blocks, re-delivered after a simulated crash.
	// This must NOT hard-fail: Create sees ErrTxExists (tolerated) and Spend sees
	// the same spending data (idempotent success, not ErrSpent).
	for _, block := range replayBlocks {
		_, _, _, prepErr := sm.prepareSubtrees(ctx, block)
		require.NoError(t, prepErr, "replay delivery must be idempotent at height %d", block.Height())
	}

	// Spend state is unchanged: same spender, still exactly one spending record.
	after, err := store.Get(ctx, &spentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotEmpty(t, after.SpendingDatas, "tx_a must remain spent after replay")
	require.NotNil(t, after.SpendingDatas[0], "tx_a spending data must remain recorded after replay")
	require.Equal(t, firstSpender, *after.SpendingDatas[0].TxID,
		"replay must not change the spender identity (idempotent same-spender re-spend)")
}

// TestSqlStore_SameSpenderReSpend_Idempotent is the focused store-level lock-in for
// Task 9(b): a Spend of an outpoint already spent by THE SAME transaction returns
// idempotent success, not ErrSpent. This is the invariant the fail-closed replay
// depends on (a genuine different-spender clash still returns ErrSpent — that is
// the fail-closed hard-fail path, covered by the fail_closed tests).
func TestSqlStore_SameSpenderReSpend_Idempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings, _ := newOutpointOnlySettings(t, true, true, 1000)

	storeURL, err := url.Parse("sqlitememory:///same_spender")
	require.NoError(t, err)
	store, err := utxosql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	corpus := buildParityCorpus(t)

	// Create tx_a (block 500) and tx_c (block 501, spends tx_a[0]).
	txABt, err := wireMsgTxToBt(t, corpus.blocks[0].Transactions()[1].MsgTx()) // tx_a
	require.NoError(t, err)
	txCBt, err := wireMsgTxToBt(t, corpus.blocks[1].Transactions()[1].MsgTx()) // tx_c
	require.NoError(t, err)

	_, err = store.Create(ctx, txABt, 500,
		utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 101, BlockHeight: 500}),
	)
	require.NoError(t, err)

	spend := func() error {
		_, e := store.Spend(ctx, txCBt, 501, utxo.IgnoreFlags{SkipUTXOHashCheck: true, IgnoreLocked: true})
		return e
	}

	require.NoError(t, spend(), "first spend of tx_a[0] by tx_c must succeed")
	require.NoError(t, spend(), "same-spender re-spend must be idempotent, not ErrSpent")

	// Sanity: the spender recorded is tx_c.
	m, err := store.Get(ctx, txABt.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.NotEmpty(t, m.SpendingDatas)
	require.NotNil(t, m.SpendingDatas[0])
	require.Equal(t, *txCBt.TxIDChainHash(), *m.SpendingDatas[0].TxID, "spender must be tx_c")

	_ = bt.NewTx // keep the go-bt import stable for future spend-shape assertions
}
