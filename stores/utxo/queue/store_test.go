package queue

import (
	"context"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testDSN = "postgresql://teranode:teranode@localhost:5432/teranode_test"

func setupTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()

	// Clean slate -- drop all tables (including old v3 tables if present)
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgresqueue"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Stop()
	})

	return store, ctx
}

// testExtendedTx returns an extended-format transaction parsed from a known hex string.
// This is the same tx used in the v3 sql_test.go tests.
// It is NOT a coinbase. It has 1 input (prevSatoshis=5BTC) and 2 outputs (5.56M + 44.44M sat), fee=0.
func testExtendedTx(t *testing.T) *bt.Tx {
	t.Helper()
	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	return tx
}

func TestSchemaCreation(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Verify all 7 snapshot tables exist by querying them
	tables := []string{"transactions", "inputs", "outputs", "spends", "tx_state", "block_ids", "conflicting_children"}
	for _, table := range tables {
		var count int
		err := store.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		require.NoError(t, err, "table %s should exist", table)
	}

	// Verify partitions exist (64 per table = 448 total)
	for _, table := range tables {
		var partCount int
		err := store.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_inherits
			JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
			WHERE parent.relname = $1
		`, table).Scan(&partCount)
		require.NoError(t, err, "should be able to count partitions for %s", table)
		require.Equal(t, 64, partCount, "table %s should have 64 partitions", table)
	}

	// Verify tx_state partial indexes exist
	var idxCount int
	err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_indexes
		WHERE indexname IN ('px_unmined_since', 'px_delete_at_height')
	`).Scan(&idxCount)
	require.NoError(t, err)
	require.Equal(t, 2, idxCount, "tx_state should have 2 partial indexes")
}

func TestHealth(t *testing.T) {
	store, ctx := setupTestStore(t)

	code, name, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, code)
	require.Equal(t, "Queue UTXO Store", name)
}

func TestBlockState(t *testing.T) {
	store, _ := setupTestStore(t)

	require.Equal(t, uint32(0), store.GetBlockHeight())
	require.Equal(t, uint32(0), store.GetMedianBlockTime())

	err := store.SetBlockHeight(12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), store.GetBlockHeight())

	err = store.SetMedianBlockTime(1700000000)
	require.NoError(t, err)
	require.Equal(t, uint32(1700000000), store.GetMedianBlockTime())

	state := store.GetBlockState()
	require.Equal(t, uint32(12345), state.Height)
	require.Equal(t, uint32(1700000000), state.MedianTime)
}

// ---------------------------------------------------------------------------
// Task 3 + 4 tests
// ---------------------------------------------------------------------------

func TestCreateAndGet(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as unmined (no block info)
	md, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	require.NotNil(t, md)

	// Verify basic metadata
	require.False(t, md.IsCoinbase, "should not be coinbase")
	require.Equal(t, uint64(259), md.SizeInBytes)
	require.Equal(t, blockHeight, md.UnminedSince, "unmined tx should have unminedSince = blockHeight")
	require.False(t, md.Conflicting)
	require.False(t, md.Locked)
	require.False(t, md.Frozen)

	// Get with all fields
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Verify transaction metadata
	require.Equal(t, md.SizeInBytes, got.SizeInBytes)
	require.Equal(t, md.Fee, got.Fee)
	require.False(t, got.IsCoinbase)
	require.Equal(t, blockHeight, got.UnminedSince)
	require.False(t, got.Conflicting)
	require.False(t, got.Locked)
	require.False(t, got.Frozen)

	// Verify Tx reconstruction
	require.NotNil(t, got.Tx, "Get with default fields should include Tx")
	require.Equal(t, tx.Version, got.Tx.Version)
	require.Equal(t, tx.LockTime, got.Tx.LockTime)
	require.Equal(t, len(tx.Inputs), len(got.Tx.Inputs), "input count mismatch")
	require.Equal(t, len(tx.Outputs), len(got.Tx.Outputs), "output count mismatch")

	// Verify input details
	for i, input := range tx.Inputs {
		gotInput := got.Tx.Inputs[i]
		require.Equal(t, input.PreviousTxOutIndex, gotInput.PreviousTxOutIndex)
		require.Equal(t, input.SequenceNumber, gotInput.SequenceNumber)
	}

	// Verify output details
	for i, output := range tx.Outputs {
		if output == nil {
			continue
		}
		gotOutput := got.Tx.Outputs[i]
		require.Equal(t, output.Satoshis, gotOutput.Satoshis)
	}

	// Get with only specific fields
	got2, err := store.Get(ctx, txHash, fields.Fee, fields.SizeInBytes)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, md.Fee, got2.Fee)
	require.Equal(t, md.SizeInBytes, got2.SizeInBytes)
	require.Nil(t, got2.Tx, "should not include Tx when not requested")
}

func TestCreateAndGetMined(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as mined
	blockInfo := utxo.MinedBlockInfo{
		BlockID:     42,
		BlockHeight: 100,
		SubtreeIdx:  7,
	}
	md, err := store.Create(ctx, tx, blockHeight, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, uint32(0), md.UnminedSince, "mined tx should have zero unminedSince")

	// Get and verify block IDs
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.BlockIDs, fields.Fee)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint32(0), got.UnminedSince, "mined tx should have zero unminedSince from DB")
	require.Len(t, got.BlockIDs, 1)
	require.Equal(t, uint32(42), got.BlockIDs[0])
	require.Len(t, got.BlockHeights, 1)
	require.Equal(t, uint32(100), got.BlockHeights[0])
	require.Len(t, got.SubtreeIdxs, 1)
	require.Equal(t, 7, got.SubtreeIdxs[0])
}

func TestGetNonExistent(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	got, err := store.Get(ctx, fakeHash)
	require.Error(t, err)
	require.Nil(t, got)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "should return ErrTxNotFound, got: %v", err)
}

func TestCreateWithLocked(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithLocked(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Locked)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "locked flag should be true in DB")
}

func TestCreateWithConflicting(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithConflicting(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Conflicting)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "conflicting flag should be true in DB")
}

func TestCreateWithFrozen(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithFrozen(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Frozen)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.True(t, got.Frozen, "frozen flag should be true in DB")
}

func TestCreateDuplicate(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	// Second create should fail with TxExists
	_, err = store.Create(ctx, tx, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists), "should return ErrTxExists, got: %v", err)
}

func TestGetSpendNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     fakeHash,
		Vout:     0,
		UTXOHash: fakeHash,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int(utxo.Status_NOT_FOUND), resp.Status)
}

func TestGetSpendOK(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	// Compute UTXO hash for output 0
	txHash := tx.TxIDChainHash()
	utxoHashes, err := utxo.GetUtxoHashes(tx, txHash)
	require.NoError(t, err)
	require.True(t, len(utxoHashes) > 0)

	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     txHash,
		Vout:     0,
		UTXOHash: utxoHashes[0],
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Non-coinbase: coinbaseSpendingHeight=0, so status should be OK (unspent).
	require.Equal(t, int(utxo.Status_OK), resp.Status, "non-coinbase unspent output should be OK")
}

func TestGetMeta(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	var data meta.Data
	err = store.GetMeta(ctx, txHash, &data)
	require.NoError(t, err)
	require.False(t, data.IsCoinbase)
	require.Equal(t, uint64(259), data.SizeInBytes)
}

func TestBatchDecorate(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	items := []*utxo.UnresolvedMetaData{
		{Hash: *txHash, Idx: 0},
	}

	err = store.BatchDecorate(ctx, items, fields.Fee, fields.SizeInBytes, fields.IsCoinbase)
	require.NoError(t, err)
	require.Nil(t, items[0].Err)
	require.NotNil(t, items[0].Data)
	require.False(t, items[0].Data.IsCoinbase)
	require.Equal(t, uint64(259), items[0].Data.SizeInBytes)
}

func TestBatchDecorateNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *fakeHash, Idx: 0},
	}

	err = store.BatchDecorate(ctx, items, fields.Fee)
	require.NoError(t, err) // BatchDecorate itself does not error on not-found
	require.NotNil(t, items[0].Err, "item should have not-found error")
	require.True(t, errors.Is(items[0].Err, errors.ErrTxNotFound))
}

func TestGetWithTxInpoints(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.TxInpoints)
	require.NoError(t, err)
	require.NotNil(t, got)
	// Coinbase tx has empty inpoints (no real parents)
	require.NotNil(t, got.TxInpoints)
}

func TestGetWithUtxos(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Utxos, fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.SpendingDatas, "should have SpendingDatas slice")
	require.Equal(t, len(tx.Outputs), len(got.SpendingDatas), "should have one SpendingData per output")
	// All outputs should be unspent (nil SpendingData)
	for i, sd := range got.SpendingDatas {
		require.Nil(t, sd, "output %d should be unspent (nil spending data)", i)
	}
}

// ---------------------------------------------------------------------------
// Test helpers for Spend + Conflicting tests
// ---------------------------------------------------------------------------

// getSpendingTx creates a transaction that spends the given output indices from parentTx.
// Each input gets a dummy unlocking script so the tx can be stored via Create.
func getSpendingTx(t *testing.T, parentTx *bt.Tx, vOut ...uint32) *bt.Tx {
	t.Helper()
	newTx := bt.NewTx()

	for _, outIdx := range vOut {
		err := newTx.From(
			parentTx.TxIDChainHash().String(),
			outIdx,
			parentTx.Outputs[outIdx].LockingScript.String(),
			parentTx.Outputs[outIdx].Satoshis,
		)
		require.NoError(t, err)
	}

	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1000+1)
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1000+1)
	_ = newTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	// Ensure each input has a non-nil unlocking script for the NOT NULL constraint.
	for _, input := range newTx.Inputs {
		if input.UnlockingScript == nil || len(*input.UnlockingScript) == 0 {
			input.UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		}
	}

	return newTx
}

// ---------------------------------------------------------------------------
// Task 5 + 6 tests
// ---------------------------------------------------------------------------

func TestSpendOutput(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create the parent transaction.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build a spending transaction that spends output 0 of the parent.
	spendTx := getSpendingTx(t, parentTx, 0)

	// Spend should succeed.
	spends, err := store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)
	require.Len(t, spends, 1, "spending tx has 1 input, so 1 spend")

	// Verify the output is now spent via Get with Utxos field.
	parentHash := parentTx.TxIDChainHash()
	got, err := store.Get(ctx, parentHash, fields.Utxos, fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.SpendingDatas, "should have SpendingDatas slice")

	// Output 0 should be spent.
	require.NotNil(t, got.SpendingDatas[0], "output 0 should be spent")
	require.Equal(t, spendTx.TxIDChainHash().String(), got.SpendingDatas[0].TxID.String())
	require.Equal(t, 0, got.SpendingDatas[0].Vin)

	// Other outputs should still be unspent.
	for i := 1; i < len(got.SpendingDatas); i++ {
		require.Nil(t, got.SpendingDatas[i], "output %d should still be unspent", i)
	}
}

func TestDoubleSpend(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create the parent transaction.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build two competing spending transactions, both spending output 0.
	spendTxA := getSpendingTx(t, parentTx, 0)
	spendTxB := getSpendingTx(t, parentTx, 0)

	// First spend should succeed.
	_, err = store.Spend(ctx, spendTxA, blockHeight+1)
	require.NoError(t, err)

	// Second spend should fail with ErrSpent.
	spends, err := store.Spend(ctx, spendTxB, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent), "should return ErrSpent, got: %v", err)
	require.NotEmpty(t, spends)
	require.NotNil(t, spends[0].ConflictingTxID, "should have ConflictingTxID set")
	require.Equal(t, spendTxA.TxIDChainHash().String(), spends[0].ConflictingTxID.String(),
		"ConflictingTxID should be the first spending tx")
}

func TestSpendIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// First spend.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	// Same spend again should be idempotent (no error, same spending data).
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)
}

func TestSpendNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)
	blockHeight := uint32(100)

	err := store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build a spending tx that references a non-existent parent.
	fakeTx := bt.NewTx()
	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000099")
	require.NoError(t, err)
	_ = fakeTx.From(fakeHash.String(), 0, "76a91488ac", 5000)
	_ = fakeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)

	_, err = store.Spend(ctx, fakeTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "should return ErrTxNotFound, got: %v", err)
}

func TestSpendFrozenTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithFrozen(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "should return ErrFrozen, got: %v", err)
}

func TestSpendLockedTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithLocked(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// Without ignore flag: should fail with ErrLocked.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxLocked), "should return ErrLocked, got: %v", err)

	// With IgnoreLocked: should succeed.
	_, err = store.Spend(ctx, spendTx, blockHeight+1, utxo.IgnoreFlags{IgnoreLocked: true})
	require.NoError(t, err)
}

func TestSpendConflictingTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithConflicting(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// Without ignore flag: should fail with ErrTxConflicting.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "should return ErrTxConflicting, got: %v", err)

	// With IgnoreConflicting: should succeed.
	_, err = store.Spend(ctx, spendTx, blockHeight+1, utxo.IgnoreFlags{IgnoreConflicting: true})
	require.NoError(t, err)
}

func TestSetLocked(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create with locked=true.
	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithLocked(true))
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Verify locked=true.
	got, err := store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "should be locked")

	// SetLocked(false).
	err = store.SetLocked(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	// Verify locked=false.
	got, err = store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, got.Locked, "should be unlocked after SetLocked(false)")

	// SetLocked(true) again.
	err = store.SetLocked(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "should be locked again")
}

func TestSetConflicting(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Initially not conflicting.
	got, err := store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, got.Conflicting, "should not be conflicting initially")

	// Set conflicting=true.
	affectedSpends, childTxHashes, err := store.SetConflicting(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)
	// Parent spends = inputs of the conflicting tx (1 input).
	require.Len(t, affectedSpends, 1, "should have 1 affected parent spend")
	// No child spends since outputs are unspent.
	require.Empty(t, childTxHashes, "no spending children expected")

	// Verify conflicting=true.
	got, err = store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "should be conflicting after SetConflicting(true)")

	// Set conflicting=false.
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, got.Conflicting, "should not be conflicting after SetConflicting(false)")
}

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as unmined.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Verify unmined_since is set.
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, blockHeight, got.UnminedSince, "should be unmined")

	// Mark on longest chain (mined).
	err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(0), got.UnminedSince, "should be mined (unmined_since=0)")

	// Mark off longest chain (unmined again).
	err = store.SetBlockHeight(200)
	require.NoError(t, err)
	err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(200), got.UnminedSince, "should be unmined again at current block height")
}

func TestSetConflictingWithSpendingChildren(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create parent tx.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Create and execute a spending tx that spends output 0.
	spendTx := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spendTx, blockHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Set parent as conflicting -- should return the spending child.
	_, childTxHashes, err := store.SetConflicting(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)
	require.Len(t, childTxHashes, 1, "should have 1 spending child tx")
	require.Equal(t, spendTx.TxIDChainHash().String(), childTxHashes[0].String())
}

func TestUnspend(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	spends, err := store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	// Verify spent.
	parentHash := parentTx.TxIDChainHash()
	got, err := store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, got.SpendingDatas[0], "output 0 should be spent")

	// Unspend.
	err = store.Unspend(ctx, spends)
	require.NoError(t, err)

	// Verify unspent.
	got, err = store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.Nil(t, got.SpendingDatas[0], "output 0 should be unspent after Unspend")
}

// Ensure unused imports are used.
var _ = meta.Data{}
var _ = rand.Uint64
