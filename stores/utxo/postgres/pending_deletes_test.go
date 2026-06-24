package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestSchema_PendingDeletes_FlagOn(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_deletes CASCADE`)
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, true)) // flag ON

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_deletes_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 8, n, "8 pending_deletes leaves")

	var hasBrin bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.False(t, hasBrin, "BRIN dropped when flag on")
}

// newPendingDeletesTestStore builds a Store with PostgresUsePendingDeletesTable=true
// using a fresh schema. Skips if no postgres is reachable.
func newPendingDeletesTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP TABLE IF EXISTS pending_deletes CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw, dah_watermark, dah_part_watermark, dah_sweep_control,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.PostgresUsePendingDeletesTable = true

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { store.Stop() })
	return store
}

func TestPendingDeletes_SweepStampPopulatesList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(105))

	// Create + mine + fully-spend a tx so sweep will stamp it.
	parent := newMinedSingleOutputTx(t, st, 100)
	spendAllOutputs(t, st, parent, 101)

	// Run one sweep cycle.
	_, err := procSweepUpTo(st, ctx, 105)
	require.NoError(t, err)

	// Assert the hash is in pending_deletes.
	var dah *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`,
		parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "stamped tx must be in pending_deletes after sweep")

	// Expire the preservation.
	require.NoError(t, st.ProcessExpiredPreservations(ctx, uint32(125)))

	// Row should be back in pending_deletes with updated DAH.
	var dah2 *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`,
		parent.TxIDChainHash()[:]).Scan(&dah2))
	require.NotNil(t, dah2, "after ProcessExpiredPreservations, tx should be back in pending_deletes")
}

func TestPendingDeletes_MinedZeroSpendable(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(100))

	// Create a zero-spendable (OP_RETURN only) tx — stamped inline when mined
	// via SetMinedMulti with OnLongestChain=true (the S3 site).
	tx := bt.NewTx()
	// OP_RETURN output (unspendable: OP_RETURN opcode 0x6a).
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x6a}),
	})

	// Create unmined first (block_ids NULL so spendable_count=0 is set but
	// delete_at_height is not yet stamped).
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	// Now mine via SetMinedMulti — this is the S3 stamp site: zero-spendable
	// tx gets delete_at_height stamped inline and, with flag ON, also inserted
	// into pending_deletes.
	h := tx.TxIDChainHash()
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	var dah *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`,
		h[:]).Scan(&dah))
	require.NotNil(t, dah, "zero-spendable mined tx must be in pending_deletes after inline stamp (S3)")
}

func TestPendingDeletes_ConflictingStamp(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(100))

	parent := testExtendedTx(t)
	_, err := st.Create(ctx, parent, 100)
	require.NoError(t, err)

	h := parent.TxIDChainHash()
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, true)
	require.NoError(t, err)

	var dah *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`,
		h[:]).Scan(&dah))
	require.NotNil(t, dah, "conflicting tx must be in pending_deletes after SetConflicting(true) (S4)")
}

// createMinedFullySpentTx creates a mined, fully-spent parent tx and returns its
// chain hash. The sweep will stamp it with delete_at_height once runOneSweep is called.
func createMinedFullySpentTx(t *testing.T, st *Store) chainhash.Hash {
	t.Helper()
	require.NoError(t, st.SetBlockHeight(100))
	parent := newMinedSingleOutputTx(t, st, 100)
	spendAllOutputs(t, st, parent, 101)
	return *parent.TxIDChainHash()
}

// runOneSweep drives one sweep cycle up to height 105 and asserts the given hash
// was stamped into pending_deletes.
func runOneSweep(t *testing.T, st *Store, h chainhash.Hash) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(105))
	_, err := procSweepUpTo(st, ctx, 105)
	require.NoError(t, err)

	var dah *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`, h[:]).Scan(&dah))
	require.NotNil(t, dah, "setup: hash must be stamped in pending_deletes before reorg test")
}

// unspendTx reverses all spends for the outputs of the tx identified by h.
// It queries the spends table to reconstruct the []*utxo.Spend slice needed by Unspend.
func unspendTx(t *testing.T, st *Store, h chainhash.Hash) {
	t.Helper()
	ctx := context.Background()

	rows, err := st.pool.Query(ctx,
		`SELECT prev_output_idx, spending_data FROM spends WHERE prev_tx_hash = $1`, h[:])
	require.NoError(t, err)
	defer rows.Close()

	var spends []*utxo.Spend
	for rows.Next() {
		var vout int32
		var sdBytes []byte
		require.NoError(t, rows.Scan(&vout, &sdBytes))
		sd, err := spendpkg.NewSpendingDataFromBytes(sdBytes)
		require.NoError(t, err)
		hCopy := h
		spends = append(spends, &utxo.Spend{
			TxID:         &hCopy,
			Vout:         uint32(vout),
			SpendingData: sd,
		})
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, spends, "setup: parent tx must have at least one spend row before Unspend")

	require.NoError(t, st.Unspend(ctx, spends))
}

// TestPendingDeletes_UnspendRemovesFromList is the reorg test for C6:
// after a tx is stamped into pending_deletes by the sweep, Unspend must
// remove it so the pruner can never wrongly select it.
func TestPendingDeletes_UnspendRemovesFromList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()

	h := createMinedFullySpentTx(t, st)
	runOneSweep(t, st, h)

	// Reorg: reverse the spend.
	unspendTx(t, st, h)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "revived tx must be removed from pending_deletes (no wrong-delete)")
}

// TestPendingDeletes_SetLockedRemovesFromList tests C2: SetLocked(true) clears DAH
// and must also remove the hash from pending_deletes.
func TestPendingDeletes_SetLockedRemovesFromList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()

	h := createMinedFullySpentTx(t, st)
	runOneSweep(t, st, h)

	// Lock the tx — SetLocked(true) clears delete_at_height.
	require.NoError(t, st.SetLocked(ctx, []chainhash.Hash{h}, true))

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "locked tx must be removed from pending_deletes (C2)")
}

// TestPendingDeletes_MarkOffLongestChainRemovesFromList tests C3:
// MarkTransactionsOnLongestChain(false) clears DAH and must remove from pending_deletes.
func TestPendingDeletes_MarkOffLongestChainRemovesFromList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()

	h := createMinedFullySpentTx(t, st)
	runOneSweep(t, st, h)

	// Reorg: mark the tx as off the longest chain.
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{h}, false))

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "reorged tx must be removed from pending_deletes (C3)")
}

func TestSchema_PendingDeletes_FlagOff(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Ensure clean slate: drop pending_deletes if a prior FlagOn test left it.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_deletes CASCADE`)
	// Ensure BRIN is absent so we can confirm creation.
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_delete_at_height`)

	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false)) // flag OFF

	// No pending_deletes leaves should exist.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_deletes_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 0, n, "no pending_deletes leaves when flag off")

	// BRIN index must be present.
	var hasBrin bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.True(t, hasBrin, "BRIN present when flag off")
}
