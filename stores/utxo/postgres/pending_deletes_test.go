package postgres

import (
	"context"
	"fmt"
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

// newTestStoreWithFlag builds a Store with a fresh schema. flagOn controls
// PostgresUsePendingDeletesTable. Skips if no postgres is reachable.
func newTestStoreWithFlag(t *testing.T, flagOn bool) *Store {
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
	tSettings.UtxoStore.PostgresUsePendingDeletesTable = flagOn

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { store.Stop() })
	return store
}

// newPendingDeletesTestStore builds a Store with PostgresUsePendingDeletesTable=true
// using a fresh schema. Skips if no postgres is reachable.
func newPendingDeletesTestStore(t *testing.T) *Store {
	t.Helper()
	return newTestStoreWithFlag(t, true)
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

// TestPendingDeletes_SetConflictingFalseRemovesFromList tests C1:
// SetConflicting(false) clears delete_at_height and must also remove the hash
// from pending_deletes so the pruner cannot wrongly select a revived tx.
func TestPendingDeletes_SetConflictingFalseRemovesFromList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()

	// Stamp the tx into pending_deletes via the sweep (precondition).
	h := createMinedFullySpentTx(t, st)
	runOneSweep(t, st, h)

	// Clear conflicting — this is the C1 clear site.
	_, _, err := st.SetConflicting(ctx, []chainhash.Hash{h}, false)
	require.NoError(t, err)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "SetConflicting(false) must remove tx from pending_deletes (C1)")
}

// TestPendingDeletes_UnsetMinedRemovesFromList tests C5:
// SetMinedMulti with UnsetMined=true clears delete_at_height and must also
// remove the hash from pending_deletes so the pruner cannot wrongly select
// a reorged-out tx.
func TestPendingDeletes_UnsetMinedRemovesFromList(t *testing.T) {
	st := newPendingDeletesTestStore(t)
	ctx := context.Background()

	// Stamp the tx into pending_deletes via the sweep (precondition).
	// createMinedFullySpentTx mines the parent at blockID/blockHeight=100.
	h := createMinedFullySpentTx(t, st)
	runOneSweep(t, st, h)

	// Advance block height before the unset-mined call (mirrors TestUnsetMined).
	require.NoError(t, st.SetBlockHeight(150))

	// Unset mined (reorg) — this is the C5 clear site.
	_, err := st.SetMinedMulti(ctx, []*chainhash.Hash{&h}, utxo.MinedBlockInfo{
		BlockID:    100,
		UnsetMined: true,
	})
	require.NoError(t, err)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "SetMinedMulti(UnsetMined=true) must remove tx from pending_deletes (C5)")
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

// TestPendingDeletes_PruneEquivalence is the headline correctness gate for the whole
// feature (spec §8): with the pending_deletes flag ON the pruner must delete EXACTLY
// the same SET of tx hashes — the doomed (mined+fully-spent) parents — and leave EXACTLY
// the same survivors as with the flag OFF. A count-only check would pass even if the ON
// path pruned a different row than OFF (wrong set, same total), so the required assertion
// is require.ElementsMatch on the actual hash SETS.
//
// Each run builds its workload on a truly fresh per-flag DB. The spending-child txs that
// spendAllOutputs creates carry RANDOM txids (getSpendingTx uses rand.Uint64), so the raw
// hashes are NOT reproducible across the two runs — comparing the two runs' survivor lists
// directly would spuriously fail. Instead each run reports, against ITS OWN created hashes,
// which were pruned and which survived; the gate then asserts:
//   - in BOTH runs the pruned set equals exactly the doomed set (ElementsMatch) — i.e.
//     every doomed parent removed and NO survivor removed (catches "wrong set, same count");
//   - in BOTH runs the survivor set equals exactly created−doomed (ElementsMatch);
//   - the two runs prune the same number of rows and the same number survive.
//
// This proves the side-table path prunes exactly what the BRIN path prunes, set-for-set,
// and is non-vacuous (5 doomed parents removed, 7 non-doomed rows survive).
func TestPendingDeletes_PruneEquivalence(t *testing.T) {
	off := runWorkloadAndPrune(t, false)
	on := runWorkloadAndPrune(t, true)

	// Per-run set gate: pruned == doomed exactly, survivors == created−doomed exactly.
	// (assertRoleSets runs these for each flag and fails loudly identifying the flag.)
	off.assertRoleSets(t, false)
	on.assertRoleSets(t, true)

	// Cross-run equivalence: identical pruned-count and survivor-count. (The per-run
	// role-set checks above already prove WHICH logical rows each path removes; the raw
	// hashes differ only by the random child txids, so counts are the cross-run gate.)
	require.Equal(t, len(off.prunedHashes), len(on.prunedHashes),
		"flag ON must prune the same number of rows as flag OFF")
	require.Equal(t, len(off.survivingHashes), len(on.survivingHashes),
		"flag ON must leave the same number of survivors as flag OFF")
}

// pruneResult captures, for one workload run, the role-tagged hash sets (hex) so the
// equivalence gate can assert SETS (not counts) of what was pruned vs. what survived.
type pruneResult struct {
	doomedHashes    []string // mined+fully-spent parents that MUST be pruned
	createdHashes   []string // every tx this run inserted into txs (parents, survivors, children)
	prunedHashes    []string // created hashes absent from txs after prune (read back from DB)
	survivingHashes []string // created hashes still present in txs after prune (read back from DB)
}

// assertRoleSets is the spec §8 set assertion for a single run: the pruned set must equal
// exactly the doomed set, and the survivor set must equal exactly created−doomed. Using
// ElementsMatch (not counts) is what catches a "wrong set, same count" prune.
func (r pruneResult) assertRoleSets(t *testing.T, flagOn bool) {
	t.Helper()
	require.ElementsMatch(t, r.doomedHashes, r.prunedHashes,
		"flagOn=%v: pruner must delete exactly the doomed set (no survivor deleted, no doomed kept)", flagOn)

	expectedSurvivors := make([]string, 0, len(r.createdHashes))
	doomedSet := make(map[string]struct{}, len(r.doomedHashes))
	for _, h := range r.doomedHashes {
		doomedSet[h] = struct{}{}
	}
	for _, h := range r.createdHashes {
		if _, isDoomed := doomedSet[h]; !isDoomed {
			expectedSurvivors = append(expectedSurvivors, h)
		}
	}
	require.ElementsMatch(t, expectedSurvivors, r.survivingHashes,
		"flagOn=%v: survivors must be exactly created−doomed", flagOn)
	require.NotEmpty(t, r.survivingHashes, "flagOn=%v: workload must leave survivors (non-vacuous)", flagOn)
	require.NotEmpty(t, r.prunedHashes, "flagOn=%v: workload must prune something (non-vacuous)", flagOn)
}

// runWorkloadAndPrune builds a workload on a fresh store (flag ON or OFF), sweeps,
// prunes, and returns the role-tagged hash sets for the equivalence gate.
//
// It creates 5 mined+fully-spent txs (doomed) + 5 spending children + 2 mined-but-unspent
// survivor txs. After pruning it reads back, against the hashes this run created, which
// were removed (pruned) and which remain (survived).
func runWorkloadAndPrune(t *testing.T, flagOn bool) pruneResult {
	t.Helper()
	ctx := context.Background()
	st := newTestStoreWithFlag(t, flagOn)

	hexOf := func(b []byte) string { return fmt.Sprintf("%x", b) }

	// newUniqueMined creates and mines a tx with a unique P2PKH output so every txid
	// is distinct. Uniqueness is guaranteed by the monotonically increasing satoshi
	// amount; within a single run there is no collision risk.
	var seqSatoshis uint64 = 100_000
	newUniqueMined := func(height uint32) *bt.Tx {
		t.Helper()
		seqSatoshis += 1_000
		tx := bt.NewTx()
		//nolint:gosec
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", seqSatoshis)
		blockInfo := utxo.MinedBlockInfo{
			BlockID:        height,
			BlockHeight:    height,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}
		_, err := st.Create(ctx, tx, height, utxo.WithMinedBlockInfo(blockInfo))
		require.NoError(t, err)
		return tx
	}

	require.NoError(t, st.SetBlockHeight(110))

	// Create 5 mined+fully-spent txs (heights 100-104). Track their hashes as doomed.
	// Spent at heights 101-105; with retention=10, DAH = spent+1+10 = 112-116.
	// Prune height is chosen as 120 so all 5 have DAH <= 120. spendAllOutputs also
	// inserts a spending child tx (random txid) — a non-doomed row that must survive.
	var doomed [][]byte
	for h := uint32(100); h < 105; h++ {
		parent := newUniqueMined(h)
		spendAllOutputs(t, st, parent, h+1)
		doomed = append(doomed, parent.TxIDChainHash()[:])
	}

	// Create 2 survivor txs — mined but NOT spent, so sweep won't stamp them.
	for h := uint32(105); h < 107; h++ {
		_ = newUniqueMined(h)
	}

	// Snapshot EVERY hash now in txs as this run's created set (parents + survivors +
	// the random-txid spending children). We read it from the DB rather than tracking
	// return values because spendAllOutputs does not surface the child it inserts.
	var created [][]byte
	createdRows, err := st.pool.Query(ctx, `SELECT hash FROM txs`)
	require.NoError(t, err)
	for createdRows.Next() {
		var h []byte
		require.NoError(t, createdRows.Scan(&h))
		created = append(created, h)
	}
	require.NoError(t, createdRows.Err())
	createdRows.Close()

	// Sweep up to height 120 to stamp the tombstoned txs (DAH max = 116 <= 120).
	require.NoError(t, st.SetBlockHeight(120))
	_, err = procSweepUpTo(st, ctx, 120)
	require.NoError(t, err)

	// Every doomed tx must be stamped (sanity check that setup is correct).
	for _, h := range doomed {
		var dah *int32
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash=$1`, h).Scan(&dah))
		require.NotNil(t, dah,
			"doomed tx must have delete_at_height stamped before prune (flagOn=%v)", flagOn)
	}

	// Prune at height 120 (all 5 doomed txs have DAH <= 120).
	prunerSvc, err := st.GetPrunerService()
	require.NoError(t, err)
	_, err = prunerSvc.Prune(ctx, 120, "equivalence-test")
	require.NoError(t, err)

	// Read back, per created hash, whether it was pruned (absent) or survived (present).
	res := pruneResult{}
	for _, h := range doomed {
		res.doomedHashes = append(res.doomedHashes, hexOf(h))
	}
	for _, h := range created {
		res.createdHashes = append(res.createdHashes, hexOf(h))
		var exists bool
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM txs WHERE hash=$1)`, h).Scan(&exists))
		if exists {
			res.survivingHashes = append(res.survivingHashes, hexOf(h))
		} else {
			res.prunedHashes = append(res.prunedHashes, hexOf(h))
		}
	}
	return res
}

// TestPendingDeletes_BackfillOnEnable verifies the one-time backfill migration:
// when the PostgresUsePendingDeletesTable flag is turned ON against an existing DB
// that already has txs rows with non-NULL delete_at_height, those rows must be
// copied into pending_deletes and the BRIN index must be dropped.
//
// Steps:
//  1. Build a store with flag OFF (BRIN present, no pending_deletes).
//  2. Insert a tx with a non-NULL delete_at_height via direct UPDATE (simulates
//     a pre-existing stamp from an earlier run without the flag).
//  3. Re-init the schema with flag ON (same pool via createSchemaWithPoolFlag).
//  4. Assert: stamped hash is in pending_deletes with correct delete_at_height,
//     and the BRIN px_delete_at_height is gone.
//  5. Idempotency: call flag-ON init again; no error, no duplicate (PK),
//     and the BRIN absence is stable.
func TestPendingDeletes_BackfillOnEnable(t *testing.T) {
	ctx := context.Background()

	// Step 1: build a fresh store with flag OFF (BRIN present, no pending_deletes).
	st := newTestStoreWithFlag(t, false)
	pool := st.pool

	// Confirm precondition: BRIN present, no pending_deletes.
	var hasBrin bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.True(t, hasBrin, "precondition: BRIN must be present with flag OFF")

	var hasPD bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='pending_deletes' AND relkind='p')`).Scan(&hasPD))
	require.False(t, hasPD, "precondition: pending_deletes must not exist with flag OFF")

	// Step 2: insert a tx row with a non-NULL delete_at_height (simulate pre-existing stamp).
	// Use testExtendedTx to get a valid tx, Create it via the store (populates txs), then
	// UPDATE its delete_at_height directly.
	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	hashBytes := tx.TxIDChainHash()[:]
	const stampedDAH = int32(500)
	_, err = pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $1 WHERE hash = $2`, stampedDAH, hashBytes)
	require.NoError(t, err)

	// Confirm the stamp is in txs.
	var dah *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, hashBytes).Scan(&dah))
	require.NotNil(t, dah, "precondition: tx must have delete_at_height stamped in txs")
	require.Equal(t, stampedDAH, *dah)

	// Step 3: re-init schema with flag ON. This must backfill pending_deletes
	// (INSERT … SELECT from txs while BRIN still exists) then drop the BRIN.
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, true))

	// Step 4a: stamped hash must be in pending_deletes with correct delete_at_height.
	var pdDAH *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, hashBytes).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "stamped tx must be present in pending_deletes after backfill")
	require.Equal(t, stampedDAH, *pdDAH, "delete_at_height in pending_deletes must match txs value")

	// Step 4b: BRIN must be gone.
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.False(t, hasBrin, "BRIN px_delete_at_height must be dropped after flag-ON init")

	// Step 5: idempotency — call flag-ON init again; must succeed, no duplicate error,
	// and the hash is still present exactly once.
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, true), "second flag-ON init must be idempotent")

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash = $1`, hashBytes).Scan(&count))
	require.Equal(t, 1, count, "idempotent re-run must not duplicate the row in pending_deletes")

	// BRIN must still be absent.
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasBrin))
	require.False(t, hasBrin, "BRIN must remain absent after second flag-ON init")
}

// ---------------------------------------------------------------------------
// Design-C: mine-time DAH completion stamp (S6) — Task 1 tests
// ---------------------------------------------------------------------------

// mineOnLongestChain calls SetMinedMulti(OnLongestChain=true) for the tx hash at the
// given height. This is the S6 stamp trigger.
func mineOnLongestChain(t *testing.T, st *Store, h chainhash.Hash, height uint32) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(height))
	_, err := st.SetMinedMulti(ctx, []*chainhash.Hash{&h}, utxo.MinedBlockInfo{
		BlockID:        height,
		BlockHeight:    height,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
}

// hashInPendingDeletes returns true if the hash is present in the pending_deletes table.
func hashInPendingDeletes(t *testing.T, st *Store, h chainhash.Hash) bool {
	t.Helper()
	ctx := context.Background()
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_deletes WHERE hash=$1)`, h[:]).Scan(&exists))
	return exists
}

// pendingDeleteHeight returns the delete_at_height from pending_deletes for the hash.
func pendingDeleteHeight(t *testing.T, st *Store, h chainhash.Hash) int32 {
	t.Helper()
	ctx := context.Background()
	var dah int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`, h[:]).Scan(&dah))
	return dah
}

// retentionForTest returns the block-height retention delta for the given store's settings.
func retentionForTest(st *Store) int32 {
	return int32(st.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec
}

// runSweepOnly drives one sweep cycle up to height 105 WITHOUT asserting anything about
// the hash. Used to confirm the spends-driven sweep skips an unmined tx (the mined-gate).
func runSweepOnly(t *testing.T, st *Store) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(105))
	_, err := procSweepUpTo(st, ctx, 105)
	require.NoError(t, err)
}

// TestPendingDeletes_MineTimeStampClosesOrphan is the Design-C correctness gate (spec §7):
// a parent whose outputs are all spent while unmined is skipped by the spends-driven sweep
// (mined-gate). Once that parent is mined onto the longest chain (SetMinedMulti onLongestChain),
// the mine-time stamp (S6) must stamp delete_at_height and upsert into pending_deletes —
// closing the "spent-before-mined orphan" gap without any backstop.
func TestPendingDeletes_MineTimeStampClosesOrphan(t *testing.T) {
	st := newTestStoreWithFlag(t, true) // pending_deletes flag ON

	pTx := newUnminedSingleOutputTx(t, st) // unmined, 2 spendable outputs
	p := *pTx.TxIDChainHash()

	spendAllOutputs(t, st, pTx, 50) // spend at height 50 while P unmined

	runSweepOnly(t, st) // spends-driven sweep MUST skip P (mined-gate: block_ids IS NULL)

	require.False(t, hashInPendingDeletes(t, st, p), "unmined P must NOT be stamped by the spends-driven sweep")

	// Mine P onto the longest chain. S6 must stamp P now (mined + fully-spent).
	mineOnLongestChain(t, st, p, 200)

	require.True(t, hashInPendingDeletes(t, st, p), "mine-time stamp (S6) must stamp now-mined+fully-spent P into pending_deletes")
}

// TestPendingDeletes_MineTimeNotFullySpent verifies that a partially-spent tx (not all
// spendable outputs spent) is NOT stamped at mine time.
func TestPendingDeletes_MineTimeNotFullySpent(t *testing.T) {
	st := newTestStoreWithFlag(t, true)

	pTx := newUnminedSingleOutputTx(t, st) // 2 spendable outputs
	p := *pTx.TxIDChainHash()

	spendOneOutput(t, st, pTx, 0, 50) // spend ONLY output 0 — still partially spent

	mineOnLongestChain(t, st, p, 200)

	require.False(t, hashInPendingDeletes(t, st, p), "partially-spent tx must NOT be stamped at mine time")
}

// TestPendingDeletes_MineTimeCompletionHeightUsesGreatest verifies the DAH formula:
// GREATEST(max(spent_at_height), minedHeight) + 1 + retention. When the tx is mined
// much later than it was spent, DAH must derive from minedHeight (the larger value).
func TestPendingDeletes_MineTimeCompletionHeightUsesGreatest(t *testing.T) {
	st := newTestStoreWithFlag(t, true)

	pTx := newUnminedSingleOutputTx(t, st) // 2 spendable outputs
	p := *pTx.TxIDChainHash()

	spendAllOutputs(t, st, pTx, 50) // spend at height 50

	const minedHeight = uint32(5000) // mine much later than spend (GREATEST picks minedHeight)
	mineOnLongestChain(t, st, p, minedHeight)

	retention := retentionForTest(st)
	dah := pendingDeleteHeight(t, st, p)
	// GREATEST(50, 5000) = 5000; DAH = 5000 + 1 + retention
	require.Equal(t, int32(minedHeight)+1+retention, dah, //nolint:gosec
		"late mine: DAH must be derived from GREATEST(max_spent_height, minedHeight)")
}

// ---------------------------------------------------------------------------
// Design-C Task 2: prune-equivalence without backstop
// ---------------------------------------------------------------------------

// TestPendingDeletes_PruneEquivalenceNoBackstop is the TDD gate for backstop deletion
// (spec §7 "prune-equivalence"). It runs a deterministic workload that INCLUDES the
// spent-before-mined (S6) path and asserts that the pruner removes exactly the doomed
// set and leaves the live set — with NO backstop involved. This test must pass BEFORE
// the backstop is deleted (proving S6 covers the orphan gap alone), and must continue
// to pass AFTER the backstop is deleted.
//
// Workload:
//   - 5 "doomed" parents: each is created unmined, all outputs are spent while still
//     unmined (the S6 / spent-before-mined ordering), then the parent is mined onto
//     the longest chain. S6 stamps delete_at_height at mine time.
//   - 3 "live" parents: mined but NOT fully spent (one output spent, one kept) so
//     they must NOT be pruned.
//
// After advancing height and calling Prune, the test asserts:
//   - Every doomed parent is absent from txs (pruned).
//   - Every live parent is still present in txs (survived).
func TestPendingDeletes_PruneEquivalenceNoBackstop(t *testing.T) {
	ctx := context.Background()
	st := newTestStoreWithFlag(t, true) // pending_deletes flag ON

	const (
		spendHeight = uint32(50)  // all outputs spent at this height (while parents unmined)
		mineHeight  = uint32(200) // parents mined at this height (GREATEST picks mineHeight)
	)

	// Build 5 doomed parents: spent-while-unmined, then mined (S6 path).
	// S6 stamps delete_at_height at mineHeight. DAH = mineHeight+1+retention <= pruneHeight.
	var doomed [][]byte
	for i := 0; i < 5; i++ {
		pTx := newUniqueUnminedTx(t, st)
		p := *pTx.TxIDChainHash()
		spendAllOutputs(t, st, pTx, spendHeight) // spend all outputs while unmined
		mineOnLongestChain(t, st, p, mineHeight) // S6 stamps DAH here
		doomed = append(doomed, p[:])
	}

	// Build 3 live parents: mined but NOT fully spent (only one output spent).
	// The sweep will never stamp them; they must survive prune.
	var live [][]byte
	for i := 0; i < 3; i++ {
		pTx := newUniqueUnminedTx(t, st)
		mineTx(t, st, pTx, mineHeight)
		spendOneOutput(t, st, pTx, 0, mineHeight+1) // only output 0 spent → still partially spent
		live = append(live, pTx.TxIDChainHash()[:])
	}

	// All 5 doomed parents must already be stamped by S6 (no sweep needed, no backstop).
	for _, h := range doomed {
		var dah *int32
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash=$1`, h).Scan(&dah))
		require.NotNil(t, dah, "doomed parent must be S6-stamped before prune (no backstop)")
	}

	// Advance height and prune. 500 is well past all DAHs (mineHeight+1+retention <= ~210+10=221).
	require.NoError(t, st.SetBlockHeight(500))
	prunerSvc, err := st.GetPrunerService()
	require.NoError(t, err)
	_, err = prunerSvc.Prune(ctx, 500, "equivalence-no-backstop")
	require.NoError(t, err)

	// All doomed parents must be gone from txs.
	doomedHex := make([]string, 0, len(doomed))
	prunedHex := make([]string, 0, len(doomed))
	for _, h := range doomed {
		doomedHex = append(doomedHex, fmt.Sprintf("%x", h))
		var exists bool
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM txs WHERE hash=$1)`, h).Scan(&exists))
		if !exists {
			prunedHex = append(prunedHex, fmt.Sprintf("%x", h))
		}
	}
	require.ElementsMatch(t, doomedHex, prunedHex,
		"all spent-before-mined parents must be pruned by S6 alone (no backstop)")

	// All live parents must still be present.
	for _, h := range live {
		var exists bool
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM txs WHERE hash=$1)`, h).Scan(&exists))
		require.True(t, exists, "partially-spent live parent must survive prune: %x", h)
	}
}
