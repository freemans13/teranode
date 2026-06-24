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
