package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSetMinedMulti_DeletesPendingUnmined_CTEBranch verifies that when the
// PostgresUsePendingDeletesTable flag is ON (CTE branch), SetMinedMulti removes
// the tx hash from pending_unmined atomically with the txs UPDATE.
//
// Precondition: Task 4 must be complete — Create() inserts an unmined tx into
// pending_unmined. If that row is absent the test fails loudly.
func TestSetMinedMulti_DeletesPendingUnmined_CTEBranch(t *testing.T) {
	ctx := context.Background()
	// flag=true forces the CTE branch in SetMinedMulti (pending_deletes flag ON).
	st := newTestStoreWithFlag(t, true)
	require.NoError(t, st.SetBlockHeight(100))

	// Step 1: Create an unmined tx. Task 4 inserts a row into pending_unmined.
	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	hashBytes := h[:]

	// Step 2: Precondition — pending_unmined row must exist (Task 4 wrote it).
	var unminedSince int32
	err = st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, hashBytes).Scan(&unminedSince)
	require.NoError(t, err, "pending_unmined row must exist after Create (Task 4 precondition)")
	require.Equal(t, int32(100), unminedSince)

	// Step 3: Mine via SetMinedMulti (CTE branch, OnLongestChain=true, flag ON).
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// Step 4: pending_unmined row must be deleted.
	var exists bool
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "pending_unmined row must be deleted after SetMinedMulti (CTE branch)")

	// Step 5: txs row must survive with block_ids updated.
	var blockIDs []int32
	err = st.pool.QueryRow(ctx,
		`SELECT block_ids FROM txs WHERE hash=$1`, hashBytes).Scan(&blockIDs)
	require.NoError(t, err, "txs row must still exist after mine")
	require.Equal(t, 1, len(blockIDs))
	require.Equal(t, int32(100), blockIDs[0])

	// Step 6: unmined_since must be NULL in txs after mine.
	var unminedSinceAfter *int32
	err = st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, hashBytes).Scan(&unminedSinceAfter)
	require.NoError(t, err)
	require.Nil(t, unminedSinceAfter, "unmined_since must be NULL in txs after mine")
}

// TestSetMinedMulti_DeletesPendingUnmined_PlainBranch verifies that when the
// PostgresUsePendingDeletesTable flag is OFF (plain UPDATE branch), SetMinedMulti
// also removes the tx hash from pending_unmined.
func TestSetMinedMulti_DeletesPendingUnmined_PlainBranch(t *testing.T) {
	ctx := context.Background()
	// flag=false forces the plain branch in SetMinedMulti.
	st := newTestStoreWithFlag(t, false)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	hashBytes := h[:]

	// Precondition: pending_unmined must have the row (Task 4).
	var unminedSince int32
	err = st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, hashBytes).Scan(&unminedSince)
	require.NoError(t, err, "pending_unmined row must exist after Create (Task 4 precondition)")

	// Mine via SetMinedMulti (plain branch, flag OFF).
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// pending_unmined row must be deleted.
	var exists bool
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "pending_unmined row must be deleted after SetMinedMulti (plain branch)")

	// unmined_since must be NULL in txs.
	var unminedSinceAfter *int32
	err = st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, hashBytes).Scan(&unminedSinceAfter)
	require.NoError(t, err)
	require.Nil(t, unminedSinceAfter, "unmined_since must be NULL in txs after mine (plain branch)")
}

// TestSetMinedMulti_DeletesPendingUnmined_NonLongestChain verifies that the
// non-OnLongestChain branch also deletes from pending_unmined.
func TestSetMinedMulti_DeletesPendingUnmined_NonLongestChain(t *testing.T) {
	ctx := context.Background()
	st := newTestStoreWithFlag(t, false)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	hashBytes := h[:]

	// Precondition: pending_unmined row must exist.
	var exists bool
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "pending_unmined row must exist before mine (Task 4 precondition)")

	// Mine on a non-longest chain (OnLongestChain=false).
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: false,
	})
	require.NoError(t, err)

	// pending_unmined row must be deleted even on non-longest-chain mine.
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "pending_unmined row must be deleted after non-longest-chain mine")
}

// TestSetMinedMulti_DeletesPendingUnmined_Batch verifies that a multi-hash batch mine
// deletes all matching pending_unmined rows in one chunk statement (not per-hash).
func TestSetMinedMulti_DeletesPendingUnmined_Batch(t *testing.T) {
	ctx := context.Background()
	st := newTestStoreWithFlag(t, true)
	require.NoError(t, st.SetBlockHeight(100))

	// Create 3 distinct unmined txs using the unique helpers from the existing test suite.
	txs := []*struct{ hash chainhash.Hash }{}
	var hashes []*chainhash.Hash
	var allHashBytes [][]byte

	for i := 0; i < 3; i++ {
		tx := makeBenchCreateTx()
		_, err := st.Create(ctx, tx, 100)
		require.NoError(t, err)
		h := tx.TxIDChainHash()
		txs = append(txs, &struct{ hash chainhash.Hash }{hash: *h})
		hashes = append(hashes, h)
		allHashBytes = append(allHashBytes, h[:])
	}

	// Verify all 3 are in pending_unmined.
	var countBefore int
	err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_unmined WHERE hash = ANY($1::bytea[])`, allHashBytes).Scan(&countBefore)
	require.NoError(t, err)
	require.Equal(t, 3, countBefore, "all 3 tx hashes must be in pending_unmined before mine")

	// Mine all 3 in a single SetMinedMulti call.
	_, err = st.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// All 3 must be gone from pending_unmined.
	var countAfter int
	err = st.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_unmined WHERE hash = ANY($1::bytea[])`, allHashBytes).Scan(&countAfter)
	require.NoError(t, err)
	require.Equal(t, 0, countAfter, "all 3 pending_unmined rows must be deleted after batch mine")

	// All 3 must still be in txs with block_ids populated.
	var minedCount int
	err = st.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE hash = ANY($1::bytea[]) AND block_ids IS NOT NULL`, allHashBytes).Scan(&minedCount)
	require.NoError(t, err)
	require.Equal(t, 3, minedCount, "all 3 txs must be mined (block_ids not NULL)")
}

// TestSetMinedMulti_EmptyPendingUnmined_IBDPath verifies that SetMinedMulti is a
// near-free no-op on pending_unmined during IBD (txs created mined → no rows to delete).
// This is the hot path: empty pending_unmined + large batch → zero rows deleted, no error.
func TestSetMinedMulti_EmptyPendingUnmined_IBDPath(t *testing.T) {
	ctx := context.Background()
	st := newTestStoreWithFlag(t, true)
	require.NoError(t, st.SetBlockHeight(100))

	// Create the tx directly MINED (IBD path: no row inserted into pending_unmined).
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID: 50, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	hashBytes := h[:]

	// Precondition: NO row in pending_unmined (tx was created mined).
	var exists bool
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "mined tx must NOT be in pending_unmined (IBD path)")

	// Re-mine via SetMinedMulti (e.g. re-processing same block) — the _pu DELETE is
	// a no-op against an empty/miss row: must succeed cleanly.
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 50, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err, "SetMinedMulti must not error when pending_unmined has no row (IBD path)")

	// pending_unmined must still be empty.
	err = st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hashBytes).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "pending_unmined must remain empty after IBD-path mine")
}
