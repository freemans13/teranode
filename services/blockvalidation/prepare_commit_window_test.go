//go:build testtxmetacache

package blockvalidation

// Tests for the PrepareWindow/CommitWindow split as INDEPENDENTLY CALLABLE gRPC
// methods (the boundary contract item #1 flagged after slice 1): every existing
// test exercises ProcessBlockWindow, which is composed internally as
// PrepareWindow-then-CommitWindow in a single Go call — it never proves the two
// methods behave correctly when invoked as two SEPARATE RPCs (e.g. across a
// network hop, or as the netsync two-stage pipeline does). These tests close
// that gap directly against the real sqlitememory store + local blockchain
// client harness (newProcessWindowHarness / buildWindowChainData /
// writeWindowChainToStore / cloneBlocksForHarness, shared with
// process_block_window_test.go in this package).

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// blockRankMapFor mirrors the block-rank comparison used by
// TestProcessBlockWindow_ParityWithSerial, factored out so both that test and
// the new two-RPC tests below can share it without duplicating the mapping
// logic. It is order-independent: it maps each tx hash to the RELATIVE order in
// which its BlockID was first observed, so it compares two harnesses whose
// underlying block.ID values may differ in absolute number but must agree in
// relative commit order.
func blockRankMapFor(t *testing.T, bv *BlockValidation, ctx context.Context, txsPerBlock [][]*bt.Tx) map[chainhash.Hash]int {
	t.Helper()

	rankOf := make(map[uint32]int)
	result := make(map[chainhash.Hash]int)

	for _, txs := range txsPerBlock {
		for _, tx := range txs {
			h := *tx.TxIDChainHash()
			m, err := bv.utxoStore.Get(ctx, &h, fields.BlockIDs)
			require.NoError(t, err, "Get(BlockIDs) failed for tx %s", h)
			require.NotNil(t, m)
			require.NotEmpty(t, m.BlockIDs, "tx %s has no BlockIDs", h)

			bid := m.BlockIDs[0]
			if _, seen := rankOf[bid]; !seen {
				rankOf[bid] = len(rankOf)
			}

			result[h] = rankOf[bid]
		}
	}

	return result
}

// TestPrepareThenCommitWindow_AsTwoSeparateRPCs_ParityWithProcessBlockWindow
// proves PrepareWindow and CommitWindow, called as two INDEPENDENT calls (the
// shape the netsync two-stage pipeline and any future gRPC hop use — not the
// single in-process composition ProcessBlockWindow performs), produce IDENTICAL
// UTXO state (BlockID rank + cross-block spender identity) to the reference
// ProcessBlockWindow path, on a 3-block below-checkpoint chain with cross-block
// spends (tx0b[1] -> tx1a, tx1a[1] -> tx2a).
func TestPrepareThenCommitWindow_AsTwoSeparateRPCs_ParityWithProcessBlockWindow(t *testing.T) {
	bvA, ctxA, cancelA := newProcessWindowHarness(t, "prepcommit_A")
	defer cancelA()

	bvB, ctxB, cancelB := newProcessWindowHarness(t, "prepcommit_B")
	defer cancelB()

	chain := buildWindowChainData(t, bvA.settings.ChainCfgParams.GenesisHash)
	chainB := cloneBlocksForHarness(t, chain)

	writeWindowChainToStore(t, ctxA, bvA, chain)
	writeWindowChainToStore(t, ctxB, bvB, chainB)

	// Path A: PrepareWindow and CommitWindow as two SEPARATE calls (simulating
	// PrepareBlockWindow/CommitBlockWindow as two independent gRPC round-trips).
	require.NoError(t, bvA.PrepareWindow(ctxA, chain.blocks, "test-peer"), "Path A: PrepareWindow failed")
	require.NoError(t, bvA.CommitWindow(ctxA, chain.blocks, "test-peer"), "Path A: CommitWindow failed")

	// Path B: the reference single-call composition.
	require.NoError(t, bvB.ProcessBlockWindow(ctxB, chainB.blocks, "test-peer"), "Path B: ProcessBlockWindow failed")

	txsPerBlock := chain.regularTxs

	rankA := blockRankMapFor(t, bvA, ctxA, txsPerBlock)
	rankB := blockRankMapFor(t, bvB, ctxB, txsPerBlock)
	require.Equal(t, rankA, rankB, "PARITY: block-rank mismatch between two-RPC Prepare+Commit and single-call ProcessBlockWindow")

	type crossBlockSpend struct {
		parent    *bt.Tx
		spentVout int
		spender   *bt.Tx
	}
	crossSpends := []crossBlockSpend{
		{txsPerBlock[0][1], 1, txsPerBlock[1][0]}, // tx0b[1] -> tx1a
		{txsPerBlock[1][0], 1, txsPerBlock[2][0]}, // tx1a[1] -> tx2a
	}

	for _, cs := range crossSpends {
		parentH := *cs.parent.TxIDChainHash()
		wantSpender := *cs.spender.TxIDChainHash()
		vout := cs.spentVout

		for _, tc := range []struct {
			name string
			bv   *BlockValidation
			ctx  context.Context
		}{
			{"Path A (Prepare+Commit as two RPCs)", bvA, ctxA},
			{"Path B (ProcessBlockWindow)", bvB, ctxB},
		} {
			m, getErr := tc.bv.utxoStore.Get(tc.ctx, &parentH, fields.Utxos)
			require.NoError(t, getErr, "%s: Get(Utxos) failed for tx %s", tc.name, parentH)
			require.NotNil(t, m)
			require.True(t, len(m.SpendingDatas) > vout, "%s: SpendingDatas too short for tx %s vout %d", tc.name, parentH, vout)
			require.NotNil(t, m.SpendingDatas[vout], "%s: SpendingDatas[%d] nil for tx %s", tc.name, vout, parentH)
			require.NotNil(t, m.SpendingDatas[vout].TxID, "%s: SpendingDatas[%d].TxID nil for tx %s", tc.name, vout, parentH)
			require.Equal(t, wantSpender, *m.SpendingDatas[vout].TxID,
				"%s: wrong spender for tx %s vout %d", tc.name, parentH, vout)
		}
	}
}

// TestCommitWindow_CalledTwice_IsIdempotentNoDoubleCommit proves CommitWindow's
// re-run of the idempotent block-ID pre-pass plus commitBlock's ErrBlockExists
// tolerance make it safe to call TWICE for the same window — the shape a
// retried/duplicated gRPC call, or a prepare-stage recovery success followed by
// the normal commit-stage forward (see netsync's commitWindowJobPrepare doc),
// produces. The second call must return nil and must not move the chain tip or
// duplicate any BlockID assignment.
func TestCommitWindow_CalledTwice_IsIdempotentNoDoubleCommit(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "commit_twice")
	defer cancel()

	chain := buildWindowChainData(t, bv.settings.ChainCfgParams.GenesisHash)
	writeWindowChainToStore(t, ctx, bv, chain)

	require.NoError(t, bv.PrepareWindow(ctx, chain.blocks, "test-peer"))
	require.NoError(t, bv.CommitWindow(ctx, chain.blocks, "test-peer"), "first CommitWindow call must succeed")

	// The blockchain store's reported "Height" is chain DEPTH from genesis, not
	// the model.Block.Height field these synthetic test blocks carry (they chain
	// directly onto genesis) — so assert on the TIP HASH, which unambiguously
	// identifies the committed chain, rather than assuming a specific depth.
	bestHashAfterFirst, metaAfterFirst, err := bv.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, *chain.blocks[len(chain.blocks)-1].Hash(), *bestHashAfterFirst.Hash(),
		"chain tip must be the last block of the 3-block window after the first commit")

	rankAfterFirst := blockRankMapFor(t, bv, ctx, chain.regularTxs)

	// Second call: same blocks, same (already-assigned) IDs. commitBlock treats
	// ErrBlockExists as an idempotent skip (quick_validate.go), so this must
	// succeed without moving the tip or changing any BlockID.
	require.NoError(t, bv.CommitWindow(ctx, chain.blocks, "test-peer"), "second CommitWindow call must be an idempotent no-op, not an error")

	_, metaAfterSecond, err := bv.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, metaAfterFirst.Height, metaAfterSecond.Height,
		"chain tip must not move on a repeated CommitWindow call")

	rankAfterSecond := blockRankMapFor(t, bv, ctx, chain.regularTxs)
	require.Equal(t, rankAfterFirst, rankAfterSecond,
		"BlockID assignment must be unchanged after a repeated CommitWindow call — no double-commit")
}

// TestAssignWindowBlockIDs_CalledTwice_IdempotentPerHash proves the documented
// contract the whole Prepare/Commit-as-two-RPCs split depends on: "AssignBlockID
// is idempotent per hash", so CommitWindow's own re-run of this exact pre-pass
// (needed to make it stateless when called as a separate RPC from PrepareWindow)
// always resolves to the SAME ids PrepareWindow already assigned. This exercises
// assignWindowBlockIDs directly (a serial, single-goroutine per-block loop) rather
// than a second full PrepareWindow call: re-running the whole prepare stage's C1
// against already-mined transactions drives the sqlitememory store's known
// single-connection concurrent-update limitation (see
// stores/utxo/sql/unlock_batcher_postgres_test.go's TestConflictWALCrashRecovery_Postgres
// doc comment) — a real store (Postgres) has a genuine connection pool and does
// not hit it; the idempotency guarantee itself lives in assignWindowBlockIDs /
// AssignBlockID, not in the concurrent C1 create pass, so testing it directly
// here is the precise and store-portable check.
func TestAssignWindowBlockIDs_CalledTwice_IdempotentPerHash(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "assign_ids_twice")
	defer cancel()

	chain := buildWindowChainData(t, bv.settings.ChainCfgParams.GenesisHash)
	writeWindowChainToStore(t, ctx, bv, chain)

	require.NoError(t, bv.assignWindowBlockIDs(ctx, chain.blocks), "first pre-pass run must succeed")

	firstIDs := make([]uint32, len(chain.blocks))
	for i, blk := range chain.blocks {
		firstIDs[i] = blk.ID
	}

	// Re-run the SAME pre-pass CommitWindow performs when it re-derives ids as a
	// separate RPC from PrepareWindow.
	require.NoError(t, bv.assignWindowBlockIDs(ctx, chain.blocks), "second pre-pass run (idempotent) must also succeed")

	for i, blk := range chain.blocks {
		require.Equal(t, firstIDs[i], blk.ID, "block ID at height %d must be identical across two idempotent pre-pass runs", blk.Height)
	}

	// The window still prepares and commits cleanly afterwards using the
	// (unchanged) ids, proving the double pre-pass left nothing inconsistent.
	require.NoError(t, bv.PrepareWindow(ctx, chain.blocks, "test-peer"))
	require.NoError(t, bv.CommitWindow(ctx, chain.blocks, "test-peer"))

	bestHash, _, err := bv.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, *chain.blocks[len(chain.blocks)-1].Hash(), *bestHash.Hash())
}
