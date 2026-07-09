//go:build testtxmetacache

package blockvalidation

// TestProcessBlockWindowRPC — Step-8 Increment-3 Task-1 acceptance tests.
//
// These tests exercise the Server.ProcessBlockWindow gRPC handler: the RPC
// serialise → deserialise → sort → engine path, proving that the handler layer
// preserves correctness end-to-end.
//
// Three sub-tests:
//
//  1. RPC_RoundTrip: the handler commits the same K-block window as calling the
//     engine directly. Verifies: BlockID rank and cross-block spender identity
//     identical between engine-direct (Path A) and RPC-handler (Path B) paths.
//
//  2. RPC_SortsByHeight: blocks submitted in DESCENDING order via the RPC request
//     are committed in ASCENDING order (the handler's sort step is exercised).
//
//  3. RPC_MismatchedArrays: a request with mismatched block/height/block_id array
//     lengths is rejected with an error before any engine work begins.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// newRPCServer wraps a *BlockValidation into a minimal *Server suitable for
// calling Server.ProcessBlockWindow in tests. Only the blockValidation field
// is required by that handler.
func newRPCServer(bv *BlockValidation) *Server {
	return &Server{blockValidation: bv}
}

// buildRPCRequest serialises the provided blocks into a ProcessBlockWindowRequest.
// The caller controls the order of blocks in the slice (ascending or reversed)
// to exercise the handler's sort logic.
func buildRPCRequest(t *testing.T, blocks []*model.Block, peerID string) *blockvalidation_api.ProcessBlockWindowRequest {
	t.Helper()

	blockSlices := make([][]byte, len(blocks))
	heights := make([]uint32, len(blocks))
	blockIDs := make([]uint32, len(blocks))

	for i, blk := range blocks {
		b, err := blk.Bytes()
		require.NoError(t, err, "Bytes() failed for block at height %d", blk.Height)
		blockSlices[i] = b
		heights[i] = blk.Height
		blockIDs[i] = blk.ID
	}

	return &blockvalidation_api.ProcessBlockWindowRequest{
		Block:   blockSlices,
		Height:  heights,
		BlockId: blockIDs,
		PeerId:  peerID,
		BaseUrl: "legacy",
	}
}

// TestProcessBlockWindowRPC_RoundTrip verifies that Server.ProcessBlockWindow
// commits identically to calling BlockValidation.ProcessBlockWindow (the engine)
// directly on an equivalent chain.
//
// Two harnesses receive the same 3-block below-checkpoint chain with cross-block
// spends:
//   - Path A: engine-direct call (newProcessWindowHarness)
//   - Path B: via Server.ProcessBlockWindow handler (newRPCServer wrapping the same
//     harness type built with a distinct URL suffix)
//
// After processing, BlockID rank and cross-block spender identity must be equal.
func TestProcessBlockWindowRPC_RoundTrip(t *testing.T) {
	// Path A: engine-direct.
	bvA, ctxA, cancelA := newProcessWindowHarness(t, "rpc_rt_A")
	defer cancelA()

	// Path B: via RPC handler; reuses the same harness constructor with a distinct suffix.
	bvB, ctxB, cancelB := newProcessWindowHarness(t, "rpc_rt_B")
	defer cancelB()

	srvB := newRPCServer(bvB)

	// Build chain data once (deterministic tx hashes → same content in both stores).
	chain := buildWindowChainData(t, bvA.settings.ChainCfgParams.GenesisHash)
	chainB := cloneBlocksForHarness(t, chain)

	writeWindowChainToStore(t, ctxA, bvA, chain)
	writeWindowChainToStore(t, ctxB, bvB, chainB)

	// Path A: direct engine call.
	require.NoError(t, bvA.ProcessBlockWindow(ctxA, chain.blocks, "test-peer"),
		"Path A: engine-direct ProcessBlockWindow failed")

	// Path B: through the RPC handler.
	req := buildRPCRequest(t, chainB.blocks, "test-peer")
	_, err := srvB.ProcessBlockWindow(ctxB, req)
	require.NoError(t, err, "Path B: Server.ProcessBlockWindow handler failed")

	// --- UTXO parity assertions ---

	txsPerBlock := chain.regularTxs

	// blockRankMap assigns a rank (0, 1, 2…) to each BlockID in encounter order and
	// returns a map from tx hash → block rank. Two paths are parity-equivalent when
	// every tx maps to the same relative rank (the absolute BlockID may differ between
	// harnesses because they use independent SQLite memory stores).
	blockRankMap := func(bv *BlockValidation, bvCtx context.Context) map[chainhash.Hash]int { //nolint:staticcheck // context is a value type here
		t.Helper()
		rankOf := make(map[uint32]int)
		result := make(map[chainhash.Hash]int)
		for _, txs := range txsPerBlock {
			for _, tx := range txs {
				h := *tx.TxIDChainHash()
				m, getErr := bv.utxoStore.Get(bvCtx, &h, fields.BlockIDs)
				require.NoError(t, getErr, "Get(BlockIDs) failed for tx %s", h)
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

	rankA := blockRankMap(bvA, ctxA)
	rankB := blockRankMap(bvB, ctxB)
	require.Equal(t, rankA, rankB, "RPC_RoundTrip: BlockID rank mismatch between engine-direct and RPC-handler paths")

	// Cross-block spender identity parity: tx0b[1] → tx1a, tx1a[1] → tx2a.
	crossSpends := []struct {
		blockIdx     int
		txIdx        int
		spentVout    int
		spenderBlock int
		spenderTx    int
	}{
		{0, 1, 1, 1, 0}, // tx0b[1] → tx1a
		{1, 0, 1, 2, 0}, // tx1a[1] → tx2a
	}

	for _, cs := range crossSpends {
		parentH := *txsPerBlock[cs.blockIdx][cs.txIdx].TxIDChainHash()
		wantSpender := *txsPerBlock[cs.spenderBlock][cs.spenderTx].TxIDChainHash()

		for _, tc := range []struct {
			name  string
			bv    *BlockValidation
			bvCtx context.Context //nolint:staticcheck
		}{
			{"Path A (engine-direct)", bvA, ctxA},
			{"Path B (RPC handler)", bvB, ctxB},
		} {
			m, getErr := tc.bv.utxoStore.Get(tc.bvCtx, &parentH, fields.Utxos)
			require.NoError(t, getErr, "%s: Get(Utxos) failed for tx %s", tc.name, parentH)
			require.NotNil(t, m)
			require.True(t, len(m.SpendingDatas) > cs.spentVout,
				"%s: SpendingDatas too short for tx %s vout %d", tc.name, parentH, cs.spentVout)
			require.NotNil(t, m.SpendingDatas[cs.spentVout],
				"%s: SpendingDatas[%d] nil for tx %s", tc.name, cs.spentVout, parentH)
			require.NotNil(t, m.SpendingDatas[cs.spentVout].TxID,
				"%s: SpendingDatas[%d].TxID nil for tx %s", tc.name, cs.spentVout, parentH)
			require.Equal(t, wantSpender, *m.SpendingDatas[cs.spentVout].TxID,
				"%s: wrong spender for tx %s vout %d", tc.name, parentH, cs.spentVout)
		}
	}
}

// TestProcessBlockWindowRPC_SortsByHeight verifies that the RPC handler sorts
// blocks into ascending height order even when the request delivers them in
// descending order.
//
// The 3-block chain (heights 100, 101, 102) is serialised in reverse order
// (102, 101, 100) into the request. The handler must sort them before calling
// the engine; if it does not, commitBlock will fail because the chain FSM rejects
// out-of-order commits.
func TestProcessBlockWindowRPC_SortsByHeight(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "rpc_sort")
	defer cancel()

	srv := newRPCServer(bv)

	chain := buildWindowChainData(t, bv.settings.ChainCfgParams.GenesisHash)
	writeWindowChainToStore(t, ctx, bv, chain)

	// Reverse blocks before serialising so the request is in descending height order.
	k := len(chain.blocks)
	reversed := make([]*model.Block, k)
	for i, blk := range chain.blocks {
		reversed[k-1-i] = blk
	}

	req := buildRPCRequest(t, reversed, "sort-test-peer")
	_, err := srv.ProcessBlockWindow(ctx, req)
	require.NoError(t, err, "handler must sort ascending — descending input must still succeed")

	// Verify all regular txs are committed (present in UTXO store with a BlockID).
	for blockIdx, txs := range chain.regularTxs {
		for _, tx := range txs {
			h := *tx.TxIDChainHash()
			m, getErr := bv.utxoStore.Get(ctx, &h, fields.BlockIDs)
			require.NoError(t, getErr, "Get(BlockIDs) failed for tx %s (blockIdx=%d)", h, blockIdx)
			require.NotNil(t, m, "tx %s (blockIdx=%d) not found in UTXO store", h, blockIdx)
			require.NotEmpty(t, m.BlockIDs, "tx %s (blockIdx=%d) has no BlockIDs", h, blockIdx)
		}
	}
}

// TestProcessBlockWindowRPC_MismatchedArrays verifies that the handler rejects
// a malformed request where the three index-aligned arrays have different lengths.
// No engine work must begin — this is a guard-clause check in the handler.
func TestProcessBlockWindowRPC_MismatchedArrays(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "rpc_mismatch")
	defer cancel()

	srv := newRPCServer(bv)

	req := &blockvalidation_api.ProcessBlockWindowRequest{
		Block:   [][]byte{[]byte("fake-block-bytes")},
		Height:  []uint32{100, 101}, // two heights for one block → mismatch
		BlockId: []uint32{1},
		PeerId:  "test-peer",
		BaseUrl: "legacy",
	}

	_, err := srv.ProcessBlockWindow(ctx, req)
	require.Error(t, err, "mismatched array lengths must be rejected with an error")
}
