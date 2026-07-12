package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestQuickValidatedEligible_Gate is the corruption-critical truth table for the
// server-side QuickValidated stamp derivation. The stamp opens ONLY when every conjunct
// holds: the fail-closed lever is on AND the source is legacy AND CheckBlockSubtrees
// blessed the block WITHOUT re-validating any subtree AND a block ID is assigned AND the
// shared outpoint-only below-checkpoint gate holds. Any conjunct missing keeps the stamp
// closed so block assembly reconciles (fail-safe). This directly guards the two corruption
// cases: gating on block.ID alone, and stamping a block whose subtrees were re-validated
// server-side (which could have written a conflicting node).
func TestQuickValidatedEligible_Gate(t *testing.T) {
	const cp = int32(2000)

	tests := []struct {
		name                       string
		failClosed                 bool
		outpointOnly               bool
		supports                   bool
		baseURL                    string
		height                     uint32
		id                         uint32
		blessedWithoutRevalidation bool
		want                       bool
	}{
		{name: "all conditions hold", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, id: 7, blessedWithoutRevalidation: true, want: true},
		{name: "at checkpoint", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 2000, id: 7, blessedWithoutRevalidation: true, want: true},
		{name: "above checkpoint", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 2001, id: 7, blessedWithoutRevalidation: true, want: false},
		{name: "fail-closed flag off", failClosed: false, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, id: 7, blessedWithoutRevalidation: true, want: false},
		{name: "outpoint-only flag off", failClosed: true, outpointOnly: false, supports: true, baseURL: "legacy", height: 1000, id: 7, blessedWithoutRevalidation: true, want: false},
		{name: "store unsupported", failClosed: true, outpointOnly: true, supports: false, baseURL: "legacy", height: 1000, id: 7, blessedWithoutRevalidation: true, want: false},
		{name: "non-legacy source", failClosed: true, outpointOnly: true, supports: true, baseURL: "http://peer:8090", height: 1000, id: 7, blessedWithoutRevalidation: true, want: false},
		{name: "subtree revalidated server-side", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, id: 7, blessedWithoutRevalidation: false, want: false},
		{name: "no block ID assigned", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, id: 0, blessedWithoutRevalidation: true, want: false},
		{name: "height 0", failClosed: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 0, id: 7, blessedWithoutRevalidation: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = tt.failClosed
			tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = tt.outpointOnly

			params := chaincfg.RegressionNetParams
			params.Checkpoints = []chaincfg.Checkpoint{{Height: cp}}
			tSettings.ChainCfgParams = &params

			// unifiedRouteStore (from Server_unified_route_test.go) stubs only
			// SupportsOutpointOnlySpend; any other store call panics, proving the gate
			// touches nothing else.
			u := &BlockValidation{
				settings:  tSettings,
				utxoStore: &unifiedRouteStore{supports: tt.supports},
			}

			block := &model.Block{Height: tt.height, ID: tt.id}
			require.Equal(t, tt.want, u.quickValidatedEligible(block, tt.baseURL, tt.blessedWithoutRevalidation))
		})
	}
}

// buildStampTestBlock builds a minimal below-checkpoint block (coinbase-only subtree).
//
// storeAs controls how the subtree is placed in the store:
//   - fileformat.FileTypeSubtree: the "already validated" marker. CheckBlockSubtrees' existence
//     check (which looks for FileTypeSubtree) finds it and short-circuits to blessed WITHOUT
//     re-validating (blessedWithoutRevalidation = true).
//   - fileformat.FileTypeSubtreeToCheck: the "downloaded, pending validation" marker. The
//     existence check treats it as missing, so CheckBlockSubtrees re-validates it locally via
//     findLocalSubtreeFile under WithCreateConflicting (blessedWithoutRevalidation = false).
//   - "" (empty): store nothing; the subtree is genuinely absent.
func buildStampTestBlock(t *testing.T, ctx context.Context, subtreeStore blob.Store, prevHash *chainhash.Hash, storeAs fileformat.FileType, height, id uint32) *model.Block {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	// Vary the coinbase output value by id so each block has a unique subtree root and
	// block hash even when several are built against the same store in one test.
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(id)))

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	if storeAs != "" {
		subtreeBytes, err := subtree.SerializeNodes()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], storeAs, subtreeBytes))

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))
	}

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: subtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix()) + id,
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), 123123, height, id)
	require.NoError(t, err)

	return block
}

// TestQuickValidatedStamp_StoreRoundTrip verifies the flag the gate produces survives the
// real blockchain store write + read. buildAddBlockOpts is the single production site that
// turns eligibility into a WithQuickValidated store option; this exercises it against a real
// sqlitememory blockchain store and reads meta.QuickValidated back through GetBlockHeader.
//
// This is the storage half of the safety argument: WithQuickValidated(true) is persisted and
// read back true only when buildAddBlockOpts is passed quickValidatedEligible=true, and false
// otherwise — so a store refactor that drops the flag on write is caught here. The
// ineligible subtest is the "gate on ID alone" corruption case: an ID-bearing (mined_set)
// block that is NOT eligible must not carry the QuickValidated stamp.
func TestQuickValidatedStamp_StoreRoundTrip(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	_, _, blockchainClient, _, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	bv := &BlockValidation{settings: tSettings}

	// Chain the test blocks onto the store's genesis so AddBlock's previous-block lookup
	// succeeds regardless of which network the setup store was seeded with.
	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	nextHeight := bestMeta.Height + 1

	t.Run("eligible stamps quick_validated true", func(t *testing.T) {
		block := buildStampTestBlock(t, ctx, subtreeStore, bestHeader.Hash(), "", nextHeight, 1)

		opts := bv.buildAddBlockOpts(block, true)
		require.NoError(t, blockchainClient.AddBlock(ctx, block, "legacy", opts...))

		_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
		require.NoError(t, err)
		require.True(t, meta.QuickValidated, "eligible block must persist quick_validated=true")
	})

	t.Run("ineligible does not stamp quick_validated", func(t *testing.T) {
		block := buildStampTestBlock(t, ctx, subtreeStore, bestHeader.Hash(), "", nextHeight, 2)

		opts := bv.buildAddBlockOpts(block, false)
		require.NoError(t, blockchainClient.AddBlock(ctx, block, "legacy", opts...))

		_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
		require.NoError(t, err)
		require.False(t, meta.QuickValidated, "ineligible ID-bearing block must NOT persist quick_validated (mined_set only)")
	})
}

// TestQuickValidatedStamp_BlessedWithoutRevalidationSignal proves the positive end-to-end
// path: with all subtrees pre-present, validateBlockSubtrees returns blessed-without-
// revalidation=true (driven through the real subtree validation server over the
// request/response path), and quickValidatedEligible then opens for a below-checkpoint
// legacy fail-closed block. The negative direction — a re-validated subtree flipping the
// signal to false, and false keeping the gate closed — is proven where the machinery lives:
// the subtreevalidation server sets RevalidatedSubtrees=true on the re-validation path (see
// TestCheckBlockSubtrees_AssembledPath), and the gate truth table
// (TestQuickValidatedEligible_Gate, "subtree revalidated server-side") pins that false input
// keeps the stamp closed.
func TestQuickValidatedStamp_BlessedWithoutRevalidationSignal(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = true
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	// Checkpoints high enough that the below-checkpoint test block qualifies.
	params := chaincfg.MainNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 2000}}
	tSettings.ChainCfgParams = &params

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	nextHeight := bestMeta.Height + 1

	block := buildStampTestBlock(t, ctx, subtreeStore, bestHeader.Hash(), fileformat.FileTypeSubtree, nextHeight, 10)

	blessedWithoutRevalidation, err := bv.validateBlockSubtrees(ctx, block, "", "legacy")
	require.NoError(t, err)
	require.True(t, blessedWithoutRevalidation, "pre-present subtrees must bless WITHOUT revalidation")
	require.True(t, bv.quickValidatedEligible(block, "legacy", blessedWithoutRevalidation),
		"blessed-without-revalidation below-checkpoint legacy block must be stamp-eligible")
}
