package blockvalidation

// Unification parity across the surviving below-checkpoint routing configs.
//
// The below-checkpoint opt-in flags were retired; routing is now purely
// store-capability driven:
//
//	unified route (supporting store below checkpoint): routes through
//	    quickValidateBlock, commitBlock stamps quick_validated=true.
//	full/inline path (non-supporting store, or above checkpoint): buildAddBlockOpts
//	    stamps only mined_set, leaving quick_validated=false so block assembly
//	    reconciles as before.
//
// This locks in that the unified route persists quick_validated=true while the
// full-validation path leaves it false.

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

// TestUnification_Parity drives the two surviving routes against a real sqlitememory
// blockchain store and asserts the persisted quick_validated flag matches the route's
// contract. Each block is chained onto the store's current tip and given a distinct ID
// so the rows are independent.
func TestUnification_Parity(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	// A single hard-coded checkpoint well above the test heights so the shared
	// below-checkpoint gate (model.OutpointOnlyEligible) holds.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 100000}}

	tests := []struct {
		name           string
		unifiedRoute   bool // true: commit via the unified route; false: full-validation AddBlock
		id             uint32
		wantQuickValid bool
	}{
		{name: "unified route stamps quick_validated", unifiedRoute: true, id: 71, wantQuickValid: true},
		{name: "full validation leaves quick_validated false", unifiedRoute: false, id: 73, wantQuickValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			p := params
			tSettings.ChainCfgParams = &p

			bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

			// Chain each block onto the current tip so AddBlock's previous-block
			// lookup succeeds. Below-checkpoint height (well under 100000).
			bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			block := buildStampTestBlock(t, ctx, subtreeStore, bestHeader.Hash(), "", bestMeta.Height+1, tt.id)

			if tt.unifiedRoute {
				// Unified route: commitBlock unconditionally stamps quick_validated=true.
				require.NoError(t, bv.commitBlock(ctx, block, "legacy", "TestUnification_Parity"))
			} else {
				// Full-validation path: buildAddBlockOpts stamps only mined_set for an
				// ID-bearing block, so quick_validated stays false.
				opts := bv.buildAddBlockOpts(block)
				require.NoError(t, blockchainClient.AddBlock(ctx, block, "legacy", opts...))
			}

			_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
			require.NoError(t, err)
			require.Equal(t, tt.wantQuickValid, meta.QuickValidated,
				"config %q must persist quick_validated=%v", tt.name, tt.wantQuickValid)
		})
	}
}

// buildStampTestBlock builds a minimal below-checkpoint block (coinbase-only subtree).
//
// storeAs controls how the subtree is placed in the store:
//   - fileformat.FileTypeSubtree: the "already validated" marker. CheckBlockSubtrees' existence
//     check (which looks for FileTypeSubtree) finds it and short-circuits to blessed WITHOUT
//     re-validating.
//   - fileformat.FileTypeSubtreeToCheck: the "downloaded, pending validation" marker. The
//     existence check treats it as missing, so CheckBlockSubtrees re-validates it locally.
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
