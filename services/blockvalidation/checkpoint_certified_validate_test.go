package blockvalidation

// TestValidateBlockWithOptions_CheckpointCertified reproduces the shape of the live
// production bug (mainnet block 2817): a below-checkpoint block whose coinbase pays
// subsidy + real fees, but whose subtree fee was stamped 0 by the legacy
// outpoint-only fast-path create. Without the checkpoint-certified handoff,
// checkpointConfirmedAncestor cannot be satisfied during from-genesis legacy IBD
// (the pinned checkpoint header does not exist in the blockchain store yet), so the
// coinbase no-inflation check runs unconditionally and the block is wrongly rejected
// as BLOCK_INVALID. ValidateBlockOptions.CheckpointCertified — the in-process proof
// carried from the legacy netsync headers-first handoff — must let the skip engage
// without needing that blockchain-state lookup to succeed.
//
// The store-not-supporting-outpoint-only leg of the store gate is covered directly
// (and more precisely, without the cost of a full block-validation fixture) by
// TestBlockValidation_checkpointConfirmedAncestor in checkpoint_ancestor_test.go.
//
// Fixture setup mirrors TestBlockValidation_ParentAndChildInSameBlock: reuse the
// package's setup(t) helper for the utxo/subtree stores and a real
// subtreeValidationClient, but build a dedicated blockchainClient/settings pair so
// the checkpoint list can be controlled.

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

const (
	certifiedFixtureCheckpointHeight = uint32(1000)
	certifiedFixtureBlockHeight      = uint32(500) // below the checkpoint
)

// checkpointCertifiedFixture bundles a below-checkpoint block (fee=0 subtree, coinbase
// paying subsidy + extra "real fee" satoshis) with a BlockValidation instance whose
// blockchain store has no header at the checkpoint height — the from-genesis gap.
type checkpointCertifiedFixture struct {
	blockValidation *BlockValidation
	block           *model.Block
}

func newCheckpointCertifiedFixture(t *testing.T) *checkpointCertifiedFixture {
	t.Helper()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	// A hardcoded checkpoint above the block under test, but with NO corresponding
	// header ever written to the blockchain store — the from-genesis legacy IBD gap
	// the fix closes: checkpointConfirmedAncestor's normal (non-certified) lookup
	// path cannot be satisfied here. Clone the base regtest params (GenesisHash,
	// subsidy schedule, etc.) and only override Checkpoints.
	paramsWithCheckpoint := *tSettings.ChainCfgParams
	paramsWithCheckpoint.Checkpoints = []chaincfg.Checkpoint{
		{Height: int32(certifiedFixtureCheckpointHeight), Hash: &chainhash.Hash{0xAA}},
	}
	tSettings.ChainCfgParams = &paramsWithCheckpoint

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	subsidy := util.GetBlockSubsidyForHeight(certifiedFixtureBlockHeight, tSettings.ChainCfgParams)
	const extraSatoshis = uint64(100000000) // 1 BTC "real fee" the fast path never computed

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, subsidy+extraSatoshis))

	spendTx := bt.NewTx()
	require.NoError(t, spendTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, spendTx.AddP2PKHOutputFromAddress(address.AddressString, subsidy+extraSatoshis))
	require.NoError(t, spendTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	// Both txs must be pre-recorded in the (shared, subtreeValidationClient-backed) utxo
	// store as already-mined, mirroring TestBlockValidation_ParentAndChildInSameBlock —
	// block.Valid's parent/ordering checks resolve against store state, not the spend flow.
	_, err = utxoStore.Create(ctx, coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, spendTx, certifiedFixtureBlockHeight, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     certifiedFixtureBlockHeight,
		BlockHeight: certifiedFixtureBlockHeight,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	// Fee stamped 0 regardless of spendTx's real economics — the minimal-create
	// outpoint-only fast-path shape being reproduced.
	require.NoError(t, subtree.AddNode(*spendTx.TxIDChainHash(), uint64(spendTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(spendTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		[]*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()),                 //nolint:gosec
		uint64(coinbaseTx.Size()+spendTx.Size()), //nolint:gosec
		certifiedFixtureBlockHeight, 0,
	)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	return &checkpointCertifiedFixture{blockValidation: blockValidation, block: block}
}

func TestValidateBlockWithOptions_CheckpointCertified(t *testing.T) {
	t.Run("certified=true, store supports outpoint-only: below-checkpoint no-inflation skip engages -> block stored VALID", func(t *testing.T) {
		fx := newCheckpointCertifiedFixture(t)

		opts := &ValidateBlockOptions{
			DisableOptimisticMining: true,
			CheckpointCertified:     true,
		}

		err := fx.blockValidation.ValidateBlockWithOptions(context.Background(), fx.block, "legacy", opts)
		require.NoError(t, err, "checkpoint-certified below-checkpoint block with fee=0 subtree and coinbase paying subsidy+fees must validate")
	})

	t.Run("certified=false: no-inflation check runs unconditionally -> BLOCK_INVALID (pre-fix / existing behaviour)", func(t *testing.T) {
		fx := newCheckpointCertifiedFixture(t)

		opts := &ValidateBlockOptions{
			DisableOptimisticMining: true,
			CheckpointCertified:     false,
		}

		err := fx.blockValidation.ValidateBlockWithOptions(context.Background(), fx.block, "legacy", opts)
		require.Error(t, err, "uncertified below-checkpoint block with fee=0 subtree and inflated coinbase must be rejected")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "must be a consensus BLOCK_INVALID, got: %v", err)
	})
}
