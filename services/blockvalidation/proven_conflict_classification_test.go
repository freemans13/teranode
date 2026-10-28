package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// conflictBlockFixture is a block whose one subtree is fetched fresh and carries
// one transaction the UTXO store holds as conflicting: it spends parent:0, whose
// recorded spender is counter.
type conflictBlockFixture struct {
	utxoStore utxostore.Store
	bv        *BlockValidation
	block     *model.Block
	parent    *bt.Tx
	counter   *bt.Tx
}

// p2pkhTx builds a tx with one P2PKH output, spending the given outputs. salt
// keeps otherwise identical txs apart.
func p2pkhTx(t *testing.T, salt uint32, spends ...*bt.Tx) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = salt

	for _, parent := range spends {
		require.NoError(t, tx.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		tx.Inputs[len(tx.Inputs)-1].UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

// spendAndStore stores tx as the recorded spender of its inputs.
func spendAndStore(t *testing.T, ctx context.Context, store utxostore.Store, tx *bt.Tx, opts ...utxostore.CreateOption) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, 1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true))
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, tx, 1, append([]utxostore.CreateOption{utxostore.WithCreateOnly()}, opts...)...)
	require.NoError(t, err)
}

// newConflictBlockFixture builds the fixture on a fresh setup. The parent is
// mined in genesis, the candidate block's parent, so a counter-spender stored
// with mined is mined on the candidate chain.
func newConflictBlockFixture(t *testing.T, counterOpts []utxostore.CreateOption, configure ...func(*settings.Settings)) *conflictBlockFixture {
	t.Helper()

	initPrometheusMetrics()

	ctx := context.Background()

	// the subtree validation server's chain must hold the candidate's parent, so
	// it can resolve the candidate chain's block ids
	syntheticPoW := func(s *settings.Settings) {
		s.ChainCfgParams = test.MainNetParamsForSyntheticPoW()
		s.ChainCfgParams.DaaForkHeight = 0
	}

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t, append([]func(*settings.Settings){syntheticPoW}, configure...)...)
	t.Cleanup(deferFunc)

	inGenesis := utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0})

	parent := p2pkhTx(t, 7001)
	_, _, err := utxoStore.SpendAndCreate(ctx, parent, 0, inGenesis, utxostore.WithCreateOnly())
	require.NoError(t, err)

	counter := p2pkhTx(t, 7002, parent)
	spendAndStore(t, ctx, utxoStore, counter, counterOpts...)

	conflicting := p2pkhTx(t, 7003, parent)
	_, _, err = utxoStore.SpendAndCreate(ctx, conflicting, 0, utxostore.WithConflicting(true), utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*conflicting.TxIDChainHash(), 100, uint64(conflicting.Size()))) //nolint:gosec

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(conflicting, 1))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))
	httpmock.RegisterResponder("GET", `=~^/subtree_data/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, subtreeDataBytes))

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec

	tSettings := test.CreateBaseTestSettings(t)
	syntheticPoW(tSettings)
	tSettings.GlobalBlockHeightRetention = uint32(0)

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: replicatedSubtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	// height 100 is below mainnet's lowest checkpoint, so difficulty is skipped
	block, err := model.NewBlock(blockHeader, coinbase, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), 123123, 100, 0) //nolint:gosec
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	return &conflictBlockFixture{
		utxoStore: utxoStore,
		bv:        NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient),
		block:     block,
		parent:    parent,
		counter:   counter,
	}
}

// A block whose fresh subtree carries a conflicting tx with a counter-spender
// mined on the candidate chain is rejected, end to end. Subtree validation proves
// the conflict and returns TX_INVALID. Its fresh path used to wrap that in
// CONTEXT_CANCELED, as it wraps every failure there, and block validation then
// took the verdict for this node's own cancellation: the Kafka path queued the
// block for another retry (isRecoverableBlockMessageError) and catch-up scored it
// local_context_cancelled, so the block was retried indefinitely. The other
// failures of that path keep the wrap, and stay retryable.
func TestValidateBlock_ProvenConflictIsNotACancellation(t *testing.T) {
	ctx := context.Background()

	t.Run("a mined counter-spender rejects the block", func(t *testing.T) {
		f := newConflictBlockFixture(t, []utxostore.CreateOption{utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0})})

		subtreeErr := f.bv.validateBlockSubtrees(ctx, f.block, "", "http://localhost:8000")
		require.ErrorIs(t, subtreeErr, errors.ErrTxInvalid)
		require.NotErrorIs(t, subtreeErr, errors.ErrContextCanceled)
		require.False(t, isRecoverableBlockMessageError(subtreeErr))

		err := f.bv.ValidateBlock(ctx, f.block, "http://localhost:8000")
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.True(t, errors.IsBlockCorrupt(err), "an invalid tx in an unbound subtree list is a corrupt-body verdict")
		require.NotErrorIs(t, err, errors.ErrContextCanceled)
		require.False(t, errors.IsContextError(err))

		require.False(t, isRecoverableBlockMessageError(err), "the Kafka path must not queue the block for another retry")
		require.False(t, isLocalCatchupFault(err), "catch-up must not score it as this node's own fault")
		require.True(t, isUnboundTxInvalidVerdict(err), "catch-up scores it as a consensus rejection")

		// catch-up's classifier, as a catch-up run ending on this error applies it
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		catchupCtx := &CatchupContext{
			blockUpTo: f.block,
			baseURL:   "http://localhost:8000",
			peerID:    "primary",
			startTime: time.Now(),
		}
		require.NoError(t, server.acquireCatchupLock(catchupCtx))

		server.releaseCatchupLock(catchupCtx, &err)
		require.NotNil(t, server.previousCatchupAttempt)
		require.Equal(t, "validation_failure", server.previousCatchupAttempt.ErrorType, "not local_context_cancelled")
	})

	t.Run("a pruned parent is a retryable failure", func(t *testing.T) {
		f := newConflictBlockFixture(t, nil)
		require.NoError(t, f.utxoStore.Delete(ctx, f.parent.TxIDChainHash()))

		subtreeErr := f.bv.validateBlockSubtrees(ctx, f.block, "", "http://localhost:8000")
		require.ErrorIs(t, subtreeErr, errors.ErrTxNotFound)
		require.ErrorIs(t, subtreeErr, errors.ErrContextCanceled)
		require.NotErrorIs(t, subtreeErr, errors.ErrTxInvalid)
		require.True(t, isRecoverableBlockMessageError(subtreeErr))
	})

	t.Run("a walk over its budget is a retryable failure", func(t *testing.T) {
		// two roots (the conflicting tx and its counter-spender): a budget of 2,
		// against a counter-spender cone of 3
		f := newConflictBlockFixture(t, nil, func(s *settings.Settings) {
			s.UtxoStore.ConflictingChildrenMaxNodes = 1
		})

		prev := f.counter
		for i := uint32(0); i < 3; i++ {
			child := p2pkhTx(t, 7100+i, prev)
			spendAndStore(t, ctx, f.utxoStore, child)
			prev = child
		}

		subtreeErr := f.bv.validateBlockSubtrees(ctx, f.block, "", "http://localhost:8000")
		require.ErrorIs(t, subtreeErr, errors.ErrUtxoWalkLimitExceeded)
		require.ErrorIs(t, subtreeErr, errors.ErrContextCanceled)
		require.NotErrorIs(t, subtreeErr, errors.ErrTxInvalid)
		require.True(t, isRecoverableBlockMessageError(subtreeErr))
	})

	// The block path does not fail fast, so the missing-tx threshold cannot be
	// driven through ValidateBlock. This is the shape processTxMetaUsingStore
	// returns for it (Test_processTxMetaUsingStore_passedConflictCheckKeepsEntries
	// pins that), under CheckBlockSubtrees' wraps and the gRPC boundary.
	t.Run("the missing-tx threshold is a retryable failure", func(t *testing.T) {
		thresholdErr := errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in batch 1",
			errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta store",
				errors.NewContextCanceledError("[processTxMetaUsingStore]",
					errors.NewThresholdExceededError("threshold exceeded for missing txs: 2 > 1"))))

		subtreeErr := errors.UnwrapGRPC(errors.WrapGRPC(thresholdErr))
		require.ErrorIs(t, subtreeErr, errors.ErrThresholdExceeded)
		require.True(t, isRecoverableBlockMessageError(subtreeErr))
	})
}
