package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_CachedHeadersShortWindowIsRefused drives the hazard from the other
// direction to the catchup-package test: it takes the route that would break IF the cached
// catchup headers ever reached the median-time-past check.
//
// Today three coincidental facts keep them apart — catchup always sets
// DisableOptimisticMining alongside CachedHeaders, that forces the non-optimistic branch, and
// that branch re-fetches a full run from the store. Flip any one of them and blocks 2..11 of
// every catchup batch arrive with a 1..10-header run whose oldest entry is the common
// ancestor rather than genesis. This test performs that flip deliberately
// (DisableOptimisticMining: false with a short non-genesis CachedHeaders run) and pins that
// the result is a retryable header-context error rather than a median computed over too few
// blocks, or a block marked invalid.
//
// Issue #1499 covers making the cache top up short windows; until then, this is the gate.
func TestValidateBlock_CachedHeadersShortWindowIsRefused(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	// The flip: optimistic mining ON is what routes CachedHeaders into CheckHeaderContextual.
	tSettings.BlockValidation.OptimisticMining = true

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())

	// A two-header run from the MIDDLE of a chain: the oldest header's parent is a common
	// ancestor, not the all-zero genesis hash. This is the shape collectPreviousHeaders
	// produces for the second block of a catchup batch.
	commonAncestor := &chainhash.Hash{}
	copy(commonAncestor[:], []byte("not-genesis-common-ancestor-hash"))

	grandparent := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  commonAncestor,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      now,
		Bits:           *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  grandparent.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      now + 1,
		Bits:           *nBits,
	}
	mineHeader(t, parent)

	// newest-first, anchored at the parent — exactly what the cache hands over.
	cachedHeaders := []*model.BlockHeader{parent, grandparent}
	require.False(t, cachedHeaders[len(cachedHeaders)-1].HashPrevBlock.IsEqual(&chainhash.Hash{}),
		"the run must not reach genesis, or the carve-out would legitimately accept it")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent.Hash(),
		HashMerkleRoot: subtree.RootHash(),
		Timestamp:      now + 100,
		Bits:           *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           100,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(parent, &model.BlockHeaderMeta{ID: 1, Height: 99}, nil).Maybe()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(cachedHeaders, []*model.BlockHeaderMeta{{ID: 1, Height: 99}, {ID: 0, Height: 98}}, nil).Maybe()
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(parent, &model.BlockHeaderMeta{Height: 99}, nil).Maybe()
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{
		Header: parent, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Height: 99,
	}, nil).Maybe()

	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	// If the check ever stopped firing, the block would be added — so a failed expectation on
	// AddBlock is itself part of the assertion.
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Prime the subtree store so subtree validation passes and the run reaches the header check.
	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	err = bv.ValidateBlockWithOptions(ctx, block, "", &ValidateBlockOptions{
		CachedHeaders:           cachedHeaders,
		DisableOptimisticMining: false,
		IsRequeuedRetry:         true, // keep the failure from enqueueing a retry in the test
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "does not reach genesis")
	require.True(t, errors.Is(err, errors.ErrBlockHeaderContext))
	require.True(t, errors.Is(err, errors.ErrProcessing), "must stay retryable for existing handling")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the block is fine; our header context was not")

	mockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestValidateBlock_HeaderContextFailureIsRetriedIntoAddBlock is the assertion that the two
// previous attempts at this fix were missing: a header-context failure must end with the block
// STORED, not merely re-queued.
//
// The first attempt routed the retry to reValidateBlock, which has no AddBlock on any path. The
// second routed it correctly but enqueued from inside the runOncePerBlock closure, so the retry
// re-entered inside the result-grace window and was answered by the very failure that caused
// it — validated nothing, added nothing. Both looked right and both dropped the block. Only an
// assertion on AddBlock actually being reached distinguishes them, so that is what this pins.
//
// The failure is made transient by having the store return a short non-genesis run once and a
// proper genesis-terminated run afterwards, which is the shape of the reorg race the re-queue
// exists for.
func TestValidateBlock_HeaderContextFailureIsRetriedIntoAddBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())

	// A genesis-rooted parent chain: the run the store SHOULD return.
	genesisHash := &chainhash.Hash{}
	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: genesisHash, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, parent)

	goodRun := []*model.BlockHeader{parent, grandparent}
	goodMeta := []*model.BlockHeaderMeta{{ID: 1, Height: 1}, {ID: 0, Height: 0}}

	// The racy run: anchored at the parent but rooted on a non-genesis ancestor, so it is too
	// short to hold a median window.
	notGenesis := &chainhash.Hash{}
	copy(notGenesis[:], []byte("not-genesis-common-ancestor-hash"))
	badGrandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: notGenesis, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, badGrandparent)

	badParent := &model.BlockHeader{
		Version: 1, HashPrevBlock: badGrandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, badParent)

	blockHeader := &model.BlockHeader{
		Version: 1, HashPrevBlock: parent.Hash(), HashMerkleRoot: subtree.RootHash(),
		Timestamp: now + 100, Bits: *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(parent, &model.BlockHeaderMeta{ID: 1, Height: 1}, nil).Maybe()

	// First call hands back the racy short run; every later call is healthy. Ordered
	// expectations: testify consumes the Once() first, then falls through to the general one.
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{badParent, badGrandparent}, goodMeta, nil).Once()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(goodRun, goodMeta, nil).Maybe()

	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(parent, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{
		Header: parent, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Height: 1,
	}, nil).Maybe()
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 0}, nil).Maybe()
	mockBlockchain.On("SetBlockMinedSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Maybe()
	mockBlockchain.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
		Return(goodRun, goodMeta, nil).Maybe()
	mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()

	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	var added atomic.Int32

	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(mock.Arguments) { added.Add(1) }).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	// No IsRequeuedRetry: the re-queue must actually fire.
	err = bv.ValidateBlockWithOptions(ctx, block, "", &ValidateBlockOptions{})
	require.Error(t, err, "the first attempt sees the racy short run")
	require.True(t, errors.Is(err, errors.ErrBlockHeaderContext))

	// The retry is scheduled past the result-grace window, then runs on the drain worker.
	require.Eventually(t, func() bool { return added.Load() > 0 }, 15*time.Second, 50*time.Millisecond,
		"the re-queued attempt must reach AddBlock — re-queuing alone is not recovery")
}

// mineHeader grinds a nonce until the header meets its own (regtest-easy) target.
func mineHeader(t *testing.T, h *model.BlockHeader) {
	t.Helper()

	for {
		h.Nonce++

		if ok, _, _ := h.HasMetTargetDifficulty(); ok {
			return
		}

		require.Less(t, h.Nonce, uint32(10_000_000), "could not find a valid nonce")
	}
}
