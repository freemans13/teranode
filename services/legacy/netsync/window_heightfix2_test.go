package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockMsgWithWindow_HeightFromHeaderChain_ParentAbsent is the
// regression test for the live-testnet wedge where handleBlockMsgWithWindow
// derived a windowed block's height by looking up its PARENT header
// (GetBlockHeader(prevBlockHash)) and adding one. The window streams blocks
// ahead of commit (early-ack): block N+1 is processed before block N is
// committed, so N's header is not yet in the store. The parent lookup then
// failed with BLOCK_NOT_FOUND and the code returned
// "failed to get prev block header for height determination", wedging the node.
//
// The fix surfaces the authoritative, PoW-verified, parent-independent height
// that the shared preamble already reads off the headers-first header chain
// (headerNode.height) and prefers it over the parent lookup.
//
// This test is a genuine discriminator because it forces the exact
// parent-absent condition of the live failure: the blockchain mock's
// GetBlockHeader ALWAYS returns BLOCK_NOT_FOUND for any hash (the streamed
// block's parent is deliberately not committed). Simultaneously the block's
// bsvutil wrapper reports height -1 (a raw peer wire.MsgBlock carries no
// height), so before the fix the code MUST fall through to the parent lookup
// and MUST fail. The only other height source is the headers-first list, which
// we seed with a front headerNode at height H matching the block's hash.
//
// Before the fix: returns the BLOCK_NOT_FOUND error (RED).
// After the fix: derives height H from the header chain, never consults
// GetBlockHeader, and the block is admitted to the window (GREEN).
func TestHandleBlockMsgWithWindow_HeightFromHeaderChain_ParentAbsent(t *testing.T) {
	initPrometheusMetrics()

	// H is below the checkpoint so the block is window-eligible, and > 0 so the
	// header-chain height is the preferred source.
	const headerChainHeight = int32(500)
	const checkpointHeight = int32(1000)

	// Build a block exactly as a real peer delivers it: a raw wire.MsgBlock with
	// a single coinbase tx and the regtest easy-target (0x207fffff), which any
	// header hash satisfies at nonce 0 so prepareBlockForWindow's PoW check
	// passes without solving.
	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: [32]byte{0x01}, // a parent that is NOT in the store
			Timestamp: time.Unix(1231006505, 0),
			Bits:      0x207fffff,
			Nonce:     0,
		},
	}

	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: [32]byte{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)

	blockHash := msgBlock.BlockHash()

	// Document the trap: a freshly wrapped raw peer block reports height -1, so
	// the block.Height() branch cannot supply the height — the code must rely on
	// either the header chain (fixed) or the parent lookup (the broken path).
	require.Equal(t, int32(-1), bsvutil.NewBlock(msgBlock).Height(),
		"a raw peer block's wrapper must report height -1 (documents the trap)")

	// Settings/params that make the below-checkpoint block window-eligible.
	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	// Blockchain mock: FSM is CATCHINGBLOCKS, and — crucially — GetBlockHeader
	// ALWAYS fails with BLOCK_NOT_FOUND. This is the parent-absent condition: the
	// streamed-ahead block's parent has not been committed. If the code consults
	// the parent lookup at all, the test fails with this error.
	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The block is streamed ahead of commit and does not yet exist — the
	// already-committed guard must see false and let it into the window.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet (streamed ahead of commit)"))
	// After the block is admitted to the window, the pump asks for the best
	// block header (to decide whether to request more). This is incidental
	// post-admission machinery, not the code under test; a low best height keeps
	// !sm.current() true (as it is during real below-checkpoint catchup).
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil)

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(testPeer, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	// Headers-first mode with the block's header at the front of the list at
	// height H. This is the authoritative, parent-independent height source the
	// fix surfaces. nextCheckpoint's hash differs from the block hash, so the
	// block is NOT treated as a checkpoint block (which would bypass the window).
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: headerChainHeight, hash: &blockHash})
	// A non-matching checkpoint hash so the block is not treated as a checkpoint.
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	wa := newWindowAccumulator(100*1024*1024, 20)

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1, // as delivered by the fetch path (never stamped)
		peer:        testPeer,
	}

	outcome, err := sm.handleBlockMsgWithWindow(bmsg, wa, func() {}, func() {}, nil)
	addedToWindow := outcome == blockAdmitWindowed

	// The core assertion: no parent-not-found failure. Before the fix this
	// returned the BLOCK_NOT_FOUND / "failed to get prev block header" error.
	require.NoError(t, err,
		"windowed height must be derived from the headers-first chain, not a parent lookup")
	require.False(t, errors.Is(err, errors.ErrBlockNotFound),
		"the parent lookup must not be consulted for a windowed block")

	// The block was admitted to the window (eligible, prepared, wa.add-ed).
	require.True(t, addedToWindow, "eligible windowed block must be added to the window")
	require.Len(t, wa.entries, 1, "window must hold the one admitted block")

	// The prepared block carries the header-chain height H, not a parent-derived
	// value (there is no committed parent to derive from).
	require.Equal(t, uint32(headerChainHeight), wa.entries[0].block.Height,
		"admitted block must carry the header-chain height H")

	// GetBlockHeader must never have been called: the header chain fully supplied
	// the height.
	blockchainClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}
