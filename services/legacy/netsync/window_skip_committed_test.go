package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newSkipCommittedSyncManager builds a window-enabled SyncManager wired for a
// single below-checkpoint block at blockHash, seated in the headers-first chain
// at headerChainHeight. It mirrors the harness used by the window back-pressure
// test (TestHandleBlockMsgWithWindow_BackPressureBeforeAdd) so the only variable
// under test is the GetBlockExists guard. The store is a decorate-counting spy so
// the test can assert whether the prepare/createBlockUTXOs subtree work engaged.
func newSkipCommittedSyncManager(t *testing.T, blockHash chainhash.Hash, headerChainHeight, checkpointHeight int32,
	blockchainClient *blockchain2.Mock,
) (*SyncManager, *outpointOnlySpyStore, *peer.Peer, func()) {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	// Small lag tolerance so, if the block ever reaches the window-add path, the
	// far-behind block assembly forces WaitForBlockAssemblyReady to retry — a
	// clear, observable "proceeded past the guard" signal for the negative case.
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 2

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() { state.requestedBlocks.Stop() })
	state.requestedBlocks.Set(blockHash, struct{}{})

	store := &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	ctx, cancel := context.WithCancel(context.Background())

	// Block assembly stuck far behind, so if the block ever reaches the window-add
	// path the wait retries indefinitely (the negative subtest cancels ctx to break
	// out). The skip case never reaches this.
	ba := &stubLaggingBlockAssembly{currentHeight: 1}

	sm := &SyncManager{
		ctx:              ctx,
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		blockAssembly:    ba,
		utxoStore:        store,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })
	t.Cleanup(cancel)

	sm.peerStates.Set(testPeer, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	hash := blockHash
	sm.headerList.PushBack(&headerNode{height: headerChainHeight, hash: &hash})
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	return sm, store, testPeer, cancel
}

// TestHandleBlockMsgWithWindow_SkipsAlreadyCommittedBlock is the regression test
// for the pruned-subtree deadlock. A peer can re-deliver an old, already-committed
// block; without the GetBlockExists guard the window path re-runs
// prepareBlockForWindow/createBlockUTXOs, which re-reads subtree data that has
// since been DAH-pruned, fails "subtree not found", and loops recovery forever —
// wedging the whole node. The guard (mirrored from HandleBlockDirect) must SKIP
// such a block: return (addedToWindow=false, err=nil), do NO subtree/UTXO work,
// and let the caller ack the peer normally.
//
// RED before the fix: the window path never calls GetBlockExists, so it proceeds
// to the window-add path (decorate/back-pressure) instead of skipping.
func TestHandleBlockMsgWithWindow_SkipsAlreadyCommittedBlock(t *testing.T) {
	initPrometheusMetrics()

	const headerChainHeight = int32(500)
	const checkpointHeight = int32(1000)

	msgBlock, blockHash := buildWindowBlockMsg(t)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The block is ALREADY committed to our chain (peer re-delivered an old block).
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	// GetBlockHeader is deliberately NOT mocked: if the guard fails and the block
	// proceeds past it, the height-determination fallback would call GetBlockHeader
	// and the mock would panic on the unexpected call — an extra safety net that
	// the skip happens before any further work.

	sm, store, testPeer, _ := newSkipCommittedSyncManager(t, blockHash, headerChainHeight, checkpointHeight, blockchainClient)

	wa := newWindowAccumulator(100*1024*1024, 20)

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1,
		peer:        testPeer,
	}

	addedToWindow, err := sm.handleBlockMsgWithWindow(bmsg, wa, func() {})

	require.NoError(t, err, "an already-committed block must be skipped cleanly (no error)")
	require.False(t, addedToWindow,
		"an already-committed block must NOT be added to the window — return (false, nil) so the caller acks it as processed-not-added")
	require.Empty(t, wa.entries, "no block may be added to the window on the skip path")
	require.Equal(t, int32(0), store.decorateCalls.Load(),
		"no subtree/UTXO work (BatchPreviousOutputsDecorate) may run for an already-committed block")
	blockchainClient.AssertCalled(t, "GetBlockExists", mock.Anything, mock.Anything)
}

// TestHandleBlockMsgWithWindow_ProcessesNotYetExistingBlock proves the guard does
// not disturb the normal flow: a block that does NOT yet exist must flow past the
// GetBlockExists check into the window path exactly as before. Here we observe it
// by the block reaching WaitForBlockAssemblyReady (GetBlockAssemblyState called),
// which sits AFTER the guard on the window-add path; the far-behind block assembly
// holds it there, so we cancel the context to return.
func TestHandleBlockMsgWithWindow_ProcessesNotYetExistingBlock(t *testing.T) {
	initPrometheusMetrics()

	const headerChainHeight = int32(500)
	const checkpointHeight = int32(1000)

	msgBlock, blockHash := buildWindowBlockMsg(t)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The block does NOT yet exist — normal case, must proceed into the window path.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// Height fallback lookups (only used if header-chain height is unavailable);
	// harmless if unused.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet"))

	sm, _, testPeer, cancel := newSkipCommittedSyncManager(t, blockHash, headerChainHeight, checkpointHeight, blockchainClient)

	wa := newWindowAccumulator(100*1024*1024, 20)

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1,
		peer:        testPeer,
	}

	ba := sm.blockAssembly.(*stubLaggingBlockAssembly)

	// Break the back-pressure retry loop shortly after the call reaches it.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	addedToWindow, _ := sm.handleBlockMsgWithWindow(bmsg, wa, func() {})

	require.Greater(t, ba.calls, 0,
		"a not-yet-existing block must flow PAST the guard into the window path (reaching WaitForBlockAssemblyReady)")
	require.False(t, addedToWindow,
		"the block is held by back-pressure (block assembly far behind), so it is not admitted — but it did proceed past the guard")
}
