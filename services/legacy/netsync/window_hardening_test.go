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
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestWindowAccumulator_FullEnforcesKCap asserts F3: full() must return true
// once the accumulated block-count reaches maxBlocks, even when the byte budget
// is far from exhausted. Before the fix full() was byte-only, so many tiny
// blocks could grow the window to thousands of entries and blow past
// MaxBlocksBehindBlockAssembly.
func TestWindowAccumulator_FullEnforcesKCap(t *testing.T) {
	const maxBlocks = 4

	// A large byte budget the tiny blocks can never exhaust.
	wa := newWindowAccumulator(1<<40, maxBlocks)

	// Each block is 1 KiB — total stays far below the 1 TiB budget.
	tiny := func(height uint32) *model.Block {
		b := newMinimalModelBlock(t, height)
		b.SizeInBytes = 1024
		return b
	}

	require.False(t, wa.full(), "empty window must not be full")

	for i := 0; i < maxBlocks-1; i++ {
		wa.add(tiny(uint32(100 + i)))
		require.False(t, wa.full(),
			"window with %d < maxBlocks entries must not be full (bytes far below budget)", len(wa.entries))
	}

	// The maxBlocks-th block hits the K cap.
	wa.add(tiny(uint32(100 + maxBlocks - 1)))
	require.Len(t, wa.entries, maxBlocks)
	require.Less(t, wa.bytesAccum, wa.windowBudget,
		"byte budget must still be far from exhausted (proves K cap, not byte cap, fired)")
	require.True(t, wa.full(),
		"window with len(entries) >= maxBlocks must be full even below the byte budget")
}

// stubLaggingBlockAssembly reports a CurrentHeight far behind the block being
// processed, so WaitForBlockAssemblyReady must retry (back-pressure) rather than
// pass through. It counts GetBlockAssemblyState calls so the test can assert the
// wait was actually invoked on the window path.
type stubLaggingBlockAssembly struct {
	blockassembly.ClientI
	currentHeight uint32
	calls         int
}

func (s *stubLaggingBlockAssembly) GetBlockAssemblyState(_ context.Context) (*blockassembly_api.StateMessage, error) {
	s.calls++
	return &blockassembly_api.StateMessage{CurrentHeight: s.currentHeight}, nil
}

// buildWindowBlockMsg builds a raw-peer wire.MsgBlock + blockQueueMsg for a
// window-eligible block at the given hash, mirroring the heightfix2 harness.
func buildWindowBlockMsg(t *testing.T) (*wire.MsgBlock, chainhash.Hash) {
	t.Helper()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: [32]byte{0x01},
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

	return msgBlock, msgBlock.BlockHash()
}

// TestHandleBlockMsgWithWindow_BackPressureBeforeAdd asserts F2: the window-add
// path must call WaitForBlockAssemblyReady (using the header-chain height and
// MaxBlocksBehindBlockAssembly) BEFORE wa.add. The proven HandleBlockDirect path
// does this; the window path had dropped it. With block assembly reported far
// behind and maxBlocksBehind small, the wait must fire (GetBlockAssemblyState
// called) and, because it never catches up, the add must NOT happen.
//
// Before the fix: GetBlockAssemblyState is never called on the window path and
// the block is admitted immediately (RED).
func TestHandleBlockMsgWithWindow_BackPressureBeforeAdd(t *testing.T) {
	initPrometheusMetrics()

	const headerChainHeight = int32(500)
	const checkpointHeight = int32(1000)

	msgBlock, blockHash := buildWindowBlockMsg(t)

	tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	// Small lag tolerance so a far-behind block assembly forces the wait to retry.
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 2

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The block does not yet exist — the already-committed guard must let it
	// proceed to the window-add path where back-pressure is asserted.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet"))

	// Block assembly is stuck at height 1 — far behind the block at height 500,
	// so WaitForBlockAssemblyReady must keep retrying (it never catches up).
	ba := &stubLaggingBlockAssembly{currentHeight: 1}

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	// Cancellable context so the retry loop terminates promptly once we have
	// observed the wait being invoked.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm := &SyncManager{
		ctx:              ctx,
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		blockAssembly:    ba,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(testPeer, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: headerChainHeight, hash: &blockHash})
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	wa := newWindowAccumulator(100*1024*1024, 20)

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1,
		peer:        testPeer,
	}

	// Cancel the context shortly after the call starts so the retry loop exits
	// instead of spinning its full retry budget. The assertion is that the wait
	// was invoked at all (GetBlockAssemblyState called) before any wa.add.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	outcome, _ := sm.handleBlockMsgWithWindow(bmsg, wa, func() {}, func() {}, nil)
	addedToWindow := outcome == blockAdmitWindowed

	require.Greater(t, ba.calls, 0,
		"window-add path must invoke WaitForBlockAssemblyReady (GetBlockAssemblyState) before adding")
	require.False(t, addedToWindow,
		"block must not be admitted while block assembly is behind (back-pressure holds)")
	require.Empty(t, wa.entries,
		"nothing must be added to the window while block assembly back-pressure holds")
}

// TestHandleBlockMsgWithWindow_FlushPrefixBeforeReject asserts S1: when a block
// is routed to the direct path (ineligible / checkpoint), the already-accumulated
// window prefix must be flushed BEFORE the ineligible block is handled, so the
// committed chain stays contiguous and ascending. handleBlockMsgWithWindow calls
// the injected flush callback on the direct branch before HandleBlockDirect.
//
// Before the fix: flush ran only after handleBlockMsgWithWindow returned (in the
// drain loop's !added branch), so the ineligible block was committed by
// HandleBlockDirect BEFORE the prefix — an out-of-order commit (RED).
func TestHandleBlockMsgWithWindow_FlushPrefixBeforeReject(t *testing.T) {
	initPrometheusMetrics()

	// The arriving block is ABOVE the checkpoint → ineligible → direct path.
	const checkpointHeight = int32(400)
	const aboveCheckpointHeight = int32(500)

	msgBlock, blockHash := buildWindowBlockMsg(t)

	tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet"))
	// HandleBlockDirect (the direct/ineligible path) fails fast: GetBlockExists
	// returns false, then the parent-header lookup returns BLOCK_NOT_FOUND, so
	// HandleBlockDirect returns an error and handleBlockMsgWithWindow returns
	// (false, err) WITHOUT running the post-processing machinery. This keeps the
	// test focused on the flush-ordering assertion — the flush must have already
	// run before HandleBlockDirect touched the store.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// The BLOCK_NOT_FOUND direct-path branch requests missing blocks from the
	// peer; these lookups return cleanly so handleBlockMsgWithWindow returns
	// (false, nil) without panicking.
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{}, nil)

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

	// Header chain seats the arriving block at an ABOVE-checkpoint height so it is
	// ineligible for the window and routed to the direct path.
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: aboveCheckpointHeight, hash: &blockHash})
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	// Accumulate a two-block prefix (N, N+1) in the window.
	wa := newWindowAccumulator(100*1024*1024, 20)
	wa.add(newMinimalModelBlock(t, 300))
	wa.add(newMinimalModelBlock(t, 301))
	require.Len(t, wa.entries, 2, "prefix must be accumulated before the ineligible block arrives")

	// Spy flush callback records whether the prefix was flushed and how many
	// entries were pending at flush time (i.e. before HandleBlockDirect drains
	// the window). handleBlockMsgWithWindow must invoke this on the direct branch.
	flushed := false
	pendingAtFlush := -1
	flushSpy := func() {
		flushed = true
		pendingAtFlush = len(wa.entries)
		// emulate the real flushWindow draining the accumulator
		wa.entries = wa.entries[:0]
		wa.bytesAccum = 0
	}

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: -1,
		peer:        testPeer,
	}

	outcome, _ := sm.handleBlockMsgWithWindow(bmsg, wa, flushSpy, flushSpy, nil)
	addedToWindow := outcome == blockAdmitWindowed

	require.False(t, addedToWindow, "above-checkpoint block must take the direct path")
	require.True(t, flushed,
		"the accumulated prefix must be flushed on the direct/reject branch before the block is handled")
	require.Equal(t, 2, pendingAtFlush,
		"the two-block prefix must still be pending at flush time (flushed BEFORE the ineligible block)")
}

// TestWindow_BoundaryContiguity documents S2: flushing window W (heights h..h+k)
// and then the first block of window W+1 (h+k+1) must produce ascending,
// contiguous committed heights across the flush boundary. No production change
// was needed for this — windowAccumulator.flush already sorts entries ascending
// by height, and the height is derived from the PoW-verified headers-first chain,
// so the last block of W (h+k) is committed before W+1's first block (h+k+1) is
// ever admitted. This test asserts that invariant so a future regression is caught.
func TestWindow_BoundaryContiguity(t *testing.T) {
	const checkpointHeight = int32(10000)

	tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	spy := &spyBlockValidation{}

	sm := &SyncManager{
		settings:        tSettings,
		chainParams:     params,
		logger:          ulogger.TestLogger{},
		utxoStore:       &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		blockValidation: spy,
	}

	const h = uint32(500)
	const k = uint32(3)

	// Window W: heights h..h+k, added out of order to exercise the ascending sort.
	waW := newWindowAccumulator(100*1024*1024, 20)
	waW.add(newMinimalModelBlock(t, h+2))
	waW.add(newMinimalModelBlock(t, h))
	waW.add(newMinimalModelBlock(t, h+k))
	waW.add(newMinimalModelBlock(t, h+1))
	waW.flush(context.Background(), sm)

	// Window W+1: first block is h+k+1 (contiguous with W's last committed height).
	waNext := newWindowAccumulator(100*1024*1024, 20)
	waNext.add(newMinimalModelBlock(t, h+k+1))
	waNext.flush(context.Background(), sm)

	require.Len(t, spy.batches, 2, "two windows must produce two ProcessBlockWindow batches")

	// Concatenate the committed heights in flush order and assert strictly
	// ascending, contiguous by 1 across the whole sequence (including the boundary).
	var committed []uint32
	for _, batch := range spy.batches {
		for _, b := range batch {
			committed = append(committed, b.Height)
		}
	}

	require.Equal(t, int(k)+2, len(committed), "all blocks from both windows must be committed")

	for i := 1; i < len(committed); i++ {
		require.Equal(t, committed[i-1]+1, committed[i],
			"committed heights must be strictly ascending and contiguous across the window boundary (index %d)", i)
	}

	require.Equal(t, h, committed[0], "first committed height must be h")
	require.Equal(t, h+k+1, committed[len(committed)-1],
		"last committed height must be h+k+1 (the first block of window W+1)")
}
