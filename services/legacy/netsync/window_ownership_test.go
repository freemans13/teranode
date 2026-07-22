package netsync

// Window-ownership regression tests (the parked-twin double-commit).
//
// Production evidence (mainnet, stu/684-window): the parkStore is an unkeyed
// slice with no dedup on insert, and a parked block is invisible to every
// fetch-side ledger (handleBlockPreamble wipes in-flight tracking on arrival;
// GetBlockExists only covers COMMITTED blocks). Stall-driven peer-rotation
// re-walks therefore re-request blocks that are already parked; the re-fetched
// copy is fully re-prepared and parked as a TWIN of the same height.
// releaseParkedBlocks releases the contiguous run and stops at the first
// duplicate, deterministically splitting the twins across two successive
// flush jobs — the single FIFO flushWorker then commits the same block twice,
// one job apart (AddBlock success, then blocks_pkey unique violation absorbed
// as ErrBlockExists).
//
// The fix is a hash-keyed ownership ledger (windowOwnedBlocks): a block is
// claimed when it enters the park/window and released when its flush job is
// handled (committed, fatal, poisoned-discarded) or when the park drops it.
// While owned, a re-delivery is skipped at admission (before any prepare work)
// and the multi-peer walk will not re-request it.

import (
	"bytes"
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// grindAntiPoW increments the header nonce until its hash does NOT meet the
// encoded target, so a re-run of prepareBlockForWindow deterministically fails
// its PoW check — making "prepare ran" directly observable in a test.
func grindAntiPoW(t *testing.T, header *wire.BlockHeader) {
	t.Helper()

	for {
		var buf bytes.Buffer
		require.NoError(t, header.Serialize(&buf))

		modelHdr, err := model.NewBlockHeaderFromBytes(buf.Bytes())
		require.NoError(t, err)

		if valid, _, _ := modelHdr.HasMetTargetDifficulty(); !valid {
			return
		}

		header.Nonce++
	}
}

// newParkedTwinBlockMsg builds a coinbase-only block with ground (valid) PoW,
// mirroring the construction in manager_window_lastblocktime_test.go, so the
// full prepareBlockForWindow path succeeds end-to-end.
func newParkedTwinBlockMsg(t *testing.T) (*wire.MsgBlock, chainhash.Hash) {
	t.Helper()

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	header := wire.BlockHeader{
		Version:   1,
		PrevBlock: chainhash.Hash{0x02},
		Timestamp: time.Unix(1700000000, 0),
		Bits:      0x207fffff, // regtest easy target
		Nonce:     0,
	}
	require.NoError(t, grindPoW(&header))

	msgBlock := &wire.MsgBlock{Header: header, Transactions: []*wire.MsgTx{coinbase}}

	return msgBlock, msgBlock.Header.BlockHash()
}

// newOwnershipSyncManager wires a window-enabled SyncManager whose
// block-assembly gate is evaluable and far behind, so a delivered
// below-checkpoint block routes to the PARK path (parkThisBlock).
func newOwnershipSyncManager(t *testing.T, blockchainClient *blockchain2.Mock) (*SyncManager, *peer.Peer, *peerSyncState) {
	t.Helper()

	const checkpointHeight = int32(1000)

	tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	// Gate: cached=100, maxBehind=20 -> ceiling 120 << block height 500 -> park.
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20

	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() {
		state.requestedTxns.Stop()
		state.requestedBlocks.Stop()
	})

	sm := &SyncManager{
		ctx:               context.Background(),
		settings:          tSettings,
		chainParams:       params,
		logger:            ulogger.TestLogger{},
		blockchainClient:  blockchainClient,
		utxoStore:         &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		subtreeStore:      memory.New(),
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
		blockSizeTracker:  newBlockSizeTracker(10),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(p, state)
	sm.headersFirstMode.Store(false)
	sm.cachedBlockAssemblyHeight.Store(100)
	sm.baHeightPolled.Store(true)

	return sm, p, state
}

// markRequested registers the block as requested (peer + manager ledgers), as a
// real getdata does. The preamble rejects unrequested blocks and CLEARS these
// ledgers on every arrival, so each delivery must re-register — exactly what
// happens in production when a rotation re-walk re-requests a parked block.
func markRequested(sm *SyncManager, state *peerSyncState, blockHash chainhash.Hash) {
	state.requestedBlocks.Set(blockHash, struct{}{})
	sm.requestedBlocks.Set(blockHash, struct{}{})
}

// TestWindowOwnership_SecondDeliveryOfParkedBlockIsSkipped is the direct
// regression test for the mainnet parked-twin double-commit (blocks 601691/
// 601692, ids 601883/601884): a second delivery of a block that is already
// PARKED must be skipped at admission — no re-prepare, no twin in the park.
//
// Without the ownership guard, the second delivery re-runs the full prepare
// pass and park.add appends a twin (park.len()==2) — the exact precondition
// for the double-commit.
func TestWindowOwnership_SecondDeliveryOfParkedBlockIsSkipped(t *testing.T) {
	initPrometheusMetrics()

	msgBlock, blockHash := newParkedTwinBlockMsg(t)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// Height determination: prev lookup yields 499 -> block height 500.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 499}, nil)
	// The skip path must still pump the sync-request loop (parity with the
	// already-committed skip), which reads the best header.
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 400}, nil)

	sm, p, state := newOwnershipSyncManager(t, blockchainClient)

	wa := newWindowAccumulator(1<<40, 20)
	park := newParkStore(1<<30, 100)
	noop := func() {}

	// First delivery: parks (and claims ownership).
	markRequested(sm, state, blockHash)

	firstCopy := *msgBlock
	outcome, err := sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:     &firstCopy,
		blockHash: blockHash,
		peer:      p,
	}, wa, noop, noop, park)
	require.NoError(t, err, "first delivery must park cleanly")
	require.Equal(t, blockAdmitParked, outcome, "first delivery of a beyond-gate block must park")
	require.Equal(t, 1, park.len(), "first delivery parks exactly one entry")
	require.True(t, sm.windowBlockOwned(blockHash), "a parked block must be owned by the window pipeline")

	// Second delivery of the SAME block (peer-rotation re-walk re-requested it,
	// so it is back in the requested ledgers when it arrives).
	markRequested(sm, state, blockHash)

	secondCopy := *msgBlock
	outcome, err = sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:     &secondCopy,
		blockHash: blockHash,
		peer:      p,
	}, wa, noop, noop, park)

	require.NoError(t, err, "a re-delivered owned block must be skipped cleanly (no error)")
	require.Equal(t, blockAdmitDirect, outcome, "a re-delivered owned block must be skipped, not parked or windowed")
	require.Equal(t, 1, park.len(), "the park must NOT hold a twin of the same block after a re-delivery")
	require.Empty(t, wa.entries, "a re-delivered owned block must not enter the window accumulator")
}

// TestWindowOwnership_ParkBytesFullCheckedBeforePrepare proves the byte-cap is
// enforced BEFORE the expensive prepare pass, exactly like the count-cap. The
// mainnet log showed the same block paying the full prepareSubtrees/
// writeSubtree pipeline on every delivery only to be rejected by the
// bytes-full check afterwards — five full passes for one block.
//
// The delivered block has deliberately INVALID PoW, so if prepare runs at all
// it fails with a PoW error; a bytes-full park must instead reject with the
// park-buffer-full error without ever preparing. SizeInBytes is exactly
// MsgBlock().SerializeSize() (prepareBlockForWindow), so the pre-check is
// exact. Without the pre-check, this test sees prepare's PoW error instead of
// the park-buffer-full error.
func TestWindowOwnership_ParkBytesFullCheckedBeforePrepare(t *testing.T) {
	initPrometheusMetrics()

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	header := wire.BlockHeader{
		Version:   1,
		PrevBlock: chainhash.Hash{0x03},
		Timestamp: time.Unix(1700000000, 0),
		Bits:      0x207fffff,
		Nonce:     0,
	}
	grindAntiPoW(t, &header)

	msgBlock := &wire.MsgBlock{Header: header, Transactions: []*wire.MsgTx{coinbase}}
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 499}, nil)

	sm, p, state := newOwnershipSyncManager(t, blockchainClient)

	wa := newWindowAccumulator(1<<40, 20)
	// Byte budget of 1: any block busts it, so the pre-check must fire.
	park := newParkStore(1, 100)
	noop := func() {}

	markRequested(sm, state, blockHash)

	outcome, err := sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:     msgBlock,
		blockHash: blockHash,
		peer:      p,
	}, wa, noop, noop, park)

	require.Equal(t, blockAdmitDirect, outcome)
	require.Error(t, err, "a bytes-full park must reject the block for re-fetch")
	require.Contains(t, err.Error(), "park buffer full (bytes)",
		"the bytes-cap must reject BEFORE prepare runs (a PoW error here means prepare ran first)")
	require.Equal(t, 0, park.len(), "nothing may be parked on the bytes-full path")
	require.False(t, sm.windowBlockOwned(blockHash), "a rejected block must not be claimed")
}

// TestAssignBlocks_SkipsWindowOwnedBlock mirrors TestAssignBlocks_SkipsInFlightBlock:
// the multi-peer forward walk must not re-request a block the window pipeline
// already owns (parked or in an in-flight flush job). requestedBlocks does not
// cover this — handleBlockPreamble wipes it when the block ARRIVES, but the
// block then lives on in the park for minutes, invisible to every fetch guard.
// This re-buy is what manufactured the parked twins on mainnet.
func TestAssignBlocks_SkipsWindowOwnedBlock(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xc2}
	const runway = 5
	_, hashes := buildHeaderChain(base, runway, 9800)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 73, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 74, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 75, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 8
	tSettings.Legacy.BlockDownloadWindow = 18

	syncState := newEligiblePeerState()
	stateB := newEligiblePeerState()
	stateC := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()
	defer stateB.requestedBlocks.Stop()
	defer stateB.requestedTxns.Stop()
	defer stateC.requestedBlocks.Stop()
	defer stateC.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:               context.Background(),
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       &chainParams,
		blockchainClient:  blockchainClient,
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next()

	// hashes[2] is OWNED by the window pipeline (parked, delivered earlier, so it
	// is no longer in requestedBlocks). The walk must NOT re-request it.
	owned := hashes[2]
	sm.claimWindowBlock(owned, 9803)

	sm.assignBlocksAcrossPeers()

	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()
		return len(collectInvHashes(got1))+len(collectInvHashes(got2))+len(collectInvHashes(got3)) >= runway-1
	}, 2*time.Second, 10*time.Millisecond, "the non-owned runway blocks must be requested")

	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	all := append(append(collectInvHashes(got1), collectInvHashes(got2)...), collectInvHashes(got3)...)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	for _, h := range all {
		require.NotEqual(t, owned, h, "a window-owned (parked/in-flight-job) block must NOT be re-requested by the walk")
	}

	seen := make(map[chainhash.Hash]bool)
	for _, h := range all {
		seen[h] = true
	}
	for i := 0; i < runway; i++ {
		if hashes[i] == owned {
			continue
		}
		require.True(t, seen[hashes[i]], "runway block %d must still be requested", i)
	}
}

// newOwnedTestBlock builds a minimal *model.Block with a UNIQUE hash per
// height (newMinimalModelBlock shares one header, hence one hash, across
// heights — useless for a hash-keyed ledger test).
func newOwnedTestBlock(t *testing.T, height uint32) *model.Block {
	t.Helper()

	hdr := wire.BlockHeader{Version: 1, Bits: 0x1d00ffff, Nonce: height}

	var hdrBuf bytes.Buffer
	require.NoError(t, hdr.Serialize(&hdrBuf))

	modelHdr, err := model.NewBlockHeaderFromBytes(hdrBuf.Bytes())
	require.NoError(t, err)

	zeroRoot := chainhash.Hash{}

	blk, err := model.NewBlock(modelHdr, nil, []*chainhash.Hash{&zeroRoot}, 1, 100, height, 0)
	require.NoError(t, err)

	return blk
}

// TestWindowOwnership_ReleasedWhenJobHandled is the anti-wedge lifecycle test
// (non-negotiable): ownership MUST be released when a flush job is handled,
// on the success path AND on the fatal path. A leaked claim would make the
// block permanently unfetchable (skipped at admission, skipped by the walk)
// until restart — a worse wedge than the duplicate it prevents.
func TestWindowOwnership_ReleasedWhenJobHandled(t *testing.T) {
	// Success path.
	spy := &commitSpyBlockValidation{}
	sm := newAckTestSyncManager(t, spy)
	sm.windowOwnedBlocks = txmap.NewSyncedMap[chainhash.Hash, uint32]()

	b1 := newOwnedTestBlock(t, 501)
	b2 := newOwnedTestBlock(t, 502)
	sm.claimWindowBlock(*b1.Hash(), 501)
	sm.claimWindowBlock(*b2.Hash(), 502)

	sm.commitWindowJob(sm.ctx, windowFlushJob{blocks: []*model.Block{b1, b2}})

	require.False(t, sm.windowBlockOwned(*b1.Hash()), "ownership must be released after a successful commit")
	require.False(t, sm.windowBlockOwned(*b2.Hash()), "ownership must be released after a successful commit")

	// Fatal path (ProcessBlockWindow and recovery both fail): the job is
	// abandoned for re-fetch after peer rotation — ownership MUST be released
	// or the re-fetch is skipped forever.
	spyFatal := &commitSpyBlockValidation{
		windowErr:         errors.NewStorageError("window commit failure"),
		perBlockErr:       errors.NewBlockInvalidError("fatal validation failure"),
		perBlockFailUntil: 1 << 30,
	}
	smFatal := newAckTestSyncManager(t, spyFatal)
	smFatal.windowOwnedBlocks = txmap.NewSyncedMap[chainhash.Hash, uint32]()

	b3 := newOwnedTestBlock(t, 503)
	smFatal.claimWindowBlock(*b3.Hash(), 503)

	smFatal.commitWindowJob(smFatal.ctx, windowFlushJob{blocks: []*model.Block{b3}})

	require.False(t, smFatal.windowBlockOwned(*b3.Hash()),
		"ownership must be released on the fatal path too (block re-syncs after rotation)")
}

// TestWindowOwnership_DropArmReleases: releaseParkedBlocks drops a parked block
// at/below the committed tip without committing it — that exit must release
// ownership as well.
func TestWindowOwnership_DropArmReleases(t *testing.T) {
	spy := &stateSpy{}
	sm := newCacheTestManager(t, spy, 20)
	sm.windowOwnedBlocks = txmap.NewSyncedMap[chainhash.Hash, uint32]()
	sm.cachedBlockAssemblyHeight.Store(100)

	wa := newWindowAccumulator(1<<30, 0)
	park := newParkStore(0, 1024)

	dropped := newOwnedTestBlock(t, 99) // at/below cached tip 100 -> drop arm
	park.add(dropped)
	sm.claimWindowBlock(*dropped.Hash(), 99)

	sm.releaseParkedBlocks(park, wa, func() {}, func() {})

	require.Equal(t, 0, park.len(), "the below-tip parked block is dropped")
	require.False(t, sm.windowBlockOwned(*dropped.Hash()),
		"a dropped parked block must have its ownership released (it may legitimately be re-delivered)")
}
