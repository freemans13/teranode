package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newParkLastBlockTimeFixture builds a beyond-gate, below-checkpoint block and a
// SyncManager whose block-assembly gate is armed to PARK (admit=false,
// evaluable=true): cached height 100 + maxBehind 20 = 120 < block height 500, and
// 500 is below the checkpoint at 1000. The sync peer's lastBlockTime is seeded
// deliberately stale — the value handleCheckSyncPeer would rotate on if the park
// paths never refreshed it. Returns the manager, the block message, the seeded
// stale time, the sync peer state, and a cleanup func.
func newParkLastBlockTimeFixture(t *testing.T) (*SyncManager, *blockQueueMsg, time.Time, *syncPeerState, func()) {
	t.Helper()

	const checkpointHeight = int32(1000)
	const blockHeight = uint32(500)

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20

	// Coinbase-only block: prepareBlockForWindow takes the txCount<=1 early exit,
	// so only the PoW precondition remains. Grind the nonce to meet the regtest
	// easy target so the block is deterministic.
	prevHash := chainhash.Hash{0x01}
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	header := wire.BlockHeader{
		Version:   1,
		PrevBlock: prevHash,
		Timestamp: time.Unix(1700000000, 0),
		Bits:      0x207fffff,
		Nonce:     0,
	}
	require.NoError(t, grindPoW(&header), "must find a nonce meeting the regtest target")

	msgBlock := &wire.MsgBlock{
		Header:       header,
		Transactions: []*wire.MsgTx{coinbase},
	}
	blockHash := msgBlock.Header.BlockHash()

	// Prev-block header lookup drives the default height branch (headers-first off),
	// yielding blockHeight = prevMeta.Height + 1 = 500.
	prevMeta := &model.BlockHeaderMeta{Height: blockHeight - 1}

	catchingBlocks := blockchain.FSMStateCATCHINGBLOCKS

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, prevMeta, nil)

	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		subtreeStore:     memory.New(),
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	sm.peerStates.Set(p, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})
	sm.headersFirstMode.Store(false)

	// Arm the block-assembly gate to PARK: cached 100 + maxBehind 20 = 120 < 500.
	sm.baHeightPolled.Store(true)
	sm.cachedBlockAssemblyHeight.Store(100)

	// Register p as the sync peer with a deliberately stale last-block time.
	staleTime := time.Now().Add(-10 * time.Minute)
	sps := &syncPeerState{lastBlockTime: staleTime}
	sm.storeSyncPeer(p, sps)

	cleanup := func() {
		state.requestedTxns.Stop()
		state.requestedBlocks.Stop()
		sm.requestedBlocks.Stop()
	}

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: int32(blockHeight),
		peer:        p,
	}

	return sm, bmsg, staleTime, sps, cleanup
}

// TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnPark proves that parking a
// beyond-gate block (blockAdmitParked) refreshes the sync peer's lastBlockTime.
//
// A parked block ARRIVED from the peer — proof it is alive and feeding us data —
// even though we defer committing it. Without the refresh, a long park run lets
// lastBlockTime go stale and handleCheckSyncPeer can falsely rotate a healthy sync
// peer, clearing requestedBlocks and forcing a getheaders round-trip (the observed
// multi-minute all-peer silence).
func TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnPark(t *testing.T) {
	initPrometheusMetrics()

	sm, bmsg, staleTime, sps, cleanup := newParkLastBlockTimeFixture(t)
	defer cleanup()

	// Park with ample room: the block parks rather than being refused.
	park := newParkStore(1<<40, 1024)

	wa := newWindowAccumulator(1<<40, 20)
	flushWindow := func() { t.Fatal("flushWindow must not run on the park path in this test") }

	beforeCall := time.Now()

	outcome, err := sm.handleBlockMsgWithWindow(bmsg, wa, flushWindow, flushWindow, park)

	require.NoError(t, err, "parking a beyond-gate block must not error")
	require.Equal(t, blockAdmitParked, outcome, "block must be parked ahead of block assembly")
	require.Equal(t, 1, park.len(), "park must hold the deferred block")

	after := sps.getLastBlockTime()
	require.True(t, after.After(staleTime), "lastBlockTime must advance past the stale seed on park")
	require.False(t, after.Before(beforeCall), "refreshed lastBlockTime must be no earlier than the call start")
}

// TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnParkFull proves that the
// park-full REJECT path (count cap hit) still refreshes the sync peer's
// lastBlockTime. This is the exact stall mechanism: on fast IBD with tiny blocks
// the park's count cap fills at sub-MB memory and blocks are refused in a storm.
// The refused blocks are proof the peer is alive at capacity (our own
// backpressure), so the peer must NOT look dead. Before the fix the reject path
// left lastBlockTime frozen, so handleCheckSyncPeer rotated our best data source
// mid-refusal-storm.
func TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnParkFull(t *testing.T) {
	initPrometheusMetrics()

	sm, bmsg, staleTime, sps, cleanup := newParkLastBlockTimeFixture(t)
	defer cleanup()

	// Count cap of 1, already full: countFull() is true so the block is refused on
	// the cheap pre-prepare count-full path.
	park := newParkStore(0, 1)
	park.add(mkBlock(499, 100))
	require.True(t, park.countFull(), "park must be count-full before the call")

	wa := newWindowAccumulator(1<<40, 20)
	flushWindow := func() { t.Fatal("flushWindow must not run on the reject path in this test") }

	beforeCall := time.Now()

	outcome, err := sm.handleBlockMsgWithWindow(bmsg, wa, flushWindow, flushWindow, park)

	require.Error(t, err, "a count-full park must refuse the block")
	require.Contains(t, err.Error(), "park buffer full (count)", "reject must be the count-full path")
	require.Equal(t, blockAdmitDirect, outcome, "refused block re-fetches via the direct path")
	require.Equal(t, 1, park.len(), "the refused block must not be added to the park")

	after := sps.getLastBlockTime()
	require.True(t, after.After(staleTime), "lastBlockTime must advance past the stale seed on a park-full refusal")
	require.False(t, after.Before(beforeCall), "refreshed lastBlockTime must be no earlier than the call start")
}
