// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Regression tests for the multi-peer download WEDGE: startHeader is a
// monotonic-forward cursor that is never rewound, so a block that is assigned,
// walked-past, and then loses its in-flight status WITHOUT being received (freed
// by the head-of-line stall detector, or dropped on a full send queue) becomes
// orphaned below the cursor — the forward walk can never re-reach it. Because
// the window commits strictly ascending, one such orphan pins the committed tip
// forever (height frozen, drain idle, scheduler stops fetching). The fix routes
// such blocks through an explicit re-fetch set that assignBlocksAcrossPeers
// drains FIRST each pass, independent of the cursor position.
import (
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
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestAssignBlocks_FreedHeadRefetchedBelowCursor is the RED/GREEN lock-in for
// the wedge. The head block was assigned to the sync peer, the cursor advanced
// past it (startHeader left nil = whole runway walked), then the stall detector
// freed it. Exactly the pre-fix orphan: below the cursor, outstanding to nobody.
// One scheduler pass must re-request it anyway (via the re-fetch drain) and
// clear it from the re-fetch set once its getdata goes out.
func TestAssignBlocks_FreedHeadRefetchedBelowCursor(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 60, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 61, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 62, &mu3, &got3)

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
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Record the head as assigned to the sync peer, cursor already advanced past
	// it (startHeader stays nil). Then free it, as checkHeadStall would (minus the
	// disconnect, which freePeerAssignments does not perform).
	head := chainhash.Hash{0xaa}
	syncState.requestedBlocks.Set(head, struct{}{})
	sm.requestedBlocks.Set(head, struct{}{})
	sm.assignedTo[head] = syncPeer
	sm.assignedAt[head] = time.Now().Add(-10 * time.Second)
	sm.headerHeightIndex[head] = 100

	require.Nil(t, sm.startHeader, "precondition: cursor is past the head (orphan below it)")

	sm.freePeerAssignments(syncPeer)

	sm.assignedMu.Lock()
	_, queued := sm.refetchBlocks[head]
	sm.assignedMu.Unlock()
	require.True(t, queued, "freed head must be enqueued for re-fetch")
	_, stillGlobal := sm.requestedBlocks.Get(head)
	require.False(t, stillGlobal, "freed head must be removed from global requestedBlocks")

	// One pass: the drain must re-request the head despite it being below the cursor.
	sm.assignBlocksAcrossPeers()

	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()

		for _, h := range collectInvHashes(got1) {
			if h == head {
				return true
			}
		}
		for _, h := range collectInvHashes(got2) {
			if h == head {
				return true
			}
		}
		for _, h := range collectInvHashes(got3) {
			if h == head {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "freed head must be re-requested despite being below the cursor")

	// Re-requested => back in the global ledger and dropped from the re-fetch set.
	_, backInFlight := sm.requestedBlocks.Get(head)
	require.True(t, backInFlight, "re-fetched head must be recorded in global requestedBlocks")

	sm.assignedMu.Lock()
	_, stillQueued := sm.refetchBlocks[head]
	sm.assignedMu.Unlock()
	require.False(t, stillQueued, "re-fetched head must be removed from the re-fetch set after its send")
}

// TestAssignBlocks_RefetchAndForwardWalkCompose verifies that in a single pass
// the scheduler re-requests an orphan (below the cursor, from the re-fetch set)
// AND continues the forward walk for new runway blocks — disjointly, with the
// orphan taking first claim on the budget.
func TestAssignBlocks_RefetchAndForwardWalkCompose(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xd0}
	const runway = 5
	_, hashes := buildHeaderChain(base, runway, 9700)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 63, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 64, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 65, &mu3, &got3)

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
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peer.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Forward runway available from the front of the header list.
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next()

	// An orphan below the cursor, sitting in the re-fetch set.
	orphan := chainhash.Hash{0xee}
	sm.refetchBlocks[orphan] = struct{}{}

	sm.assignBlocksAcrossPeers()

	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()
		return len(collectInvHashes(got1))+len(collectInvHashes(got2))+len(collectInvHashes(got3)) >= runway+1
	}, 2*time.Second, 10*time.Millisecond, "orphan + all runway blocks must be requested")

	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	all := append(append(collectInvHashes(got1), collectInvHashes(got2)...), collectInvHashes(got3)...)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	seen := make(map[chainhash.Hash]int)
	for _, h := range all {
		seen[h]++
	}

	require.Equal(t, 1, seen[orphan], "orphan must be re-requested exactly once")
	for i := 0; i < runway; i++ {
		require.Equal(t, 1, seen[hashes[i]], "runway block %d must be requested exactly once", i)
	}
	for h, n := range seen {
		require.Equal(t, 1, n, "hash %v must be assigned to exactly one peer (disjoint)", h)
	}
}

// TestReconcileLostAssignments_TTLExpiredOrphanRefetched covers the third orphan
// trigger: the global requestedBlocks ledger has a 60s TTL but assignedTo has
// none, so a block re-fetched to a peer that stays connected yet never delivers
// it has its requestedBlocks entry expire, stranding it in assignedTo below the
// cursor. reconcileLostAssignments must move any such hash into refetchBlocks
// while leaving still-tracked assignments untouched.
func TestReconcileLostAssignments_TTLExpiredOrphanRefetched(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3

	sm := &SyncManager{
		ctx:             context.Background(),
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		chainParams:     &chainParams,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:      make(map[chainhash.Hash]*peer.Peer),
		assignedAt:      make(map[chainhash.Hash]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
	defer sm.requestedBlocks.Stop()

	// identity-only peer value; reconcile never dereferences it.
	p := &peer.Peer{}

	// Orphan: assigned + recorded, then its requestedBlocks ledger entry expires
	// (modelled by never/no-longer being in requestedBlocks) while assignedTo lingers.
	orphan := chainhash.Hash{0x77}
	sm.assignedTo[orphan] = p
	sm.assignedAt[orphan] = time.Now().Add(-90 * time.Second)

	// Still-tracked block: present in BOTH maps; must be left alone.
	live := chainhash.Hash{0x88}
	sm.assignedTo[live] = p
	sm.assignedAt[live] = time.Now()
	sm.requestedBlocks.Set(live, struct{}{})

	sm.reconcileLostAssignments()

	_, queued := sm.refetchBlocks[orphan]
	require.True(t, queued, "TTL-expired orphan must be re-enqueued for re-fetch")
	_, stillAssigned := sm.assignedTo[orphan]
	require.False(t, stillAssigned, "TTL-expired orphan must be dropped from assignedTo")

	_, liveQueued := sm.refetchBlocks[live]
	require.False(t, liveQueued, "a still-tracked block must NOT be re-enqueued")
	_, liveAssigned := sm.assignedTo[live]
	require.True(t, liveAssigned, "a still-tracked block must remain assigned")
}

// TestReconcileLostAssignments_FlagOffNoop asserts the reconcile is a no-op with
// ParallelFetchPeers <= 1, so the single-peer path is untouched.
func TestReconcileLostAssignments_FlagOffNoop(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 1

	sm := &SyncManager{
		ctx:             context.Background(),
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		chainParams:     &chainParams,
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		assignedTo:      make(map[chainhash.Hash]*peer.Peer),
		assignedAt:      make(map[chainhash.Hash]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
	defer sm.requestedBlocks.Stop()

	orphan := chainhash.Hash{0x77}
	sm.assignedTo[orphan] = &peer.Peer{}
	sm.assignedAt[orphan] = time.Now().Add(-90 * time.Second)

	sm.reconcileLostAssignments()

	require.Empty(t, sm.refetchBlocks, "flag-off: reconcile must not enqueue anything")
	_, stillAssigned := sm.assignedTo[orphan]
	require.True(t, stillAssigned, "flag-off: reconcile must leave assignedTo untouched")
}
