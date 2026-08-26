package netsync

import (
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// headerRequestRecorder records what a peer was asked to fetch next: the stop
// hash of every getheaders, and how many getblocks went out.
type headerRequestRecorder struct {
	mu        sync.Mutex
	stopHash  []chainhash.Hash
	getBlocks int
}

func (r *headerRequestRecorder) noteGetHeaders(msg *wire.MsgGetHeaders) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopHash = append(r.stopHash, msg.HashStop)
}

func (r *headerRequestRecorder) noteGetBlocks() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getBlocks++
}

func (r *headerRequestRecorder) askedForHeadersUpTo(hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.stopHash {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

func (r *headerRequestRecorder) getBlocksCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getBlocks
}

// connectHeaderRequestPeer returns a live peer whose remote end records the two
// things a checkpoint transition can send.
func connectHeaderRequestPeer(t *testing.T, idx uint8) (*peerpkg.Peer, *headerRequestRecorder) {
	t.Helper()

	rec := &headerRequestRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetHeaders: func(_ *peerpkg.Peer, msg *wire.MsgGetHeaders) { rec.noteGetHeaders(msg) },
			OnGetBlocks:  func(_ *peerpkg.Peer, _ *wire.MsgGetBlocks) { rec.noteGetBlocks() },
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(1000)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, rec
}

// twoCheckpoints is a chain with a checkpoint at 200 and a final one at 300.
func twoCheckpoints() (*chaincfg.Params, chaincfg.Checkpoint, chaincfg.Checkpoint) {
	first := chaincfg.Checkpoint{Height: 200, Hash: &chainhash.Hash{0xc1}}
	final := chaincfg.Checkpoint{Height: 300, Hash: &chainhash.Hash{0xc2}}

	params := chaincfg.MainNetParams
	params.Checkpoints = []chaincfg.Checkpoint{first, final}

	return &params, first, final
}

// newCheckpointManager is a headers-first manager sitting on the first of two
// checkpoints.
func newCheckpointManager(t *testing.T) (*SyncManager, chaincfg.Checkpoint, chaincfg.Checkpoint) {
	t.Helper()

	params, first, final := twoCheckpoints()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	client.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 200}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)

	client.On("CatchUpBlocks", mock.Anything).Return(nil)
	client.On("Run", mock.Anything, mock.Anything).Return(nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.chainParams = params
	sm.nextCheckpoint = &first

	return sm, first, final
}

// connectCheckpointCandidate registers a live peer with the manager as an
// electable sync candidate well above our height, so the real startSync inside
// handleCheckSyncPeer has somebody to pick.
func connectCheckpointCandidate(t *testing.T, sm *SyncManager, idx uint8) (*peerpkg.Peer, *headerRequestRecorder) {
	t.Helper()

	peer, rec := connectHeaderRequestPeer(t, idx)

	state := &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)
	state.noteBestKnownHeight(peer.LastBlock())

	sm.peerStates.Set(peer, state)

	return peer, rec
}

// TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater is
// the path out of the park drain that had no way back.
//
// A parked block can be the checkpoint block, and it can be drained after the
// peer that delivered it has gone and while there is no sync peer at all. The
// nil-peer arm correctly refused to advance the checkpoint, so the node kept its
// record of the round it still owed — but nothing ever asked for it.
// checkpointBlockCommitted is reached only by a block committing, and a
// checkpoint block commits exactly once: it is in the chain afterwards,
// haveInventory answers true for it and the download walk never asks again. So
// headers-first sync stopped at that checkpoint for the life of the process.
//
// Nothing in this test invokes the transition a second time. The one call it
// makes is the one production makes, with nil, and the getheaders that follows
// has to be produced by the sync-peer check on its own. That is the difference
// from the version of this test that shipped with the nil-peer arm: it called
// checkpointBlockCommitted again by hand, which is a step production never
// performs, so it pinned idempotency rather than recovery and passed against the
// broken code.
func TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The checkpoint block commits from the park with nobody to ask.
	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	require.Equal(t, first.Height, sm.nextCheckpointSnapshot().Height,
		"nothing was asked for, so the round the node still owes must not have been advanced past")

	// A candidate turns up, and the ticker that elects sync peers runs. From
	// here the test does nothing but wait.
	_, rec := connectCheckpointCandidate(t, sm, 66)

	sm.handleCheckSyncPeer()

	require.True(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 5*time.Second),
		"the election must replay the deferred round on its own: the node still has to ask for the headers it has not got")

	require.True(t, sm.headersFirstMode.Load(),
		"there is another checkpoint to reach, so headers-first sync must still be on")
	require.Equal(t, final.Height, sm.nextCheckpointSnapshot().Height,
		"the replay is what advances the checkpoint, so it must have advanced")
	require.Nil(t, sm.pendingCheckpoint.Load(), "the deferred round has been asked for and must not be replayed again")
}

// TestSyncManager_ACheckpointReachedWithNoPeerIsStillMarkedAsTheAnchor covers
// the other half of the nil-peer arm. The checkpoint node stays in the header
// list so the next round's first header can prove it links to it, which makes it
// a block now in this node's chain that no peer will ever send again. If it is
// not marked, removeHeaderAnchorLocked — which removes by identity — cannot trim
// it, and a headers round arriving by any other route wedges on a front that
// will never be delivered.
func TestSyncManager_ACheckpointReachedWithNoPeerIsStillMarkedAsTheAnchor(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	sm.headerMu.Lock()
	sm.headerList = list.New()
	sm.headerIndex = make(map[chainhash.Hash]*list.Element)
	sm.indexHeaderLocked(sm.headerList.PushBack(&headerNode{height: first.Height, hash: first.Hash}), *first.Hash)
	sm.headerMu.Unlock()

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	sm.headerMu.Lock()
	node, ok := sm.headerIndex[*first.Hash].Value.(*headerNode)
	sm.headerMu.Unlock()

	require.True(t, ok)
	require.True(t, node.isAnchor, "a committed checkpoint left in the list is the next round's anchor, peer or no peer")
}

// TestSyncManager_TheFinalCheckpointLeavesHeadersFirstMode is the other half of
// the same function, and the reason the nil-peer guard cannot simply return
// early without thinking: past the last checkpoint the node has to stop driving
// sync from the header list and go back to asking for blocks by inventory.
func TestSyncManager_TheFinalCheckpointLeavesHeadersFirstMode(t *testing.T) {
	sm, _, final := newCheckpointManager(t)
	sm.nextCheckpoint = &final
	sm.headersFirstMode.Store(true)

	peer, rec := connectHeaderRequestPeer(t, 67)
	sm.storeSyncPeer(peer, &syncPeerState{})

	require.NoError(t, sm.checkpointBlockCommitted(peer, *final.Hash))

	require.True(t, WaitUntil(func() bool { return rec.getBlocksCount() > 0 }, 5*time.Second),
		"past the final checkpoint the node asks for blocks by inventory")

	require.False(t, sm.headersFirstMode.Load(), "headers-first sync is over")
	require.Nil(t, sm.nextCheckpointSnapshot(), "there is no checkpoint left to head for")
}

// TestSyncManager_ACheckpointCommittedWithNothingLeftToReachIsANoOp covers the
// third way in: a block that was the checkpoint of a list built before the last
// checkpoint was passed. There is nothing to advance and nobody to ask.
func TestSyncManager_ACheckpointCommittedWithNothingLeftToReachIsANoOp(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.nextCheckpoint = nil

	peer, rec := connectHeaderRequestPeer(t, 68)
	sm.storeSyncPeer(peer, &syncPeerState{})

	require.NoError(t, sm.checkpointBlockCommitted(peer, *first.Hash))

	require.Zero(t, rec.getBlocksCount(), "nothing to ask for")
	require.False(t, rec.askedForHeadersUpTo(*first.Hash))
}
