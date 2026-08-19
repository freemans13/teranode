package netsync

import (
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

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.chainParams = params
	sm.nextCheckpoint = &first

	return sm, first, final
}

// TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater is
// the ordering this function got wrong.
//
// A parked block can be the checkpoint block, and it can be drained after the
// peer that delivered it has gone and while there is no sync peer at all. The
// nil-peer case was handled — but only after the checkpoint had already been
// moved on. Nothing had been asked for, and the node had forgotten which round
// of headers it still needed, so when a peer did arrive it asked for the round
// after the one it was missing and headers-first sync never filled the gap.
//
// The end state asserted here is the one that keeps sync going: whatever
// happened while there was nobody to ask, the node still asks for the headers it
// has not got.
func TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The checkpoint block commits from the park with nobody to ask.
	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	// A peer turns up and the transition is made again.
	peer, rec := connectHeaderRequestPeer(t, 66)
	sm.storeSyncPeer(peer, &syncPeerState{})

	require.NoError(t, sm.checkpointBlockCommitted(peer, *first.Hash))

	require.True(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 5*time.Second),
		"the node must still ask for the round of headers it has not got")

	require.True(t, sm.headersFirstMode.Load(),
		"there is another checkpoint to reach, so headers-first sync must still be on")
	require.Zero(t, rec.getBlocksCount(),
		"headers, not blocks by inventory: the final checkpoint has not been reached")
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
