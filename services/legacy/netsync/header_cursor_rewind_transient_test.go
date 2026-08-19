package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// refusedBlock is a headers-first manager walked to the one moment that matters:
// the front block of the header list has been asked for, has arrived, and the
// local store has refused it with a fault that says nothing about the block. Its
// header is off the front of the walk and its download record has been released,
// so nothing but a rewind can bring it back.
type refusedBlock struct {
	sm    *SyncManager
	hash  chainhash.Hash
	block *wire.MsgBlock
	peer  *peerpkg.Peer
}

// newRefusedBlock builds that state. backoffBase is how long the block's
// transient-failure throttle should last, which is the difference between the
// two tests below.
func newRefusedBlock(t *testing.T, peerIdx uint8, backoffBase time.Duration) *refusedBlock {
	t.Helper()

	// The headers are built first, because the storage mock has to be told which
	// block it is going to refuse.
	var nonce uint32

	anchor := chainhash.Hash{0xb1}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)
	failing := hashes[0]

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	client.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)
	// A busy local store, on this one block only.
	client.On("GetBlockExists", mock.Anything, &failing).
		Return(false, errors.NewStorageError("the store is busy"))
	client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Hour)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	sm.settings.Legacy.BlockFailureBackoffBase = backoffBase
	sm.settings.Legacy.BlockFailureBackoffMaxDuration = time.Hour

	t.Cleanup(func() { sm.blockFailureBackoff.Stop(); sm.recentlyFailedBlocks.Stop() })

	checkpointHash := chainhash.Hash{0xcd}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	syncPeer, _, _ := connectRacePeer(t, peerIdx, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The anchor block arrives, so the first real header becomes the front.
	sm.blockDownloads.Add(syncPeer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return sm.blockDownloads.RequestedWithin(failing, time.Minute) }, 5*time.Second),
		"the front block should have been asked for")

	block := &wire.MsgBlock{Header: *msg.Headers[0]}

	require.Error(t, sm.handleBlockMsg(&blockQueueMsg{
		block:     block,
		blockHash: failing,
		peer:      syncPeer,
	}), "the store refused the block")

	require.False(t, sm.blockDownloads.RequestedWithin(failing, time.Minute),
		"delivering the block released the peer's obligation, so nothing is outstanding for it any more")

	return &refusedBlock{sm: sm, hash: failing, block: block, peer: syncPeer}
}

// TestSyncManager_ABlockTheStoreRefusesIsAskedForAgain is the case the rewind
// machinery was built for and was not applied to.
//
// A block that fails to store with a local fault — the store is busy, a batch
// timed out — is not a bad block. But its header has already been taken off the
// front of the header list, and in headers-first mode that list is the only
// thing that fetches anything: the getblocks the drop paths send is thrown away
// by processInvMsg while headers-first mode is on. So the block left the walk
// and nothing ever asked for it again; the node sat still until the 180-second
// stall detector rotated the sync peer and rebuilt the header list from scratch.
//
// The end state asserted here is the one a stalled node never reaches: the block
// is outstanding again — and with no other block arriving and no headers message
// coming, so nothing but the rewind and the sweep's own top-up can have done it.
func TestSyncManager_ABlockTheStoreRefusesIsAskedForAgain(t *testing.T) {
	r := newRefusedBlock(t, 63, time.Millisecond)

	require.True(t, WaitUntil(func() bool {
		// What the park sweep's ticker calls, once every 30 seconds.
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a block the store refused must be asked for again once its backoff has expired")
}

// TestSyncManager_ABlockInsideItsBackoffIsNotAskedForYet is the brake the rewind
// cannot ship without.
//
// The per-block backoff exists so a block that has just failed on a struggling
// store is not immediately downloaded and decorated all over again. Putting the
// walk back on the block would undo that by itself, because the very next round
// would ask for it. So the walk stops at a block still inside its backoff — and
// stops rather than skips, because skipping moves the cursor past it and loses
// the block a second time, which is the failure the rewind exists to prevent.
func TestSyncManager_ABlockInsideItsBackoffIsNotAskedForYet(t *testing.T) {
	r := newRefusedBlock(t, 64, time.Hour)

	// A full walk, synchronously: a block is recorded in the download ledger
	// before its getdata is queued, so the ledger says what this round asked for
	// without waiting on the wire.
	r.sm.fetchHeaderBlocks()
	r.sm.resumeHeaderWalk()

	require.False(t, r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute),
		"a block still inside its transient-failure backoff must not be asked for again yet")
}

// TestSyncManager_ABlockReDeliveredInsideItsBackoffKeepsItsPlaceInTheWalk
// closes the loop the previous two tests open.
//
// Once the walk is back on a block the store refused, a second copy can still
// arrive — a peer racing the frontier, or a getdata answered twice — and it
// arrives while the block is still inside its backoff. That copy is skipped
// without being processed, which is right; what is not right is that skipping it
// also takes its header off the front of the walk. Undoing the rewind on the
// strength of a duplicate delivery leaves the block in exactly the state the
// rewind was there to prevent: downloaded, dropped, and never asked for again.
func TestSyncManager_ABlockReDeliveredInsideItsBackoffKeepsItsPlaceInTheWalk(t *testing.T) {
	// An hour-long backoff, so the duplicate below is certainly inside it and
	// the test is not racing a clock.
	r := newRefusedBlock(t, 65, time.Hour)

	fs, ok := r.sm.blockFailureBackoff.Get(r.hash)
	require.True(t, ok, "the refusal should have started a backoff")
	require.True(t, fs.nextRetry.After(time.Now()), "the backoff should still be running")

	// A second peer was asked for the same block while the sync peer was slow on
	// it — that is what the frontier race does — so a second copy is still owed
	// and still on its way.
	r.sm.blockDownloads.Add(r.peer, r.hash)

	// It turns up while the backoff is running.
	require.Error(t, r.sm.handleBlockMsg(&blockQueueMsg{
		block:     r.block,
		blockHash: r.hash,
		peer:      r.peer,
	}), "a block inside its backoff is skipped rather than processed")

	// The store recovers and the throttle lifts.
	r.sm.blockFailureBackoff.Delete(r.hash)

	require.True(t, WaitUntil(func() bool {
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a duplicate delivery must not cost the block its place in the download walk")
}
