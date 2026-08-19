package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newTestTracker builds a ledger whose clock the test drives, so assignments can
// be aged without sleeping.
func newTestTracker(ttl time.Duration) (*blockDownloadTracker, func(time.Duration)) {
	tr := newBlockDownloadTracker(ttl)

	base := time.Now()
	offset := time.Duration(0)
	tr.now = func() time.Time { return base.Add(offset) }

	return tr, func(d time.Duration) { offset += d }
}

// TestClearRequestedState_ActuallyReleasesTheHash pins the behaviour
// clearRequestedState has always claimed in its comment and has never had: when
// a peer goes away, the blocks we were waiting on from it must stop being owed
// by anybody, so the next inv that announces one re-requests it from somewhere
// else. A hash that stays recorded is never asked for again, and sync waits on a
// peer that has already gone.
func TestClearRequestedState_ActuallyReleasesTheHash(t *testing.T) {
	h := chainhash.Hash{0x42}

	sm := &SyncManager{
		logger:         ulogger.TestLogger{},
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
	}

	p := newTestPeer(t, "localhost:18333")

	state := &peerSyncState{
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour),
	}
	sm.peerStates.Set(p, state)

	// Record the request exactly as fetchHeaderBlocks does.
	sm.blockDownloads.Add(p, h)
	require.True(t, sm.blockDownloads.HasOwner(p, h), "sanity: the peer owes us the block before it leaves")

	sm.handleDonePeerMsg(p)

	require.Zero(t, sm.blockDownloads.Len(), "a departing peer's outstanding block must be released so the next inv re-requests it")
	require.False(t, sm.blockDownloads.RequestedWithin(h, blockRequestRetryInterval), "nobody is owed the block once its only peer has gone")
}

// TestBlockDownloadTracker_MultipleOwnersPerHash pins the shape the two maps this
// replaced could not express. The frontier race deliberately asks a second peer
// for a block the sync peer already owes us, and both are then entitled to
// deliver it without losing their connection.
func TestBlockDownloadTracker_MultipleOwnersPerHash(t *testing.T) {
	tr, _ := newTestTracker(time.Hour)

	p1 := newTestPeer(t, "localhost:18401")
	p2 := newTestPeer(t, "localhost:18402")
	h := chainhash.Hash{0x11}

	tr.Add(p1, h)
	tr.Add(p2, h)

	require.True(t, tr.HasOwner(p1, h))
	require.True(t, tr.HasOwner(p2, h), "a raced block is owed by both peers at once")
	require.Equal(t, 1, tr.Len(), "one block, however many peers were asked")
	require.Equal(t, 1, tr.CountForPeer(p1))
	require.Equal(t, 1, tr.CountForPeer(p2))
	require.Equal(t, 2, tr.PeersWithDownloads())

	// Cancelling one peer's obligation must leave the other's alone.
	tr.RemoveOwner(p1, h)
	require.False(t, tr.HasOwner(p1, h))
	require.True(t, tr.HasOwner(p2, h), "cancelling one peer must not cancel the other")
	require.Equal(t, 0, tr.CountForPeer(p1))
	require.Equal(t, 1, tr.PeersWithDownloads())

	// The block arriving drops it for everybody.
	tr.Remove(h)
	require.False(t, tr.HasOwner(p2, h))
	require.Zero(t, tr.Len())
	require.Zero(t, tr.PeersWithDownloads())
}

// TestBlockDownloadTracker_AssignmentsExpire pins the safety net the expiring
// maps used to provide and a plain map would have thrown away. ClearPeer is not
// guaranteed to run: handleDonePeerMsg returns early for any peer not registered
// in peerStates, which includes the stream sub-peers a BlockPriority association
// resolves through. An assignment nothing ever clears must age out by itself.
func TestBlockDownloadTracker_AssignmentsExpire(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	p := newTestPeer(t, "localhost:18403")
	h := chainhash.Hash{0x22}

	tr.Add(p, h)
	require.True(t, tr.HasOwner(p, h))

	advance(2 * blockRequestAssignmentTTL)

	require.False(t, tr.HasOwner(p, h), "an assignment past its lifetime is no longer owed")
	require.False(t, tr.RequestedWithin(h, blockRequestAssignmentTTL))
	require.Zero(t, tr.CountForPeer(p))
	require.Zero(t, tr.PeersWithDownloads())
	require.Zero(t, tr.Len())

	// And it is genuinely gone, not merely hidden — otherwise a peer that never
	// leaves pins its hashes for the life of the process.
	tr.mu.Lock()
	byHash, byPeer := len(tr.byHash), len(tr.byPeer)
	tr.mu.Unlock()
	require.Zero(t, byHash, "the expired assignment must be swept, not just ignored")
	require.Zero(t, byPeer, "the expired assignment must be swept, not just ignored")
}

// TestBlockDownloadTracker_RetryWindowIsShorterThanTheOwnershipCeiling pins the
// two lifetimes the old pair of maps carried between them, now held in one
// store. They answer different questions and must not be collapsed: after a
// minute we are willing to ask somebody else for the block, but for a full hour
// the peer we originally asked is still answering our question and must keep its
// connection when its copy finally lands.
func TestBlockDownloadTracker_RetryWindowIsShorterThanTheOwnershipCeiling(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	p := newTestPeer(t, "localhost:18404")
	h := chainhash.Hash{0x33}

	tr.Add(p, h)
	require.True(t, tr.RequestedWithin(h, blockRequestRetryInterval), "just asked, so no need to ask again")

	advance(90 * time.Second)

	require.False(t, tr.RequestedWithin(h, blockRequestRetryInterval),
		"past the retry window the inv path must be free to ask another peer, or a lost getdata stalls sync")
	require.True(t, tr.HasOwner(p, h),
		"still inside the ownership ceiling, so a peer delivering a big block late must not be disconnected")

	advance(blockRequestAssignmentTTL)

	require.False(t, tr.HasOwner(p, h), "past the ceiling the peer no longer owes us anything")
}

// TestBlockDownloadTracker_ClearPeerReleasesEveryHash covers the departing-peer
// path at the ledger level: every hash that peer owed is released, hashes still
// owed by somebody else survive, and the peer's own row goes.
func TestBlockDownloadTracker_ClearPeerReleasesEveryHash(t *testing.T) {
	tr, _ := newTestTracker(time.Hour)

	leaving := newTestPeer(t, "localhost:18405")
	staying := newTestPeer(t, "localhost:18406")

	mine := chainhash.Hash{0x44}
	shared := chainhash.Hash{0x45}
	theirs := chainhash.Hash{0x46}

	tr.Add(leaving, mine)
	tr.Add(leaving, shared)
	tr.Add(staying, shared)
	tr.Add(staying, theirs)

	tr.ClearPeer(leaving)

	require.False(t, tr.HasOwner(leaving, mine))
	require.False(t, tr.HasOwner(leaving, shared))
	require.Zero(t, tr.CountForPeer(leaving))
	require.True(t, tr.HasOwner(staying, shared), "a hash another peer still owes must survive")
	require.True(t, tr.HasOwner(staying, theirs))
	require.Equal(t, 2, tr.Len(), "only the block nobody else was asked for goes")

	tr.mu.Lock()
	_, stillListed := tr.byPeer[leaving]
	tr.mu.Unlock()
	require.False(t, stillListed, "the departed peer must not be left as an empty row")
}

// TestBlockDownloadTracker_NilIsSafeAndFailsClosed covers a manager built as a
// struct literal without a ledger. Reads must answer "nothing is owed" rather
// than panic — which means an arriving block reads as unrequested, the safe
// direction, because admitting a block nobody asked for is worse than dropping
// a peer that should not have sent one.
func TestBlockDownloadTracker_NilIsSafeAndFailsClosed(t *testing.T) {
	var tr *blockDownloadTracker

	p := newTestPeer(t, "localhost:18407")
	h := chainhash.Hash{0x55}

	require.NotPanics(t, func() {
		tr.Add(p, h)
		tr.Remove(h)
		tr.RemoveOwner(p, h)
		tr.ClearPeer(p)
		tr.ForgetForRetry(blockRequestRetryInterval)
	})

	require.False(t, tr.HasOwner(p, h))
	require.False(t, tr.RequestedWithin(h, blockRequestRetryInterval))
	require.Zero(t, tr.CountForPeer(p))
	require.Zero(t, tr.PeersWithDownloads())
	require.Zero(t, tr.Len())
}

// TestPeersWithBlockDownloads_ExpiredAssignmentDoesNotInflateTheCount pins that
// an assignment past its lifetime stops counting as a live download. The count
// this feeds multiplies every peer's block deadline, and the same number caps
// how much fetchHeaderBlocks will ask for next — a cap that falls to a single
// block once blocks get large, at which point one dead entry stops all fetching.
func TestPeersWithBlockDownloads_ExpiredAssignmentDoesNotInflateTheCount(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	sm := &SyncManager{
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockDownloads: tr,
	}

	p := newTestPeer(t, "localhost:18334")
	sm.peerStates.Set(p, &peerSyncState{})
	tr.Add(p, chainhash.Hash{0x01})

	require.Equal(t, 1, sm.PeersWithBlockDownloads(), "sanity: a live assignment counts")

	advance(2 * blockRequestAssignmentTTL)

	require.Equal(t, 0, sm.PeersWithBlockDownloads(), "an assignment past its lifetime must not count as a live download")
}

// TestHandleBlockMsg_UnrequestedBlockStillDisconnects is the parity check that
// matters most in this change: the ledger now answers the question the
// disconnect decision asks, so getting the identity wrong would either punish
// honest peers or admit blocks nobody asked for. A block from a peer we asked is
// admitted; the same block from a peer we did not ask costs that peer its
// connection.
func TestHandleBlockMsg_UnrequestedBlockStillDisconnects(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	asked, _, _ := connectRacePeer(t, 40, 1000)
	stranger, _, _ := connectRacePeer(t, 41, 1000)

	registerRacePeer(sm, asked)
	registerRacePeer(sm, stranger)

	h := chainhash.Hash{0x66}
	sm.blockDownloads.Add(asked, h)

	// Carrying no block makes handleBlockMsg bail straight after the check under
	// test, so the error only tells us whether it got past the disconnect.
	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: h, peer: asked})
	require.Error(t, err)
	require.True(t, asked.Connected(), "a peer answering our own request must keep its connection")

	err = sm.handleBlockMsg(&blockQueueMsg{blockHash: h, peer: stranger})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError))
	require.True(t, WaitUntil(func() bool { return !stranger.Connected() }, 2*time.Second),
		"a peer we never asked must be disconnected for an unrequested block")
}

// TestHandleBlockMsg_DeliveryDoesNotCancelTheOtherPeersWeAsked pins which
// obligations a delivered block clears. The two maps this replaced cleared
// different things — the global record went, the delivering peer's own record
// went, and any other peer we had also asked kept its record and so kept its
// pass on the late copy still travelling towards us. Collapsing that into
// "forget the block entirely" would disconnect a peer for answering a question
// we asked it.
func TestHandleBlockMsg_DeliveryDoesNotCancelTheOtherPeersWeAsked(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	first, _, _ := connectRacePeer(t, 42, 1000)
	second, _, _ := connectRacePeer(t, 43, 1000)

	registerRacePeer(sm, first)
	registerRacePeer(sm, second)

	h := chainhash.Hash{0x67}
	sm.blockDownloads.Add(first, h)
	sm.blockDownloads.Add(second, h)

	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: h, peer: first})
	require.Error(t, err)

	require.False(t, sm.blockDownloads.HasOwner(first, h), "the peer that delivered no longer owes us the block")
	require.True(t, sm.blockDownloads.HasOwner(second, h), "the other peer we asked keeps its pass on the copy already on the way")

	// And that pass is real: the second copy arriving must not cost that peer
	// its connection.
	err = sm.handleBlockMsg(&blockQueueMsg{blockHash: h, peer: second})
	require.Error(t, err)
	require.True(t, second.Connected(), "a second copy from a peer we also asked must not be treated as unrequested")
}

// TestBlockDownloadTracker_ForgetForRetryMovesOnlyTheRetryWindow pins the two
// windows apart at the ledger level. Reopening a block for re-request must make
// the inv path willing to ask somebody else immediately, and must leave the peer
// we already asked free to deliver — the delivery ceiling is an hour, and losing
// a minute of it is the whole cost.
func TestBlockDownloadTracker_ForgetForRetryMovesOnlyTheRetryWindow(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	fresh := chainhash.Hash{0x01}
	stale := chainhash.Hash{0x02}

	freshPeer := newTestPeer(t, "localhost:18421")
	stalePeer := newTestPeer(t, "localhost:18422")

	// One assignment made half an hour ago, one made just now.
	tr.Add(stalePeer, stale)
	advance(30 * time.Minute)
	tr.Add(freshPeer, fresh)

	require.True(t, tr.RequestedWithin(fresh, blockRequestRetryInterval), "sanity: just asked")
	require.False(t, tr.RequestedWithin(stale, blockRequestRetryInterval), "sanity: asked long ago")

	tr.ForgetForRetry(blockRequestRetryInterval)

	require.False(t, tr.RequestedWithin(fresh, blockRequestRetryInterval),
		"the fresh request must be reopened, so the next inv fetches the block from the new sync peer")
	require.True(t, tr.HasOwner(freshPeer, fresh),
		"the peer we asked keeps its permission to deliver")
	require.True(t, tr.HasOwner(stalePeer, stale),
		"an older assignment keeps its permission to deliver too")

	// The half-hour-old assignment must not have been made younger: it has to
	// keep ageing out of the delivery ceiling on its original schedule.
	advance(30 * time.Minute)
	require.False(t, tr.HasOwner(stalePeer, stale),
		"reopening must not reset an assignment's age against the delivery ceiling")
	require.True(t, tr.HasOwner(freshPeer, fresh),
		"the fresh assignment is only a minute older than it was, well inside the ceiling")
}
