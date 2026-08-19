package netsync

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
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getDataRecorder collects the block hashes a peer's remote end is asked for.
type getDataRecorder struct {
	mu     sync.Mutex
	hashes []chainhash.Hash
}

func (r *getDataRecorder) record(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.hashes = append(r.hashes, iv.Hash)
		}
	}
}

func (r *getDataRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.hashes)
}

func (r *getDataRecorder) all() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]chainhash.Hash, len(r.hashes))
	copy(out, r.hashes)

	return out
}

// connectRacePeer returns a live peer whose remote end records every getdata it
// is sent. The remote peer is returned too so the caller keeps it alive for the
// duration of the test.
func connectRacePeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	rec := &getDataRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData: func(_ *peerpkg.Peer, msg *wire.MsgGetData) {
				rec.record(msg)
			},
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

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, remote, rec
}

// newRaceManager builds the smallest SyncManager the frontier-race path needs.
// Settings come from the real loader, so the defaults under test are the ones an
// unconfigured node actually gets.
func newRaceManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:         ulogger.TestLogger{},
		settings:       test.CreateBaseTestSettings(t),
		chainParams:    &chaincfg.MainNetParams,
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		headerList:     list.New(),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
		racedBlocks: expiringmap.New[chainhash.Hash, map[*peerpkg.Peer]struct{}](racedBlockGraceTTL).
			WithMaxSize(racedBlockGraceMaxTracked),
	}
	sm.headersFirstMode.Store(true)

	t.Cleanup(func() { sm.racedBlocks.Stop() })

	return sm
}

// registerRacePeer adds a peer to the manager as a sync candidate and returns
// its state.
func registerRacePeer(sm *SyncManager, p *peerpkg.Peer) *peerSyncState {
	state := &peerSyncState{
		syncCandidate: true,
	}
	sm.peerStates.Set(p, state)

	return state
}

// TestFrontierRace_AsksASecondPeerForTheStuckBlock is the anchor test. The sync
// peer has gone quiet on the one block everything else is queued behind, so a
// second connected peer must be asked for that same block — and only that block
// — while the original request is left alone.
func TestFrontierRace_AsksASecondPeerForTheStuckBlock(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, syncRec := connectRacePeer(t, 1, 1000)
	other, _, otherRec := connectRacePeer(t, 2, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// The sync peer was asked for this block and has not answered for 30
	// seconds, comfortably past the 20 second default.
	frontier := chainhash.Hash{0xaa}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 5*time.Second),
		"the second peer should have been asked for the stuck block")
	require.Equal(t, []chainhash.Hash{frontier}, otherRec.all(),
		"the second peer should be asked for the stuck block and nothing else")
	require.Zero(t, syncRec.count(), "the original peer must not be asked again")

	// The reply has to be authorised, or the delivering peer would be
	// disconnected for sending a block we never asked for.
	_, exists := sm.peerStates.Get(other)
	require.True(t, exists)
	authorised := sm.blockDownloads.HasOwner(other, frontier)
	require.True(t, authorised, "the second peer's reply must be authorised in advance")

	// The original request stands: we added a copy, we did not move the block.
	stillAsked := sm.blockDownloads.HasOwner(syncPeer, frontier)
	require.True(t, stillAsked, "the original request must be left in place")
}

// TestFrontierRace_RunsFromTheBlockHandler proves the feature is actually
// reachable in a running node rather than only when a test calls it directly:
// the block handler's own timer must fire the race with no help.
func TestFrontierRace_RunsFromTheBlockHandler(t *testing.T) {
	sm := newRaceManager(t)
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{}, 1)

	syncPeer, _, _ := connectRacePeer(t, 3, 1000)
	other, _, otherRec := connectRacePeer(t, 4, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xbb}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	go sm.blockHandler()

	t.Cleanup(func() {
		close(sm.quit)
		<-sm.handlerDone
	})

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 3*frontierCheckInterval),
		"the block handler's timer should have raced the stuck block without being prompted")
	require.Equal(t, []chainhash.Hash{frontier}, otherRec.all())
}

// TestFrontierRaceTarget covers every reason not to race. Each case must return
// false; the last one is the case that must return true, so a change that made
// the gates unconditionally strict would not slip through.
func TestFrontierRaceTarget(t *testing.T) {
	syncPeer, _, _ := connectRacePeer(t, 5, 1000)
	other, _, _ := connectRacePeer(t, 6, 1000)
	shortPeer, _, _ := connectRacePeer(t, 7, 100)

	stale := func() time.Time { return time.Now().Add(-30 * time.Second) }

	// setup builds a manager in the state where a race SHOULD happen, so each
	// case only has to break the one thing it is about.
	setup := func(t *testing.T) *SyncManager {
		sm := newRaceManager(t)
		registerRacePeer(sm, syncPeer)
		registerRacePeer(sm, other)
		sm.storeSyncPeer(syncPeer, &syncPeerState{})
		sm.setFrontier(chainhash.Hash{0xcc}, 500, stale())

		return sm
	}

	t.Run("races when the frontier is stuck and nothing else is wrong", func(t *testing.T) {
		sm := setup(t)
		hash, height, target, ok := sm.frontierRaceTarget(time.Now())
		require.True(t, ok)
		require.Equal(t, chainhash.Hash{0xcc}, hash)
		require.Equal(t, int32(500), height)
		require.Equal(t, other, target)
	})

	t.Run("no frontier", func(t *testing.T) {
		sm := setup(t)
		sm.clearFrontier()
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("frontier not stuck for long enough yet", func(t *testing.T) {
		sm := setup(t)
		sm.setFrontier(chainhash.Hash{0xdd}, 500, time.Now().Add(-2*time.Second))
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("racing switched off by legacy_maxBlockParallelFetch", func(t *testing.T) {
		sm := setup(t)
		sm.settings.Legacy.MaxBlockParallelFetch = 1
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("racing switched off by legacy_blockSlowFetchTimeout", func(t *testing.T) {
		sm := setup(t)
		sm.settings.Legacy.BlockSlowFetchTimeout = 0
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("not in headers-first mode", func(t *testing.T) {
		sm := setup(t)
		sm.headersFirstMode.Store(false)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("already at the configured number of peers", func(t *testing.T) {
		sm := setup(t)
		require.True(t, sm.registerFrontierRacer(chainhash.Hash{0xcc}, other))
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "default of 2 is the holder plus one racer, so no third peer")
	})

	t.Run("we are throttling our own reads", func(t *testing.T) {
		sm := setup(t)
		sm.blockBacklog.Store(1)
		require.True(t, sm.localReadBackpressured())
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("sync peer is still pulling bytes", func(t *testing.T) {
		sm := setup(t)
		sm.storeSyncPeer(syncPeer, &syncPeerState{
			ticks:                  1,
			assocReadBytes:         64 << 20,
			assocReadBytesLastTick: 0,
		})
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "a peer mid-transfer of a large block is slow, not stalled")
	})

	t.Run("no sync peer", func(t *testing.T) {
		sm := setup(t)
		sm.storeSyncPeer(nil, nil)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer is not a sync candidate", func(t *testing.T) {
		sm := setup(t)
		state, exists := sm.peerStates.Get(other)
		require.True(t, exists)
		state.syncCandidate = false
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer is not connected", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		registerRacePeer(sm, &peerpkg.Peer{})
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer does not have the block yet", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		registerRacePeer(sm, shortPeer)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("there is no other peer at all", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "with only the peer that already owes us the block, do nothing")
	})
}

// TestNoteRaceWinner_CancelsTheRequestWithEveryoneElse is the guard against
// leaving requests behind that will never be answered. One stale entry counts
// against the in-flight limit fetchHeaderBlocks uses, and that limit drops to a
// single block once blocks get large, so a leftover would stop fetching outright.
func TestNoteRaceWinner_CancelsTheRequestWithEveryoneElse(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, _ := connectRacePeer(t, 8, 1000)
	other, _, _ := connectRacePeer(t, 9, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xee}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	asked := sm.blockDownloads.HasOwner(other, frontier)
	require.True(t, asked, "precondition: the second peer was asked")

	sm.noteRaceWinner(frontier)

	stillAsked := sm.blockDownloads.HasOwner(syncPeer, frontier)
	require.False(t, stillAsked, "the original peer's request must be cancelled once the block arrives")
	stillAskedOther := sm.blockDownloads.HasOwner(other, frontier)
	require.False(t, stillAskedOther, "the second peer's request must be cancelled too")

	sm.frontierMu.Lock()
	racers := len(sm.frontierRacers)
	sm.frontierMu.Unlock()
	require.Zero(t, racers, "the race is over")
}

// TestBlockRacedTo_OnlyForPeersWeAsked checks the one place this change relaxes
// an existing defence. A peer that sends a block nobody asked for is still
// disconnected; only the specific peers we asked for the specific block we raced
// get a pass.
func TestBlockRacedTo_OnlyForPeersWeAsked(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, _ := connectRacePeer(t, 10, 1000)
	other, _, _ := connectRacePeer(t, 11, 1000)
	stranger, _, _ := connectRacePeer(t, 12, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0x0f}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// Only one other peer is registered at this point, so it is the one raced to.
	sm.raceFrontierBlock(time.Now())

	// Nothing is forgiven until the race is actually decided.
	require.False(t, sm.BlockRacedTo(other, &frontier))

	sm.noteRaceWinner(frontier)

	registerRacePeer(sm, stranger)

	require.True(t, sm.BlockRacedTo(other, &frontier), "we asked this peer for this block")
	require.True(t, sm.BlockRacedTo(syncPeer, &frontier), "we asked this peer for this block first")
	require.False(t, sm.BlockRacedTo(stranger, &frontier), "we never asked this peer")

	unrelated := chainhash.Hash{0x99}
	require.False(t, sm.BlockRacedTo(other, &unrelated), "we never asked for this block")
}

// TestHandleBlockMsg_LateCopyOfARacedBlockIsNotPunished checks the exception is
// wired into the block handler itself, not just available as a helper. A peer
// that answers a request we cancelled keeps its connection; a peer that sends a
// block nobody asked for still loses it.
func TestHandleBlockMsg_LateCopyOfARacedBlockIsNotPunished(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	syncPeer, _, _ := connectRacePeer(t, 14, 1000)
	loser, _, _ := connectRacePeer(t, 15, 1000)
	stranger, _, _ := connectRacePeer(t, 16, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, loser)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0x77}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// Race it, then let it be delivered, which cancels the request with the
	// loser and leaves it with a copy still on the way. Only one other peer is
	// registered at this point, so it is the one raced to.
	sm.raceFrontierBlock(time.Now())
	sm.noteRaceWinner(frontier)

	registerRacePeer(sm, stranger)

	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: frontier, peer: loser})
	require.Error(t, err, "the late copy is discarded")
	require.True(t, errors.IsTransientLocalError(err), "discarding a late copy must not read as misbehaviour")
	require.True(t, loser.Connected(), "a peer that answered our own request must not be disconnected")

	err = sm.handleBlockMsg(&blockQueueMsg{blockHash: frontier, peer: stranger})
	require.Error(t, err)
	require.True(t, WaitUntil(func() bool { return !stranger.Connected() }, 2*time.Second),
		"a peer we never asked must still be disconnected for an unrequested block")
}

// TestFetchHeaderBlocks_PublishesTheFrontier proves the frontier is actually
// recorded by the code that sends the block requests. Nothing else sets it, so
// without this call the race could never find a block to run on.
func TestFetchHeaderBlocks_PublishesTheFrontier(t *testing.T) {
	blockchainClient := &blockchain2.Mock{}
	// No block we ask about is already in our chain, so all of them get requested.
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	syncPeer, _, syncRec := connectRacePeer(t, 17, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	first := chainhash.Hash{0xf1}
	second := chainhash.Hash{0xf2}

	// Seeded through the index, as every production push is: fetchHeaderBlocks
	// asks the index whether startHeader is still the live holder of its hash
	// before acting on lookups made with the header lock released.
	sm.headerMu.Lock()
	sm.startHeader = sm.headerList.PushBack(&headerNode{height: 10, hash: &first})
	sm.indexHeaderLocked(sm.startHeader, first)
	sm.indexHeaderLocked(sm.headerList.PushBack(&headerNode{height: 11, hash: &second}), second)
	sm.headerMu.Unlock()

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 2 }, 5*time.Second),
		"both blocks should have been requested from the sync peer")

	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()
	require.Equal(t, first, sm.frontierHash, "the oldest requested block is what everything else waits on")
	require.Equal(t, int32(10), sm.frontierHeight)
}

// TestHandleBlockMsg_AdvancesTheFrontier proves the other half of the wiring:
// when the block everything was queued behind arrives, the block handler moves
// the frontier on. If it did not, the race would keep firing at a block that has
// already been delivered.
func TestHandleBlockMsg_AdvancesTheFrontier(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	checkpointHash := chainhash.Hash{0xcf}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 999, Hash: &checkpointHash}

	syncPeer, _, _ := connectRacePeer(t, 18, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	arriving := chainhash.Hash{0xa1}
	next := chainhash.Hash{0xa2}
	sm.headerList.PushBack(&headerNode{height: 10, hash: &arriving})
	sm.headerList.PushBack(&headerNode{height: 11, hash: &next})
	sm.startHeader = nil // everything in the list has been requested

	sm.blockDownloads.Add(syncPeer, arriving)
	sm.setFrontier(arriving, 10, time.Now().Add(-30*time.Second))

	// Carrying no block makes handleBlockMsg bail immediately after the
	// header-list bookkeeping, which is the part under test.
	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: arriving, peer: syncPeer})
	require.Error(t, err)

	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()
	require.Equal(t, next, sm.frontierHash, "the next block in the list is now what everything waits on")
	require.Equal(t, int32(11), sm.frontierHeight)
}

// TestSetFrontier_AgeIsMeasuredFromTheBlockNotTheUpdate pins the behaviour the
// age gate depends on. fetchHeaderBlocks republishes the same frontier every
// time it tops up the request pipeline; if each republish reset the clock, the
// block would never look stuck and the race could never start.
func TestSetFrontier_AgeIsMeasuredFromTheBlockNotTheUpdate(t *testing.T) {
	sm := newRaceManager(t)

	first := time.Now().Add(-30 * time.Second)
	sm.setFrontier(chainhash.Hash{0x01}, 10, first)
	sm.setFrontier(chainhash.Hash{0x01}, 10, time.Now())

	require.WithinDuration(t, first, sm.frontierStartedAt(), time.Millisecond,
		"republishing the same frontier must not restart its clock")

	// A genuinely new frontier does restart it, and forgets the old racers.
	other, _, _ := connectRacePeer(t, 13, 1000)
	require.True(t, sm.registerFrontierRacer(chainhash.Hash{0x01}, other))

	moved := time.Now()
	sm.setFrontier(chainhash.Hash{0x02}, 11, moved)

	require.WithinDuration(t, moved, sm.frontierStartedAt(), time.Millisecond)

	sm.frontierMu.Lock()
	racers := len(sm.frontierRacers)
	sm.frontierMu.Unlock()
	require.Zero(t, racers, "peers racing the previous block are no longer racing anything")
}

// TestPublishFrontier covers what counts as a block worth racing.
func TestPublishFrontier(t *testing.T) {
	hashA := chainhash.Hash{0xa1}
	hashB := chainhash.Hash{0xb2}

	t.Run("publishes the oldest block already asked for", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		last := sm.headerList.PushBack(&headerNode{height: 11, hash: &hashB})
		sm.startHeader = last // everything before it has been requested

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, hashA, sm.frontierHash)
		require.Equal(t, int32(10), sm.frontierHeight)
	})

	t.Run("nothing is stuck when the front block has not been asked for", func(t *testing.T) {
		sm := newRaceManager(t)
		front := sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		sm.startHeader = front

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})

	t.Run("nothing is stuck outside headers-first mode", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		sm.headerList.PushBack(&headerNode{height: 11, hash: &hashB})
		sm.headersFirstMode.Store(false)

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})

	t.Run("resetting the header state clears the frontier", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.setFrontier(hashA, 10, time.Now())
		sm.resetHeaderState(&hashB, 11)

		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})
}
