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
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Tests for F4: unrequested / non-connecting header batches are counted as
// CONSECUTIVE offences against a per-peer tolerance instead of costing the peer
// its connection on the first one.
//
// What each test is really defending:
//   - the tolerance actually tolerates, and re-anchors the peer rather than
//     leaving it stuck on a frontier we never corrected;
//   - the detector is NOT disabled — a peer that keeps sending garbage still
//     dies on the Kth offence, which is the whole justification for softening
//     the first K-1;
//   - "consecutive" means consecutive: one good batch wipes the slate, so a
//     long-lived healthy peer can never accumulate its way to a disconnect;
//   - the setting dialled down to <= 1 is the rollback lever and reproduces the
//     pre-F4 instant kill exactly;
//   - the counter lives on peerSyncState, so one noisy peer cannot spend
//     another peer's budget.
//
// These tests use blockchain2.Mock rather than the sqlitememory store because
// that is what every other handleHeadersMsg test in this package does, and the
// only blockchain calls on this path are the two reads the re-anchor makes
// (GetBestBlockHeader / GetBlockLocator). Reusing the package's existing
// SyncManager scaffolding is worth more here than store fidelity.

// unconnectingHarness is the minimal SyncManager + connected peer pair needed to
// drive handleHeadersMsg's offence paths.
type unconnectingHarness struct {
	sm            *SyncManager
	peer          *peer.Peer
	state         *peerSyncState
	getHeaders    chan *wire.MsgGetHeaders
	chainParams   *chaincfg.Params
	headerListTip *chainhash.Hash
}

// newUnconnectingHarness builds a SyncManager with the given tolerance and one
// registered peer whose outbound getheaders messages are observable on the
// returned channel. index must be unique per peer within a test so the pipe
// addresses do not collide.
func newUnconnectingHarness(t *testing.T, tolerance int, index uint8) *unconnectingHarness {
	t.Helper()

	chainParams := chaincfg.MainNetParams
	// A checkpoint far above anything we deliver, so headerCheckpoint stays
	// non-nil and handleHeadersMsg does not take the past-final-checkpoint exit
	// before it reaches the linkage check we are testing.
	cpHash := chainhash.Hash{0x9c}
	chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}}

	getHeaders := make(chan *wire.MsgGetHeaders, 32)
	counterpartCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, msg *wire.MsgGetHeaders) {
				select {
				case getHeaders <- msg:
				default:
				}
			},
			OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	_, syncPeer, err := MakeConnectedPeers(t, counterpartCfg, syncPeerCfg, index)
	require.NoError(t, err)

	t.Cleanup(func() { syncPeer.DisconnectWithInfo("test done") })

	// The re-anchor asks the blockchain where we actually are; a stable answer
	// keeps the locator stable, which is what lets PushGetHeadersMsg's duplicate
	// filter bound repeated re-anchors to a single wire message.
	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{bestHeader.Hash()}, nil).Maybe()
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.UnconnectingHeadersTolerance = tolerance

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(state.requestedTxns.Stop)
	t.Cleanup(state.requestedBlocks.Stop)

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
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	t.Cleanup(sm.requestedBlocks.Stop)

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headerCheckpoint = &chainParams.Checkpoints[0]

	// Seed the header list so the linkage check has a previous element to
	// compare against. lastHeaderResetAt is deliberately left zero: the
	// post-reset grace window would swallow non-connecting batches silently and
	// mask the counter entirely.
	seed := chainhash.Hash{0x01, index}
	sm.headerList.PushBack(&headerNode{height: 0, hash: &seed})

	return &unconnectingHarness{
		sm:            sm,
		peer:          syncPeer,
		state:         state,
		getHeaders:    getHeaders,
		chainParams:   &chainParams,
		headerListTip: &seed,
	}
}

// strayHeaders returns a headers message that cannot possibly link to our list:
// its parent is a hash we have never seen. This is the "does not properly
// connect to the chain" path.
func strayHeaders(nonce uint32) *wire.MsgHeaders {
	orphanParent := chainhash.Hash{0xde, 0xad, byte(nonce), byte(nonce >> 8)}
	headers, _ := buildHeaderChain(orphanParent, 1, nonce)

	msg := wire.NewMsgHeaders()
	_ = msg.AddBlockHeader(headers[0])

	return msg
}

// deliverStray drives one non-connecting batch through the real entry point in
// headers-first mode.
func (h *unconnectingHarness) deliverStray(nonce uint32) {
	h.sm.headersFirstMode.Store(true)
	h.sm.handleHeadersMsg(&headersMsg{headers: strayHeaders(nonce), peer: h.peer})
}

// deliverConnecting extends the header list by one properly linking header,
// which is the event that clears the offence counter.
func (h *unconnectingHarness) deliverConnecting(t *testing.T, nonce uint32) {
	t.Helper()

	h.sm.headersFirstMode.Store(true)

	headers, hashes := buildHeaderChain(*h.headerListTip, 1, nonce)

	msg := wire.NewMsgHeaders()
	require.NoError(t, msg.AddBlockHeader(headers[0]))

	h.sm.handleHeadersMsg(&headersMsg{headers: msg, peer: h.peer})

	h.headerListTip = &hashes[0]
}

func (h *unconnectingHarness) drainGetHeaders() int {
	n := 0

	for {
		select {
		case <-h.getHeaders:
			n++
		case <-time.After(150 * time.Millisecond):
			return n
		}
	}
}

// TestUnconnectingHeaders_ToleratesBelowLimit proves the core F4 claim: K-1
// consecutive non-connecting batches cost the peer nothing but a counter
// increment, and the peer is told where we actually are (a re-anchoring
// getheaders) rather than being left on the wrong frontier.
//
// It also pins how the re-anchor interacts with the peer's duplicate-getheaders
// filter. The re-anchor clears that filter after sending, because leaving its
// (DB-best tip, checkpoint) pair cached would make netsync's OWN next getheaders
// to this peer look like a duplicate and be dropped silently — wedging the
// frontier. The visible consequence is that every tolerated offence puts a real
// getheaders on the wire. That is bounded by the offence counter, not by the
// filter: at most K-1 of them, and any connecting batch resets the run to zero.
func TestUnconnectingHeaders_ToleratesBelowLimit(t *testing.T) {
	const tolerance = 10

	h := newUnconnectingHarness(t, tolerance, 21)

	for i := 0; i < tolerance-1; i++ {
		h.deliverStray(uint32(1000 + i)) //nolint:gosec
		require.True(t, h.peer.Connected(),
			"peer must survive offence %d of tolerance %d", i+1, tolerance)
	}

	require.EqualValues(t, tolerance-1, h.state.unconnectingHeaders.Load(),
		"every tolerated offence must be counted")

	sent := h.drainGetHeaders()
	require.Equal(t, tolerance-1, sent,
		"every tolerated offence must re-anchor the peer on the wire; the dedup cache must not be left holding the pair startSync will need")
}

// TestUnconnectingHeaders_DisconnectsOnKthOffence is the test that keeps F4
// honest. Softening the first K-1 offences is only defensible if the Kth still
// kills: a peer that sends an unbroken run of garbage is misbehaving, and it
// must still lose its connection.
func TestUnconnectingHeaders_DisconnectsOnKthOffence(t *testing.T) {
	const tolerance = 4

	h := newUnconnectingHarness(t, tolerance, 22)

	for i := 0; i < tolerance-1; i++ {
		h.deliverStray(uint32(2000 + i)) //nolint:gosec
		require.True(t, h.peer.Connected(), "peer must survive offence %d", i+1)
	}

	h.deliverStray(2999)

	require.True(t, WaitUntil(func() bool { return !h.peer.Connected() }, 2*time.Second),
		"the Kth consecutive offence must still disconnect a genuinely spamming peer")
}

// TestUnconnectingHeaders_ConnectingBatchResetsCounter proves the offences must
// be CONSECUTIVE. Without the reset the counter would be a lifetime total, and
// any long-lived peer on a busy network would eventually accumulate K benign
// races and be disconnected for nothing — which is the exact failure F4 exists
// to remove.
//
// The run here is (K-1) offences, one good batch, then (K-1) offences again.
// That is 2(K-1) = 6 offences in total against a tolerance of 4, so a
// non-resetting counter would have disconnected the peer partway through.
func TestUnconnectingHeaders_ConnectingBatchResetsCounter(t *testing.T) {
	const tolerance = 4

	h := newUnconnectingHarness(t, tolerance, 23)

	for i := 0; i < tolerance-1; i++ {
		h.deliverStray(uint32(3000 + i)) //nolint:gosec
	}

	require.EqualValues(t, tolerance-1, h.state.unconnectingHeaders.Load(),
		"precondition: peer is one offence away from disconnection")
	require.True(t, h.peer.Connected())

	h.deliverConnecting(t, 3100)

	require.EqualValues(t, 0, h.state.unconnectingHeaders.Load(),
		"a batch that connects must clear the run")
	require.True(t, h.peer.Connected(), "a connecting batch must never disconnect")

	for i := 0; i < tolerance-1; i++ {
		h.deliverStray(uint32(3200 + i)) //nolint:gosec
		require.True(t, h.peer.Connected(),
			"peer must survive offence %d of a fresh run after the reset", i+1)
	}

	require.EqualValues(t, tolerance-1, h.state.unconnectingHeaders.Load(),
		"the second run must be counted from zero, not from where the first left off")
}

// TestUnconnectingHeaders_ToleranceOneIsPreF4Behaviour exercises the rollback
// lever. With legacy_unconnectingHeadersTolerance dialled to 1 (or 0) the very
// first offence disconnects, no counter is touched and no re-anchor is sent —
// byte-identical to the behaviour we are replacing. This is what makes the
// change safe to back out in production without a code change.
func TestUnconnectingHeaders_ToleranceOneIsPreF4Behaviour(t *testing.T) {
	for _, tolerance := range []int{0, 1} {
		t.Run(map[int]string{0: "zero", 1: "one"}[tolerance], func(t *testing.T) {
			h := newUnconnectingHarness(t, tolerance, uint8(24+tolerance)) //nolint:gosec

			h.deliverStray(4000)

			require.True(t, WaitUntil(func() bool { return !h.peer.Connected() }, 2*time.Second),
				"tolerance %d must disconnect on the first offence", tolerance)
			require.EqualValues(t, 0, h.state.unconnectingHeaders.Load(),
				"the pre-F4 path must not touch the counter at all")
			require.Equal(t, 0, h.drainGetHeaders(),
				"the pre-F4 path must not re-anchor — it only disconnects")
		})
	}
}

// TestUnconnectingHeaders_CounterIsPerPeer proves the counter lives on
// peerSyncState and not on the manager. A global counter would let a single
// misbehaving peer spend the whole node's budget and take down every innocent
// peer with it — during IBD, with dozens of peers, that would turn one bad
// actor into exactly the mass-disconnect storm F4 is meant to stop.
func TestUnconnectingHeaders_CounterIsPerPeer(t *testing.T) {
	const tolerance = 4

	peerA := newUnconnectingHarness(t, tolerance, 26)
	peerB := newUnconnectingHarness(t, tolerance, 27)

	// Interleave so that a shared counter would reach the tolerance well before
	// either peer individually does.
	for i := 0; i < tolerance-1; i++ {
		peerA.deliverStray(uint32(5000 + i)) //nolint:gosec
		peerB.deliverStray(uint32(6000 + i)) //nolint:gosec
	}

	require.True(t, peerA.peer.Connected(), "peer A must survive its own K-1 offences")
	require.True(t, peerB.peer.Connected(), "peer B must survive its own K-1 offences")
	require.EqualValues(t, tolerance-1, peerA.state.unconnectingHeaders.Load())
	require.EqualValues(t, tolerance-1, peerB.state.unconnectingHeaders.Load())

	// Tipping A over the limit must not cost B its connection.
	peerA.deliverStray(5999)

	require.True(t, WaitUntil(func() bool { return !peerA.peer.Connected() }, 2*time.Second),
		"peer A reached the tolerance and must be disconnected")
	require.True(t, peerB.peer.Connected(),
		"peer B must be untouched by peer A's disconnection")
	require.EqualValues(t, tolerance-1, peerB.state.unconnectingHeaders.Load(),
		"peer B's counter must be unaffected by peer A")
}
