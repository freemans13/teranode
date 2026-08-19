package netsync

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// peerMsgRecorder collects what a peer's remote end is actually sent. The park
// paths are all defined by what reaches the peer — a getblocks that keeps it
// sending, a getdata that asks for a block again, a reject that tells it a block
// was bad — so every assertion here is made on the far side of the wire rather
// than on manager state.
type peerMsgRecorder struct {
	mu        sync.Mutex
	getData   []chainhash.Hash
	getBlocks int
	rejects   []chainhash.Hash
}

func (r *peerMsgRecorder) recordGetData(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.getData = append(r.getData, iv.Hash)
		}
	}
}

func (r *peerMsgRecorder) recordGetBlocks() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getBlocks++
}

func (r *peerMsgRecorder) recordReject(msg *wire.MsgReject) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rejects = append(r.rejects, msg.Hash)
}

func (r *peerMsgRecorder) getBlocksCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getBlocks
}

func (r *peerMsgRecorder) getDataCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.getData)
}

// askedForSince reports whether hash appears in a getdata recorded after the
// first from getdata block hashes already seen.
func (r *peerMsgRecorder) askedForSince(from int, hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if from > len(r.getData) {
		return false
	}

	for _, got := range r.getData[from:] {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

func (r *peerMsgRecorder) wasRejected(hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.rejects {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

// connectRecordingPeer returns a live peer whose remote end records the three
// message kinds the park paths are supposed to send.
func connectRecordingPeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *peerpkg.Peer, *peerMsgRecorder) {
	t.Helper()

	rec := &peerMsgRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData:   func(_ *peerpkg.Peer, msg *wire.MsgGetData) { rec.recordGetData(msg) },
			OnGetBlocks: func(_ *peerpkg.Peer, _ *wire.MsgGetBlocks) { rec.recordGetBlocks() },
			OnReject:    func(_ *peerpkg.Peer, msg *wire.MsgReject) { rec.recordReject(msg) },
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

// TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks is the one the
// park broke. In the legacy sync protocol an orphan is not only a block out of
// order: the peer pushes its tip after delivering a batch and then waits for the
// next getblocks before it sends anything else. Keeping the block instead of
// throwing it away must not swallow that answer — the park keeps the download,
// the getblocks fetches the gap, and they are not alternatives.
//
// Out of headers-first mode, which is every node past the final checkpoint,
// nothing else sends anything: fetchMoreHeaderBlocks returns immediately. The
// peer would sit silent until the stall detector rotated it.
func TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks(t *testing.T) {
	for _, tc := range []struct {
		name         string
		headersFirst bool
	}{
		{name: "past the final checkpoint", headersFirst: false},
		{name: "in headers-first mode", headersFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)
			h.sm.headersFirstMode.Store(tc.headersFirst)

			child := h.blocks[1].MsgBlock().BlockHash()

			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

			require.NoError(t, h.deliver(t, 1))

			require.Equal(t, 1, h.sm.blockPark.Len(), "the block must be kept, not thrown away")
			require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock")

			require.True(t, WaitUntil(func() bool { return h.rec.getBlocksCount() > 0 }, 5*time.Second),
				"an orphan must be answered with a getblocks or the peer sends nothing more")
		})
	}
}

// TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp is the case
// the rewind machinery exists for and nothing exercised. A block that arrives as
// the front of the header list has its header removed and unindexed before the
// park ever sees it, so by the time the park gives the block up there is nothing
// left to look the header up by. Unless the park carried that header node with
// the block, the rewind finds nothing, and the block is in neither the header
// list, nor the park, nor any download ledger — sync cannot pass it again
// without a peer rotation that rebuilds the whole walk.
func TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp(t *testing.T) {
	h := newParkWiringHarness(t, true)

	front := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 0))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the front block parks like any other orphan")

	// This is what makes the case different: its header left the list on
	// arrival, so an index lookup can no longer find it.
	h.sm.headerMu.Lock()
	_, stillIndexed := h.sm.headerIndex[front]
	h.sm.headerMu.Unlock()

	require.False(t, stillIndexed, "an arriving front block's header is removed before the park sees it")

	// The parent never arrives and the block's time runs out.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, front) }, 5*time.Second),
		"a block given up on must go back into the download walk, or nothing ever asks for it again")
}
