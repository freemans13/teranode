package netsync

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getDataRecorder collects the block hashes a peer's remote end is asked for.
//
// Shared by most of this package's tests, not just the ones about any one
// mechanism, which is why it lives in this file rather than beside a
// particular feature's tests.
type getDataRecorder struct {
	mu     sync.Mutex
	hashes []chainhash.Hash
	msgs   int
}

func (r *getDataRecorder) record(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs++

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.hashes = append(r.hashes, iv.Hash)
		}
	}
}

// messages reports how many getdata messages arrived, as distinct from how many
// hashes they carried between them.
func (r *getDataRecorder) messages() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.msgs
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
//
// The name is inherited from the frontier-race tests this helper was first
// written for; it is now just this package's general-purpose connected-peer
// fixture.
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

// newRaceManager builds the smallest SyncManager most of this package's tests
// need. Settings come from the real loader, so the defaults under test are the
// ones an unconfigured node actually gets.
//
// The name predates the wanted-range pass: it was written for the frontier-race
// tests, which are gone, and it is kept rather than mechanically renamed across
// the two dozen files that now build a manager through it.
func newRaceManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:         ulogger.TestLogger{},
		settings:       test.CreateBaseTestSettings(t),
		chainParams:    &chaincfg.MainNetParams,
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		headerCache:    newHeaderCache(),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
	}
	sm.headersFirstMode.Store(true)

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

// linkedHeaders builds a headers message whose headers chain from prev, and
// returns the hashes in order. nonce is bumped per header so every hash is
// distinct even when two batches are built in the same second.
func linkedHeaders(prev chainhash.Hash, n int, nonce *uint32) (*wire.MsgHeaders, []chainhash.Hash) {
	msg := wire.NewMsgHeaders()
	hashes := make([]chainhash.Hash, 0, n)
	cur := prev

	for i := 0; i < n; i++ {
		*nonce++
		bh := wire.NewBlockHeader(1, &cur, &chainhash.Hash{}, 0x1d00ffff, *nonce)
		_ = msg.AddBlockHeader(bh)
		cur = bh.BlockHash()
		hashes = append(hashes, cur)
	}

	return msg, hashes
}

// newHeaderLockManager builds a SyncManager wired for the header-cache-driven
// download paths: headers-first mode on, and a blockchain mock that never
// claims to already have a block, so fetchHeaderBlocks always requests what
// the cache names.
//
// It used to also plant a stored checkpoint far above anything these tests
// generate, so the checkpoint branches never fired. That field is gone now:
// isCheckpointHash compares a block's hash against the real checkpoints in
// chainParams (MainNet, here), and a randomly nonced test header hash never
// coincides with one of those, so nothing needs faking.
func newHeaderLockManager(t *testing.T, gate chan struct{}, entered chan struct{}) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	best := blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	if gate != nil {
		var once sync.Once

		best.Run(func(mock.Arguments) {
			if entered != nil {
				once.Do(func() { close(entered) })
			}

			<-gate
		})
	}

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	return sm
}
