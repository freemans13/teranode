package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newFetchLockManager builds a manager wired for fetchHeaderBlocks. Any hash in
// alreadyHave is answered as a block we already hold, so the walk skips it;
// everything else comes back not-found and is therefore requested. When gate is
// non-nil the blockchain lookup parks on it, and entered is closed the first
// time the lookup is reached, which is how a test gets a competing goroutine to
// run at the exact moment the lookup is in progress.
func newFetchLockManager(t *testing.T, alreadyHave []chainhash.Hash, gate, entered chan struct{}) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	// Specific expectations must be registered before the catch-all, because
	// testify takes the first matching one.
	for i := range alreadyHave {
		hash := alreadyHave[i]
		blockchainClient.Mock.On("GetBlockHeader", mock.Anything, &hash).
			Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	}

	lookup := blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	if gate != nil {
		var once sync.Once

		lookup.Run(func(mock.Arguments) {
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

	// Far above anything these tests generate, so the checkpoint branches never
	// fire and the plain header-walk path is what runs.
	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	return sm
}

// seedFetchHeaders lands a prebuilt headers batch behind anchor, leaving
// startHeader on the first of them — exactly the state a real node is in when a
// headers batch has just arrived. The batch is passed in rather than built here
// because a caller may need the hashes before the manager exists.
func seedFetchHeaders(t *testing.T, sm *SyncManager, p *peerpkg.Peer, anchor chainhash.Hash, msg *wire.MsgHeaders) {
	t.Helper()

	sm.resetHeaderState(&anchor, 10)
	// resetHeaderState turns headers-first mode off; the walk under test only
	// runs in headers-first mode.
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: p})

	require.Equal(t, len(msg.Headers)+1, sm.headerListLen(), "the seeded headers should all have linked")
}

// TestFetchHeaderBlocks_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup is the
// stall this fix exists to remove. fetchHeaderBlocks asks the blockchain service
// whether we already have each candidate block, and that is a gRPC round-trip on
// a context with no deadline. Holding the header lock across it makes every
// other header-list user — including the block-queue consumer, the narrowest
// goroutine in the service — wait on a remote service.
//
// Bounding the number of round-trips is not the same as bounding the time they
// take, and time is the property that matters to a goroutine waiting on the
// lock.
func TestFetchHeaderBlocks_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)

	syncPeer, _, _ := connectRacePeer(t, 40, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa1}
	msg, _ := linkedHeaders(anchor, 25, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.fetchHeaderBlocks()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("fetchHeaderBlocks never reached the blockchain lookup")
	}

	// The block-queue consumer's first act in headers-first mode is to take this
	// same lock, so this stand-in for it must not be stuck behind a remote call.
	acquired := make(chan struct{})

	go func() {
		_ = sm.headerListLen()

		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		close(gate)
		<-done
		t.Fatal("reading the header list blocked while fetchHeaderBlocks waited on the blockchain lookup")
	}

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("fetchHeaderBlocks never returned")
	}
}

// TestFetchHeaderBlocks_RequestsExactlyTheBlocksItRequestedBefore pins the
// contents of the getdata, so the restructure cannot quietly change what is
// asked for. With nothing racing, one pass must ask for the first
// maxInFlightBlocks headers it does not already have, in list order, skipping
// the ones we hold without requesting them and without leaving them behind, and
// must leave startHeader on the first header it did not consider.
func TestFetchHeaderBlocks_RequestsExactlyTheBlocksItRequestedBefore(t *testing.T) {
	const seeded = 30

	// The batch is built before the manager so the mock can be told which of
	// these blocks we already hold.
	var nonce uint32

	anchor := chainhash.Hash{0xa2}
	msg, hashes := linkedHeaders(anchor, seeded, &nonce)

	// Two of the headers are blocks we already hold: one at the very front, so
	// the skip happens before anything is requested, and one in the middle.
	have := []chainhash.Hash{hashes[0], hashes[5]}

	sm := newFetchLockManager(t, have, nil, nil)

	syncPeer, _, rec := connectRacePeer(t, 42, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	maxBlocks := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	// Everything in list order except the two we already hold, capped at the
	// in-flight limit.
	want := make([]chainhash.Hash, 0, maxBlocks)
	lastConsidered := 0

	for i := 0; i < seeded && len(want) < maxBlocks; i++ {
		lastConsidered = i

		if hashes[i] == have[0] || hashes[i] == have[1] {
			continue
		}

		want = append(want, hashes[i])
	}

	require.Eventually(t, func() bool { return rec.count() >= len(want) }, 5*time.Second, 10*time.Millisecond,
		"the getdata never arrived at the peer")
	require.Equal(t, want, rec.all(), "the getdata must ask for the same blocks, in the same order, as before")

	for _, h := range want {
		require.True(t, sm.blockDownloads.HasOwner(syncPeer, h), "every requested block must be recorded against the peer we asked")
	}

	for _, h := range have {
		require.False(t, sm.blockDownloads.HasOwner(syncPeer, h), "a block we already hold must not be recorded as requested")
	}

	sm.headerMu.Lock()
	startHeader := sm.startHeader
	sm.headerMu.Unlock()

	require.NotNil(t, startHeader, "startHeader must still be anchored in the list")

	node, ok := startHeader.Value.(*headerNode)
	require.True(t, ok)
	require.Equal(t, hashes[lastConsidered+1], *node.hash,
		"startHeader must be left on the first header the walk did not consider")
}
