package netsync

// Tests for the checkpoint-certified block handoff (PR #1178 follow-up fix).
//
// Background: the legacy netsync headers-first path proves, in-process, that a
// delivered block is an ancestor of a hardcoded checkpoint — handleHeadersMsg only
// calls fetchHeaderBlocks() after link-verifying a header segment AND matching its
// terminal header against sm.nextCheckpoint.Hash (any mismatch disconnects the
// peer instead). checkpointCertifiedForBlock is the seam that turns "this hash was
// requested by fetchHeaderBlocks, and headers-first mode is still active" into the
// checkpointCertified bool threaded down to blockvalidation and to the create-side
// legacyOutpointOnly gate. These tests exercise that seam directly (handleBlockMsg
// itself needs a full block-processing pipeline and is exercised elsewhere).

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_checkpointCertifiedForBlock is the truth table for the
// certification predicate: certified only when the hash was requested by
// fetchHeaderBlocks (the headers-first getdata path) AND headers-first mode is
// still active at delivery time. Conservative by construction: any other
// combination — including a fallback to normal/getblocks mode after headers-first
// requested the hash, and a hash that was never requested via fetchHeaderBlocks —
// yields false, matching pre-fix (no fast path) behaviour.
func TestSyncManager_checkpointCertifiedForBlock(t *testing.T) {
	newSM := func(headersFirst bool) *SyncManager {
		sm := &SyncManager{
			logger:                     ulogger.TestLogger{},
			headerFirstRequestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		}
		sm.headersFirstMode.Store(headersFirst)

		return sm
	}

	t.Run("headers-first delivery of a requested hash -> certified", func(t *testing.T) {
		sm := newSM(true)
		defer sm.headerFirstRequestedBlocks.Stop()

		h := chainhash.Hash{0x01}
		sm.headerFirstRequestedBlocks.Set(h, struct{}{})

		require.True(t, sm.checkpointCertifiedForBlock(h))
	})

	t.Run("certification consumes the tracking entry (single use)", func(t *testing.T) {
		sm := newSM(true)
		defer sm.headerFirstRequestedBlocks.Stop()

		h := chainhash.Hash{0x02}
		sm.headerFirstRequestedBlocks.Set(h, struct{}{})

		require.True(t, sm.checkpointCertifiedForBlock(h))
		// A second delivery claiming the same hash must not be certified again —
		// the proof is single-use, tied to the one fetchHeaderBlocks request.
		require.False(t, sm.checkpointCertifiedForBlock(h))
	})

	t.Run("unrequested hash in headers-first mode -> not certified", func(t *testing.T) {
		sm := newSM(true)
		defer sm.headerFirstRequestedBlocks.Stop()

		h := chainhash.Hash{0x03}
		require.False(t, sm.checkpointCertifiedForBlock(h))
	})

	t.Run("normal/getblocks mode (headers-first ended or never entered) -> never certified, even if the hash lingers from a prior segment", func(t *testing.T) {
		sm := newSM(false)
		defer sm.headerFirstRequestedBlocks.Stop()

		h := chainhash.Hash{0x04}
		sm.headerFirstRequestedBlocks.Set(h, struct{}{}) // stale entry, e.g. from before falling back to getblocks

		require.False(t, sm.checkpointCertifiedForBlock(h))
	})

	t.Run("nil tracking map (SyncManager not fully wired, e.g. minimal test harness) -> conservative false", func(t *testing.T) {
		sm := &SyncManager{logger: ulogger.TestLogger{}}
		sm.headersFirstMode.Store(true)

		require.False(t, sm.checkpointCertifiedForBlock(chainhash.Hash{0x05}))
	})
}

// TestSyncManager_fetchHeaderBlocks_TracksCheckpointCertification verifies the
// write side of the wiring: fetchHeaderBlocks (called only after handleHeadersMsg
// has link-verified a header segment and matched its terminal header against a
// hardcoded checkpoint) marks every requested hash in headerFirstRequestedBlocks,
// which is what makes checkpointCertifiedForBlock able to certify it later.
func TestSyncManager_fetchHeaderBlocks_TracksCheckpointCertification(t *testing.T) {
	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	blockchainClient := &teranodeblockchain.Mock{}
	// haveInventory treats a lookup failure as "we don't have it", so every
	// header in the segment gets added to the getdata request.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found"))

	hash1 := chainhash.Hash{0x11}
	hash2 := chainhash.Hash{0x12}

	headerList := list.New()
	headerList.PushBack(&headerNode{height: 100, hash: &hash1})
	headerList.PushBack(&headerNode{height: 101, hash: &hash2})

	sm := &SyncManager{
		ctx:                        context.Background(),
		logger:                     ulogger.TestLogger{},
		blockchainClient:           blockchainClient,
		peerStates:                 txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:            expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerFirstRequestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:                 headerList,
		startHeader:                headerList.Front(),
		blockSizeTracker:           newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	defer sm.headerFirstRequestedBlocks.Stop()

	sm.storeSyncPeer(p, &syncPeerState{})
	sm.peerStates.Set(p, &peerSyncState{requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute)})

	sm.fetchHeaderBlocks()

	_, ok1 := sm.headerFirstRequestedBlocks.Get(hash1)
	_, ok2 := sm.headerFirstRequestedBlocks.Get(hash2)
	require.True(t, ok1, "hash1 must be tracked as headers-first requested")
	require.True(t, ok2, "hash2 must be tracked as headers-first requested")

	// And, downstream, both are certified for as long as headers-first mode holds.
	sm.headersFirstMode.Store(true)
	require.True(t, sm.checkpointCertifiedForBlock(hash1))
	require.True(t, sm.checkpointCertifiedForBlock(hash2))
}
