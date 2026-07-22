package netsync

// Tests for the checkpoint-block deferral (the checkpoint-boundary stall
// elimination). Root cause proven live across four boundaries (11111, 33333,
// 74000, 105000): the checkpoint block's only delivery arrived ~11-31s before
// its parent committed, the direct path's ErrBlockNotFound arm returned nil
// (never requeued), every fetch ledger had been wiped on arrival, and nothing
// could re-request the block until the 3-minute sync-peer rotation re-walked
// the interval. The fix keeps that delivery in a one-slot deferral and commits
// it from the refill tick within ~one tick of the parent landing.

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newCheckpointDeferralManager wires a SyncManager whose next delivery is THE
// checkpoint block (hash == nextCheckpoint.Hash) with its height served by the
// headers-first index, so the only GetBlockHeader call is HandleBlockDirect's
// pre-flight parent lookup.
func newCheckpointDeferralManager(t *testing.T, blockchainClient *blockchain2.Mock, blockHash chainhash.Hash, height int32) (*SyncManager, *peer.Peer, *peerSyncState) {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, height)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() {
		state.requestedTxns.Stop()
		state.requestedBlocks.Stop()
	})

	sm := &SyncManager{
		ctx:               context.Background(),
		settings:          tSettings,
		chainParams:       params,
		logger:            ulogger.TestLogger{},
		blockchainClient:  blockchainClient,
		utxoStore:         &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		subtreeStore:      memory.New(),
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
		headerList:        list.New(),
		headerHeightIndex: map[chainhash.Hash]int32{blockHash: height},
		blockSizeTracker:  newBlockSizeTracker(10),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })

	sm.peerStates.Set(p, state)
	sm.headersFirstMode.Store(true)

	hash := blockHash
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: height, Hash: &hash}

	markRequested(sm, state, blockHash)

	return sm, p, state
}

// TestCheckpointDeferral_MissingParentDefersInsteadOfDrops: the checkpoint
// block whose parent is uncommitted must be DEFERRED — retained in the slot,
// acked cleanly, and the dead getblocks fallback skipped (GetBestBlockHeader
// deliberately unmocked: a call would panic, proving the old arm ran).
func TestCheckpointDeferral_MissingParentDefersInsteadOfDrops(t *testing.T) {
	initPrometheusMetrics()

	msgBlock, blockHash := newParkedTwinBlockMsg(t)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// The ONLY GetBlockHeader call is HandleBlockDirect's parent pre-flight:
	// parent uncommitted.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet"))

	sm, p, _ := newCheckpointDeferralManager(t, blockchainClient, blockHash, 500)

	wa := newWindowAccumulator(1<<40, 20)
	noop := func() {}

	outcome, err := sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:     msgBlock,
		blockHash: blockHash,
		peer:      p,
	}, wa, noop, noop, nil)

	require.NoError(t, err, "a deferred checkpoint block must ack cleanly (nil, as the old arm did)")
	require.Equal(t, blockAdmitDirect, outcome)
	require.NotNil(t, sm.deferredCheckpoint, "the delivery must be RETAINED, not dropped")
	require.Equal(t, blockHash, sm.deferredCheckpoint.bmsg.blockHash)
	blockchainClient.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestCheckpointDeferral_RetryNoopWhileParentMissing: the refill-tick retry
// must keep the slot (and touch nothing else) while the parent is still
// uncommitted.
func TestCheckpointDeferral_RetryNoopWhileParentMissing(t *testing.T) {
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent still committing"))

	msgBlock, blockHash := newParkedTwinBlockMsg(t)
	sm, p, state := newCheckpointDeferralManager(t, blockchainClient, blockHash, 500)

	sm.deferredCheckpoint = &deferredCheckpointBlock{
		msgBlock:   msgBlock,
		bmsg:       &blockQueueMsg{blockHash: blockHash, peer: p},
		peer:       p,
		state:      state,
		prevHash:   msgBlock.Header.PrevBlock,
		deferredAt: time.Now(),
	}

	sm.retryDeferredCheckpoint()

	require.NotNil(t, sm.deferredCheckpoint, "slot must be retained while the parent is missing")
}

// TestCheckpointDeferral_DeadlineRequeuesAndBars: past the deadline the slot
// clears, the hash is requeued for re-fetch, and re-deferral is barred so the
// rotation backstop cannot be indefinitely re-armed away.
func TestCheckpointDeferral_DeadlineRequeuesAndBars(t *testing.T) {
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	msgBlock, blockHash := newParkedTwinBlockMsg(t)
	sm, p, state := newCheckpointDeferralManager(t, blockchainClient, blockHash, 500)
	sm.refetchBlocks = make(map[chainhash.Hash]struct{})

	sm.deferredCheckpoint = &deferredCheckpointBlock{
		msgBlock:   msgBlock,
		bmsg:       &blockQueueMsg{blockHash: blockHash, peer: p},
		peer:       p,
		state:      state,
		prevHash:   msgBlock.Header.PrevBlock,
		deferredAt: time.Now().Add(-deferredCheckpointMaxWait - time.Second),
	}

	sm.retryDeferredCheckpoint()

	require.Nil(t, sm.deferredCheckpoint, "slot must clear at the deadline")
	require.Equal(t, blockHash, sm.deferBarredCheckpoint, "the hash must be barred from re-deferral")
	_, requeued := sm.refetchBlocks[blockHash]
	require.True(t, requeued, "the hash must be requeued for a normal re-fetch")
}

// TestCheckpointDeferral_ClearsWhenCommittedElsewhere: if a rotation's
// re-delivery already committed the block, the retry just drops the slot.
func TestCheckpointDeferral_ClearsWhenCommittedElsewhere(t *testing.T) {
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	msgBlock, blockHash := newParkedTwinBlockMsg(t)
	sm, p, state := newCheckpointDeferralManager(t, blockchainClient, blockHash, 500)

	sm.deferredCheckpoint = &deferredCheckpointBlock{
		msgBlock:   msgBlock,
		bmsg:       &blockQueueMsg{blockHash: blockHash, peer: p},
		peer:       p,
		state:      state,
		prevHash:   msgBlock.Header.PrevBlock,
		deferredAt: time.Now(),
	}

	sm.retryDeferredCheckpoint()

	require.Nil(t, sm.deferredCheckpoint, "slot must clear once the block exists on chain")
}

// TestCheckpointDeferral_BarredHashTakesOldArm: after a deadline bar, the next
// delivery of the same hash must take the ORIGINAL arm (getblocks fallback and
// nil return) — observable by GetBestBlockHeader being called this time.
func TestCheckpointDeferral_BarredHashTakesOldArm(t *testing.T) {
	initPrometheusMetrics()

	msgBlock, blockHash := newParkedTwinBlockMsg(t)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet"))
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 400}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{&blockHash}, nil)

	sm, p, _ := newCheckpointDeferralManager(t, blockchainClient, blockHash, 500)
	sm.deferBarredCheckpoint = blockHash // deadline already fired once

	wa := newWindowAccumulator(1<<40, 20)
	noop := func() {}

	outcome, err := sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:     msgBlock,
		blockHash: blockHash,
		peer:      p,
	}, wa, noop, noop, nil)

	require.NoError(t, err)
	require.Equal(t, blockAdmitDirect, outcome)
	require.Nil(t, sm.deferredCheckpoint, "a barred hash must NOT re-defer")
	blockchainClient.AssertCalled(t, "GetBestBlockHeader", mock.Anything)
}
