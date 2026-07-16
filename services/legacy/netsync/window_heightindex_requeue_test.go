// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Regression test for the 8-peer park-ahead height-determination livelock.
//
// handleBlockPreamble CONSUMES a block's headerHeightIndex entry at early-ack
// ADMISSION (front-removal), before handleBlockMsgWithWindow decides the block's
// fate. If the block is then tolerated-error-rejected (e.g. the park is
// transiently full during the catch-up burst) the drain loop requeues it — but
// with its index entry already gone, the re-fetched OUT-OF-ORDER arrival resolves
// headerHeight=-1, falls into the default parent-lookup arm, fails BLOCK_NOT_FOUND
// for a not-yet-committed parent (itself tolerated), and requeues again: a tight
// "prev block header not found" spin with zero commits (observed on mainnet at
// parallelFetchPeers=8, 12GB of error log). The fix restores the PoW-verified
// height on any tolerated-error exit so the re-fetch stays height-resolvable.
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
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockMsgWithWindow_ToleratedRequeueRestoresHeightIndex(t *testing.T) {
	initPrometheusMetrics()

	const H = int32(500)                 // header-chain (PoW-verified) height of the block
	const checkpointHeight = int32(1000) // H is below it => window-eligible

	// A raw peer block (wrapper reports height -1) with the regtest easy target so
	// prepareBlockForWindow's PoW check would pass — though we never reach prepare
	// because the park is forced full first.
	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: [32]byte{0x01}, // a parent deliberately NOT in the store
			Timestamp: time.Unix(1231006505, 0),
			Bits:      0x207fffff,
			Nonce:     0,
		},
	}
	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: [32]byte{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)
	blockHash := msgBlock.BlockHash()

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1
	tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 20

	// GetBlockHeader ALWAYS fails (the parent is not committed): if height
	// determination ever consults the parent lookup, we see BLOCK_NOT_FOUND.
	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("parent not committed yet (streamed ahead of commit)"))
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute)}
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:               context.Background(),
		settings:          tSettings,
		chainParams:       params,
		logger:            ulogger.TestLogger{},
		blockchainClient:  blockchainClient,
		utxoStore:         &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		peerStates:        txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		blockSizeTracker:  newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(testPeer, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	// Block assembly is far behind (100), so a height-500 block is BEYOND the
	// maturity gate (100+20) and takes the park branch.
	sm.cachedBlockAssemblyHeight.Store(100)
	sm.baHeightPolled.Store(true)

	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: H, hash: &blockHash})
	sm.headerHeightIndex[blockHash] = H
	nonMatchingCheckpointHash := chainhash.Hash{0xaa}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpointHeight, Hash: &nonMatchingCheckpointHash}

	wa := newWindowAccumulator(100*1024*1024, 20)

	// A park that is already at its count cap, so any beyond-gate block is
	// tolerated-error rejected ("park buffer full (count)") before prepare.
	parkFull := &parkStore{entries: make([]windowEntry, 1), budget: 1 << 30, maxBlocks: 1}

	bmsg := &blockQueueMsg{block: msgBlock, blockHash: blockHash, blockHeight: -1, peer: testPeer}

	// PHASE A — first arrival at the header-list front. Front-match sources
	// headerHeight=H and DELETES the index entry (admission-time consumption); the
	// beyond-gate block then hits the full park and returns a tolerated error. The
	// fix's defer must RESTORE the index entry because the drain loop will requeue.
	outcome, err := sm.handleBlockMsgWithWindow(bmsg, wa, func() {}, func() {}, parkFull)

	require.Equal(t, blockAdmitDirect, outcome)
	require.Error(t, err)
	require.Contains(t, err.Error(), "park buffer full", "beyond-gate block into a full park is a tolerated park-full error")
	require.False(t, BlockProcessingErrorIsPeerFault(err), "park-full is a tolerated (non-peer-fault) error → the drain loop requeues")

	require.Equal(t, 0, sm.headerList.Len(), "front-match consumed the header-list node")

	sm.headerMu.Lock()
	got, ok := sm.headerHeightIndex[blockHash]
	sm.headerMu.Unlock()
	require.True(t, ok, "RED before fix: a tolerated-error requeue must RESTORE the header-height index entry consumed at admission")
	require.Equal(t, H, got, "restored entry must carry the authoritative PoW height")

	// PHASE B — the requeued block is re-fetched and re-arrives OUT OF ORDER (the
	// header-list front node is gone). With the entry restored, the index fallback
	// resolves headerHeight=H, so it reaches the park branch again — it must NEVER
	// fall into the parent-lookup arm and start the "prev block header" spin.
	state.requestedBlocks.Set(blockHash, struct{}{})
	sm.requestedBlocks.Set(blockHash, struct{}{})

	// A re-delivery carries the block again (the prior call consumed bmsg.block).
	bmsg2 := &blockQueueMsg{block: msgBlock, blockHash: blockHash, blockHeight: -1, peer: testPeer}
	outcome2, err2 := sm.handleBlockMsgWithWindow(bmsg2, wa, func() {}, func() {}, parkFull)

	require.Equal(t, blockAdmitDirect, outcome2)
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "park buffer full", "re-fetch resolves height via the restored index and reaches the park branch")
	require.NotContains(t, err2.Error(), "prev block header", "re-fetch must NOT hit the parent-lookup wedge")
	require.False(t, errors.Is(err2, errors.ErrBlockNotFound), "parent lookup must never be consulted for this block")
	blockchainClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}
