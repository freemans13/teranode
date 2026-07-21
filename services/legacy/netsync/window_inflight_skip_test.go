// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Source-fix regression: the multi-peer forward walk (assignBlocksAcrossPeers) must
// NOT re-request a block that is already in flight. resetHeaderState rewinds the
// cursor on every sync-peer rotation without clearing the in-flight ledger, so
// before this guard the re-walk re-requested still-outstanding blocks to a second
// peer — both copies arrived and one was admitted to the window twice, colliding on
// the txs unique index (40P01). The guard mirrors the check the refetch drain
// already uses (sm.requestedBlocks); haveInventory only covers committed blocks.
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
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAssignBlocks_SkipsInFlightBlock(t *testing.T) {
	chainParams := chaincfg.MainNetParams

	base := chainhash.Hash{0xc1}
	const runway = 5
	_, hashes := buildHeaderChain(base, runway, 9800)

	var (
		mu1, mu2, mu3    sync.Mutex
		got1, got2, got3 []*wire.MsgGetData
	)
	syncPeer := captureGetDataPeer(t, &chainParams, 70, &mu1, &got1)
	peerB := captureGetDataPeer(t, &chainParams, 71, &mu2, &got2)
	peerC := captureGetDataPeer(t, &chainParams, 72, &mu3, &got3)

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 3
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 8
	tSettings.Legacy.BlockDownloadWindow = 18

	syncState := newEligiblePeerState()
	stateB := newEligiblePeerState()
	stateC := newEligiblePeerState()
	defer syncState.requestedBlocks.Stop()
	defer syncState.requestedTxns.Stop()
	defer stateB.requestedBlocks.Stop()
	defer stateB.requestedTxns.Stop()
	defer stateC.requestedBlocks.Stop()
	defer stateC.requestedTxns.Stop()

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
		assignedTo:        make(map[chainhash.Hash]map[*peer.Peer]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker:  newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, syncState)
	sm.peerStates.Set(peerB, stateB)
	sm.peerStates.Set(peerC, stateC)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})
	for i := range hashes {
		sm.headerList.PushBack(&headerNode{height: int32(i + 1), hash: &hashes[i]})
	}
	sm.startHeader = sm.headerList.Front().Next()

	// hashes[2] is ALREADY in flight (simulating the original request outstanding
	// when a resetHeaderState re-walk reaches it). It must NOT be re-requested.
	inFlight := hashes[2]
	sm.requestedBlocks.Set(inFlight, struct{}{})

	sm.assignBlocksAcrossPeers()

	require.Eventually(t, func() bool {
		mu1.Lock()
		mu2.Lock()
		mu3.Lock()
		defer mu1.Unlock()
		defer mu2.Unlock()
		defer mu3.Unlock()
		// The other four runway blocks should all be requested.
		return len(collectInvHashes(got1))+len(collectInvHashes(got2))+len(collectInvHashes(got3)) >= runway-1
	}, 2*time.Second, 10*time.Millisecond, "the non-in-flight runway blocks must be requested")

	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	all := append(append(collectInvHashes(got1), collectInvHashes(got2)...), collectInvHashes(got3)...)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	for _, h := range all {
		require.NotEqual(t, inFlight, h, "an already-in-flight block must NOT be re-requested by the multi-peer walk")
	}

	// The other runway blocks WERE requested (walk still makes progress).
	seen := make(map[chainhash.Hash]bool)
	for _, h := range all {
		seen[h] = true
	}
	for i := 0; i < runway; i++ {
		if hashes[i] == inFlight {
			continue
		}
		require.True(t, seen[hashes[i]], "runway block %d must be requested", i)
	}
}
