package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBlocksReceivedCounter_AcceptedRequestedBlock verifies that
// prometheusLegacyNetsyncBlocksReceived increments by 1 for the delivering
// peer's label when handleBlockPreamble processes a requested block that passes
// the auth gate. RED before the increment is wired, GREEN after.
func TestBlocksReceivedCounter_AcceptedRequestedBlock(t *testing.T) {
	initPrometheusMetrics()

	// Build a minimal valid block so blockHash is deterministic.
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	// Create an inbound peer; its Addr() is the label value we will assert.
	tSettings := test.CreateBaseTestSettings(t)
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})
	peerAddr := p.Addr()

	// Stub the blockchain client: FSMState = RUNNING (not CATCHINGBLOCKS).
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	// Register the block as requested so it passes the auth gate.
	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(p, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	// Capture the counter value before the call.
	counter := prometheusLegacyNetsyncBlocksReceived.WithLabelValues(peerAddr)
	before := testutil.ToFloat64(counter)

	// Drive handleBlockPreamble with a matching requested block.
	_, _, _, _, _, err := sm.handleBlockPreamble("test", &blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 2,
		peer:        p,
	})
	require.NoError(t, err)

	// The counter must have incremented by exactly 1.
	after := testutil.ToFloat64(counter)
	require.Equal(t, before+1, after, "blocks_received_total must increment by 1 for peer %q", peerAddr)
}
