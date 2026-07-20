package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests exercise the delivered-block auth gate in handleBlockPreamble for a
// block that is neither in the per-peer ledger nor still-needed (the rotation-
// orphan case measured live on mainnet 2026-07-20). The branch TAKEN is identified
// by the returned error string, which is the authoritative signal:
//   - "dropped unrequested block ... during IBD"  -> the new drop-not-disconnect path
//   - "Got unrequested block ..."                 -> the disconnect path
// (Peer connection state is not asserted: a bare NewInboundPeer has no associated
// connection, so its Connected() flag is not a reliable observation point — the
// error string tells us unambiguously which branch fired.)

func buildUnrequestedBlockManager(t *testing.T, fsm blockchain_api.FSMStateType, tolerate bool) (*SyncManager, *peer.Peer, *wire.MsgBlock, chainhash.Hash) {
	t.Helper()
	initPrometheusMetrics()

	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TolerateUnrequestedBlocksInIBD = tolerate
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := fsm
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	// Peer state with EMPTY requestedBlocks — the block is unrequested.
	ps := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() { ps.requestedTxns.Stop(); ps.requestedBlocks.Stop() })

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      tSettings.ChainCfgParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		// Empty header index + nil carryover => blockStillNeeded is false, so the
		// block is genuinely unrequested-and-not-needed.
		headerHeightIndex: make(map[chainhash.Hash]int32),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })
	sm.peerStates.Set(p, ps)

	require.False(t, sm.blockStillNeeded(blockHash), "precondition: the block must not be still-needed")

	return sm, p, msgBlock, blockHash
}

func deliverUnrequested(t *testing.T, sm *SyncManager, p *peer.Peer, b *wire.MsgBlock, h chainhash.Hash) error {
	t.Helper()
	_, _, _, _, _, err := sm.handleBlockPreamble("test", &blockQueueMsg{
		block: b, blockHash: h, blockHeight: 2, peer: p,
	})
	return err
}

// TestUnrequestedBlock_DroppedDuringIBD: during CATCHINGBLOCKS with the default
// setting, an unrequested late-duplicate block takes the drop-not-disconnect path,
// which is what breaks the rotation-orphan cascade that collapses the header
// frontier and freezes the tip.
func TestUnrequestedBlock_DroppedDuringIBD(t *testing.T) {
	sm, p, b, h := buildUnrequestedBlockManager(t, blockchain2.FSMStateCATCHINGBLOCKS, true)
	err := deliverUnrequested(t, sm, p, b, h)
	require.Error(t, err, "the unrequested block must not be processed")
	require.Contains(t, err.Error(), "dropped unrequested block",
		"during IBD the block must be dropped, not routed to the disconnect path")
	require.NotContains(t, err.Error(), "Got unrequested block")
}

// TestUnrequestedBlock_DisconnectsAtTip: at the chain tip (RUNNING) an unrequested
// block is genuine misbehaviour and still takes the disconnect path — the spam
// guard is intact off the IBD path.
func TestUnrequestedBlock_DisconnectsAtTip(t *testing.T) {
	sm, p, b, h := buildUnrequestedBlockManager(t, blockchain2.FSMStateRUNNING, true)
	err := deliverUnrequested(t, sm, p, b, h)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Got unrequested block",
		"at the tip an unrequested block must take the disconnect path")
}

// TestUnrequestedBlock_RollbackDisconnectsInIBD: with the setting off, the pre-fix
// behaviour is restored — an unrequested block takes the disconnect path even
// during IBD (the rollback lever).
func TestUnrequestedBlock_RollbackDisconnectsInIBD(t *testing.T) {
	sm, p, b, h := buildUnrequestedBlockManager(t, blockchain2.FSMStateCATCHINGBLOCKS, false)
	err := deliverUnrequested(t, sm, p, b, h)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Got unrequested block",
		"rollback (setting off) must take the disconnect path even during IBD")
}
