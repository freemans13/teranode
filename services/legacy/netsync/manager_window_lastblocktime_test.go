package netsync

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnAccept proves the
// stall-detector parity fix: when handleBlockMsgWithWindow accepts a block into
// the window (addedToWindow == true), the sync peer's lastBlockTime is refreshed
// exactly as the non-window path (handleBlockMsg -> HandleBlockDirect) does.
//
// Without the refresh, a sustained window run lets lastBlockTime go stale even
// though windowed blocks are being accepted and committed. When blockBacklog
// later drains to 0, handleCheckSyncPeer sees a large "time since last block"
// and can falsely rotate a healthy sync peer.
//
// RED (before fix): lastBlockTime is never touched on the accept path, so the
// stale timestamp seeded below is unchanged after the call.
// GREEN (after fix): the accept-path refresh advances lastBlockTime to ~now.
func TestHandleBlockMsgWithWindow_RefreshesLastBlockTimeOnAccept(t *testing.T) {
	initPrometheusMetrics()

	// Below-checkpoint (checkpoint at 1000) so the block is window-eligible.
	const checkpointHeight = int32(1000)
	const blockHeight = uint32(500)

	tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	// A single coinbase-only block: prepareSubtrees takes the txCount<=1 early
	// exit (returns no subtree slices), so prepareBlockForWindow skips the
	// merkle-root / dedup checks. Only the PoW precondition
	// (HasMetTargetDifficulty) remains, so the header must hash below the regtest
	// easy target (0x207fffff -> 0x7fffff00...). We grind the nonce against the
	// real check so the block is deterministic and not flaky on header contents.
	prevHash := chainhash.Hash{0x01}
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	header := wire.BlockHeader{
		Version:   1,
		PrevBlock: prevHash,
		Timestamp: time.Unix(1700000000, 0), // fixed for determinism
		Bits:      0x207fffff,               // regtest easy target
		Nonce:     0,
	}
	require.NoError(t, grindPoW(&header), "must find a nonce meeting the regtest target")

	msgBlock := &wire.MsgBlock{
		Header:       header,
		Transactions: []*wire.MsgTx{coinbase},
	}
	blockHash := msgBlock.Header.BlockHash()

	// Prev-block header lookup drives the default height branch (headers-first is
	// off), yielding blockHeight = prevMeta.Height + 1 = 500.
	prevMeta := &model.BlockHeaderMeta{Height: blockHeight - 1}
	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	catchingBlocks := blockchain.FSMStateCATCHINGBLOCKS

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The block is being freshly accepted, so it does not yet exist — the
	// already-committed guard in handleBlockMsgWithWindow must see false and let it
	// proceed to the accept path.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, prevMeta, nil)
	// After wa.add the post-accept pump asks for more blocks (startHeader is nil,
	// not current) and needs the best header.
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 499}, nil)

	// Real (disconnected) peer: PushGetBlocksMsg is a no-op on it but needs a logger.
	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()
	state.requestedBlocks.Set(blockHash, struct{}{})

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		subtreeStore:     memory.New(),
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		blockSizeTracker: newBlockSizeTracker(10),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(p, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})
	sm.headersFirstMode.Store(false)

	// Register p as the current sync peer with a deliberately stale last-block
	// time — the value the stall detector would rotate on if never refreshed.
	staleTime := time.Now().Add(-10 * time.Minute)
	sps := &syncPeerState{lastBlockTime: staleTime}
	sm.storeSyncPeer(p, sps)

	// A huge window budget so the accepted block does not immediately fill and
	// flush — we only care about the accept path here.
	wa := newWindowAccumulator(1<<40, 20)
	flushWindow := func() { t.Fatal("flushWindow must not run on the accept path in this test") }

	beforeCall := time.Now()

	outcome, err := sm.handleBlockMsgWithWindow(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: int32(blockHeight),
		peer:        p,
	}, wa, flushWindow, flushWindow, nil)
	addedToWindow := outcome == blockAdmitWindowed

	require.NoError(t, err, "windowed accept must not error")
	require.True(t, addedToWindow, "block must be accepted into the window (accept path)")
	require.False(t, wa.empty(), "accumulator must hold the accepted block")

	// The fix: lastBlockTime is refreshed on the accept path. It must have moved
	// forward from the seeded stale value to a time no earlier than the moment
	// just before the call.
	after := sps.getLastBlockTime()
	require.True(t, after.After(staleTime), "lastBlockTime must advance past the stale seed on windowed accept")
	require.False(t, after.Before(beforeCall), "refreshed lastBlockTime must be no earlier than the call start")
}

// grindPoW increments the header nonce until its hash meets the encoded target,
// using the same HasMetTargetDifficulty check prepareBlockForWindow runs. This
// keeps the test block deterministic regardless of header field values (a random
// header meets the easy regtest target only ~half the time).
func grindPoW(header *wire.BlockHeader) error {
	for {
		var buf bytes.Buffer
		if err := header.Serialize(&buf); err != nil {
			return err
		}

		modelHdr, err := model.NewBlockHeaderFromBytes(buf.Bytes())
		if err != nil {
			return err
		}

		if valid, _, _ := modelHdr.HasMetTargetDifficulty(); valid {
			return nil
		}

		header.Nonce++
	}
}
