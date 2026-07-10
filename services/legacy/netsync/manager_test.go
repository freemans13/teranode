// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/txscript"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// nullTime is an empty time defined for convenience
var nullTime time.Time

type testConfig struct {
	dbName      string
	chainParams *chaincfg.Params
}

type testContext struct {
	cfg          testConfig
	peerNotifier *MockPeerNotifier
	syncManager  *SyncManager
}

func (tc *testContext) Setup(t *testing.T, config *testConfig) error {
	tc.cfg = *config

	tSettings := test.CreateBaseTestSettings(t)

	peerNotifier := NewMockPeerNotifier()

	storeURL, _ := url.Parse("sqlitememory://")

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	if err != nil {
		return errors.NewServiceError("failed to create blockchain store", err)
	}

	blockchainClient, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	if err != nil {
		return errors.NewServiceError("failed to create blockchain client", err)
	}

	blockAssemblyClient, err := blockassembly.NewClient(context.Background(), ulogger.TestLogger{}, tSettings)
	if err != nil {
		return errors.NewServiceError("failed to create block assembly client", err)
	}

	ctx := context.Background()

	logger := ulogger.TestLogger{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		return errors.NewServiceError("failed to create utxo store", err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		return errors.NewServiceError("failed to create utxo store", err)
	}

	validatorClient, err := validator.New(context.Background(), ulogger.TestLogger{}, tSettings, utxoStore, nil, nil, nil, blockAssemblyClient, nil)
	if err != nil {
		return errors.NewServiceError("failed to create validator client", err)
	}

	subtreeStore := blob_memory.New()

	subtreeValidation := &subtreevalidation.MockSubtreeValidation{}

	blockvalidationClient, err := blockvalidation.NewClient(context.Background(), ulogger.TestLogger{}, tSettings, "manager_test")
	if err != nil {
		return errors.NewServiceError("failed to create block validation client", err)
	}

	syncMgr, err := New(context.Background(),
		ulogger.TestLogger{},
		tSettings,
		blockchainClient,
		validatorClient,
		utxoStore,
		subtreeStore,
		subtreeValidation,
		blockvalidationClient,
		nil,
		&Config{
			PeerNotifier: peerNotifier,
			ChainParams:  tc.cfg.chainParams,
			MaxPeers:     8,
		})
	if err != nil {
		return errors.NewServiceError("failed to create SyncManager", err)
	}

	tc.syncManager = syncMgr
	tc.peerNotifier = peerNotifier

	return nil
}

func (tc *testContext) Teardown() {
}

// TestPeerConnections tests that the SyncManager tracks the set of connected
// peers.
func TestPeerConnections(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	var ctx testContext

	err := ctx.Setup(t, &testConfig{
		dbName:      "TestPeerConnections",
		chainParams: chainParams,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer ctx.Teardown()

	syncMgr := ctx.syncManager
	syncMgr.Start()

	peerCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         0,
	}

	_, localNode1, err := MakeConnectedPeers(t, peerCfg, peerCfg, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Used to synchronize with calls to SyncManager
	syncChan := make(chan struct{})

	// Register the peer with the sync manager. SyncManager should not start
	// syncing from this peer because it is not a full node.
	syncMgr.NewPeer(localNode1, syncChan)
	select {
	case <-syncChan:
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode1.ID())
	}

	if syncMgr.SyncPeerID() != 0 {
		t.Fatalf("Sync manager is syncing from an unexpected peer %d",
			syncMgr.SyncPeerID())
	}

	// Now connect the SyncManager to a full node, which it should start syncing
	// from.
	peerCfg.Services = wire.SFNodeNetwork

	_, localNode2, err := MakeConnectedPeers(t, peerCfg, peerCfg, 1)
	if err != nil {
		t.Fatal(err)
	}

	localNode2.UpdateLastBlockHeight(100)

	syncMgr.NewPeer(localNode2, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode2.ID())
	}

	if syncMgr.SyncPeerID() != localNode2.ID() {
		t.Fatalf("Expected sync manager to be syncing from peer %d got %d",
			localNode2.ID(), syncMgr.SyncPeerID())
	}

	// Register another full node peer with the manager. Even though the new
	// peer is a valid sync peer, manager should not change from the first one.
	_, localNode3, err := MakeConnectedPeers(t, peerCfg, peerCfg, 2)
	if err != nil {
		t.Fatal(err)
	}

	localNode3.UpdateLastBlockHeight(100)

	syncMgr.NewPeer(localNode3, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode3.ID())
	}

	if syncMgr.SyncPeerID() != localNode2.ID() {
		t.Fatalf("Sync manager is syncing from an unexpected peer %d; "+
			"expected %d", syncMgr.SyncPeerID(), localNode2.ID())
	}

	// SyncManager should unregister peer when it is done. When sync peer drops,
	// manager should start syncing from another valid peer.
	syncMgr.DonePeer(localNode2, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to unregister peer %d",
			localNode2.ID())
	}

	if syncMgr.SyncPeerID() != localNode3.ID() {
		t.Fatalf("Expected sync manager to be syncing from peer %d",
			localNode3.ID())
	}

	// Expect SyncManager to stop syncing when last valid peer is disconnected.
	syncMgr.DonePeer(localNode3, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to unregister peer %d",
			localNode3.ID())
	}

	if syncMgr.SyncPeerID() != 0 {
		t.Fatalf("Expected sync manager to stop syncing after peer disconnect")
	}

	err = syncMgr.Stop()
	if err != nil {
		t.Fatalf("failed to stop SyncManager: %v", err)
	}
}

func TestSyncManager_QueueInv(t *testing.T) {
	t.Run("empty message - no kafka", func(t *testing.T) {
		msgChan := make(chan interface{})
		sm := SyncManager{
			msgChan: msgChan,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			msg := <-msgChan
			invMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, invMsg.inv.InvList, 0)
			wg.Done()
		}()

		sm.QueueInv(&wire.MsgInv{}, nil)

		wg.Wait()
	})

	t.Run("tx message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			// no message should be sent here
			msg := <-msgChan
			require.Nil(t, msg)
		}()

		go func() {
			msg := <-legacyKafkaInvCh

			var value kafkamessage.KafkaInvTopicMessage
			err := proto.Unmarshal(msg.Value, &value)
			require.NoError(t, err)

			wireInvMsg, err := sm.newInvFromKafkaMessage(&value)
			require.NoError(t, err)
			assert.Len(t, wireInvMsg.inv.InvList, 2)
			wg.Done()
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})

	t.Run("block message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			msg := <-msgChan
			wireInvMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, wireInvMsg.inv.InvList, 2)
			wg.Done()
		}()

		go func() {
			msg := <-legacyKafkaInvCh
			require.Nil(t, msg)
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})

	t.Run("mixed message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			// no message should be sent here
			msg := <-msgChan
			wireInvMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, wireInvMsg.inv.InvList, 1)
			wg.Done()
		}()

		go func() {
			msg := <-legacyKafkaInvCh

			var value kafkamessage.KafkaInvTopicMessage
			err := proto.Unmarshal(msg.Value, &value)
			require.NoError(t, err)

			wireInvMsg, err := sm.newInvFromKafkaMessage(&value)
			require.NoError(t, err)
			assert.Len(t, wireInvMsg.inv.InvList, 1)
			wg.Done()
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})
}

func setupQueueInvTests() (chan interface{}, chan *kafka.Message, *SyncManager, *peer.Peer) {
	msgChan := make(chan interface{})
	legacyKafkaInvCh := make(chan *kafka.Message)

	sm := SyncManager{
		msgChan:          msgChan,
		legacyKafkaInvCh: legacyKafkaInvCh,
		logger:           ulogger.TestLogger{},
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	smPeer := &peer.Peer{}
	sm.peerStates.Set(smPeer, &peerSyncState{})

	return msgChan, legacyKafkaInvCh, &sm, smPeer
}

func TestSendDuringShutdown(t *testing.T) {
	t.Run("open channel delivers", func(t *testing.T) {
		ch := make(chan int, 1)
		require.True(t, sendDuringShutdown(ch, 7))
		require.Equal(t, 7, <-ch)
	})

	t.Run("closed channel drops without panic", func(t *testing.T) {
		ch := make(chan int)
		close(ch)
		require.NotPanics(t, func() {
			require.False(t, sendDuringShutdown(ch, 1))
		})
	})
}

// TestQueueInv_NoPanicWhenChannelsClosedDuringShutdown reproduces the shutdown
// race that previously crashed the process: inv delivery runs on a peer
// goroutine (OnInv -> QueueInv) while teardown closes the target channels — the
// kafka async producer closes legacyKafkaInvCh in its Stop(), and the block
// handler stops draining msgChan. QueueInv's shutdown-flag check cannot make the
// subsequent send atomic against that close, so a late inv hit a closed channel
// and panicked. The send must now drop the inv instead.
func TestQueueInv_NoPanicWhenChannelsClosedDuringShutdown(t *testing.T) {
	t.Run("tx inv after legacyKafkaInvCh closed", func(t *testing.T) {
		_, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()
		close(legacyKafkaInvCh)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})

	t.Run("block inv after msgChan closed", func(t *testing.T) {
		msgChan, _, sm, smPeer := setupQueueInvTests()
		close(msgChan)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})

	t.Run("non-kafka path after msgChan closed", func(t *testing.T) {
		msgChan, _, sm, smPeer := setupQueueInvTests()
		sm.legacyKafkaInvCh = nil // exercise the else branch
		close(msgChan)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})
}

// Test blockchain syncing protocol. SyncManager should request, processes, and
// relay blocks to/from peers.
// TODO: Test is timing out, needs to be fixed.
func TestBlockchainSync(t *testing.T) {
	t.Skip("skipping")

	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1

	var ctx testContext

	err := ctx.Setup(t, &testConfig{
		dbName:      "TestBlockchainSync",
		chainParams: &chainParams,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer ctx.Teardown()

	syncMgr := ctx.syncManager
	syncMgr.Start()

	remoteMessages := newMessageChans()
	remotePeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetBlocks: func(p *peer.Peer, msg *wire.MsgGetBlocks) {
				remoteMessages.getBlocksChan <- msg
			},
			OnGetData: func(p *peer.Peer, msg *wire.MsgGetData) {
				remoteMessages.getDataChan <- msg
			},
			OnReject: func(p *peer.Peer, msg *wire.MsgReject) {
				remoteMessages.rejectChan <- msg
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	localMessages := newMessageChans()
	localPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnInv: func(p *peer.Peer, msg *wire.MsgInv) {
				localMessages.invChan <- msg
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	_, localNode, err := MakeConnectedPeers(t, remotePeerCfg, localPeerCfg, 0)
	if err != nil {
		t.Fatal(err)
	}

	syncMgr.NewPeer(localNode, nil)

	// SyncManager should send a getblocks message to start block download
	select {
	case msg := <-remoteMessages.getBlocksChan:
		if msg.HashStop != zeroHash {
			t.Fatalf("Expected no hash stop in getblocks, got %v", msg.HashStop)
		}

		if len(msg.BlockLocatorHashes) != 1 ||
			*msg.BlockLocatorHashes[0] != *chainParams.GenesisHash {
			t.Fatal("Received unexpected block locator in getblocks message")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getblocks message")
	}

	// Address is an anyone-can-spend P2SH script
	address, scriptSig, err := GenerateAnyoneCanspendAddress(&chainParams)
	if err != nil {
		t.Fatalf("Error constructing P2SH address: %v", err)
	}

	genesisBlock := bsvutil.NewBlock(chainParams.GenesisBlock)

	// Generate chain of 3 blocks
	blocks := make([]*bsvutil.Block, 0, 3)
	blockVersion := int32(2)
	prevBlock := genesisBlock

	for i := 0; i < 3; i++ {
		block, err := CreateBlock(prevBlock, nil, blockVersion,
			nullTime, address, []wire.TxOut{}, &chainParams)
		if err != nil {
			t.Fatalf("failed to generate block: %v", err)
		}

		blocks = append(blocks, block)
		prevBlock = block
	}

	// Remote node replies to getblocks with an inv
	invMsg := wire.NewMsgInv()

	for _, block := range blocks {
		invVect := wire.NewInvVect(wire.InvTypeBlock, block.Hash())
		err := invMsg.AddInvVect(invVect)
		require.NoError(t, err)
	}

	syncMgr.QueueInv(invMsg, localNode)

	// SyncManager should send a getdata message requesting blocks
	select {
	case msg := <-remoteMessages.getDataChan:
		if len(msg.InvList) != len(blocks) {
			t.Fatalf("Expected %d blocks in getdata message, got %d",
				len(blocks), len(msg.InvList))
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}
	// Remote node sends first 3 blocks
	errChan := make(chan error)
	for _, block := range blocks {
		syncMgr.QueueBlock(block, localNode, errChan)

		select {
		case err := <-errChan:
			t.Fatalf("Error in sync manager to process block %d: %v", block.Height(), err)
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for sync manager to process block %d", block.Height())
		}
	}

	if localNode.LastBlock() != 3 {
		t.Fatalf("Expected peer's LastBlock to be 3, got %d",
			localNode.LastBlock())
	}

	if syncMgr.IsCurrent() {
		t.Fatal("Expected IsCurrent() to be false as blocks have old " +
			"timestamps")
	}

	// Check that no blocks were relayed to peers since syncer is not current
	select {
	case <-ctx.peerNotifier.relayInventoryChan:
		t.Fatal("PeerNotifier received unexpected RelayInventory call")
	default:
	}

	// Create current block with a non-Coinbase transaction
	prevTx, err := blocks[0].Tx(0)
	if err != nil {
		t.Fatal(err)
	}

	spendTx, err := createSpendingTx(prevTx, 0, scriptSig, address)
	if err != nil {
		t.Fatal(err)
	}

	timestamp := time.Now().Truncate(time.Second)
	prevBlock = blocks[len(blocks)-1]
	txns := []*bsvutil.Tx{spendTx}

	block, err := CreateBlock(prevBlock, txns, blockVersion,
		timestamp, address, []wire.TxOut{}, &chainParams)
	if err != nil {
		t.Fatal(err)
	}

	// SyncManager should send a getdata message requesting blocks
	syncMgr.QueueInv(buildBlockInv(block), localNode)
	select {
	case msg := <-remoteMessages.getDataChan:
		if len(msg.InvList) != 1 {
			t.Fatalf("Expected 1 block in getdata message, got %d",
				len(msg.InvList))
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}

	// Remote node sends new block
	syncMgr.QueueBlock(block, localNode, errChan)
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to process block %d",
			block.Height())
	}

	// Assert calls made to PeerNotifier
	select {
	case call := <-ctx.peerNotifier.transactionConfirmedChan:
		if !call.tx.Hash().IsEqual(spendTx.Hash()) {
			t.Fatalf("PeerNotifier received TransactionConfirmed with "+
				"unexpected tx %v, expected %v", call.tx.Hash(),
				spendTx.Hash())
		}
	default:
		t.Fatal("Expected SyncManager to make TransactionConfirmed call to " +
			"PeerNotifier")
	}

	select {
	case <-ctx.peerNotifier.announceNewTransactionsChan:
	default:
		t.Fatal("Expected SyncManager to make AnnounceNewTransactions call " +
			"to PeerNotifier")
	}

	select {
	case call := <-ctx.peerNotifier.relayInventoryChan:
		if call.invVect.Type != wire.InvTypeBlock ||
			call.invVect.Hash != *block.Hash() {
			t.Fatalf("PeerNotifier received unexpected RelayInventory call: "+
				"%v", call.invVect)
		}
	default:
		t.Fatal("Expected SyncManager to make RelayInventory call to " +
			"PeerNotifier")
	}

	if localNode.LastBlock() != 4 {
		t.Fatalf("Expected peer's LastBlock to be 4, got %d",
			localNode.LastBlock())
	}

	// SyncManager should now be current since last block was recent
	if !syncMgr.IsCurrent() {
		t.Fatal("Expected IsCurrent() to be true")
	}

	// Send invalid block with timestamp in the far future
	prevBlock = block
	timestamp = time.Now().Truncate(time.Second).Add(1000 * time.Hour)

	block, err = CreateBlock(prevBlock, nil, blockVersion,
		timestamp, address, []wire.TxOut{}, &chainParams)
	if err != nil {
		t.Fatal(err)
	}

	syncMgr.QueueInv(buildBlockInv(block), localNode)
	select {
	case <-remoteMessages.getDataChan:
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}

	syncMgr.QueueBlock(block, localNode, errChan)
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to process block %d",
			block.Height())
	}

	// Expect block to not be added to chain
	if localNode.LastBlock() != 4 {
		t.Fatalf("Expected peer's LastBlock to be 4, got %d",
			localNode.LastBlock())
	}

	// Expect node to send reject in response to invalid block
	select {
	case msg := <-remoteMessages.rejectChan:
		if msg.Code != wire.RejectInvalid {
			t.Fatalf("Reject message has unexpected code %s, expected %s",
				msg.Code, wire.RejectInvalid)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive reject message")
	}

	err = syncMgr.Stop()
	if err != nil {
		t.Fatalf("failed to stop SyncManager: %v", err)
	}
}

type msgChans struct {
	memPoolChan    chan *wire.MsgMemPool
	txChan         chan *wire.MsgTx
	blockChan      chan *wire.MsgBlock
	invChan        chan *wire.MsgInv
	headersChan    chan *wire.MsgHeaders
	getDataChan    chan *wire.MsgGetData
	getBlocksChan  chan *wire.MsgGetBlocks
	getHeadersChan chan *wire.MsgGetHeaders
	rejectChan     chan *wire.MsgReject
}

func newMessageChans() *msgChans {
	var instance msgChans
	instance.memPoolChan = make(chan *wire.MsgMemPool)
	instance.txChan = make(chan *wire.MsgTx)
	instance.blockChan = make(chan *wire.MsgBlock)
	instance.invChan = make(chan *wire.MsgInv)
	instance.headersChan = make(chan *wire.MsgHeaders)
	instance.getDataChan = make(chan *wire.MsgGetData)
	instance.getBlocksChan = make(chan *wire.MsgGetBlocks)
	instance.getHeadersChan = make(chan *wire.MsgGetHeaders)
	instance.rejectChan = make(chan *wire.MsgReject)

	return &instance
}

func buildBlockInv(blocks ...*bsvutil.Block) *wire.MsgInv {
	msg := wire.NewMsgInv()

	for _, block := range blocks {
		invVect := wire.NewInvVect(wire.InvTypeBlock, block.Hash())
		_ = msg.AddInvVect(invVect)
	}

	return msg
}

// createSpendingTx constructs a transaction spending from the provided one
// which sends the entire value of one output to the given address.
func createSpendingTx(prevTx *bsvutil.Tx, index uint32, scriptSig []byte, address bsvutil.Address) (*bsvutil.Tx, error) {
	scriptPubKey, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}

	prevTxMsg := prevTx.MsgTx()
	prevOut := prevTxMsg.TxOut[index]
	prevOutPoint := &wire.OutPoint{Hash: prevTxMsg.TxHash(), Index: index}

	spendTx := wire.NewMsgTx(1)
	spendTx.AddTxIn(wire.NewTxIn(prevOutPoint, scriptSig))
	spendTx.AddTxOut(wire.NewTxOut(prevOut.Value, scriptPubKey))

	return bsvutil.NewTx(spendTx), nil
}

func TestHandleCheckSyncPeer_HeadersFirstMode(t *testing.T) {
	t.Run("headers-first mode detects last block time violation", func(t *testing.T) {
		sp := &peer.Peer{} // zero-value peer is sufficient for this test
		sps := &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute), // way past maxLastBlockTime (3 min)
			ticks:         1,                                 // non-zero so validNetworkSpeed runs
		}

		sm := &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// Last-block-time violations are no longer skipped during headers-first mode.
		// The violation is detected and the peer rotation path is entered, which panics
		// here because the test uses a minimal SyncManager without full peer setup.
		assert.Panics(t, func() {
			sm.handleCheckSyncPeer()
		})
	})

	t.Run("headers-first mode skips network speed violation", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime: time.Now(), // recent — no time violation
			ticks:         1,
			violations:    maxNetworkViolations, // at violation threshold
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 1000, // high threshold to ensure violation
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		sm.handleCheckSyncPeer()

		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("normal mode retains peer when no violations", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime: time.Now(), // recent — no violation
			ticks:         1,
		}

		sm := &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false) // normal mode
		sm.peerStates.Set(sp, &peerSyncState{})

		sm.handleCheckSyncPeer()

		// No violations, sync peer should still be set
		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("headers-first mode keeps actively-downloading peer despite last-block-time", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime:          time.Now().Add(-10 * time.Minute), // past maxLastBlockTime
			ticks:                  1,
			assocReadBytes:         10 * 1024 * 1024, // 10 MB pulled in over the last tick
			assocReadBytesLastTick: 0,
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// A large block is still streaming in (healthy association throughput),
		// so the peer must NOT be rotated even though no block completed within
		// maxLastBlockTime. If it rotated, the minimal SyncManager would panic.
		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates a slow-drip peer once past the wall-clock cap", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			// No completed block for longer than peer.MaxBlockDownloadTime.
			lastBlockTime:          time.Now().Add(-peer.MaxBlockDownloadTime - time.Minute),
			ticks:                  1,
			assocReadBytes:         10 * 1024 * 1024, // still "healthy" throughput
			assocReadBytesLastTick: 0,
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// Past the cap, throughput no longer protects the peer — it is rotated
		// (which panics in this minimal SyncManager).
		assert.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}

// TestHandleBlockMsg_OrphanDuringCatchup verifies a block with an unknown
// parent arriving during legacy sync / catching blocks triggers a getblocks
// continuation request instead of being silently dropped. In the legacy sync
// protocol the peer announces its tip after delivering a getblocks batch; that
// orphan tip is the only signal to request the next batch, so swallowing it
// stalls the sync until the stall detector rotates the peer.
func TestHandleBlockMsg_OrphanDuringCatchup(t *testing.T) {
	prevHash := chainhash.Hash{0x01}

	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS

	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// Parent header lookup fails — the block is an orphan to us.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not found"))
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	// Real (unconnected) peer: PushGetBlocksMsg needs a logger, and
	// QueueMessage is a no-op on a disconnected peer.
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
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer sm.requestedBlocks.Stop()
	sm.peerStates.Set(p, state)
	sm.requestedBlocks.Set(blockHash, struct{}{})

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	// The orphan is dropped without error or peer disconnect...
	require.NoError(t, err)

	// ...but it must trigger the batch-continuation request: a block locator
	// from our best block, pushed to the peer as getblocks.
	blockchainClient.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}

// TestHandleCheckSyncPeer_LocalBacklog verifies the stall detector does not
// blame the sync peer for backpressure the node inflicts on itself: while
// blocks are queued or mid-validation locally, OnBlock stops reading from the
// peer, so zero throughput and a stale last-block-time say nothing about the
// peer's health.
func TestHandleCheckSyncPeer_LocalBacklog(t *testing.T) {
	// Zero throughput (recvBytes == recvBytesLastTick) one violation short of
	// the rotation threshold, plus a last-block-time far past maxLastBlockTime:
	// without a backlog this tick rotates the sync peer.
	newStalledState := func() *syncPeerState {
		return &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute),
			ticks:         1,
			violations:    maxNetworkViolations - 1,
		}
	}

	newSyncManager := func(sp *peer.Peer, sps *syncPeerState) *SyncManager {
		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false)
		sm.peerStates.Set(sp, &peerSyncState{})

		return sm
	}

	t.Run("keeps sync peer and accrues no violation while backlog pending", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := newStalledState()
		sm := newSyncManager(sp, sps)

		sm.blockBacklog.Add(1) // a block is queued or mid-validation locally

		// Rotation would panic in this minimal SyncManager (no blockchain
		// client), so NotPanics proves the peer was kept.
		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		assert.Equal(t, sp, sm.loadSyncPeer())
		assert.Equal(t, maxNetworkViolations-1, sps.getViolations())
	})

	t.Run("still rotates on zero throughput once backlog drained", func(t *testing.T) {
		sp := &peer.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// No local backlog: the same zero-throughput state is a real peer
		// stall, so the rotation path runs (and panics in this minimal setup).
		assert.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}

// TestProcessTXmetaBatchMessage_SkipsInBlockTx verifies the tx announce path
// drops txmeta entries flagged InBlock. The txmeta Kafka topic carries every
// validated transaction — including those that arrived as part of a block or
// announced subtree (block validation, subtree validation, legacy sync, which
// feed the subtree-validation cache) — and announcing those as fresh mempool
// txs floods peers with getdata for transactions that are long mined and
// often already pruned.
func TestProcessTXmetaBatchMessage_SkipsInBlockTx(t *testing.T) {
	inBlockHash := chainhash.Hash{0xAA}
	mempoolHash := chainhash.Hash{0xBB}

	inBlockBytes, err := (&meta.Data{Fee: 1, SizeInBytes: 100, InBlock: true}).MetaBytes()
	require.NoError(t, err)

	mempoolBytes, err := (&meta.Data{Fee: 2, SizeInBytes: 200}).MetaBytes()
	require.NoError(t, err)

	// Build a v1 wire message with both entries.
	buf := new(bytes.Buffer)
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(2)))

	for _, entry := range []struct {
		hash    chainhash.Hash
		content []byte
	}{
		{inBlockHash, inBlockBytes},
		{mempoolHash, mempoolBytes},
	} {
		buf.Write(entry.hash[:])
		buf.WriteByte(txmetacache.WireActionADD)
		require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(len(entry.content))))
		buf.Write(entry.content)
	}

	var (
		mu        sync.Mutex
		announced []chainhash.Hash
	)

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	sm.txAnnounceBatcher = batcher.NewWithDeduplicationAndPool[TxHashAndFee](10, 10*time.Millisecond, func(batch []*TxHashAndFee) {
		mu.Lock()
		defer mu.Unlock()
		for _, item := range batch {
			announced = append(announced, item.TxHash)
		}
	}, true)

	require.NoError(t, sm.processTXmetaBatchMessage(buf.Bytes()))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(announced) > 0
	}, 2*time.Second, 10*time.Millisecond, "expected the mempool tx to be announced")

	// Give the batcher one more flush window so a wrongly-announced in-block
	// tx would have surfaced.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []chainhash.Hash{mempoolHash}, announced, "only the mempool tx must be announced")
}

func TestHasHealthyDownloadThroughput(t *testing.T) {
	const minSpeed = 51200 // 50 KiB/s, matches default minSyncPeerNetworkSpeed

	t.Run("no prior sample", func(t *testing.T) {
		sps := &syncPeerState{ticks: 0, assocReadBytes: 10 * 1024 * 1024}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("no bytes moved is never healthy, even with zero threshold", func(t *testing.T) {
		sps := &syncPeerState{ticks: 1, assocReadBytes: 100, assocReadBytesLastTick: 100}
		require.False(t, sps.hasHealthyDownloadThroughput(0))
	})

	t.Run("chatter below threshold", func(t *testing.T) {
		// ~33 B/s over a 30s tick — far below 50 KiB/s.
		sps := &syncPeerState{ticks: 1, assocReadBytes: 1000, assocReadBytesLastTick: 0}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("active download above threshold", func(t *testing.T) {
		// 10 MB over the tick — well above 50 KiB/s.
		sps := &syncPeerState{ticks: 1, assocReadBytes: 10 * 1024 * 1024, assocReadBytesLastTick: 0}
		require.True(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("counter decrease (stream removed) is not healthy", func(t *testing.T) {
		// A stream dropped between samples, so the association sum fell. The
		// unsigned subtraction must not wrap to a huge "healthy" value.
		sps := &syncPeerState{ticks: 2, assocReadBytes: 1000, assocReadBytesLastTick: 10 * 1024 * 1024}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})
}

// TestSyncPeerStateFor verifies the last-block-time refresh matches not just the
// sync peer itself but any stream of its association — under BlockPriority the
// block is delivered on the DATA1 stream peer, not the GENERAL sync peer.
func TestSyncPeerStateFor(t *testing.T) {
	general := &peer.Peer{}
	sps := &syncPeerState{lastBlockTime: time.Now()}

	sm := &SyncManager{
		logger:     ulogger.TestLogger{},
		peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}
	sm.storeSyncPeer(general, sps)

	assoc := peer.NewAssociation([]byte{0x01}, general)
	general.SetAssociation(assoc)

	// The DATA1 stream peer is a different Peer sharing the same association.
	data1 := &peer.Peer{}
	require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
	data1.SetAssociation(assoc)

	t.Run("sync peer itself matches", func(t *testing.T) {
		got, ok := sm.syncPeerStateFor(general)
		require.True(t, ok)
		require.Equal(t, sps, got)
	})

	t.Run("association sibling (DATA1) matches", func(t *testing.T) {
		got, ok := sm.syncPeerStateFor(data1)
		require.True(t, ok)
		require.Equal(t, sps, got)
	})

	t.Run("unrelated peer does not match", func(t *testing.T) {
		other := &peer.Peer{}
		_, ok := sm.syncPeerStateFor(other)
		require.False(t, ok)
	})
}

// TestHandleNewPeerMsg_NilFSMState exercises the path where the blockchain
// client returns (nil, err) from GetFSMCurrentState — common during transient
// gRPC failures or service restarts. The pre-fix code dereferenced the nil
// pointer and panicked. The fix must guard the dereference and still register
// the peer.
func TestHandleNewPeerMsg_NilFSMState(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return((*blockchain2.FSMStateType)(nil), errors.NewServiceError("transient gRPC error"))

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// Use a real connected peer: handleNewPeerMsg now refuses to register peers
	// whose socket has been torn down by the time the newPeerMsg drains.
	peerCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         0,
	}
	_, smPeer, err := MakeConnectedPeers(t, peerCfg, peerCfg, 99)
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			require.Failf(t, "handleNewPeerMsg panicked", "panic: %v", r)
		}
	}()

	sm.handleNewPeerMsg(smPeer)

	require.True(t, sm.peerStates.Exists(smPeer), "peer must be registered even when FSM state is unavailable")
	require.Equal(t, uint64(0), sm.currentFeeFilter.Load(), "fee filter must not be set when FSM state is unavailable")
}

// TestHandleNewPeerMsg_SetsFeeFilterWhenCatchingBlocks verifies that EVERY peer
// connecting while the node is catching up is asked (via a raised feefilter) to
// hold back transaction announcements, reducing load during sync. It asserts the
// observable behaviour — the feefilter message is actually delivered to each
// peer's remote end — not just the internal marker, and covers the second peer
// (regression guard: an earlier version only raised it for the first connector).
// The filter is restored to the policy default once the node reaches RUNNING
// (resetFeeFilterToDefault).
func TestHandleNewPeerMsg_SetsFeeFilterWhenCatchingBlocks(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return(&catchingBlocks, nil)

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// connectPeer returns a peer for handleNewPeerMsg to operate on; gotFee
	// records the MinFee of any feefilter its remote end receives.
	connectPeer := func(idx uint8, gotFee *atomic.Int64) *peer.Peer {
		remoteCfg := peer.Config{
			Listeners: peer.MessageListeners{
				OnFeeFilter: func(_ *peer.Peer, msg *wire.MsgFeeFilter) {
					gotFee.Store(msg.MinFee)
				},
			},
			UserAgentName:    "btcdtest",
			UserAgentVersion: "1.0",
			ChainParams:      chainParams,
		}
		localCfg := peer.Config{
			Listeners:        peer.MessageListeners{},
			UserAgentName:    "btcdtest",
			UserAgentVersion: "1.0",
			ChainParams:      chainParams,
		}
		remote, smPeer, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
		require.NoError(t, err)
		require.True(t, remote.Connected())
		return smPeer
	}

	var fee1, fee2 atomic.Int64
	p1 := connectPeer(101, &fee1)
	p2 := connectPeer(102, &fee2)

	sm.handleNewPeerMsg(p1)
	sm.handleNewPeerMsg(p2)

	want := int64(bsvutil.SatoshiPerBitcoin)
	require.True(t, WaitUntil(func() bool { return fee1.Load() == want }, 2*time.Second),
		"first peer must receive the raised feefilter")
	require.True(t, WaitUntil(func() bool { return fee2.Load() == want }, 2*time.Second),
		"second peer must also receive the raised feefilter, not just the first")

	require.Equal(t, uint64(bsvutil.SatoshiPerBitcoin), sm.currentFeeFilter.Load(),
		"fee filter marker must be set while catching up")
	require.True(t, sm.peerStates.Exists(p1), "first peer must be registered")
	require.True(t, sm.peerStates.Exists(p2), "second peer must be registered")
}

// TestHandleNewPeerMsg_SkipsDisconnectedPeer verifies that a peer whose socket
// was torn down before the queued newPeerMsg drained is not inserted into
// peerStates. Pairs with the Connected() filter in startSync to prevent a dead
// pointer from being elected as the sync peer.
func TestHandleNewPeerMsg_SkipsDisconnectedPeer(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return((*blockchain2.FSMStateType)(nil), errors.NewServiceError("transient gRPC error")).Maybe()

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// A zero-value Peer has connected=0, so Connected() returns false. This
	// mirrors the state of a peer whose underlying socket has already closed
	// by the time handleNewPeerMsg pulls its newPeerMsg off msgChan.
	disconnectedPeer := &peer.Peer{}

	sm.handleNewPeerMsg(disconnectedPeer)

	require.False(t, sm.peerStates.Exists(disconnectedPeer), "disconnected peer must not be registered in peerStates")
}

// buildHeaderChain constructs a synthetic, properly-linked chain of block
// headers starting from prevHash. Each header's PrevBlock is set to the
// previous header's real BlockHash(), so the chain links exactly as the
// headers-first state machine's PrevBlock check requires. It returns the
// headers together with their computed hashes so the test can build
// checkpoints whose Hash fields match the real header hashes at the chosen
// heights. The nonce seeds each header uniquely so hashes differ.
func buildHeaderChain(prevHash chainhash.Hash, count int, startNonce uint32) ([]*wire.BlockHeader, []chainhash.Hash) {
	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := prevHash

	for i := 0; i < count; i++ {
		h := wire.NewBlockHeader(1, &prev, &chainhash.Hash{}, 0, startNonce+uint32(i)) //nolint:gosec
		hash := h.BlockHash()
		headers = append(headers, h)
		hashes = append(hashes, hash)
		prev = hash
	}

	return headers, hashes
}

// TestHandleHeadersMsg_PipelinesNextIntervalHeaders is the core pipelining
// test. It drives the headers-first state machine across a checkpoint boundary
// and asserts that the NEXT interval's getheaders request is issued as soon as
// the current interval's headers are fully received (at receivedCheckpoint) —
// WITHOUT waiting for the checkpoint block to be processed.
//
// RED against pre-pipelining code: at receivedCheckpoint the old code only
// called fetchHeaderBlocks() and returned; the next-interval getheaders was
// deferred until the checkpoint BLOCK arrived in the block handler. So no
// getheaders reaches the peer here and the assertion times out.
//
// GREEN after the change: handleHeadersMsg advances the header-request cursor
// and issues the next-interval getheaders immediately, so the peer observes it.
//
// The sync peer is a real, connected peer (via MakeConnectedPeers). When
// handleHeadersMsg calls PushGetHeadersMsg on it, the message travels the pipe
// to the counterpart peer, whose OnGetHeaders callback records the actual
// locator/stop-hash — a faithful observation of the real request.
func TestHandleHeadersMsg_PipelinesNextIntervalHeaders(t *testing.T) {
	// Base (genesis-equivalent) block already in our chain; the first header
	// batch links to it.
	base := chainhash.Hash{0xaa}

	// Interval 1: base -> ... -> checkpoint1 (5 headers).
	interval1, hashes1 := buildHeaderChain(base, 5, 1000)
	cp1Height := int32(5)
	cp1Hash := hashes1[len(hashes1)-1]

	// Interval 2 starts at checkpoint1's hash; checkpoint2 sits a few headers
	// further on. We only need its hash to build the checkpoint entry.
	_, hashes2 := buildHeaderChain(cp1Hash, 5, 2000)
	cp2Height := int32(10)
	cp2Hash := hashes2[len(hashes2)-1]

	chainParams := chaincfg.MainNetParams
	chainParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: cp1Height, Hash: &cp1Hash},
		{Height: cp2Height, Hash: &cp2Hash},
	}

	// Capture getheaders that the sync peer sends to its counterpart.
	captured := make(chan *wire.MsgGetHeaders, 4)
	counterpartCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, msg *wire.MsgGetHeaders) {
				captured <- msg
			},
			// getdata is emitted by fetchHeaderBlocks; drain it so the peer's
			// output pipeline never blocks.
			OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	// counterpart sees the sync peer's outbound getheaders.
	_, syncPeer, err := MakeConnectedPeers(t, counterpartCfg, syncPeerCfg, 0)
	require.NoError(t, err)

	defer syncPeer.DisconnectWithInfo("test done")

	// haveInventory (called by fetchHeaderBlocks) queries GetBlockHeader; return
	// not-found so blocks are requested (getdata), which is harmless here.
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Point the block-level checkpoint tracker at checkpoint 1 and seed the
	// header list with the base block (the "dummy" anchor) exactly as
	// resetHeaderState does.
	sm.nextCheckpoint = &chainParams.Checkpoints[0]
	// The header-request cursor starts aligned with the block-level tracker,
	// exactly as startSync/resetHeaderState initialise it.
	sm.headerCheckpoint = &chainParams.Checkpoints[0]
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})

	// Deliver interval-1 headers, the last of which is checkpoint 1.
	msg := wire.NewMsgHeaders()
	for _, h := range interval1 {
		require.NoError(t, msg.AddBlockHeader(h))
	}

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	// The checkpoint was verified, so headers-first mode must remain on and the
	// peer must not have been disconnected.
	require.True(t, sm.headersFirstMode.Load(), "headers-first mode must stay on after a verified checkpoint")
	require.True(t, syncPeer.Connected(), "sync peer must not be disconnected on a matching checkpoint")

	// Core assertion: the next interval's getheaders must fire NOW, toward
	// checkpoint 2, without waiting for the checkpoint block. Its stop hash is
	// checkpoint 2's hash and its locator begins at checkpoint 1's hash.
	select {
	case got := <-captured:
		require.True(t, got.HashStop.IsEqual(&cp2Hash),
			"next-interval getheaders must stop at checkpoint 2 hash, got %s", got.HashStop)
		require.NotEmpty(t, got.BlockLocatorHashes, "getheaders must carry a locator")
		require.True(t, got.BlockLocatorHashes[0].IsEqual(&cp1Hash),
			"next-interval getheaders locator must begin at checkpoint 1 hash, got %s", got.BlockLocatorHashes[0])
	case <-time.After(3 * time.Second):
		t.Fatal("next-interval getheaders was NOT issued at receivedCheckpoint (boundary stall)")
	}
}

// TestHandleHeadersMsg_CheckpointMismatchDisconnects verifies the checkpoint
// verification invariant is preserved: a header at the checkpoint height whose
// hash does NOT match the expected checkpoint hash disconnects the peer and
// does not advance the pipeline.
func TestHandleHeadersMsg_CheckpointMismatchDisconnects(t *testing.T) {
	base := chainhash.Hash{0xbb}

	// Build a properly-linked interval so linkage passes and the ONLY failure
	// is the checkpoint-hash mismatch at the checkpoint height.
	interval1, hashes1 := buildHeaderChain(base, 5, 5000)

	// The checkpoint expects a DIFFERENT hash than the real header at height 5.
	wrongHash := chainhash.Hash{0xde, 0xad}
	require.False(t, hashes1[len(hashes1)-1].IsEqual(&wrongHash))

	chainParams := chaincfg.MainNetParams
	chainParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: 5, Hash: &wrongHash},
		{Height: 10, Hash: &chainhash.Hash{0xee}},
	}

	syncPeerCfg := peer.Config{
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	counterpartCfg := syncPeerCfg

	_, syncPeer, err := MakeConnectedPeers(t, counterpartCfg, syncPeerCfg, 1)
	require.NoError(t, err)

	sm := &SyncManager{
		ctx:             context.Background(),
		logger:          ulogger.TestLogger{},
		chainParams:     &chainParams,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:      list.New(),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	})
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.nextCheckpoint = &chainParams.Checkpoints[0]
	sm.headerCheckpoint = &chainParams.Checkpoints[0]
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})

	msg := wire.NewMsgHeaders()
	for _, h := range interval1 {
		require.NoError(t, msg.AddBlockHeader(h))
	}

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	// The mismatch must have disconnected the peer.
	disconnected := WaitUntil(func() bool { return !syncPeer.Connected() }, 3*time.Second)
	require.True(t, disconnected, "checkpoint-mismatched header must disconnect the peer")
}

// TestHandleHeadersMsg_ConcurrentPipelineIsRaceFree drives the REAL production
// dispatch (QueueHeaders -> msgChan -> blockHandler) and delivers several
// intervals' header batches back-to-back. Before the fix the block handler ran
// each headers message on its OWN goroutine (`go sm.handleHeadersMsg(msg)`), so
// with the pipeline delivering the next interval before the previous one
// finished, multiple handleHeadersMsg goroutines mutated the shared
// headers-first state (headerList, startHeader, headerCheckpoint) at the same
// time. That is a genuine data race, and `go test -race` on 2ea50d9c4 flags it.
//
// After the fix headers are serialised onto the same single drain goroutine as
// blocks, so the batches are processed one at a time and no shared state is
// touched concurrently. This test therefore RED-fails under -race on the old
// code and passes on the new code. It runs the real concurrent path: the drain
// goroutine and the outer msgChan dispatch goroutine are both live, exactly as
// in production.
func TestHandleHeadersMsg_ConcurrentPipelineIsRaceFree(t *testing.T) {
	const intervals = 8

	// Build one continuous header chain of `intervals` segments of 5 headers
	// each; a checkpoint sits at the last header of every segment. Segment i's
	// first header links onto segment i-1's checkpoint, so when delivered in
	// order the pipeline advances cleanly through every checkpoint.
	base := chainhash.Hash{0xc0, 0x1d}

	segments := make([][]*wire.BlockHeader, 0, intervals)
	checkpoints := make([]chaincfg.Checkpoint, 0, intervals)

	prev := base
	for i := 0; i < intervals; i++ {
		hdrs, hashes := buildHeaderChain(prev, 5, uint32(1000*(i+1)))
		segments = append(segments, hdrs)

		cpHeight := int32(5 * (i + 1))
		cpHash := hashes[len(hashes)-1]
		checkpoints = append(checkpoints, chaincfg.Checkpoint{Height: cpHeight, Hash: &cpHash})

		prev = hashes[len(hashes)-1]
	}

	chainParams := chaincfg.MainNetParams
	chainParams.Checkpoints = checkpoints

	// The counterpart records every outbound getheaders the sync peer sends.
	// Each processed interval fires exactly one pipelined getheaders, so
	// counting them is a progress signal that touches no shared header state
	// from the test goroutine (which would otherwise be its own data race).
	captured := make(chan struct{}, 4*intervals)
	counterpartCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {
				select {
				case captured <- struct{}{}:
				default:
				}
			},
			OnGetData: func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	syncPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {},
			OnGetData:    func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	_, syncPeer, err := MakeConnectedPeers(t, counterpartCfg, syncPeerCfg, 2)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	// FSM returns RUNNING so blockHandler's msgChan branch skips the expensive
	// current() call. haveInventory (via fetchHeaderBlocks) gets not-found so
	// blocks are "requested" harmlessly.
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&running, nil).Maybe()
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()

	// ParallelWindowMemoryFraction stays 0 so the drain goroutine uses the
	// simple (non-window) block path; we never enqueue blocks here anyway.
	tSettings := test.CreateBaseTestSettings(t)

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
		msgChan:          make(chan interface{}, 256),
		quit:             make(chan struct{}),
		handlerDone:      make(chan struct{}),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.nextCheckpoint = &chainParams.Checkpoints[0]
	sm.headerCheckpoint = &chainParams.Checkpoints[0]
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})

	// Run the real block handler (spawns the inner drain goroutine too). Shut it
	// down cleanly at the end and wait for it to fully stop BEFORE any assertion
	// that reads shared header state, so the test goroutine never races the
	// handler over that state.
	go sm.blockHandler()

	// Fire every interval's headers batch as fast as possible. On the old code
	// these produce overlapping handleHeadersMsg goroutines racing on the
	// shared header state; on the new code they queue behind one another on the
	// single drain goroutine.
	for i := 0; i < intervals; i++ {
		msg := wire.NewMsgHeaders()
		for _, h := range segments[i] {
			require.NoError(t, msg.AddBlockHeader(h))
		}
		sm.QueueHeaders(msg, syncPeer)
	}

	// Also deliver a batch AFTER the final checkpoint has been reached, i.e.
	// once headerCheckpoint has advanced to nil. This exercises the C3 nil-guard
	// on the concurrent path: it must be ignored, not panic.
	trailing := wire.NewMsgHeaders()
	for _, h := range segments[intervals-1] {
		require.NoError(t, trailing.AddBlockHeader(h))
	}
	sm.QueueHeaders(trailing, syncPeer)

	// Every one of the `intervals` batches that is NOT the final checkpoint
	// fires one pipelined getheaders; the final one advances the cursor to nil
	// and fires none. So expect at least intervals-1 getheaders. Waiting on this
	// observable (not on shared state) confirms all batches were processed.
	for got := 0; got < intervals-1; {
		select {
		case <-captured:
			got++
		case <-time.After(5 * time.Second):
			t.Fatalf("pipeline stalled: only %d/%d pipelined getheaders observed", got, intervals-1)
		}
	}

	// Peer must survive: every delivered header links and matches its
	// checkpoint, so there is never a disconnect. Under -race the primary
	// assertion of this test is implicit: NO data race is reported while the
	// intervals are processed concurrently with the trailing (post-final)
	// batch. On 2ea50d9c4 the per-message `go handleHeadersMsg` produced
	// overlapping mutations of headerList/startHeader/headerCheckpoint and the
	// detector fired here; serialised onto the drain goroutine it is clean.
	require.True(t, syncPeer.Connected(), "sync peer must not be disconnected: all headers link and match their checkpoints")

	// Stop the outer handler. We deliberately do NOT read shared header state
	// (e.g. headerCheckpoint) from the test goroutine: handlerDone is closed by
	// the outer loop and does not synchronise with the inner drain goroutine,
	// so such a read would itself be a data race independent of the fix.
	close(sm.quit)
	<-sm.handlerDone
}

// TestHandleHeadersMsg_RemovesStaleAnchorByIdentity is the deterministic C2
// test. Once intervals overlap, the front of the header list can be a LIVE
// earlier-interval header whose block is still being fetched, while the stale
// anchor the current batch links onto sits at the BACK. The old code removed
// Front() blindly at the checkpoint boundary, dropping the live header and
// stranding its in-flight block request. The fix removes the anchor by
// identity, so the live header survives and the anchor is gone.
func TestHandleHeadersMsg_RemovesStaleAnchorByIdentity(t *testing.T) {
	// liveHash: an interval-N header whose block is still in flight (must NOT be
	// removed). anchorHash: interval-N's checkpoint node, the parent the new
	// batch links onto (the stale anchor that SHOULD be removed this round).
	liveHash := chainhash.Hash{0x11}
	anchorHash := chainhash.Hash{0x22}

	// Interval N+1 links onto the anchor and ends at its checkpoint.
	interval, hashes := buildHeaderChain(anchorHash, 5, 7000)
	cpHeight := int32(20) // anchor sits at height 15, batch runs 16..20
	cpHash := hashes[len(hashes)-1]

	chainParams := chaincfg.MainNetParams
	chainParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: cpHeight, Hash: &cpHash},
	}

	syncPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {},
			OnGetData:    func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, syncPeer, err := MakeConnectedPeers(t, syncPeerCfg, syncPeerCfg, 3)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.nextCheckpoint = &chainParams.Checkpoints[0]
	sm.headerCheckpoint = &chainParams.Checkpoints[0]

	// Seed the list so Front() is the live header and Back() is the anchor the
	// new batch will link onto — the overlapping-interval situation.
	liveEl := sm.headerList.PushBack(&headerNode{height: 14, hash: &liveHash})
	anchorEl := sm.headerList.PushBack(&headerNode{height: 15, hash: &anchorHash})

	require.Same(t, liveEl, sm.headerList.Front(), "precondition: live header is at the front")
	require.Same(t, anchorEl, sm.headerList.Back(), "precondition: anchor is at the back")

	msg := wire.NewMsgHeaders()
	for _, h := range interval {
		require.NoError(t, msg.AddBlockHeader(h))
	}

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	require.True(t, syncPeer.Connected(), "peer must stay connected on a matching checkpoint")

	// C2: the live header must NOT have been dropped; the stale anchor must be gone.
	var liveFound, anchorFound bool
	for e := sm.headerList.Front(); e != nil; e = e.Next() {
		n := e.Value.(*headerNode)
		if n.hash.IsEqual(&liveHash) {
			liveFound = true
		}
		if n.hash.IsEqual(&anchorHash) {
			anchorFound = true
		}
	}

	require.True(t, liveFound, "live earlier-interval header must survive the checkpoint boundary (not dropped by a blind Front() removal)")
	require.False(t, anchorFound, "the stale anchor node must have been removed by identity")
}

// TestHandleHeadersMsg_NoPanicPastFinalCheckpoint is the deterministic C3 test.
// After the final checkpoint headerCheckpoint is nil but headersFirstMode is
// still on (it is cleared later by the block handler). A headers message
// arriving in that window must be ignored, not dereference a nil cursor.
func TestHandleHeadersMsg_NoPanicPastFinalCheckpoint(t *testing.T) {
	base := chainhash.Hash{0x33}
	interval, _ := buildHeaderChain(base, 3, 9000)

	chainParams := chaincfg.MainNetParams
	// One checkpoint far above the delivered headers so the loop would reach
	// the nil deref site if unguarded.
	chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 100, Hash: &chainhash.Hash{0x44}}}

	syncPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {},
			OnGetData:    func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, syncPeer, err := MakeConnectedPeers(t, syncPeerCfg, syncPeerCfg, 4)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)

	// Simulate the post-final-checkpoint window: cursor is nil but headers-first
	// mode is still on.
	sm.nextCheckpoint = nil
	sm.headerCheckpoint = nil
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})

	msg := wire.NewMsgHeaders()
	for _, h := range interval {
		require.NoError(t, msg.AddBlockHeader(h))
	}

	// Must not panic (the fix returns early on a nil cursor). Without the guard
	// this dereferences sm.headerCheckpoint.Height and panics.
	require.NotPanics(t, func() {
		sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})
	})

	require.True(t, syncPeer.Connected(), "a headers message past the final checkpoint must be ignored, not fatal")
}

// TestHandleBlockMsg_CheckpointRecognizedByHashAfterPipelineRanAhead is the
// C-NEW regression test. It interleaves REAL block processing (handleBlockMsg ->
// handleBlockPreamble -> HandleBlockDirect) with header pipelining, in the exact
// order the pipeline produces in production: interval N+1's headers are fully
// processed FIRST — which removes checkpoint-N's anchor node from the header
// list by identity — and only THEN is checkpoint-N's block processed.
//
// On acafe3725 (position-based recognition) this wedges: once cp1's node is
// gone from the list, handleBlockPreamble's Front() no longer matches cp1's
// hash, isCheckpointBlock stays false, and nextCheckpoint never advances past
// cp1 — so headersFirstMode is never cleared and the node stays in headers-first
// mode forever. That is the RED failure this test asserts against.
//
// After the fix (recognition by hash against nextCheckpoint) cp1 is recognised
// even though its node was already removed, nextCheckpoint advances to cp2, and
// once cp2's block is processed (the final checkpoint) headersFirstMode is
// cleared and the node transitions to normal mode. That is the GREEN behaviour.
//
// HandleBlockDirect is exercised for real; GetBlockExists returns true so it
// takes its clean early-return path (block already in our chain) instead of the
// heavyweight validate/store stack — the recognition + advance logic under test
// runs identically either way.
func TestHandleBlockMsg_CheckpointRecognizedByHashAfterPipelineRanAhead(t *testing.T) {
	base := chainhash.Hash{0xf0, 0x0d}

	// Interval 1: base -> h1..h4 -> cp1 (height 5).
	interval1, hashes1 := buildHeaderChain(base, 5, 1000)
	cp1Height := int32(5)
	cp1Hash := hashes1[len(hashes1)-1]

	// Interval 2: cp1 -> h6..h9 -> cp2 (height 10).
	interval2, hashes2 := buildHeaderChain(cp1Hash, 5, 2000)
	cp2Height := int32(10)
	cp2Hash := hashes2[len(hashes2)-1]

	chainParams := chaincfg.MainNetParams
	chainParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: cp1Height, Hash: &cp1Hash},
		{Height: cp2Height, Hash: &cp2Hash},
	}

	syncPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetHeaders: func(_ *peer.Peer, _ *wire.MsgGetHeaders) {},
			OnGetData:    func(_ *peer.Peer, _ *wire.MsgGetData) {},
		},
		UserAgentName:    "netsynctest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}
	_, syncPeer, err := MakeConnectedPeers(t, syncPeerCfg, syncPeerCfg, 5)
	require.NoError(t, err)
	defer syncPeer.DisconnectWithInfo("test done")

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS

	// Best block sits below the final checkpoint, so current() is false and the
	// FSM RUN branch in handleBlockMsg is skipped — keeps the test focused on
	// checkpoint recognition / nextCheckpoint advance.
	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil).Maybe()
	// HandleBlockDirect: block already exists -> clean early nil return.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{bestHeader.Hash()}, nil).Maybe()

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	defer state.requestedTxns.Stop()
	defer state.requestedBlocks.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		chainParams:      &chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(20),
	}
	defer sm.requestedBlocks.Stop()

	sm.peerStates.Set(syncPeer, state)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	sm.headersFirstMode.Store(true)
	sm.nextCheckpoint = &chainParams.Checkpoints[0]
	sm.headerCheckpoint = &chainParams.Checkpoints[0]
	sm.headerList.PushBack(&headerNode{height: 0, hash: &base})

	// processBlock feeds a header through the REAL block path. requestedBlocks is
	// seeded first so the preamble's unrequested-block guard passes.
	processBlock := func(h *wire.BlockHeader, height int32) error {
		msgBlock := wire.NewMsgBlock(h)
		hash := msgBlock.Header.BlockHash()
		state.requestedBlocks.Set(hash, struct{}{})
		sm.requestedBlocks.Set(hash, struct{}{})

		return sm.handleBlockMsg(&blockQueueMsg{
			block:       msgBlock,
			blockHash:   hash,
			blockHeight: height,
			peer:        syncPeer,
		})
	}

	// Step 1: process interval-1 headers. This removes the base anchor, fetches
	// interval-1 blocks and pipelines interval-2's getheaders ahead.
	msg1 := wire.NewMsgHeaders()
	for _, h := range interval1 {
		require.NoError(t, msg1.AddBlockHeader(h))
	}
	sm.handleHeadersMsg(&headersMsg{headers: msg1, peer: syncPeer})
	require.True(t, sm.headersFirstMode.Load(), "headers-first mode must stay on after interval 1")
	require.True(t, syncPeer.Connected(), "peer must stay connected after interval 1")

	// Step 2: process interval-2 headers BEFORE cp1's block. This is the crux of
	// the pipeline: interval-2's anchor is cp1's node, so receivedCheckpoint
	// removes cp1's node from the header list by identity — while cp1's BLOCK has
	// not been processed yet.
	msg2 := wire.NewMsgHeaders()
	for _, h := range interval2 {
		require.NoError(t, msg2.AddBlockHeader(h))
	}
	sm.handleHeadersMsg(&headersMsg{headers: msg2, peer: syncPeer})
	require.True(t, syncPeer.Connected(), "peer must stay connected after interval 2")

	// Confirm the interleaving precondition: cp1's node is gone from the header
	// list even though cp1's block has not been processed. This is exactly what
	// broke position-based recognition.
	cp1StillInList := false
	for e := sm.headerList.Front(); e != nil; e = e.Next() {
		if e.Value.(*headerNode).hash.IsEqual(&cp1Hash) {
			cp1StillInList = true
		}
	}
	require.False(t, cp1StillInList, "precondition: cp1's anchor node must already be removed by the pipeline before cp1's block is processed")

	// Step 3: now process interval-1's blocks in order — the non-checkpoint ones
	// (h1..h4) then cp1's block. Real block processing, interleaved after the
	// headers ran ahead.
	for i := 0; i < 4; i++ {
		require.NoError(t, processBlock(interval1[i], int32(i+1)))
	}

	require.NoError(t, processBlock(interval1[4], cp1Height))

	// CORE ASSERTION (RED on acafe3725): nextCheckpoint must have advanced from
	// cp1 to cp2. On the old code it is stuck at cp1 because Front() no longer
	// matched cp1's hash.
	require.NotNil(t, sm.nextCheckpoint, "nextCheckpoint must not be nil after cp1's block")
	require.Equal(t, cp2Height, sm.nextCheckpoint.Height,
		"nextCheckpoint must advance to cp2 after cp1's block, recognised by hash despite its node being removed from the list")
	require.True(t, sm.headersFirstMode.Load(), "still headers-first: cp2 is not the final checkpoint block yet")

	// Step 4: process interval-2's blocks then cp2's block (the final checkpoint).
	for i := 0; i < 4; i++ {
		require.NoError(t, processBlock(interval2[i], int32(i+6)))
	}

	require.NoError(t, processBlock(interval2[4], cp2Height))

	// FINAL-CHECKPOINT TRANSITION: after the last checkpoint block,
	// findNextHeaderCheckpoint returns nil, so headers-first mode is cleared and
	// the node switches to normal mode.
	require.False(t, sm.headersFirstMode.Load(),
		"final-checkpoint block must clear headers-first mode and switch to normal mode")
	require.True(t, syncPeer.Connected(), "peer must survive the whole interleaved run")
}
