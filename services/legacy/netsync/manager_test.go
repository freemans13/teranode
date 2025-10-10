// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
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
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
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

	validatorClient, err := validator.New(context.Background(), ulogger.TestLogger{}, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
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
