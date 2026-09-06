package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests pin processBlockFound's half of the quick window: a block whose parent is still
// in flight is resolved from the window rather than diverted to catchup, admission happens
// before the block-assembly gate, and a window-route block whose parent is nowhere to be found
// comes back as a local fault instead of a silent nil.
//
// The blocks are coinbase-only (len(Subtrees) == 0). That is the branch early mainnet is made
// of, it needs no subtree files on disk, and it exercises everything this task changed:
// parent resolution, height settlement from an in-flight entry, admission order and the
// never-nil rule. The subtree-carrying branch is already covered by the one-wave and window
// tests either side of this seam.

// holdingBlockchainClient is the real blockchain client (LocalClient over a sqlitememory
// blockchain store) with two test affordances bolted on: it records every block that reaches
// AddBlock, and it can hold one block's AddBlock open. Holding is what keeps a block resident
// in the window while the next block is delivered, which is the whole situation under test.
type holdingBlockchainClient struct {
	blockchain.ClientI

	mu    sync.Mutex
	added []chainhash.Hash

	holdHash   *chainhash.Hash
	arrived    chan struct{}
	arrivedOne sync.Once
	release    chan struct{}
}

// hold arms the client to block inside AddBlock for hash. The returned channel is closed once
// AddBlock has been reached; release lets it through.
func (c *holdingBlockchainClient) hold(hash *chainhash.Hash) (arrived <-chan struct{}, release func()) {
	c.mu.Lock()
	c.holdHash = hash
	c.arrived = make(chan struct{})
	c.release = make(chan struct{})
	c.mu.Unlock()

	var once sync.Once

	return c.arrived, func() { once.Do(func() { close(c.release) }) }
}

func (c *holdingBlockchainClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	held := c.holdHash != nil && c.holdHash.IsEqual(block.Hash())
	arrived, release := c.arrived, c.release
	c.mu.Unlock()

	if held {
		c.arrivedOne.Do(func() { close(arrived) })
		<-release
	}

	if err := c.ClientI.AddBlock(ctx, block, peerID, opts...); err != nil {
		return err
	}

	c.mu.Lock()
	c.added = append(c.added, *block.Hash())
	c.mu.Unlock()

	return nil
}

// addedBlocks returns the hashes AddBlock stored, in the order it stored them.
func (c *holdingBlockchainClient) addedBlocks() []chainhash.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]chainhash.Hash(nil), c.added...)
}

// countOf returns how many times hash was stored.
func (c *holdingBlockchainClient) countOf(hash *chainhash.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, h := range c.added {
		if h.IsEqual(hash) {
			n++
		}
	}

	return n
}

// windowServer is a Server wired for the unified below-checkpoint route with a two-deep quick
// window: real sqlitememory blockchain and UTXO stores, no block-assembly client so the gate
// is skipped, and no p2p client so no peer is malicious.
type windowServer struct {
	s      *Server
	client *holdingBlockchainClient
	// genesis is the store's own genesis hash, the parent every test chain starts from.
	genesis chainhash.Hash
	bits    model.NBit
}

func newWindowServer(t *testing.T, name string) *windowServer {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// CreateBaseTestSettings hands back a private copy of the regtest params, so pinning a
	// checkpoint above the test heights here affects nothing else. model.BelowCheckpoint keys
	// off this list, and without it the outpoint-only gate (and so the unified route, and so
	// the window) never opens.
	genesisHash := tSettings.ChainCfgParams.GenesisHash
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1000, Hash: genesisHash}}

	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickWindowBlocks = 2
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	client := &holdingBlockchainClient{ClientI: localClient}

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	s := New(logger, tSettings, subtreeStore, txStore, utxoStore, nil, client, nil, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, logger, tSettings, client, subtreeStore, txStore, utxoStore, nil, nil)

	require.Equal(t, 2, s.blockValidation.quickWindow.Depth(), "the settings above must produce a two-deep window")
	require.True(t, s.blockValidation.quickWindow.Enabled())

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &windowServer{s: s, client: client, genesis: *genesisHash, bits: *bits}
}

// coinbaseOnlyBlock builds a block with a coinbase and no subtrees, anchored on prev. height
// is written into block.Height the way the legacy client writes the request height.
func coinbaseOnlyBlock(t *testing.T, ws *windowServer, prev chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("quick-window-server"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/quick-window-server/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, publicKey),
	)

	prevHash := prev

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Timestamp:      1_700_000_000 + height,
			Bits:           ws.bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
		Height:           height,
	}
}

// requireStored waits for hash to appear in the blockchain store.
func requireStored(t *testing.T, ws *windowServer, hash *chainhash.Hash) {
	t.Helper()

	require.Eventually(t, func() bool {
		exists, err := ws.s.blockchainClient.GetBlockExists(context.Background(), hash)

		return err == nil && exists
	}, 10*time.Second, 10*time.Millisecond, "block %s must be stored", hash.String())
}

// TestProcessBlockFound_ParentInFlightIsResolvedFromTheWindow is the case the window exists
// for: block 2 arrives while block 1 is still in flight, so its parent is in no store yet.
// processBlockFound must find block 1 in the window, take its height from that entry, admit
// block 2 behind it and run it through quick validation. It must NOT take the catch-up divert,
// which returns nil and would have legacy sync record the block as accepted while nothing
// ever stored it.
func TestProcessBlockFound_ParentInFlightIsResolvedFromTheWindow(t *testing.T) {
	ws := newWindowServer(t, "window_server_parent_in_flight")

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)
	block2 := coinbaseOnlyBlock(t, ws, *block1.Hash(), 2)

	// Hold block 1 inside its commit, which is exactly the state legacy sync delivers block 2
	// in: block 1 admitted, its store work done, its chain-store commit not yet run.
	arrived, release := ws.client.hold(block1.Hash())

	err1 := make(chan error, 1)

	go func() {
		err1 <- ws.s.processBlockFound(context.Background(), block1.Hash(), "peer-1", "legacy", block1)
	}()

	select {
	case <-arrived:
	case err := <-err1:
		t.Fatalf("block 1 returned before reaching its commit: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("block 1 never reached its commit")
	}

	require.NotNil(t, ws.s.blockValidation.quickWindow.Lookup(block1.Hash()), "block 1 must be in flight")

	exists, err := ws.s.blockchainClient.GetBlockExists(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.False(t, exists, "block 1 must not be stored yet, otherwise block 2's parent is resolvable without the window")

	err2 := make(chan error, 1)

	go func() {
		err2 <- ws.s.processBlockFound(context.Background(), block2.Hash(), "peer-1", "legacy", block2)
	}()

	// Block 2 must reach the window, not the catchup channel.
	require.Eventually(t, func() bool {
		return ws.s.blockValidation.quickWindow.Lookup(block2.Hash()) != nil
	}, 10*time.Second, 10*time.Millisecond, "block 2 must be admitted to the window behind its in-flight parent")

	release()

	require.NoError(t, <-err1)
	require.NoError(t, <-err2)

	requireStored(t, ws, block1.Hash())
	requireStored(t, ws, block2.Hash())

	require.Equal(t, uint32(1), block1.Height, "block 1's height comes from the stored genesis parent")
	require.Equal(t, uint32(2), block2.Height, "block 2's height comes from its in-flight parent's entry")

	require.Equal(t, []chainhash.Hash{*block1.Hash(), *block2.Hash()}, ws.client.addedBlocks(),
		"the chain store must see the two blocks in height order")
}

// TestProcessBlockFound_UnknownParentOnWindowRouteIsAServiceErrorNotNil covers the rule that
// makes the window safe to sit in front of legacy sync: on the window route a parent that is
// neither stored nor in flight is a local fault, returned as a transient local error. The
// catch-up divert must not be taken, because it returns nil and legacy sync would mark the
// block accepted without anything having applied it.
func TestProcessBlockFound_UnknownParentOnWindowRouteIsAServiceErrorNotNil(t *testing.T) {
	ws := newWindowServer(t, "window_server_unknown_parent")

	// Shrink the parent wait: the production ten seconds is for a stalled predecessor, and
	// there is nothing to wait for here.
	restore := quickWindowParentWait
	quickWindowParentWait = 250 * time.Millisecond

	t.Cleanup(func() { quickWindowParentWait = restore })

	orphan := coinbaseOnlyBlock(t, ws, chainhash.Hash{0x01, 0x02, 0x03}, 2)

	err := ws.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "legacy", orphan)
	require.Error(t, err, "a window-route block with an unknown parent must never come back nil")
	require.True(t, errors.IsTransientLocalError(err), "the failure is ours, not the peer's: %v", err)
	require.Contains(t, err.Error(), "neither stored nor in flight")

	require.Empty(t, ws.client.addedBlocks(), "nothing may be stored for a block whose parent is unknown")
}

// TestProcessBlockFound_DuplicateInFlightBlockReturnsTheLiveOutcome pins the dedup: legacy
// sync can deliver the same block twice, and the second delivery must wait for the live
// attempt and return its outcome rather than start a second one.
func TestProcessBlockFound_DuplicateInFlightBlockReturnsTheLiveOutcome(t *testing.T) {
	ws := newWindowServer(t, "window_server_duplicate")

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)

	// The second delivery gets its own block struct, the way it really arrives: every
	// ProcessBlock call parses the wire bytes into a fresh model.Block. Sharing one struct
	// between the two calls would be a test-only data race on block.Height.
	raw, err := block1.Bytes()
	require.NoError(t, err)

	redelivered, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err)
	require.True(t, block1.Hash().IsEqual(redelivered.Hash()), "both deliveries must be the same block")

	arrived, release := ws.client.hold(block1.Hash())

	first := make(chan error, 1)

	go func() {
		first <- ws.s.processBlockFound(context.Background(), block1.Hash(), "peer-1", "legacy", block1)
	}()

	select {
	case <-arrived:
	case err := <-first:
		t.Fatalf("the first delivery returned before reaching its commit: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the first delivery never reached its commit")
	}

	second := make(chan error, 1)

	go func() {
		second <- ws.s.processBlockFound(context.Background(), redelivered.Hash(), "peer-2", "legacy", redelivered)
	}()

	// Give the second delivery time to get as far as it is going to get before the first is
	// allowed to finish, so the duplicate really is concurrent with the live attempt.
	time.Sleep(200 * time.Millisecond)

	release()

	require.NoError(t, <-first)
	require.NoError(t, <-second, "the duplicate must return the live attempt's outcome")

	requireStored(t, ws, block1.Hash())
	require.Equal(t, 1, ws.client.countOf(block1.Hash()), "the block must be added to the chain store exactly once")
}
