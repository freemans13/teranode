package subtreeprocessor

// End-to-end IBD fast-path tests that exercise the REAL blockchain gRPC stack.
//
// The sibling ibd_fast_path_test.go uses blockchain.Mock, which serves
// BlockHeaderMeta straight from an in-memory struct and therefore CANNOT catch a
// gRPC transport gap: if the QuickValidated bit were dropped by the AddBlock proto
// on write or the GetBlockHeader proto on read, the Mock-based tests would still
// pass. These tests close that hole. They stand up a genuine *blockchain.Blockchain
// server backed by a sqlitememory blockchain store, reach it through a real
// *blockchain.Client over a TCP listener, commit a below-checkpoint block via
// AddBlock(WithMinedSet, WithQuickValidated, WithID), wire that Client into a
// SubtreeProcessor, and call moveForwardBlock. The block header that the fast-path
// gate reads therefore travels through the gRPC serializer/deserializer, so a
// transport regression on either the write or the read leg fails the positive test.
//
// Discriminator (same as the Mock tests): the block carries a fake subtree hash
// that does not exist in any subtree store. If the FULL path runs, processBlockSubtrees
// / createTransactionMapIfNeeded fetch that hash, the store errors, and moveForwardBlock
// propagates the error. If the FAST path fires, no subtree read happens and
// moveForwardBlock returns nil,nil,nil after only resetSubtreeState + processCoinbaseUtxos.
//
// Test index:
//   1. TestIBDFastPath_GRPC_QuickValidatedFires   — positive: WithQuickValidated(true)
//      committed over gRPC → fast-path fires end-to-end. Proves the transport fix
//      (Tasks 1+2) actually carries the bit; would FAIL if the bit were dropped on the
//      gRPC hop.
//   2. TestIBDFastPath_GRPC_NotQuickValidated_FullPath — negative: WithQuickValidated(false)
//      committed over gRPC → fast-path does NOT fire (full path taken, subtree read errors),
//      and the Task-4 fall-through log fires naming quickValidated=false.

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	chaincfg "github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// captureLogger wraps a delegate Logger and records every Infof-formatted line into
// a shared, mutex-guarded slice so tests can assert which log messages fired. New and
// Duplicate return loggers that share the same capture buffer, so a SubtreeProcessor
// that re-derives its logger still records into the same place.
type captureLogger struct {
	ulogger.Logger
	mu    *sync.Mutex
	lines *[]string
}

func newCaptureLogger(delegate ulogger.Logger) *captureLogger {
	var (
		mu    sync.Mutex
		lines []string
	)

	return &captureLogger{Logger: delegate, mu: &mu, lines: &lines}
}

func (c *captureLogger) Infof(format string, args ...interface{}) {
	c.mu.Lock()
	*c.lines = append(*c.lines, fmt.Sprintf(format, args...))
	c.mu.Unlock()
	c.Logger.Infof(format, args...)
}

func (c *captureLogger) New(_ string, _ ...ulogger.Option) ulogger.Logger {
	return c
}

func (c *captureLogger) Duplicate(_ ...ulogger.Option) ulogger.Logger {
	return c
}

// contains reports whether any captured Infof line contains substr.
func (c *captureLogger) contains(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, l := range *c.lines {
		if strings.Contains(l, substr) {
			return true
		}
	}

	return false
}

// find returns the first captured Infof line containing substr, or "".
func (c *captureLogger) find(substr string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, l := range *c.lines {
		if strings.Contains(l, substr) {
			return l
		}
	}

	return ""
}

// ibdGRPCFixture bundles the real gRPC stack plus the SubtreeProcessor under test.
type ibdGRPCFixture struct {
	stp    *SubtreeProcessor
	client blockchain.ClientI
	logger *captureLogger
	// genesisHeader is the header of the auto-seeded genesis block; the test block
	// chains onto it so both StoreBlock (needs the parent) and moveForwardBlock
	// (needs HashPrevBlock == currentBlockHeader) are satisfied.
	genesisHeader *model.BlockHeader
}

// buildIBDGRPCFixture stands up a real blockchain gRPC server+client backed by
// sqlitememory and wires the client into a SubtreeProcessor. A single checkpoint is
// installed at ibdTestCheckpointHeight so a height-1 block is below checkpoint.
func buildIBDGRPCFixture(t *testing.T) *ibdGRPCFixture {
	t.Helper()

	ctx := context.Background()
	base := ulogger.NewErrorTestLogger(t)
	capLogger := newCaptureLogger(base)

	tSettings := test.CreateBaseTestSettings(t)

	// Regtest params carry no checkpoints; install one so height-1 is below checkpoint.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: ibdTestCheckpointHeight}}
	tSettings.ChainCfgParams = &params
	tSettings.BlockChain.GRPCListenAddress = ""
	tSettings.BlockChain.HTTPListenAddress = ""

	// Blockchain store (sqlitememory) — auto-seeds the genesis block on Init.
	bcStoreURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	bcStore, err := blockchainsql.New(base, bcStoreURL, tSettings)
	require.NoError(t, err)

	// Real blockchain server + Init (seeds genesis).
	server, err := blockchain.New(ctx, base, tSettings, bcStore, nil)
	require.NoError(t, err)
	require.NoError(t, server.Init(ctx))

	// Real gRPC server on an OS-assigned port.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(grpcServer.Stop)

	tSettings.BlockChain.GRPCAddress = lis.Addr().String()
	tSettings.BlockChain.MaxRetries = 1
	tSettings.BlockChain.RetrySleep = 10

	clientCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)

	client, err := blockchain.NewClientWithAddress(clientCtx, base, tSettings, lis.Addr().String(), "ibd-fast-path-e2e")
	require.NoError(t, err)

	// Genesis header, derived from the same chain params the store seeded from.
	genesisBlock, err := model.NewBlockFromMsgBlock(params.GenesisBlock, tSettings)
	require.NoError(t, err)

	// UTXO store for the SubtreeProcessor's coinbase write.
	utxoURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, base, tSettings, utxoURL)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(1))

	subtreeStore := blob_memory.New()

	stp, err := NewSubtreeProcessor(ctx, capLogger, tSettings, subtreeStore, client, utxoStore, nil)
	require.NoError(t, err)

	return &ibdGRPCFixture{
		stp:           stp,
		client:        client,
		logger:        capLogger,
		genesisHeader: genesisBlock.Header,
	}
}

// ibdGRPCBlock builds a below-checkpoint block that chains onto genesis, carrying the
// supplied (fake) subtree hash and the shared coinbaseTx fixture. Height 1 keeps it
// below ibdTestCheckpointHeight.
func ibdGRPCBlock(genesisHeader *model.BlockHeader, subtreeHash *chainhash.Hash) *model.Block {
	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  genesisHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          42,
		},
		Height:           1,
		Subtrees:         []*chainhash.Hash{subtreeHash},
		CoinbaseTx:       coinbaseTx, // package-level fixture from SubtreeProcessor_test.go
		TransactionCount: 2,
		ID:               1,
	}
}

// TestIBDFastPath_GRPC_QuickValidatedFires is the positive end-to-end test. A
// below-checkpoint block is committed over the real gRPC stack with
// WithMinedSet(true)+WithQuickValidated(true)+WithID(1). Block Assembly's
// moveForwardBlock reads the block header back over the same gRPC stack; because both
// bits survived the transport, the fast-path fires: no subtree store read (the fake
// subtree hash never errors), coinbase UTXO written, currentSubtree reset.
//
// This test would FAIL if Tasks 1+2 were incomplete (QuickValidated dropped on the
// AddBlock write leg or the GetBlockHeader read leg) — the gate would see
// QuickValidated=false and take the full path, and the fake subtree read would error.
func TestIBDFastPath_GRPC_QuickValidatedFires(t *testing.T) {
	f := buildIBDGRPCFixture(t)

	f.stp.InitCurrentBlockHeader(f.genesisHeader)

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-grpc-quick-validated"))
	block := ibdGRPCBlock(f.genesisHeader, &fakeSubtreeHash)

	// Commit the block over the REAL gRPC stack with both bits set.
	err := f.client.AddBlock(context.Background(), block, "",
		blockchainoptions.WithMinedSet(true),
		blockchainoptions.WithQuickValidated(true),
		blockchainoptions.WithID(uint64(block.ID)),
	)
	require.NoError(t, err, "AddBlock over gRPC must succeed")

	txMap, losingMap, err := f.stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true, nil,
	)

	require.NoError(t, err, "fast-path must fire end-to-end: QuickValidated+MinedSet survived gRPC, so no subtree read/full-path error")
	require.Nil(t, txMap, "fast-path returns nil transactionMap")
	require.Nil(t, losingMap, "fast-path returns nil losingTxHashesMap")

	// (a) the fast-path log fired.
	require.True(t, f.logger.contains("IBD fast-path: empty mempool + MinedSet + QuickValidated"),
		"the [moveForwardBlock] IBD fast-path log must have fired")

	// (b) the full-path fall-through log did NOT fire.
	require.False(t, f.logger.contains("IBD fast-path NOT taken"),
		"the full-path fall-through log must NOT fire when the fast-path is taken")

	// (c) coinbase UTXO processing DID run.
	cbHash := coinbaseTx.TxIDChainHash()
	_, utxoErr := f.stp.utxoStore.Get(context.Background(), cbHash)
	require.NoError(t, utxoErr, "processCoinbaseUtxos must have run: coinbase UTXO must exist")

	// resetSubtreeState installed the coinbase placeholder.
	require.Equal(t, 1, f.stp.currentSubtree.Load().Length(),
		"resetSubtreeState must have run: currentSubtree holds the coinbase placeholder")
}

// TestIBDFastPath_GRPC_NotQuickValidated_FullPath is the negative end-to-end test. The
// same below-checkpoint block is committed over gRPC with WithQuickValidated(false)
// (MinedSet still true). The gate reads QuickValidated=false over gRPC and must take
// the FULL path — the fake subtree read then errors. The Task-4 fall-through log must
// fire naming quickValidated=false (and minedSet=true, belowCheckpoint=true,
// emptyMaps=true), so an operator can see exactly why the fast-path was skipped.
func TestIBDFastPath_GRPC_NotQuickValidated_FullPath(t *testing.T) {
	f := buildIBDGRPCFixture(t)

	f.stp.InitCurrentBlockHeader(f.genesisHeader)

	require.Equal(t, 0, f.stp.currentTxMap.Length(), "precondition: mempool empty")
	require.Equal(t, int64(0), f.stp.queue.length(), "precondition: queue empty")
	require.Zero(t, f.stp.removeMap.Length(), "precondition: removeMap empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-grpc-not-quick-validated"))
	block := ibdGRPCBlock(f.genesisHeader, &fakeSubtreeHash)

	err := f.client.AddBlock(context.Background(), block, "",
		blockchainoptions.WithMinedSet(true),
		blockchainoptions.WithQuickValidated(false),
		blockchainoptions.WithID(uint64(block.ID)),
	)
	require.NoError(t, err, "AddBlock over gRPC must succeed")

	_, _, err = f.stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true, nil,
	)

	require.Error(t, err, "full path must run when QuickValidated=false: the fake subtree read must error")

	// The fast-path log must NOT have fired.
	require.False(t, f.logger.contains("IBD fast-path: empty mempool + MinedSet + QuickValidated"),
		"fast-path log must not fire when QuickValidated=false")

	// The Task-4 fall-through log must fire with the correct field values.
	line := f.logger.find("IBD fast-path NOT taken")
	require.NotEmpty(t, line, "the fall-through observability log must fire when the gate does not engage")
	require.Contains(t, line, "emptyMaps=true", "empty-maps precondition held")
	require.Contains(t, line, "belowCheckpoint=true", "below-checkpoint precondition held")
	require.Contains(t, line, "minedSet=true", "MinedSet was true")
	require.Contains(t, line, "quickValidated=false", "the failing precondition: QuickValidated=false")
}
