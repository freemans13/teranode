package subtreeprocessor

// Tests for the IBD fast-path: when the mempool (currentTxMap) and the
// pending-tx queue are both empty, and the block has already been validated
// (MinedSet=true), moveForwardBlock should skip the subtree-data read and
// transaction-map build and go straight to coinbase-UTXO creation.
//
// The discriminator in all three tests is a fake subtree hash that does not
// exist in the subtree store.  If the fast-path fires, no store read occurs
// and moveForwardBlock returns nil,nil,nil.  If the full path runs,
// CreateTransactionMap tries to fetch the fake hash, the store returns an
// error, and moveForwardBlock propagates it.  That is the RED signal.
//
// Tests:
//   - TestIBDFastPath_EmptyMempoolMinedSet — fast-path fires, coinbase created.
//   - TestIBDFastPath_FullPath_NonEmptyMempool — mempool non-empty, full path.
//   - TestIBDFastPath_FullPath_NotMinedSet — empty mempool but MinedSet=false, full path.

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildIBDFastPathSTP creates a minimal SubtreeProcessor wired with a real
// sqlite-memory UTXO store and a blockchain.Mock.  The processor is NOT
// started (Start is not called), so internal state can be inspected and
// moveForwardBlock can be called directly from the test goroutine without
// channel contention.
func buildIBDFastPathSTP(t *testing.T) (*SubtreeProcessor, *blockchain.Mock) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	u, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, u)
	require.NoError(t, err)

	// SetBlockHeight so processCoinbaseUtxos can resolve height when
	// block.Height == 0 (which is the case for our minimal test blocks).
	require.NoError(t, utxoStore.SetBlockHeight(1))

	subtreeStore := blob_memory.New()
	bcMock := &blockchain.Mock{}

	// SetBlockProcessedAt is called by finalizeBlockProcessing, which the
	// caller of moveForwardBlock drives.  moveForwardBlock itself does not call
	// it, but some internal branches do (moveBackBlock etc.).  Register a
	// catch-all so the mock does not panic on unexpected calls.
	bcMock.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, err := NewSubtreeProcessor(ctx, logger, settings, subtreeStore, bcMock, utxoStore, nil)
	require.NoError(t, err)

	return stp, bcMock
}

// minimalBlock builds a *model.Block whose HashPrevBlock matches prevHeader,
// with the given subtree hashes and the shared coinbaseTx fixture.
func minimalBlock(prevHeader *model.BlockHeader, subtrees []*chainhash.Hash) *model.Block {
	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{},
			Nonce:          42,
		},
		Subtrees:   subtrees,
		CoinbaseTx: coinbaseTx, // package-level fixture from SubtreeProcessor_test.go
	}
}

// TestIBDFastPath_EmptyMempoolMinedSet is the primary fast-path test.
//
// Setup: empty mempool + empty queue + MinedSet=true.
// The block carries a fake subtree hash that is NOT in the subtree store.
// Before the implementation: the full path calls CreateTransactionMap which
// reads the store → error → test FAILS (RED).
// After the implementation: the fast-path fires, no store read, coinbase UTXO
// is created, nil,nil,nil is returned → test PASSES (GREEN).
func TestIBDFastPath_EmptyMempoolMinedSet(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	// Anchor the current block header so the parent check passes.
	stp.InitCurrentBlockHeader(prevBlockHeader)

	// A hash that is not in the (empty) subtree store.  If the full path runs
	// it will attempt to read this from the store and fail.
	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-for-ibd-fast-path-test"))

	// MinedSet=true: this is what the fast-path checks via GetBlockHeader.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(
			prevBlockHeader, // header (value not inspected by the fast-path)
			&model.BlockHeaderMeta{MinedSet: true},
			nil,
		)

	block := minimalBlock(prevBlockHeader, []*chainhash.Hash{&fakeSubtreeHash})

	txMap, losingMap, err := stp.moveForwardBlock(
		context.Background(),
		block,
		false,
		map[chainhash.Hash]struct{}{},
		false,
		true,
	)

	// Fast-path: no error, both maps nil (mirrors empty-block branch shape).
	require.NoError(t, err, "IBD fast-path must not error on MinedSet block with empty mempool")
	require.Nil(t, txMap, "IBD fast-path must return nil transactionMap")
	require.Nil(t, losingMap, "IBD fast-path must return nil losingTxHashesMap")

	// GetBlockHeader must have been called exactly once — that is the only
	// blockchain RPC the fast-path issues.
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)

	// Coinbase UTXO must have been created.
	cbHash := coinbaseTx.TxIDChainHash()
	_, utxoErr := stp.utxoStore.Get(context.Background(), cbHash)
	require.NoError(t, utxoErr, "processCoinbaseUtxos must have been called: coinbase UTXO must exist in store")

	// currentSubtree must have been reset (contains the coinbase placeholder
	// that resetSubtreeState adds).
	require.Equal(t, 1, stp.currentSubtree.Load().Length(),
		"resetSubtreeState must have been called: currentSubtree must hold the coinbase placeholder")
}

// TestIBDFastPath_FullPath_NonEmptyMempool verifies that a non-empty mempool
// forces the full path even when MinedSet=true.
//
// Setup: mempool has one entry + MinedSet=true.
// The full path tries to read the fake subtree → error → test must see that
// error (proving the full path ran).  GetBlockHeader must NOT be called
// (we don't even reach the lookup when the mempool check fails).
func TestIBDFastPath_FullPath_NonEmptyMempool(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	// Inject one entry into currentTxMap so the mempool is non-empty.
	txHash := chainhash.HashH([]byte("mempool-tx-for-full-path-test"))
	stp.currentTxMap.SetIfNotExists(txHash, &subtreepkg.TxInpoints{})

	require.Equal(t, 1, stp.currentTxMap.Length(), "precondition: mempool must be non-empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-full-path-test"))
	block := minimalBlock(prevBlockHeader, []*chainhash.Hash{&fakeSubtreeHash})

	// GetBlockHeader must NOT be registered or called — if it were called the
	// mock would panic (unexpected call), which would surface as a test failure.

	_, _, err := stp.moveForwardBlock(
		context.Background(),
		block,
		false,
		map[chainhash.Hash]struct{}{},
		false,
		true,
	)

	// Full path ran → CreateTransactionMap tried to read the fake subtree → error.
	require.Error(t, err, "full path must error when the fake subtree hash is not in the store")

	// GetBlockHeader was never called — the emptiness check short-circuited.
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}

// TestIBDFastPath_FullPath_NotMinedSet verifies that an empty mempool alone is
// not sufficient to trigger the fast-path: MinedSet=false must force the full
// path regardless of mempool state.
//
// Setup: empty mempool + MinedSet=false.
// The full path tries to read the fake subtree → error → test must see that
// error.  GetBlockHeader IS called (we reach the check) but the false value
// routes to the full path.
func TestIBDFastPath_FullPath_NotMinedSet(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	require.Equal(t, 0, stp.currentTxMap.Length(), "precondition: mempool must be empty")
	require.Equal(t, int64(0), stp.queue.length(), "precondition: queue must be empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-not-mined-set"))

	// MinedSet=false — full path must run.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(
			prevBlockHeader,
			&model.BlockHeaderMeta{MinedSet: false},
			nil,
		)

	block := minimalBlock(prevBlockHeader, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(),
		block,
		false,
		map[chainhash.Hash]struct{}{},
		false,
		true,
	)

	// Full path ran → CreateTransactionMap tried to read the fake subtree → error.
	require.Error(t, err, "full path must error when MinedSet=false and fake subtree not in store")

	// GetBlockHeader was called exactly once.
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)
}
