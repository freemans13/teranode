package subtreeprocessor

// Tests for the IBD fast-path in moveForwardBlock.
//
// The fast-path fires when ALL of these hold:
//   (A) currentTxMap.Length()==0  (mempool empty)
//   (B) queue.length()==0         (pending-tx queue empty)
//   (C) removeMap.Length()==0     (no stale removal entries)
//   (D) block.Height is inside the below-checkpoint range
//       (model.BelowCheckpoint(params.Checkpoints, block.Height))
//   (E) GetBlockHeader returns MinedSet=true
//
// When the fast-path fires it skips createTransactionMapIfNeeded,
// processConflictingTransactions, and processRemainderTransactionsAndDequeue;
// it does only resetSubtreeState + processCoinbaseUtxos and returns nil,nil,nil
// (mirroring the existing empty-block branch).
//
// Discriminator: every test that exercises the full path passes a fake subtree
// hash that does not exist in the subtree store.  If the full path runs,
// CreateTransactionMap fetches that hash, the store returns an error, and
// moveForwardBlock propagates it.  If the fast-path fires, no store read occurs
// and moveForwardBlock returns nil,nil,nil.
//
// Test index:
//  1. TestIBDFastPath_EmptyMempoolMinedSet                        — fast-path fires (all conditions met)
//  2. TestIBDFastPath_FullPath_NonEmptyMempool                    — mempool non-empty → full path
//  3. TestIBDFastPath_FullPath_NotMinedSet                        — MinedSet=false → full path
//  4. TestIBDFastPath_FullPath_AboveCheckpoint                    — block above checkpoint → full path (RED before checkpoint gate)
//  5. TestIBDFastPath_FullPath_ReorgPopulatedMempool              — post-moveBack mempool → full path (reorg guard)
//  6. TestIBDFastPath_QuickValidated_FiresFastPath                — QuickValidated=true → fast-path fires (the positive case for the new gate)
//  7. TestIBDFastPath_FullPath_FullValidatedBelowCheckpoint       — QuickValidated=false + MinedSet=true + below-checkpoint → full path
//                                                                    (the regression: old BelowCheckpoint gate would fire; new gate must not)

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	chaincfg "github.com/bsv-blockchain/go-chaincfg"
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

const (
	// ibdTestCheckpointHeight is the checkpoint installed in the settings for
	// fast-path tests.  Blocks at or below this height are "below checkpoint"
	// and eligible for the fast-path when the other conditions also hold.
	ibdTestCheckpointHeight = int32(1000)

	// ibdTestBlockHeight is a block height that is below ibdTestCheckpointHeight.
	ibdTestBlockHeight = uint32(500)

	// ibdTestBlockHeightAboveCP is a block height above ibdTestCheckpointHeight,
	// used to verify the fast-path does NOT fire for above-checkpoint blocks.
	ibdTestBlockHeightAboveCP = uint32(1001)
)

// buildIBDFastPathSTP creates a minimal SubtreeProcessor wired with a real
// sqlite-memory UTXO store and a blockchain.Mock.  Settings are configured
// with a single checkpoint at ibdTestCheckpointHeight so that blocks at
// ibdTestBlockHeight satisfy model.BelowCheckpoint.
//
// The processor is NOT started (Start is not called); moveForwardBlock is
// called directly from the test goroutine to avoid channel complexity.
func buildIBDFastPathSTP(t *testing.T) (*SubtreeProcessor, *blockchain.Mock) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Install a real checkpoint so BelowCheckpoint works.
	// RegressionNetParams has Checkpoints=nil; copy and add one.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: ibdTestCheckpointHeight}}
	settings.ChainCfgParams = &params

	u, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, u)
	require.NoError(t, err)

	// SetBlockHeight so processCoinbaseUtxos can resolve height when
	// block.Height == 0 (the UTXO store height is the fallback).
	require.NoError(t, utxoStore.SetBlockHeight(1))

	subtreeStore := blob_memory.New()
	bcMock := &blockchain.Mock{}

	// SetBlockProcessedAt is called by finalizeBlockProcessing (caller-driven);
	// register a catch-all so the mock does not panic on unexpected calls.
	bcMock.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, err := NewSubtreeProcessor(ctx, logger, settings, subtreeStore, bcMock, utxoStore, nil)
	require.NoError(t, err)

	return stp, bcMock
}

// ibdBlock builds a *model.Block whose HashPrevBlock matches prevHeader at the
// given height, with the supplied subtree hashes and the shared coinbaseTx fixture.
func ibdBlock(prevHeader *model.BlockHeader, height uint32, subtrees []*chainhash.Hash) *model.Block {
	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{},
			Nonce:          42,
		},
		Height:     height,
		Subtrees:   subtrees,
		CoinbaseTx: coinbaseTx, // package-level fixture from SubtreeProcessor_test.go
	}
}

// TestIBDFastPath_EmptyMempoolMinedSet is the primary fast-path test.
//
// All conditions hold: empty mempool, empty queue, empty removeMap,
// block below checkpoint, MinedSet=true, QuickValidated=true.  The block
// carries a fake subtree hash not in the store; the fast-path skips the
// store read entirely.
//
// RED (before implementation): full path runs, CreateTransactionMap errors.
// GREEN (after implementation): fast-path fires, nil,nil,nil returned.
func TestIBDFastPath_EmptyMempoolMinedSet(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-for-ibd-fast-path-test"))

	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}, nil)

	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	txMap, losingMap, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	require.NoError(t, err, "IBD fast-path must not error on QuickValidated+MinedSet block with empty mempool below checkpoint")
	require.Nil(t, txMap, "IBD fast-path must return nil transactionMap")
	require.Nil(t, losingMap, "IBD fast-path must return nil losingTxHashesMap")

	// GetBlockHeader called exactly once — the only blockchain RPC on the fast-path.
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)

	// Coinbase UTXO must have been written by processCoinbaseUtxos.
	cbHash := coinbaseTx.TxIDChainHash()
	_, utxoErr := stp.utxoStore.Get(context.Background(), cbHash)
	require.NoError(t, utxoErr, "processCoinbaseUtxos must have been called: coinbase UTXO must exist in store")

	// currentSubtree must have been reset (coinbase placeholder installed by resetSubtreeState).
	require.Equal(t, 1, stp.currentSubtree.Load().Length(),
		"resetSubtreeState must have been called: currentSubtree must hold the coinbase placeholder")
}

// TestIBDFastPath_FullPath_NonEmptyMempool verifies condition A: a non-empty
// mempool forces the full path even when MinedSet=true and below checkpoint.
//
// GetBlockHeader must NOT be called (the mempool check short-circuits before
// reaching the RPC).
func TestIBDFastPath_FullPath_NonEmptyMempool(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	txHash := chainhash.HashH([]byte("mempool-tx-for-full-path-test"))
	stp.currentTxMap.SetIfNotExists(txHash, &subtreepkg.TxInpoints{})
	require.Equal(t, 1, stp.currentTxMap.Length(), "precondition: mempool must be non-empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-full-path-mempool-test"))
	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	require.Error(t, err, "full path must error when the fake subtree hash is not in the store")
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}

// TestIBDFastPath_FullPath_NotMinedSet verifies condition E: MinedSet=false
// forces the full path even when the mempool is empty and block is below
// checkpoint.  GetBlockHeader IS reached (we get to the check) but returns
// false, routing to the full path.
func TestIBDFastPath_FullPath_NotMinedSet(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	require.Equal(t, 0, stp.currentTxMap.Length(), "precondition: mempool must be empty")
	require.Equal(t, int64(0), stp.queue.length(), "precondition: queue must be empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-not-mined-set"))

	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: false}, nil)

	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	require.Error(t, err, "full path must error when MinedSet=false and fake subtree not in store")
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)
}

// TestIBDFastPath_FullPath_AboveCheckpoint verifies condition D: a block whose
// Height exceeds the highest checkpoint must NEVER take the fast-path, even
// when the mempool is empty and MinedSet=true.  Above-checkpoint blocks can
// carry conflicting subtree nodes; skipping processConflictingTransactions
// would silently lose that resolution.
//
// RED (before checkpoint gate): fast-path fires, no error → test FAILS because
// we require an error.
// GREEN (after checkpoint gate): full path runs, fake-subtree read errors.
func TestIBDFastPath_FullPath_AboveCheckpoint(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	require.Equal(t, 0, stp.currentTxMap.Length(), "precondition: mempool must be empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-above-checkpoint"))

	// GetBlockHeader returns MinedSet=true — all other conditions would pass,
	// only the above-checkpoint height should block the fast-path.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true}, nil)

	// ibdTestBlockHeightAboveCP (1001) > ibdTestCheckpointHeight (1000).
	block := ibdBlock(prevBlockHeader, ibdTestBlockHeightAboveCP, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	// Full path must run → fake-subtree read errors.
	require.Error(t, err, "full path must run for above-checkpoint block even with empty mempool and MinedSet=true")
}

// TestIBDFastPath_FullPath_ReorgPopulatedMempool verifies the reorg guard:
// after moveBackBlock repopulates currentTxMap with transactions from the
// demoted block, the emptiness check (condition A) must fail and the full path
// must run for each subsequent moveForwardBlock call.
//
// Rather than executing a full moveBackBlock (which requires stored subtrees
// and coinbase UTXOs), we replicate its net effect: directly populate
// currentTxMap with transaction hashes, as moveBackBlock does via addNode →
// currentTxMap.SetIfNotExists.  The test then asserts that moveForwardBlock
// takes the full path (errors on the fake subtree), locking in the guarantee
// that a future refactor of addNode/moveBackBlock cannot accidentally empty the
// map before moveForwardBlock sees it.
func TestIBDFastPath_FullPath_ReorgPopulatedMempool(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	// Simulate post-moveBack state: currentTxMap has the transactions from the
	// demoted block, placed there by addNode during moveBackBlock.
	for i := range 3 {
		h := chainhash.HashH([]byte{byte(i), 'r', 'e', 'o', 'r', 'g'})
		stp.currentTxMap.SetIfNotExists(h, &subtreepkg.TxInpoints{})
	}

	require.Equal(t, 3, stp.currentTxMap.Length(), "precondition: currentTxMap must be non-empty (simulating post-moveBack state)")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-reorg-test"))
	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	// Full path ran → fake-subtree read errored; the fast-path did not fire.
	require.Error(t, err, "full path must run when currentTxMap is non-empty (reorg scenario)")
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}

// TestIBDFastPath_QuickValidated_FiresFastPath verifies that the fast-path fires
// when QuickValidated=true (and MinedSet=true, empty mempool, below checkpoint).
// This is the positive case for the new QuickValidated gate.
//
// The discriminator: fake subtree not in store; fast-path skips the store read;
// no error.
//
// RED (before implementation): field does not exist, compile failure.
// GREEN (after implementation): fast-path fires, nil,nil,nil returned.
func TestIBDFastPath_QuickValidated_FiresFastPath(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-quick-validated"))

	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}, nil)

	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	txMap, losingMap, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	require.NoError(t, err, "IBD fast-path must not error when QuickValidated=true + MinedSet=true + empty mempool below checkpoint")
	require.Nil(t, txMap, "IBD fast-path must return nil transactionMap")
	require.Nil(t, losingMap, "IBD fast-path must return nil losingTxHashesMap")
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)

	// processCoinbaseUtxos must have run.
	cbHash := coinbaseTx.TxIDChainHash()
	_, utxoErr := stp.utxoStore.Get(context.Background(), cbHash)
	require.NoError(t, utxoErr, "processCoinbaseUtxos must have been called: coinbase UTXO must exist in store")
}

// TestIBDFastPath_FullPath_FullValidatedBelowCheckpoint is the critical regression
// test that proves the fix.
//
// A block that is:
//   - below checkpoint (would pass the old BelowCheckpoint gate),
//   - MinedSet=true (would pass the old MinedSet check),
//   - empty mempool, empty queue, empty removeMap,
//
// but was FULLY validated (QuickValidated=false) MUST NOT take the fast-path.
// Full validation can write conflicting subtree nodes; skipping
// processConflictingTransactions would silently drop conflict resolution.
//
// RED (before the fix): BelowCheckpoint+MinedSet gate fires → fast-path → no error
// → test FAILS because we require an error.
// GREEN (after the fix): QuickValidated=false blocks the fast-path → full path runs
// → fake-subtree read errors.
func TestIBDFastPath_FullPath_FullValidatedBelowCheckpoint(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)

	stp.InitCurrentBlockHeader(prevBlockHeader)

	require.Equal(t, 0, stp.currentTxMap.Length(), "precondition: mempool must be empty")
	require.Equal(t, int64(0), stp.queue.length(), "precondition: queue must be empty")
	require.Zero(t, stp.removeMap.Length(), "precondition: removeMap must be empty")

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-full-validated-below-cp"))

	// MinedSet=true but QuickValidated=false: this block went through full validation.
	// Below checkpoint + MinedSet=true is the OLD (buggy) condition that would fire
	// the fast-path; QuickValidated=false is what the new gate must catch.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true, QuickValidated: false}, nil)

	// Block height is BELOW the checkpoint (ibdTestBlockHeight=500 < ibdTestCheckpointHeight=1000).
	block := ibdBlock(prevBlockHeader, ibdTestBlockHeight, []*chainhash.Hash{&fakeSubtreeHash})

	_, _, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true,
	)

	// Full path must run → fake-subtree read errors.
	require.Error(t, err, "full path must run for fully-validated below-checkpoint block even with empty mempool and MinedSet=true")
}
