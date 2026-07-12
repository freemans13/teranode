//go:build testtxmetacache

package blockvalidation

// Window barrier-collapse OVERLAP tests (Task 3).
//
// These tests exercise the WindowBarrierCollapse=true path of ProcessBlockWindow,
// where block i's SPEND leg overlaps with the CREATE legs of blocks 0..i instead of
// waiting a hard C1->C2 barrier. The core safety property is the ASCENDING PREFIX
// WAIT: spend i may only run after creates 0..i have RETURNED (committed & visible),
// because below-checkpoint blocks carry cross-block-within-window tx chains and a
// spend resolves its same-window parents via JOIN txs on committed state. A missing
// parent surfaces as a non-retryable TxNotFound = HARD FAIL.
//
// Five tests:
//
//  1. SameWindowParentPrefixHolds — the load-bearing proof. Block i "spends" an output
//     whose creating tx belongs to an earlier block j<i in the SAME window (modelled in
//     the store). An instrumented store records create-return order and spend-start
//     order, and models JOIN-txs semantics: a spend whose same-window parent has not yet
//     finished creating returns TxNotFound. The test asserts every spend i starts only
//     AFTER creates 0..i returned, and no TxNotFound occurs. It FAILS if the wait is
//     weakened to "create i only".
//
//  2. CoinbaseOnlyMidWindowDoesNotStall — a coinbase-only (len(Subtrees)==0) block in
//     the middle of the window still closes its createDone so later spends proceed and
//     the whole window completes within a bounded deadline.
//
//  3. CreateErrorPoisonsNoCommit — a CREATE error aborts the window via g.Wait(); no
//     goroutine hangs (bounded deadline), no C3 commit runs (best-block not advanced),
//     the error is returned.
//
//  4. SpendErrorPoisonsNoCommit — same, but the failure is on a SPEND.
//
//  5. FlagOffByteIdentical — with the flag OFF the phased path yields the identical
//     committed outcome to the overlap path (flag ON) on the same chain.

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// TestTxNotFoundIsNotRetryable — a precondition assumption for the prefix test.
// If TxNotFound ever became retryable, a same-window parent race would be masked by
// a retry rather than surfacing as a hard fail — the prefix wait's whole reason for
// existing. Lock the assumption down.
// ---------------------------------------------------------------------------

func TestTxNotFoundIsNotRetryable(t *testing.T) {
	require.False(t, errors.IsRetryableError(errors.NewTxNotFoundError("parent missing")),
		"TxNotFound must be non-retryable so a missing same-window parent is a HARD FAIL, not a masked retry")
}

// ---------------------------------------------------------------------------
// prefixOrderStore — instrumented store for the prefix-wait proof.
//
// It models the ONE property that makes the ascending-prefix wait load-bearing: a
// spend resolves its parent against COMMITTED create state. The test declares, per
// block, the parent block whose tx must have been created before this block's spend
// may succeed. The store:
//
//   - On Create(tx): sleeps a per-block stagger so creates return in a controlled,
//     well-separated order, marks the tx created, and appends "create-return <i>".
//   - On Spend(tx): appends "spend-start <i>", then checks whether the declared
//     same-window parent tx was already created. If NOT, returns a non-retryable
//     TxNotFound (the exact hard-fail a premature spend would hit under JOIN-txs
//     semantics). Otherwise succeeds.
// ---------------------------------------------------------------------------

type prefixOrderStore struct {
	*nullstore.NullStore

	mu sync.Mutex

	blockIdxOf map[chainhash.Hash]int            // txHash -> owning block index
	parentOf   map[chainhash.Hash]chainhash.Hash // txHash -> same-window parent tx hash (zero = none)
	created    map[chainhash.Hash]bool           // txHash -> Create has returned

	events []string // ordered log: "create-return <i>", "spend-start <i>"

	createStagger time.Duration
	txNotFound    atomic.Bool // set if any spend saw a missing parent (prefix violation)
}

func newPrefixOrderStore(t *testing.T) *prefixOrderStore {
	t.Helper()
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	return &prefixOrderStore{
		NullStore:     ns,
		blockIdxOf:    make(map[chainhash.Hash]int),
		parentOf:      make(map[chainhash.Hash]chainhash.Hash),
		created:       make(map[chainhash.Hash]bool),
		createStagger: 8 * time.Millisecond,
	}
}

func (s *prefixOrderStore) SupportsOutpointOnlySpend() bool { return true }

func (s *prefixOrderStore) Create(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*utxometa.Data, error) {
	h := *tx.TxIDChainHash()

	s.mu.Lock()
	idx := s.blockIdxOf[h]
	n := len(s.blockIdxOf)
	s.mu.Unlock()

	// INVERSE stagger: EARLIER blocks create SLOWER (block 0 slowest, block k-1 fastest).
	// This is what gives the test its teeth. Block i's parent is the EARLIER block i-1,
	// which is now slower to return than block i's own create. So a weakened
	// "create i only" wait lets block i's spend fire while its parent (i-1) is still
	// creating → parentCreated=false → TxNotFound. The correct ascending-prefix wait
	// (0..i) forces the spend to also wait the slow parent, so it never trips.
	time.Sleep(time.Duration(n-idx) * s.createStagger)

	s.mu.Lock()
	s.created[h] = true
	s.events = append(s.events, fmt.Sprintf("create-return %d", idx))
	s.mu.Unlock()

	return nil, nil
}

func (s *prefixOrderStore) Spend(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	h := *tx.TxIDChainHash()

	s.mu.Lock()
	idx := s.blockIdxOf[h]
	parent := s.parentOf[h]
	s.events = append(s.events, fmt.Sprintf("spend-start %d", idx))
	hasParent := parent != (chainhash.Hash{})
	parentCreated := hasParent && s.created[parent]
	s.mu.Unlock()

	// JOIN-txs semantics: spending an output whose creating tx is not yet committed is
	// a hard, non-retryable TxNotFound — exactly what a prefix-violating spend hits.
	if hasParent && !parentCreated {
		s.txNotFound.Store(true)
		return nil, errors.NewTxNotFoundError("same-window parent %s not yet created for spender %s", parent.String(), h.String())
	}

	return nil, nil
}

func (s *prefixOrderStore) snapshotEvents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.events))
	copy(out, s.events)
	return out
}

// TestWindowOverlap_SameWindowParentPrefixHolds is the load-bearing proof of the
// ascending-prefix wait. A window of K single-tx blocks; the store declares block i's
// tx to have a same-window parent = block i-1's tx. Under a correct prefix wait, spend
// i waits creates 0..i (so block i-1's create has returned) → no TxNotFound, and every
// "spend-start i" appears after all "create-return 0..i" in the event log.
//
// It FAILS under a weakened "create i only" wait: block i's own create returns later
// than block i-1's, so a spend that only waits create i can still fire while the
// EARLIER parent (block i-1) create is in flight in some interleavings — but more
// decisively, the staggered creates mean block i's create returns AFTER block i-1's,
// so "create i only" does NOT guarantee the parent (i-1) has returned when a lower
// block's spend runs; the structural log assertion below (spend i after create-return
// 0..i) is violated the moment any spend runs before an earlier create returns.
func TestWindowOverlap_SameWindowParentPrefixHolds(t *testing.T) {
	const k = 4

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := newOverlapSettings(t, true /* flag on */)

	store := newPrefixOrderStore(t)
	blockchainClient := newLocalClient(t, logger, tSettings)
	bv := newOverlapBV(t, logger, tSettings, blockchainClient, store)

	blocks, txByBlock := buildOverlapBlocks(t, bv, ctx, bv.settings.ChainCfgParams.GenesisHash, 100, k)

	// Seed identity + same-window parent chain: block i's tx's parent is block i-1's tx.
	store.mu.Lock()
	for i := range k {
		h := *txByBlock[i].TxIDChainHash()
		store.blockIdxOf[h] = i
		if i > 0 {
			store.parentOf[h] = *txByBlock[i-1].TxIDChainHash()
		}
	}
	store.mu.Unlock()

	require.NoError(t, bv.ProcessBlockWindow(ctx, blocks, "prefix-peer"),
		"overlap window with same-window parent chain must complete without error")

	require.False(t, store.txNotFound.Load(),
		"PREFIX VIOLATION: a spend ran before its same-window parent committed (TxNotFound) — ascending-prefix wait not enforced")

	// Structural proof: "spend-start i" must appear only after "create-return 0..i".
	events := store.snapshotEvents()
	createReturnPos := make([]int, k)
	spendStartPos := make([]int, k)
	for i := range k {
		createReturnPos[i] = -1
		spendStartPos[i] = -1
	}
	for pos, e := range events {
		var kind string
		var idx int
		_, scanErr := fmt.Sscanf(e, "%s %d", &kind, &idx)
		require.NoError(t, scanErr, "unparseable event %q", e)
		switch kind {
		case "create-return":
			createReturnPos[idx] = pos
		case "spend-start":
			spendStartPos[idx] = pos
		}
	}

	for i := range k {
		require.GreaterOrEqual(t, spendStartPos[i], 0, "block %d never spent", i)
		for j := 0; j <= i; j++ {
			require.GreaterOrEqual(t, createReturnPos[j], 0, "block %d never created (needed by spend %d)", j, i)
			require.Greater(t, spendStartPos[i], createReturnPos[j],
				"PREFIX VIOLATION: spend-start %d (pos %d) ran before create-return %d (pos %d)",
				i, spendStartPos[i], j, createReturnPos[j])
		}
	}
}

// TestWindowOverlap_CoinbaseOnlyMidWindowDoesNotStall proves that a coinbase-only
// (len(Subtrees)==0) block in the middle of the window still closes its createDone
// channel, so later blocks' spend legs (which wait the full prefix including it)
// proceed and the window completes within a bounded deadline. A create leg that failed
// to close createDone for the coinbase-only block would hang the waiting spends until
// the call deadline fires → require.NoError fails on DeadlineExceeded.
func TestWindowOverlap_CoinbaseOnlyMidWindowDoesNotStall(t *testing.T) {
	bv, ctx, cancel := newProcessWindowHarness(t, "overlap_cbonly_midwindow")
	defer cancel()
	bv.settings.BlockValidation.WindowBarrierCollapse = true

	genesis := bv.settings.ChainCfgParams.GenesisHash

	callCtx, callCancel := context.WithTimeout(ctx, 15*time.Second)
	defer callCancel()

	// Block 0 (h=100): single subtree. Block 1 (h=101): coinbase-only, ZERO subtrees, in
	// the MIDDLE. Block 2 (h=102): single subtree — its spend leg waits the full prefix.
	block0 := buildOverlapSingleTxBlock(t, bv, callCtx, genesis, 100, 1, "/overlap-cb0/")
	block1 := buildCoinbaseOnlyBlock(t, block0.Hash(), 101, 2)
	block2 := buildOverlapSingleTxBlock(t, bv, callCtx, block1.Hash(), 102, 3, "/overlap-cb2/")

	require.Len(t, block1.Subtrees, 0, "block1 must be genuinely coinbase-only")

	blocks := []*model.Block{block0, block1, block2}
	require.NoError(t, bv.ProcessBlockWindow(callCtx, blocks, "cbonly-mid-peer"),
		"coinbase-only mid-window block must not stall later spends (createDone must close)")

	for i, blk := range blocks {
		exists, gerr := bv.blockchainClient.GetBlockExists(callCtx, blk.Hash())
		require.NoError(t, gerr)
		require.True(t, exists, "block %d (h=%d) must be committed", i, blk.Height)
	}
}

// legErrorStore fails Create or Spend for a designated tx hash and is otherwise a
// no-op NullStore. It exercises the overlap error-propagation path without a real
// UTXO backend.
type legErrorStore struct {
	*nullstore.NullStore
	failCreateFor chainhash.Hash
	failSpendFor  chainhash.Hash
}

func (s *legErrorStore) SupportsOutpointOnlySpend() bool { return true }

func (s *legErrorStore) Create(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*utxometa.Data, error) {
	if s.failCreateFor != (chainhash.Hash{}) && *tx.TxIDChainHash() == s.failCreateFor {
		return nil, errors.NewProcessingError("injected create failure for %s", tx.TxIDChainHash().String())
	}
	return nil, nil
}

func (s *legErrorStore) Spend(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if s.failSpendFor != (chainhash.Hash{}) && *tx.TxIDChainHash() == s.failSpendFor {
		return nil, errors.NewProcessingError("injected spend failure for %s", tx.TxIDChainHash().String())
	}
	return nil, nil
}

func TestWindowOverlap_CreateErrorPoisonsNoCommit(t *testing.T) {
	runOverlapPoisonTest(t, func(store *legErrorStore, txByBlock []*bt.Tx) {
		// Fail the create of the LAST block's tx so the error races with earlier spends.
		store.failCreateFor = *txByBlock[len(txByBlock)-1].TxIDChainHash()
	})
}

func TestWindowOverlap_SpendErrorPoisonsNoCommit(t *testing.T) {
	runOverlapPoisonTest(t, func(store *legErrorStore, txByBlock []*bt.Tx) {
		// Fail the spend of block 1's tx.
		store.failSpendFor = *txByBlock[1].TxIDChainHash()
	})
}

// runOverlapPoisonTest builds a K-block window on a legErrorStore, applies the caller's
// failure injection, runs ProcessBlockWindow (flag ON) under a BOUNDED deadline, and
// asserts: (a) an error is returned, (b) it is NOT a deadline (no goroutine hang),
// (c) C3 committed nothing (no AddBlock, best-block unadvanced).
func runOverlapPoisonTest(t *testing.T, inject func(*legErrorStore, []*bt.Tx)) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := newOverlapSettings(t, true)

	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	store := &legErrorStore{NullStore: ns}

	realClient := newLocalClient(t, logger, tSettings)
	var addBlockCalls atomic.Int64
	spyClient := &addBlockCountSpy{ClientI: realClient, counter: &addBlockCalls}

	bv := newOverlapBV(t, logger, tSettings, spyClient, store)

	const k = 3
	blocks, txByBlock := buildOverlapBlocks(t, bv, ctx, bv.settings.ChainCfgParams.GenesisHash, 100, k)

	inject(store, txByBlock)

	callCtx, callCancel := context.WithTimeout(ctx, 15*time.Second)
	defer callCancel()

	err = bv.ProcessBlockWindow(callCtx, blocks, "poison-peer")
	require.Error(t, err, "an errored create/spend must abort the window")
	require.False(t, errors.Is(err, context.DeadlineExceeded),
		"the window must fail with the injected error, not a deadline (a deadline means a goroutine hung)")
	require.Equal(t, int64(0), addBlockCalls.Load(), "no C3 commit may run when a create/spend errors")

	best, _, gerr := bv.blockchainClient.GetBestBlockHeader(callCtx)
	require.NoError(t, gerr)
	require.Equal(t, bv.settings.ChainCfgParams.GenesisHash.String(), best.Hash().String(),
		"best-block must not advance when the window is poisoned")
}

// TestWindowOverlap_FlagOffByteIdentical runs the SAME same-window chain through the
// phased path (flag OFF) and the overlap path (flag ON) and asserts an identical
// committed outcome: same blocks committed, same tip. The phased path is the existing
// unchanged code; this proves the new flag branch does not alter it.
func TestWindowOverlap_FlagOffByteIdentical(t *testing.T) {
	run := func(flagOn bool, suffix string) (*BlockValidation, context.Context, []*model.Block) {
		bv, ctx, cancel := newProcessWindowHarness(t, suffix)
		t.Cleanup(cancel)
		bv.settings.BlockValidation.WindowBarrierCollapse = flagOn

		chain := buildWindowChainData(t, bv.settings.ChainCfgParams.GenesisHash)
		writeWindowChainToStore(t, ctx, bv, chain)
		require.NoError(t, bv.ProcessBlockWindow(ctx, chain.blocks, "flagoff-peer"),
			"flagOn=%v window must complete", flagOn)
		return bv, ctx, chain.blocks
	}

	bvOff, ctxOff, blocksOff := run(false, "flagoff_off")
	bvOn, ctxOn, blocksOn := run(true, "flagoff_on")

	bestOff, _, err := bvOff.blockchainClient.GetBestBlockHeader(ctxOff)
	require.NoError(t, err)
	bestOn, _, err := bvOn.blockchainClient.GetBestBlockHeader(ctxOn)
	require.NoError(t, err)
	require.Equal(t, bestOff.Hash().String(), bestOn.Hash().String(),
		"flag ON and OFF must commit to the identical chain tip")

	for i := range blocksOff {
		exOff, e1 := bvOff.blockchainClient.GetBlockExists(ctxOff, blocksOff[i].Hash())
		require.NoError(t, e1)
		exOn, e2 := bvOn.blockchainClient.GetBlockExists(ctxOn, blocksOn[i].Hash())
		require.NoError(t, e2)
		require.True(t, exOff, "flag OFF: block %d must be committed", i)
		require.True(t, exOn, "flag ON: block %d must be committed", i)
		require.Equal(t, blocksOff[i].Hash().String(), blocksOn[i].Hash().String(),
			"both paths must build the identical block %d", i)
	}
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// newOverlapSettings builds test settings with the outpoint-only fast path engaged, a
// high checkpoint, and the WindowBarrierCollapse flag set as requested.
func newOverlapSettings(t *testing.T, flagOn bool) *settings.Settings {
	t.Helper()
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true
	tSettings.BlockValidation.WindowBarrierCollapse = flagOn

	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params
	return tSettings
}

// newLocalClient builds a real blockchain LocalClient backed by an in-memory store.
func newLocalClient(t *testing.T, logger ulogger.Logger, tSettings *settings.Settings) blockchain.ClientI {
	t.Helper()
	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)
	return blockchainClient
}

// newOverlapBV wires a BlockValidation with the given blockchain client and utxo store.
func newOverlapBV(t *testing.T, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store) *BlockValidation {
	t.Helper()
	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		utxoStore:                     utxoStore,
		subtreeStore:                  blobmemory.New(),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
	}
	t.Cleanup(func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
	})
	return bv
}

// buildOverlapBlocks builds k single-subtree, chained (by prevHash) blocks, each with
// one coinbase + one regular tx spending its own coinbase (self-contained — no real
// cross-block deps, since store stubs discard creates). It writes subtree files and
// returns the blocks plus the per-block regular tx. Distinct coinbase heights → distinct
// tx and block hashes.
func buildOverlapBlocks(t *testing.T, bv *BlockValidation, ctx context.Context, genesisHash *chainhash.Hash, startHeight uint32, k int) ([]*model.Block, []*bt.Tx) {
	t.Helper()

	blocks := make([]*model.Block, k)
	txByBlock := make([]*bt.Tx, k)
	prev := *genesisHash

	for i := range k {
		height := startHeight + uint32(i) //nolint:gosec
		blk, tx := buildOverlapSingleTxBlockRet(t, bv, ctx, &prev, height, height, fmt.Sprintf("/overlap-%d/", i))
		blocks[i] = blk
		txByBlock[i] = tx
		prev = *blk.Hash()
	}
	return blocks, txByBlock
}

// buildOverlapSingleTxBlock builds a single-subtree block (coinbase + one regular tx
// spending the coinbase) chained on prevHash and returns just the block.
func buildOverlapSingleTxBlock(t *testing.T, bv *BlockValidation, ctx context.Context, prevHash *chainhash.Hash, height, timestamp uint32, cbTag string) *model.Block {
	t.Helper()
	blk, _ := buildOverlapSingleTxBlockRet(t, bv, ctx, prevHash, height, timestamp, cbTag)
	return blk
}

// buildOverlapSingleTxBlockRet is the workhorse: builds and mines a single-subtree block
// and writes its subtree + subtree-data files, returning the block and its regular tx.
func buildOverlapSingleTxBlockRet(t *testing.T, bv *BlockValidation, ctx context.Context, prevHash *chainhash.Hash, height, timestamp uint32, cbTag string) (*model.Block, *bt.Tx) {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	cb := transactions.Create(t,
		transactions.WithCoinbaseData(height, cbTag),
		transactions.WithP2PKHOutputs(1, 50e8, privKey.PubKey()),
	)
	tx := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(cb, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size()))) //nolint:gosec

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(cb, 0))
	require.NoError(t, subtreeData.AddTx(tx, 1))
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	ph := *prevHash
	blk := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &ph,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1_000_000 + timestamp,
			Bits:           *nBits,
		},
		Height:           height,
		CoinbaseTx:       cb,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		TransactionCount: 2,
	}
	mineBlockPoW(t, blk)
	return blk, tx
}
