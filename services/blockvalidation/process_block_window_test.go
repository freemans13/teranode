//go:build testtxmetacache

package blockvalidation

// ProcessBlockWindow tests — the Step-8 Increment-2b acceptance bar.
//
// Four test groups:
//
//  1. PARITY — same 3-block below-checkpoint chain with cross-block spends
//     processed by (A) serial quickValidateBlock per block and (B) ProcessBlockWindow
//     K=all, on separate sqlitememory stores → identical UTXO state (existence,
//     BlockID grouping, spender-identity) + identical committed chain tip.
//
//  2. BARRIER PROPERTY — a spy UTXO store holds each Create goroutine open for 1 ms
//     then increments a completion counter; each Spend asserts at call time that ALL
//     creates have already completed. This is a guaranteed discriminator: a barrier-free
//     implementation would observe an incomplete create set on the first Spend.
//
//  3. COMMIT ORDER — spy blockchain client recording block heights at AddBlock;
//     asserts strictly ascending order for all K commits.
//
//  4. FAIL-CLOSED — (a) window with an above-checkpoint block rejected before any
//     creates run; (b) C1 hard-fail (block with a subtree whose file is missing)
//     aborts before any C3 commit (AddBlock never called).

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
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	storoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// newProcessWindowHarness returns a fully-wired in-memory BlockValidation for
// ProcessBlockWindow tests.
func newProcessWindowHarness(t *testing.T, urlSuffix string) (*BlockValidation, context.Context, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	// Put a high checkpoint so test heights (100-102) are firmly below it.
	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params

	utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///window_proc_%s_%s", urlSuffix, t.Name()))
	require.NoError(t, err)
	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

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
	return bv, ctx, cancel
}

// ---------------------------------------------------------------------------
// windowChainData — shared chain definition for parity tests
// ---------------------------------------------------------------------------

// windowChainData holds the raw transaction data for a 3-block below-checkpoint
// chain. Once built, it is written into each test harness's subtree store via
// writeWindowChainToStore so that both serial and parallel paths operate on
// identical content. All txs are built deterministically (fixed private key) so
// tx hashes are identical across harnesses.
//
// Chain layout (heights 100-102):
//
//	block0 (h=100): cb0 + tx0a (spends cb0[0]) + tx0b (spends tx0a[1])
//	block1 (h=101): cb1 + tx1a (spends tx0b[1])   ← cross-block spend
//	block2 (h=102): cb2 + tx2a (spends tx1a[1])   ← cross-block spend
type windowChainData struct {
	privateKey *bec.PrivateKey
	coinbases  []*bt.Tx
	regularTxs [][]*bt.Tx
	blocks     []*model.Block
}

// buildWindowChainData creates the raw transaction data for a 3-block chain.
// It does NOT write subtree files or set merkle roots — call writeWindowChainToStore
// for that. All txs use a deterministic key so tx hashes are identical across calls.
//
// Returns a windowChainData whose blocks have placeholder headers; the real
// merkle roots and PoW nonces are set by writeWindowChainToStore.
func buildWindowChainData(t *testing.T, genesisHash *chainhash.Hash) *windowChainData {
	t.Helper()

	const startHeight = uint32(100)

	privateKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Block 0: cb0 + tx0a + tx0b.
	chain0 := transactions.CreateTestTransactionChainWithCount(t, 4) // cb + 3 txs
	cb0, tx0a, tx0b := chain0[0], chain0[1], chain0[2]

	// Block 1: cb1 + tx1a (cross-block: spends tx0b[1]).
	cb1 := transactions.CreateTestTransactionChainWithCount(t, 2)[0]
	tx1a := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(tx0b, 1),
		transactions.WithP2PKHOutputs(1, 500),
		transactions.WithChangeOutput(),
	)

	// Block 2: cb2 + tx2a (cross-block: spends tx1a[1]).
	cb2 := transactions.CreateTestTransactionChainWithCount(t, 2)[0]
	tx2a := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(tx1a, 1),
		transactions.WithP2PKHOutputs(1, 200),
		transactions.WithChangeOutput(),
	)

	// Placeholder merkle root (all zeros) — overwritten by prepareBlockInStore.
	var zeroMerkle chainhash.Hash
	prevHash := *genesisHash

	// Use timestamps 1, 2, 3 to ensure distinct block hashes even with placeholder merkle roots.
	block0 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &prevHash,
			HashMerkleRoot: &zeroMerkle, Timestamp: 1, Bits: *nBits,
		},
		Height:     startHeight,
		CoinbaseTx: cb0,
	}

	// block1 and block2 prevHash will be fixed up by writeWindowChainToStore after
	// prepareBlockInStore mines block0 and gives us its real hash.
	var ph1, ph2 chainhash.Hash
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &ph1,
			HashMerkleRoot: &zeroMerkle, Timestamp: 2, Bits: *nBits,
		},
		Height:     startHeight + 1,
		CoinbaseTx: cb1,
	}
	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: &ph2,
			HashMerkleRoot: &zeroMerkle, Timestamp: 3, Bits: *nBits,
		},
		Height:     startHeight + 2,
		CoinbaseTx: cb2,
	}

	return &windowChainData{
		privateKey: privateKey,
		coinbases:  []*bt.Tx{cb0, cb1, cb2},
		regularTxs: [][]*bt.Tx{
			{tx0a, tx0b},
			{tx1a},
			{tx2a},
		},
		blocks: []*model.Block{block0, block1, block2},
	}
}

// writeWindowChainToStore writes subtree + subtree-data files for all blocks
// into bv.subtreeStore, sets merkle roots and TransactionCounts, mines PoW, and
// fixes up the HashPrevBlock chain (block[i+1].HashPrevBlock = block[i].Hash())
// so commitBlock (AddBlock) succeeds when replaying in height order.
//
// Call once per harness. Both harnesses get the same tx content but independent
// block.ID fields.
func writeWindowChainToStore(t *testing.T, ctx context.Context, bv *BlockValidation, chain *windowChainData) {
	t.Helper()
	for i, blk := range chain.blocks {
		// Fix up prevHash for block[i] based on the now-mined block[i-1] hash.
		// block[0] prevHash is already set to genesis in buildWindowChainData.
		if i > 0 {
			prev := chain.blocks[i-1]
			prevHash := *prev.Hash()
			blk.Header.HashPrevBlock = &prevHash
		}
		prepareBlockInStore(t, bv, ctx, blk, chain.coinbases[i], chain.regularTxs[i])
	}
}

// cloneBlocksForHarness creates fresh model.Block structs from chain, with the
// same tx content (CoinbaseTx pointers) but independent headers so each
// harness can accumulate its own block.ID without aliasing.
//
// MUST be called BEFORE writeWindowChainToStore for the clone harness, because
// writeWindowChainToStore will update HashMerkleRoot, Subtrees, etc. in place.
// The clones start with the same placeholder headers that buildWindowChainData set.
func cloneBlocksForHarness(t *testing.T, chain *windowChainData) *windowChainData {
	t.Helper()
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	var zeroMerkle chainhash.Hash

	clones := make([]*model.Block, len(chain.blocks))
	for i, src := range chain.blocks {
		// Snapshot the current prevHash (genesis for block0; zeros for block1/block2 —
		// writeWindowChainToStore will fix them up during its own pass).
		ph := *src.Header.HashPrevBlock
		clones[i] = &model.Block{
			Header: &model.BlockHeader{
				Version:        src.Header.Version,
				HashPrevBlock:  &ph,
				HashMerkleRoot: &zeroMerkle, // placeholder; writeWindowChainToStore updates
				Timestamp:      src.Header.Timestamp,
				Bits:           *nBits,
			},
			Height:     src.Height,
			CoinbaseTx: src.CoinbaseTx, // shared coinbase is fine — hash is identical
		}
	}
	return &windowChainData{
		privateKey: chain.privateKey,
		coinbases:  chain.coinbases,
		regularTxs: chain.regularTxs,
		blocks:     clones,
	}
}

// ---------------------------------------------------------------------------
// Test 1: PARITY
// ---------------------------------------------------------------------------

// TestProcessBlockWindow_ParityWithSerial verifies that the same 3-block
// below-checkpoint chain processed by:
//
//	Path A: serial quickValidateBlock per block (window of 1)
//	Path B: ProcessBlockWindow K=3
//
// yields identical UTXO state: BlockID rank and cross-block spender identity.
func TestProcessBlockWindow_ParityWithSerial(t *testing.T) {
	bvA, ctxA, cancelA := newProcessWindowHarness(t, "parity_A")
	defer cancelA()

	bvB, ctxB, cancelB := newProcessWindowHarness(t, "parity_B")
	defer cancelB()

	// Build chain data once. Both harnesses share the same tx content (same hashes).
	chain := buildWindowChainData(t, bvA.settings.ChainCfgParams.GenesisHash)

	// Clone BEFORE writing to either store; cloneBlocksForHarness gives bvB its own
	// independent block structs with placeholder headers.
	chainB := cloneBlocksForHarness(t, chain)

	// Write subtree files into each harness; this sets merkle roots, TransactionCounts,
	// PoW nonces, and chains HashPrevBlock correctly for commitBlock.
	writeWindowChainToStore(t, ctxA, bvA, chain)
	writeWindowChainToStore(t, ctxB, bvB, chainB)

	// Path A: serial window-of-1 (createBlockUTXOs + spendBlockUTXOs + commitBlock per block).
	// Using the phase methods directly avoids the Prometheus metrics init dependency
	// that quickValidateBlock carries; behavior is identical for the outpoint-only path.
	for _, blk := range chain.blocks {
		ws, err := bvA.createBlockUTXOs(ctxA, blk, true, nil)
		require.NoError(t, err, "Path A: createBlockUTXOs failed at height %d", blk.Height)
		require.NoError(t, bvA.spendBlockUTXOs(ctxA, blk, ws, true, nil), "Path A: spendBlockUTXOs failed at height %d", blk.Height)
		require.NoError(t, bvA.commitBlock(ctxA, blk, "test-peer", "TestProcessBlockWindow_ParityWithSerial"), "Path A: commitBlock failed at height %d", blk.Height)
	}

	// Path B: concurrent ProcessBlockWindow.
	require.NoError(t, bvB.ProcessBlockWindow(ctxB, chainB.blocks, "test-peer"), "Path B: ProcessBlockWindow failed")

	// --- UTXO parity assertions ---
	txsPerBlock := chain.regularTxs

	blockRankMap := func(bv *BlockValidation, bvCtx context.Context) map[chainhash.Hash]int {
		t.Helper()
		rankOf := make(map[uint32]int)
		result := make(map[chainhash.Hash]int)
		for _, txs := range txsPerBlock {
			for _, tx := range txs {
				h := *tx.TxIDChainHash()
				m, err := bv.utxoStore.Get(bvCtx, &h, fields.BlockIDs)
				require.NoError(t, err, "Get(BlockIDs) failed for tx %s", h)
				require.NotNil(t, m)
				require.NotEmpty(t, m.BlockIDs, "tx %s has no BlockIDs", h)
				bid := m.BlockIDs[0]
				if _, seen := rankOf[bid]; !seen {
					rankOf[bid] = len(rankOf)
				}
				result[h] = rankOf[bid]
			}
		}
		return result
	}

	rankA := blockRankMap(bvA, ctxA)
	rankB := blockRankMap(bvB, ctxB)
	require.Equal(t, rankA, rankB, "PARITY: block-rank mismatch between serial and window paths")

	// Spender-identity parity for cross-block spends.
	// tx0b output[1] is spent by tx1a; tx1a output[1] is spent by tx2a.
	type crossBlockSpend struct {
		parent    *bt.Tx
		spentVout int
		spender   *bt.Tx
	}
	crossSpends := []crossBlockSpend{
		{txsPerBlock[0][1], 1, txsPerBlock[1][0]}, // tx0b[1] → tx1a
		{txsPerBlock[1][0], 1, txsPerBlock[2][0]}, // tx1a[1] → tx2a
	}
	for _, cs := range crossSpends {
		parentH := *cs.parent.TxIDChainHash()
		wantSpender := *cs.spender.TxIDChainHash()
		vout := cs.spentVout

		for _, tc := range []struct {
			name  string
			bv    *BlockValidation
			bvCtx context.Context
		}{
			{"Path A (serial)", bvA, ctxA},
			{"Path B (window)", bvB, ctxB},
		} {
			m, getErr := tc.bv.utxoStore.Get(tc.bvCtx, &parentH, fields.Utxos)
			require.NoError(t, getErr, "%s: Get(Utxos) failed for tx %s", tc.name, parentH)
			require.NotNil(t, m)
			require.True(t, len(m.SpendingDatas) > vout, "%s: SpendingDatas too short for tx %s vout %d", tc.name, parentH, vout)
			require.NotNil(t, m.SpendingDatas[vout], "%s: SpendingDatas[%d] nil for tx %s", tc.name, vout, parentH)
			require.NotNil(t, m.SpendingDatas[vout].TxID, "%s: SpendingDatas[%d].TxID nil for tx %s", tc.name, vout, parentH)
			require.Equal(t, wantSpender, *m.SpendingDatas[vout].TxID,
				"%s: wrong spender for tx %s vout %d", tc.name, parentH, vout)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 2: BARRIER PROPERTY
// ---------------------------------------------------------------------------

// barrierSpyStore wraps NullStore and provides a guaranteed discriminator for
// the C1→C2 barrier property. The key design requirement is that the test must
// RELIABLY FAIL when the barrier is removed — not just probabilistically.
//
// Mechanism (staggered-delay gate):
//
//  1. totalExpectedCreates is the exact count of Create calls the window will issue
//     (K blocks × 1 non-coinbase tx each).
//
//  2. Each Create claims a monotonically increasing slot index via createIndex.
//     It then sleeps for (slotIndex+1) × slotDelay (5ms, 10ms, 15ms for slots 0,1,2).
//     Staggered delays ensure that Create-0 finishes 5ms before Create-1 and 10ms
//     before Create-2. After sleeping, it increments completedCreates.
//
//  3. Each Spend immediately asserts completedCreates == totalExpectedCreates.
//     If not, a violation is recorded via t.Errorf (non-fatal so all spends fire).
//
// Why staggered delays guarantee discrimination:
//
// In the real (barrier) implementation all K creates run and c1g.Wait() blocks
// until Create-2 (the slowest, 15ms) finishes. completedCreates = K when any
// Spend fires → no violation.
//
// In a barrier-free implementation (e.g. per-block: create→spend per goroutine,
// all K goroutines concurrent) block-0's goroutine finishes Create-0 (5ms) and
// immediately fires Spend-0. At t=5ms, Creates 1 and 2 are still sleeping
// (10ms and 15ms), so completedCreates = 1 ≠ 3 → VIOLATION recorded.
//
// The staggered delays remove any ambiguity about whether "all creates happened
// to finish before the first spend by scheduling luck": Create-0 is guaranteed to
// complete a full slotDelay (5ms) before Create-1, so block-0's Spend will always
// fire before Create-1 and Create-2 complete in a barrier-free impl.
type barrierSpyStore struct {
	*nullstore.NullStore
	t                    *testing.T
	totalExpectedCreates int64
	createIndex          atomic.Int64 // slot counter (0, 1, 2, …)
	completedCreates     atomic.Int64
	slotDelay            time.Duration
	violationRecorded    atomic.Bool
}

func newBarrierSpyStore(t *testing.T, totalExpectedCreates int64) *barrierSpyStore {
	t.Helper()
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	return &barrierSpyStore{
		NullStore:            ns,
		t:                    t,
		totalExpectedCreates: totalExpectedCreates,
		slotDelay:            5 * time.Millisecond,
	}
}

func (s *barrierSpyStore) SupportsOutpointOnlySpend() bool { return true }

func (s *barrierSpyStore) Create(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*utxometa.Data, error) {
	// Claim a slot (0, 1, 2, …). Slot i sleeps for (i+1) * slotDelay so that
	// each create completes strictly later than the previous one. This guarantees
	// that in a barrier-free implementation the first block's Spend fires while
	// the remaining creates are still sleeping.
	slot := s.createIndex.Add(1) - 1
	time.Sleep(time.Duration(slot+1) * s.slotDelay)
	s.completedCreates.Add(1)
	return nil, nil
}

func (s *barrierSpyStore) Spend(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	// Assert the barrier invariant at spend time: EVERY Create must have completed.
	// If completedCreates < totalExpectedCreates here, the C1→C2 barrier is not
	// being enforced — a Spend is running while creates are still in flight.
	completed := s.completedCreates.Load()
	if completed != s.totalExpectedCreates {
		s.violationRecorded.Store(true)
		s.t.Errorf(
			"BARRIER VIOLATION at Spend time: completedCreates=%d want %d — C1→C2 fence not enforced",
			completed, s.totalExpectedCreates,
		)
	}
	return nil, nil
}

// TestProcessBlockWindow_BarrierProperty proves the C1→C2 barrier is a GUARANTEED
// discriminator, not a probabilistic one.
//
// Technique: barrierSpyStore assigns each Create a slot (0..K-1) and sleeps for
// (slot+1)×5ms before recording completion. Slot-0 finishes at 5ms, slot-1 at 10ms,
// slot-2 at 15ms — staggered so they cannot complete "simultaneously". Each Spend
// asserts AT CALL TIME that ALL K creates have already completed.
//
// With the real barrier (c1g.Wait after all creates): the barrier holds until 15ms,
// then all K spends fire with completedCreates == K → PASS.
//
// Without the barrier (per-block sequential: create+spend in one goroutine per block,
// all K goroutines concurrent): block-0's Create finishes at 5ms and its Spend fires
// immediately. At 5ms, creates 1 and 2 are still sleeping (10ms, 15ms), so
// completedCreates = 1 ≠ 3 → FAIL.
//
// The spy store does not persist UTXO data, so this test isolates the barrier
// ordering guarantee from data correctness (covered by Test 1).
func TestProcessBlockWindow_BarrierProperty(t *testing.T) {
	const k = 3

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params

	// Each block has 1 non-coinbase tx → k total Create calls.
	// createBlockUTXOs calls Create once per non-coinbase tx in the block.
	const txPerBlock = 1
	totalExpectedCreates := int64(k * txPerBlock)

	spy := newBarrierSpyStore(t, totalExpectedCreates)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		utxoStore:                     spy,
		subtreeStore:                  blobmemory.New(),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
	}
	t.Cleanup(func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
	})

	// Build k independent (no cross-block dep) single-tx blocks.
	// The spy store doesn't persist creates so cross-block spends would fail;
	// we use self-contained blocks (coinbase + one intra-block tx) instead.
	blocks := buildBarrierBlocks(t, bv, ctx, bv.settings.ChainCfgParams.GenesisHash, 100, k)

	require.NoError(t, bv.ProcessBlockWindow(ctx, blocks, "barrier-test-peer"))

	// Final sanity: all creates must have been called (catches miscounted totalExpectedCreates).
	finalCompleted := spy.completedCreates.Load()
	require.Equal(t, totalExpectedCreates, finalCompleted,
		"BARRIER: expected %d Create calls, got %d — txPerBlock constant may need updating",
		totalExpectedCreates, finalCompleted)

	require.False(t, spy.violationRecorded.Load(),
		"BARRIER: one or more Spend calls observed incomplete creates — see t.Errorf above")
}

// buildBarrierBlocks builds k independent single-tx blocks for the barrier test.
// Each block: unique coinbase (distinct block height in coinbase data) + one
// regular tx spending the coinbase. No cross-block deps so the spy store (which
// discards creates) won't error on missing parents. Unique coinbase heights
// produce distinct tx hashes → distinct subtree root hashes → no BLOB_EXISTS.
func buildBarrierBlocks(t *testing.T, bv *BlockValidation, ctx context.Context, genesisHash *chainhash.Hash, startHeight uint32, k int) []*model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blocks := make([]*model.Block, k)
	prevHash := *genesisHash

	// Use the deterministic key so the tx helper can sign inputs.
	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	for i := range k {
		height := startHeight + uint32(i) //nolint:gosec

		// Unique coinbase per block: distinct height in scriptSig → unique tx hash.
		cb := transactions.Create(t,
			transactions.WithCoinbaseData(height, "/barrier-test/"),
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

		ph := prevHash
		blk := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &ph,
				HashMerkleRoot: merkleRoot,
				Timestamp:      1_000_000 + uint32(i), //nolint:gosec
				Bits:           *nBits,
			},
			Height:           height,
			CoinbaseTx:       cb,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
			TransactionCount: 2,
		}
		for {
			if ok, _, _ := blk.Header.HasMetTargetDifficulty(); ok {
				break
			}
			blk.Header.Nonce++
			if blk.Header.Nonce > 5_000_000 {
				t.Fatal("failed to find valid PoW nonce")
			}
		}
		prevHash = *blk.Hash()
		blocks[i] = blk
	}
	return blocks
}

// ---------------------------------------------------------------------------
// Test 3: COMMIT ORDER
// ---------------------------------------------------------------------------

// commitOrderSpyClient wraps a real LocalClient and records block heights
// passed to AddBlock in call order.
type commitOrderSpyClient struct {
	blockchain.ClientI
	mu      sync.Mutex
	heights []uint32
}

func (s *commitOrderSpyClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...storoptions.StoreBlockOption) error {
	s.mu.Lock()
	s.heights = append(s.heights, block.Height)
	s.mu.Unlock()
	return s.ClientI.AddBlock(ctx, block, peerID, opts...)
}

// TestProcessBlockWindow_CommitOrder asserts that commitBlock is called in
// strictly ascending height order regardless of goroutine scheduling.
func TestProcessBlockWindow_CommitOrder(t *testing.T) {
	const k = 3

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params

	utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///window_order_%s", t.Name()))
	require.NoError(t, err)
	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	realClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	spy := &commitOrderSpyClient{ClientI: realClient}

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              spy,
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

	chain := buildWindowChainData(t, bv.settings.ChainCfgParams.GenesisHash)
	writeWindowChainToStore(t, ctx, bv, chain)

	require.NoError(t, bv.ProcessBlockWindow(ctx, chain.blocks, "order-test-peer"))

	spy.mu.Lock()
	heights := make([]uint32, len(spy.heights))
	copy(heights, spy.heights)
	spy.mu.Unlock()

	require.Equal(t, k, len(heights), "AddBlock must be called exactly K=%d times (got %d)", k, len(heights))
	for i := 1; i < len(heights); i++ {
		require.Less(t, heights[i-1], heights[i],
			"COMMIT ORDER VIOLATION: AddBlock heights not ascending: heights[%d]=%d >= heights[%d]=%d",
			i-1, heights[i-1], i, heights[i])
	}
}

// ---------------------------------------------------------------------------
// Test 4: FAIL-CLOSED
// ---------------------------------------------------------------------------

// outpointOnlyNullStore wraps NullStore to return true for SupportsOutpointOnlySpend.
type outpointOnlyNullStore struct {
	*nullstore.NullStore
}

func (s *outpointOnlyNullStore) SupportsOutpointOnlySpend() bool { return true }

// addBlockCountSpy wraps a real BlockchainClient and counts AddBlock calls.
type addBlockCountSpy struct {
	blockchain.ClientI
	counter *atomic.Int64
}

func (s *addBlockCountSpy) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...storoptions.StoreBlockOption) error {
	s.counter.Add(1)
	return s.ClientI.AddBlock(ctx, block, peerID, opts...)
}

// TestProcessBlockWindow_FailClosed verifies:
//
// (a) A window with any above-checkpoint block is rejected before any creates.
// (b) A C1 hard-fail (block with a subtree whose file is missing) aborts before C3 (no AddBlock call).
func TestProcessBlockWindow_FailClosed(t *testing.T) {
	t.Run("above-checkpoint block rejected before creates", func(t *testing.T) {
		bv, ctx, cancel := newProcessWindowHarness(t, "failclosed_above")
		defer cancel()

		nBits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		var ph, zeroMerkle chainhash.Hash
		aboveBlock := &model.Block{
			Header: &model.BlockHeader{
				Version: 1, HashPrevBlock: &ph,
				HashMerkleRoot: &zeroMerkle,
				Timestamp:      1_000_000, Bits: *nBits,
			},
			Height:     1_000_001, // above the 1_000_000 hardcoded checkpoint
			CoinbaseTx: bt.NewTx(),
		}

		err = bv.ProcessBlockWindow(ctx, []*model.Block{aboveBlock}, "fail-closed-peer")
		require.Error(t, err, "window with above-checkpoint block must be rejected")
	})

	t.Run("C1 hard-fail aborts before C3 commit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		logger := ulogger.TestLogger{}
		tSettings := testutil.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
		tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

		params := *tSettings.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
		tSettings.ChainCfgParams = &params

		var addBlockCalls atomic.Int64

		blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
		require.NoError(t, err)
		realClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
		require.NoError(t, err)

		spyClient := &addBlockCountSpy{ClientI: realClient, counter: &addBlockCalls}

		ns, err := nullstore.NewNullStore()
		require.NoError(t, err)

		bv := &BlockValidation{
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              spyClient,
			utxoStore:                     &outpointOnlyNullStore{NullStore: ns},
			subtreeStore:                  blobmemory.New(),
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		}
		t.Cleanup(func() {
			bv.blockExistsCache.Stop()
			bv.lastValidatedBlocks.Stop()
		})

		nBits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		var ph, zeroMerkle, missingSubtree chainhash.Hash
		missingSubtree[0] = 0xAB // subtree file that does not exist in the (empty) subtree store
		// Block WITH a subtree whose file is absent → createBlockUTXOs fails in its
		// prefetch stage (a genuine C1 hard-fail). A 0-subtree block no longer fails
		// C1 (it commits like quickValidateBlock), so this test uses a missing-subtree
		// block to exercise the C1→C3 abort path.
		badBlock := &model.Block{
			Header: &model.BlockHeader{
				Version: 1, HashPrevBlock: &ph,
				HashMerkleRoot: &zeroMerkle,
				Timestamp:      1_000_000, Bits: *nBits,
			},
			Height:           100,
			CoinbaseTx:       bt.NewTx(),
			Subtrees:         []*chainhash.Hash{&missingSubtree}, // subtree file absent → C1 prefetch error
			TransactionCount: 2,
		}

		err = bv.ProcessBlockWindow(ctx, []*model.Block{badBlock}, "fail-closed-peer")
		require.Error(t, err, "C1 hard-fail must cause ProcessBlockWindow to return an error")
		require.Equal(t, int64(0), addBlockCalls.Load(), "C3 AddBlock must not be called when C1 fails")
	})
}
