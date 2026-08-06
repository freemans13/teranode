package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testCoinbaseMaturity is the coinbase maturity these tests run with. The
// shared base test settings use 1, which would put coinbaseRepairFloor at the
// tip and collapse every walk-back to a single height. A realistic value keeps
// the safety floor out of the way so the tests exercise the behaviour they are
// actually about; the one test that targets the floor sets its own.
const testCoinbaseMaturity = 100

// withCoinbaseMaturity sets the coinbase maturity used to derive
// coinbaseRepairFloor. It has to be applied before the stores are built rather
// than poked in afterwards: the blockchain SQL store starts a background
// goroutine at construction that reads ChainCfgParams, and a later write races
// it (caught by -race).
func withCoinbaseMaturity(maturity uint16) func(*settings.Settings) {
	return func(s *settings.Settings) {
		s.ChainCfgParams.CoinbaseMaturity = maturity
	}
}

// coinbaseTxForHeader clones the shared fixture coinbase (see addBlockWithMinedSet)
// and perturbs its scriptSig with bytes derived from the header hash, so that
// each header produces a coinbase transaction with a distinct TxID. Without
// this, every block built from the shared fixture coinbase string would
// collide on the same coinbase txid, making it impossible to seed the UTXO
// store with "the coinbase for height N" without also seeding it for every
// other height that reuses the same fixture.
func coinbaseTxForHeader(t *testing.T, header *model.BlockHeader) *bt.Tx {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	headerHash := header.Hash()

	scriptSig := make([]byte, 0, len(coinbaseTx.Inputs[0].UnlockingScript.Bytes())+len(headerHash))
	scriptSig = append(scriptSig, coinbaseTx.Inputs[0].UnlockingScript.Bytes()...)
	scriptSig = append(scriptSig, headerHash[:]...)

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)

	return coinbaseTx
}

// addCanonicalBlockWithCoinbase is a variant of addBlockWithMinedSet (see
// reset_bug_test.go) that carries a caller-supplied coinbase transaction
// instead of the shared fixture coinbase. canonicalCoinbaseAt needs to
// observe distinct coinbases per height, which addBlockWithMinedSet's
// hardcoded coinbase cannot provide.
func addCanonicalBlockWithCoinbase(ctx context.Context, t *testing.T, items *baTestItems, blockHeader *model.BlockHeader, coinbaseTx *bt.Tx) {
	t.Helper()

	err := items.blockchainClient.AddBlock(ctx, &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}, "", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)
}

// TestCanonicalCoinbaseAt exercises the divergence probe against a real
// sqlitememory UTXO store and blockchain client (per AGENTS.md testing rules
// - no mocking the blockchain client/store).
func TestCanonicalCoinbaseAt(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	// height 1: canonical block carries cb1, and the store holds cb1 -> present.
	cb1 := coinbaseTxForHeader(t, blockHeader1)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader1, cb1)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, cb1, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	present, blk, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 1)
	require.NoError(t, err)
	require.True(t, present)
	require.NotNil(t, blk)
	require.True(t, blk.CoinbaseTx.TxIDChainHash().IsEqual(cb1.TxIDChainHash()))

	// height 2: canonical block carries cb2, but cb2 was never created in the
	// store -> not present, even though a (different) coinbase exists at height 1.
	cb2 := coinbaseTxForHeader(t, blockHeader2)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader2, cb2)

	present2, blk2, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 2)
	require.NoError(t, err)
	require.False(t, present2)
	require.NotNil(t, blk2)
	require.True(t, blk2.CoinbaseTx.TxIDChainHash().IsEqual(cb2.TxIDChainHash()))
}

// buildCanonicalChain builds n chained block headers on top of regtest
// genesis, each carrying its own distinct coinbase transaction (see
// coinbaseTxForHeader), and adds them to the blockchain store as canonical
// mined blocks. It returns the headers in ascending height order, i.e.
// headers[0] is height 1, headers[n-1] is height n.
//
// Building the chain only makes each height's canonical coinbase available
// via GetBlockByHeight -- it does NOT seed the UTXO store, so
// canonicalCoinbaseAt reports every height as absent until seedCoinbase is
// called for it. This lets tests choose exactly which heights are "present"
// vs "missing" on top of one shared canonical chain.
func buildCanonicalChain(ctx context.Context, t *testing.T, items *baTestItems, n int) []*model.BlockHeader {
	t.Helper()

	headers := make([]*model.BlockHeader, 0, n)
	prevHash := chaincfg.RegressionNetParams.GenesisHash

	for i := 1; i <= n; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: &chainhash.Hash{},
			Nonce:          uint32(i), //nolint:gosec // test fixture, i is bounded by the caller's n
			Bits:           *bits,
		}

		coinbaseTx := coinbaseTxForHeader(t, header)
		addCanonicalBlockWithCoinbase(ctx, t, items, header, coinbaseTx)

		headers = append(headers, header)
		prevHash = header.Hash()
	}

	return headers
}

// seedCoinbase marks height as "present" by creating its canonical coinbase
// (recomputed deterministically from the header via coinbaseTxForHeader, the
// same derivation buildCanonicalChain used to build the canonical block) in
// the UTXO store. Heights never passed to seedCoinbase stay absent, which is
// how tests carve out gaps and holes on top of the chain buildCanonicalChain
// built.
func seedCoinbase(ctx context.Context, t *testing.T, items *baTestItems, headers []*model.BlockHeader, height uint32) {
	t.Helper()

	require.True(t, height >= 1 && int(height) <= len(headers), "height %d out of range for %d headers", height, len(headers))

	header := headers[height-1]
	coinbaseTx := coinbaseTxForHeader(t, header)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, coinbaseTx, height, utxoStore.WithCreateOnly())
	require.NoError(t, err)
}

// gapHeights extracts the heights of the blocks scopeCoinbaseGap returned, in
// the order returned, for assertions against expected height sets.
func gapHeights(gap []*model.Block) []uint32 {
	heights := make([]uint32, len(gap))
	for i, blk := range gap {
		heights[i] = blk.Height
	}

	return heights
}

// TestScopeCoinbaseGap_ContiguousAndHoled exercises scopeCoinbaseGap against a
// real sqlitememory UTXO store and blockchain client (per AGENTS.md testing
// rules - no mocking the blockchain client/store), covering both a plain
// contiguous gap and a holed gap where a present coinbase sits under a good
// tip in the middle of otherwise-missing heights. The holed case is the core
// behaviour under test: the walk must not stop at the first present
// coinbase it sees (the hole) -- it must keep walking back until it has seen
// CoinbaseRecoveryConsecutiveGood *consecutive* present coinbases.
func TestScopeCoinbaseGap_ContiguousAndHoled(t *testing.T) {
	initPrometheusMetrics()

	t.Run("contiguous gap", func(t *testing.T) {
		ctx := t.Context()
		items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
		require.NotNil(t, items)
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

		// heights 1..6: floor proven by 1,2 present; 3,4,5 missing; 6 (trigger) present.
		headers := buildCanonicalChain(ctx, t, items, 6)
		seedCoinbase(ctx, t, items, headers, 1)
		seedCoinbase(ctx, t, items, headers, 2)
		seedCoinbase(ctx, t, items, headers, 6)

		gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
		require.NoError(t, err)
		require.Equal(t, []uint32{3, 4, 5}, gapHeights(gap))
	})

	t.Run("holed gap does not stop at first present coinbase", func(t *testing.T) {
		ctx := t.Context()
		items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
		require.NotNil(t, items)
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

		// heights 1..6: floor proven by 1,2 present; 3 missing, 4 present (the
		// hole), 5 missing, 6 (trigger) present. A single-present stop
		// condition would wrongly halt at height 4; ConsecutiveGood=2 forces
		// the walk to keep going until it sees TWO consecutive present
		// coinbases (1 and 2), so 3 and 5 both come back as gap.
		headers := buildCanonicalChain(ctx, t, items, 6)
		seedCoinbase(ctx, t, items, headers, 1)
		seedCoinbase(ctx, t, items, headers, 2)
		seedCoinbase(ctx, t, items, headers, 4)
		seedCoinbase(ctx, t, items, headers, 6)

		gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
		require.NoError(t, err)
		require.Equal(t, []uint32{3, 5}, gapHeights(gap))
	})

	t.Run("gap exceeding the cap escalates", func(t *testing.T) {
		ctx := t.Context()
		items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
		require.NotNil(t, items)
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
		items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 2

		// heights 1..6: only height 1 present, 2..6 all missing -- a gap of 5
		// exceeds the cap of 2 and must escalate rather than return a slice.
		headers := buildCanonicalChain(ctx, t, items, 6)
		seedCoinbase(ctx, t, items, headers, 1)

		gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
		require.Error(t, err)
		require.True(t, errors.Is(err, errCoinbaseGapTooLarge))
		require.Nil(t, gap)
	})
}

// TestRecoverCoinbaseDivergence_RepairsGapNoConflicts exercises the full
// staged orchestration end to end against a real sqlitememory UTXO store and
// blockchain client, and the SubtreeProcessor's own goroutine (via
// ReconcileCoinbases). A gap of three missing coinbases (heights 2,3,4) sits
// above a proven-good floor (height 1); recovery is coinbase-only (it never
// inspects subtree/transaction conflict state), so Stage 1 auto-repair must
// succeed on the first attempt and recoverCoinbaseDivergence must return nil.
func TestRecoverCoinbaseDivergence_RepairsGapNoConflicts(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(ctx, t, items, 4)
	seedCoinbase(ctx, t, items, headers, 1) // floor good; 2,3,4 missing
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	repairedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))

	require.NoError(t, items.blockAssembler.recoverCoinbaseDivergence(ctx, 4))

	for h := uint32(2); h <= 4; h++ {
		present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, err)
		require.True(t, present, "coinbase at height %d must be repaired", h)
	}

	repairedAfter := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))
	require.Equal(t, float64(1), repairedAfter-repairedBefore,
		"repaired counter must increment exactly once on a successful auto-repair")
}

// TestStartupCoinbaseDivergenceCheck_Repairs exercises the startup hook
// (checkCoinbaseDivergenceOnStart) directly: the persisted tip's canonical
// coinbase is missing from the UTXO store (as it would be after an unclean
// shutdown mid fast-forward), and the hook must detect and repair it before
// the node is allowed to advance. Testing Start() end-to-end is not required
// here -- the direct call proves the detection+repair behaviour, and the
// wiring into Start is proven by the package building and the rest of the
// Start-path tests continuing to pass.
func TestStartupCoinbaseDivergenceCheck_Repairs(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(ctx, t, items, 3)
	seedCoinbase(ctx, t, items, headers, 1) // floor good; tip (3) missing its coinbase
	items.blockAssembler.setBestBlockHeader(headers[2], 3)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 3)
	require.NoError(t, err)
	require.True(t, present)
}

// TestRecoverCoinbaseDivergence_GapTooLarge_Escalates covers the escalation
// path when scopeCoinbaseGap itself refuses to scope the divergence (gap
// exceeds CoinbaseRecoveryMaxGapBlocks). recoverCoinbaseDivergence must not
// attempt any repair in this case and must return an error naming the need
// for operator intervention.
func TestRecoverCoinbaseDivergence_GapTooLarge_Escalates(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// heights 1..6: only height 1 present, 2..6 all missing -- a gap of 5
	// exceeds the cap of 1 and must escalate rather than repair.
	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 1)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	err := items.blockAssembler.recoverCoinbaseDivergence(ctx, 6)
	require.Error(t, err)

	// The gap was never repaired -- heights 2..6 must still be absent.
	for h := uint32(2); h <= 6; h++ {
		present, _, presErr := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, presErr)
		require.False(t, present, "coinbase at height %d must remain unrepaired after escalation", h)
	}

	escalatedAfter := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))
	require.Equal(t, float64(1), escalatedAfter-escalatedBefore,
		"escalated counter must increment exactly once when the gap exceeds the cap")
}

// TestScopeCoinbaseGap_StopsAtCoinbaseMaturityFloor covers the pruned-coinbase
// resurrection hazard. "Absent from the UTXO store" has two possible meanings:
// never created (the divergence this PR repairs) and created, matured, fully
// spent and then pruned by the DAH pruner (a perfectly healthy block). Both
// return ErrTxNotFound, and re-creating the second kind would put already-spent
// coinbase outputs back into the UTXO set as unspent.
//
// Coinbase maturity separates them: above tip-maturity a coinbase is too young
// to have been spent, so it cannot have been pruned. The walk must stop there
// and escalate rather than repair on a guess.
func TestScopeCoinbaseGap_StopsAtCoinbaseMaturityFloor(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	// Maturity 3 with a tip of 6 puts the safety floor at height 4, so heights
	// 1..3 are mature and could legitimately have been spent and pruned.
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(3))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	// Heights 1 and 2 are present but sit below the floor, so they cannot be
	// used to prove it. Heights 3, 4, 5 are absent and 6 (the tip) is present.
	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 1)
	seedCoinbase(ctx, t, items, headers, 2)
	seedCoinbase(ctx, t, items, headers, 6)

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
	require.Error(t, err)
	require.True(t, errors.Is(err, errCoinbaseFloorNotProven),
		"reaching the maturity floor mid-gap must be reported as an unproven floor, not a repairable gap")
	require.Nil(t, gap, "no blocks may be handed to the repair when the floor is unproven")
	require.True(t, isUnscopableCoinbaseGap(err),
		"an unproven floor is structural, so recovery must escalate rather than retry")
	require.False(t, errors.Is(err, errCoinbaseGapTooLarge),
		"an unproven floor is a distinct condition from an over-large gap")
}

// TestScopeCoinbaseGap_SameChainScopesWithFloorOutOfTheWay is the control for
// TestScopeCoinbaseGap_StopsAtCoinbaseMaturityFloor: the identical chain shape
// scopes normally once the maturity floor no longer bites, proving the refusal
// there comes from the floor and not from the shape of the chain.
func TestScopeCoinbaseGap_SameChainScopesWithFloorOutOfTheWay(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 1)
	seedCoinbase(ctx, t, items, headers, 2)
	seedCoinbase(ctx, t, items, headers, 6)

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
	require.NoError(t, err)
	require.Equal(t, []uint32{3, 4, 5}, gapHeights(gap))
}

// TestCoinbaseRepairFloor covers the arithmetic of the safety floor directly,
// including the short-chain case where every height down to 1 is still
// immature and therefore safe to probe.
func TestCoinbaseRepairFloor(t *testing.T) {
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	// Tip well above the maturity window: floor is tip-maturity+1, so exactly
	// `maturity` heights are probeable and the lowest of them is still immature.
	require.Equal(t, uint32(901), items.blockAssembler.coinbaseRepairFloor(1000))

	// Chain shorter than the maturity window: nothing has matured, so the whole
	// chain above genesis is safe.
	require.Equal(t, uint32(1), items.blockAssembler.coinbaseRepairFloor(100))
	require.Equal(t, uint32(1), items.blockAssembler.coinbaseRepairFloor(5))

	// Boundary: one block past the maturity window opens exactly one height.
	require.Equal(t, uint32(2), items.blockAssembler.coinbaseRepairFloor(101))
}

// TestCoinbaseRepairFloor_MaturityUnset covers the degenerate configuration
// where maturity is zero: no height can be proven immature, so the floor falls
// back to 1 rather than to the tip.
func TestCoinbaseRepairFloor_MaturityUnset(t *testing.T) {
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(0))
	require.NotNil(t, items)

	require.Equal(t, uint32(1), items.blockAssembler.coinbaseRepairFloor(1000))
}

// swapSubtreeProcessor installs a stand-in subtree processor for the duration
// of the test and puts the real one back before the test helper's own cleanup
// runs. Cleanups run last-registered-first, so the Stop() that
// setupBlockAssemblyTestWithUtxoStore registered still lands on the real
// processor rather than on a stand-in that was never asked to expect it.
func swapSubtreeProcessor(t *testing.T, items *baTestItems, replacement subtreeprocessor.Interface) {
	t.Helper()

	original := items.blockAssembler.subtreeProcessor
	items.blockAssembler.subtreeProcessor = replacement

	t.Cleanup(func() { items.blockAssembler.subtreeProcessor = original })
}

// TestErrCoinbaseGapTooLargeIsDistinguishable pins the property the retry logic
// in recoverCoinbaseDivergence depends on: errors.Is must say "yes" for the
// sentinel and "no" for the wrapped store/blockchain-client errors
// scopeCoinbaseGap returns from the same function.
//
// This is not theoretical. teranode's errors.Is compares two *Error values by
// error *code*, so while errCoinbaseGapTooLarge was built with
// errors.NewProcessingError it matched every other ProcessingError, and the
// "is this structural or transient?" test would have been permanently true.
func TestErrCoinbaseGapTooLargeIsDistinguishable(t *testing.T) {
	require.True(t, errors.Is(errCoinbaseGapTooLarge, errCoinbaseGapTooLarge),
		"the sentinel must match itself")

	transient := errors.NewProcessingError("[coinbaseRecovery] cannot get canonical block at height %d", 42,
		errors.NewStorageError("connection refused"))
	require.False(t, errors.Is(transient, errCoinbaseGapTooLarge),
		"a transient walk-back error must not be mistaken for the gap-too-large sentinel")

	require.True(t, errors.Is(errors.NewProcessingError("wrapped", errCoinbaseGapTooLarge), errCoinbaseGapTooLarge),
		"the sentinel must still be recognisable through a wrapper")
}

// TestRecoverCoinbaseDivergence_RetriesThenSucceeds covers the retry budget
// actually being used: ReconcileCoinbases fails once (as a transient store
// error would) and succeeds on the second attempt. Before this test,
// MaxAttempts=3 was indistinguishable from MaxAttempts=1 because no test ever
// drove a failing repair.
//
// The subtree processor is mocked here (which AGENTS.md permits - the rule is
// about the blockchain client and store, both of which stay real sqlitememory)
// because the point under test is the orchestration's reaction to a failing
// repair, and a real processor has no way to fail on demand.
func TestRecoverCoinbaseDivergence_RetriesThenSucceeds(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(ctx, t, items, 4)
	seedCoinbase(ctx, t, items, headers, 1) // floor good; 2,3,4 missing

	mockProcessor := &subtreeprocessor.MockSubtreeProcessor{}
	mockProcessor.On("ReconcileCoinbases", mock.Anything, mock.Anything).
		Return(errors.NewStorageError("transient store blip")).Once()
	mockProcessor.On("ReconcileCoinbases", mock.Anything, mock.Anything).
		Return(nil).Once()
	swapSubtreeProcessor(t, items, mockProcessor)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	repairedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))
	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	require.NoError(t, items.blockAssembler.recoverCoinbaseDivergence(ctx, 4))

	mockProcessor.AssertNumberOfCalls(t, "ReconcileCoinbases", 2)

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore)
	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))-repairedBefore,
		"a repair that needed a retry still counts as exactly one repair")
	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore,
		"a retry that eventually succeeds must not raise the operator alarm")
}

// TestRecoverCoinbaseDivergence_ExhaustsAttemptsThenEscalates covers the other
// half of the retry budget: every attempt fails, so recovery must spend the
// whole budget (not give up after one) and then escalate exactly once.
func TestRecoverCoinbaseDivergence_ExhaustsAttemptsThenEscalates(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(ctx, t, items, 4)
	seedCoinbase(ctx, t, items, headers, 1) // floor good; 2,3,4 missing

	mockProcessor := &subtreeprocessor.MockSubtreeProcessor{}
	mockProcessor.On("ReconcileCoinbases", mock.Anything, mock.Anything).
		Return(errors.NewStorageError("store is down"))
	swapSubtreeProcessor(t, items, mockProcessor)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	repairedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))
	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	err := items.blockAssembler.recoverCoinbaseDivergence(ctx, 4)
	require.Error(t, err)

	mockProcessor.AssertNumberOfCalls(t, "ReconcileCoinbases", 3)

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore)
	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))-repairedBefore)
	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore,
		"exhausting the attempt budget must escalate exactly once")
}

// TestRecoverCoinbaseDivergence_NoGapBalancesMetric pins the accounting rule
// stated on the metric: every "detected" gets exactly one follow-up outcome.
// When scoping finds nothing to repair there is neither a repair nor an
// escalation, so without the dedicated "no_gap" outcome the three counters
// would never add up.
func TestRecoverCoinbaseDivergence_NoGapBalancesMetric(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// Every height present: scoping finds no gap at all.
	headers := buildCanonicalChain(ctx, t, items, 3)
	for h := uint32(1); h <= 3; h++ {
		seedCoinbase(ctx, t, items, headers, h)
	}

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	noGapBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("no_gap"))

	require.NoError(t, items.blockAssembler.recoverCoinbaseDivergence(ctx, 3))

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore)
	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("no_gap"))-noGapBefore,
		"a detection that turns out to have nothing to repair must record the no_gap outcome")
}

// TestStartupCoinbaseDivergenceCheck_HoleBelowPresentTip is the regression test
// for the detection gap the review flagged as blocking (ChiR1). The tip's
// coinbase is present, so the old tip-only probe returned early and never ran
// the walk-back that exists precisely to repair this shape: a hole left below a
// healthy-looking tip by the concurrent fast-forward create loop. Undetected,
// that hole wedges the node on the missing coinbase's eventual maturity spend.
func TestStartupCoinbaseDivergenceCheck_HoleBelowPresentTip(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// heights 1..6 present except 3 and 4 -- the tip (6) and 5 are fine, so a
	// tip-only probe sees a healthy node.
	headers := buildCanonicalChain(ctx, t, items, 6)
	for _, h := range []uint32{1, 2, 5, 6} {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[5], 6)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	for h := uint32(1); h <= 6; h++ {
		present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, err)
		require.True(t, present, "coinbase at height %d must be present after startup recovery", h)
	}
}

// TestStartupCoinbaseDivergenceCheck_HoleBeyondWindowNotScanned states the
// honest limit of the widened startup scan: it covers a bounded window below
// the tip (CoinbaseRecoveryMaxGapBlocks heights), not the whole chain. A hole
// deeper than the window is left for the runtime detector that is deliberately
// out of scope for this change, and this test exists so that boundary is
// asserted rather than assumed.
func TestStartupCoinbaseDivergenceCheck_HoleBeyondWindowNotScanned(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3
	// Window of 2 heights: from tip 6 the scan reaches only heights 6 and 5.
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 2

	headers := buildCanonicalChain(ctx, t, items, 6)
	for _, h := range []uint32{1, 2, 4, 5, 6} {
		seedCoinbase(ctx, t, items, headers, h)
	}
	// height 3 is missing and sits below the window.

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a hole below the scan window is not detected at startup - that is the documented boundary")

	present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 3)
	require.NoError(t, err)
	require.False(t, present)
}
