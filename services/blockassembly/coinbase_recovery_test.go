package blockassembly

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
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

// TestPruneReferenceHeight_TracksTheStoreNotBlockAssembly pins the input the
// safety floor is measured against. The DAH pruner stamps delete-at heights
// against the UTXO store's height, which follows the chain tip; block
// assembly's own pointer lags it on any catch-up. Deriving the floor from the
// lagging pointer would reach into heights the pruner may already have emptied.
func TestPruneReferenceHeight_TracksTheStoreNotBlockAssembly(t *testing.T) {
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	// Store ahead of block assembly: the store's height wins.
	require.NoError(t, items.utxoStore.SetBlockHeight(5000))
	require.Equal(t, uint32(5000), items.blockAssembler.pruneReferenceHeight(1000))

	// Block assembly ahead of the store (or the store not yet initialised):
	// block assembly's height is the lower bound and wins.
	require.Equal(t, uint32(9000), items.blockAssembler.pruneReferenceHeight(9000))
}

// TestScopeCoinbaseGap_FloorFollowsStoreHeightWhenBlockAssemblyLags is the
// behavioural half of the same point (ChiR5). Block assembly restarts a long
// way behind the chain, so every height in its own tip window is old enough to
// have been spent and pruned. Scoped against block assembly's pointer the walk
// would happily collect those heights and hand them to the repair, which would
// write already-spent coinbase outputs back into the UTXO set as unspent.
// Scoped against the store's height the floor lands above the whole window, so
// the walk collects nothing at all.
//
// It returns an empty gap rather than an unproven-floor error on purpose: a
// lagging block assembler is an ordinary catch-up, and raising the
// manual-intervention alarm on every restart during one would be crying wolf.
func TestScopeCoinbaseGap_FloorFollowsStoreHeightWhenBlockAssemblyLags(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	// Maturity 3: from a block assembly tip of 6 the floor would be height 4,
	// so heights 4, 5, 6 look probeable.
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(3))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 5) // floor would be proven at height 5

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	// Control: with the store level with block assembly, height 6 scopes fine.
	require.NoError(t, items.utxoStore.SetBlockHeight(6))

	gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
	require.NoError(t, err)
	require.Equal(t, []uint32{6}, gapHeights(gap))

	// Now the chain has moved on while block assembly is still at 6. Everything
	// at or below height 6 is past maturity from the store's point of view, so
	// no height in the window is provably unpruned.
	require.NoError(t, items.utxoStore.SetBlockHeight(1000))

	gap, err = items.blockAssembler.scopeCoinbaseGap(ctx, 6)
	require.NoError(t, err)
	require.Empty(t, gap, "no prunable height may be handed to the repair")
}

// TestStartupCoinbaseDivergenceCheck_SkippedWhenBlockAssemblyBelowFloor covers
// the startup half of ChiR5: when the store height puts the whole scan window
// below the maturity floor there is nothing safe to probe, so the scan must
// record no detection at all rather than reporting pruned heights as diverged.
func TestStartupCoinbaseDivergenceCheck_SkippedWhenBlockAssemblyBelowFloor(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(3))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// Nothing seeded: every height would look "missing" to a scan that ran.
	headers := buildCanonicalChain(ctx, t, items, 6)
	items.blockAssembler.setBestBlockHeader(headers[5], 6)
	require.NoError(t, items.utxoStore.SetBlockHeight(1000))

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a window that is entirely prunable must not be reported as a divergence")
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
// where maturity is zero, and pins the direction it fails in (ChiR7).
//
// With maturity zero a coinbase is spendable the moment it is created, so no
// height is provably unspent and none of them can be safely re-created. This
// test previously asserted a floor of 1 -- the widest possible window, which
// would have let the repair resurrect any pruned coinbase on the chain. The
// safe answer is to refuse every height instead.
func TestCoinbaseRepairFloor_MaturityUnset(t *testing.T) {
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(0))
	require.NotNil(t, items)

	require.Equal(t, uint32(1001), items.blockAssembler.coinbaseRepairFloor(1000),
		"a floor above the tip admits no height, which is the safe answer when nothing can be proven immature")

	// Saturating rather than wrapping matters: tip+1 at the top of the range
	// would be 0, which reads as "no floor" and opens the whole chain.
	require.Equal(t, uint32(math.MaxUint32), items.blockAssembler.coinbaseRepairFloor(math.MaxUint32))
}

// TestScopeCoinbaseGap_MaturityUnsetRepairsNothing is the behavioural half of
// ChiR7: with maturity zero the walk must collect nothing at all, whatever the
// chain looks like.
func TestScopeCoinbaseGap_MaturityUnsetRepairsNothing(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(0))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 1) // 2..6 missing

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	gap, err := items.blockAssembler.scopeCoinbaseGap(ctx, 6)
	require.NoError(t, err)
	require.Empty(t, gap, "with maturity zero nothing is provably unspent, so nothing may be repaired")
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

// TestRecoverCoinbaseDivergence_CancellationAbortsWithoutAlarm covers ChiR10.
// A context cancelled while recovery is retrying used to fall through to the
// escalation path, so a node stopped mid-boot emitted the loudest signal the
// system has -- MANUAL INTERVENTION REQUIRED, naming resetblockassembly -- for
// an entirely ordinary shutdown. Cancellation is now its own outcome.
func TestRecoverCoinbaseDivergence_CancellationAbortsWithoutAlarm(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(t.Context())
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(t.Context(), t, items, 4)
	seedCoinbase(t.Context(), t, items, headers, 1) // floor good; 2,3,4 missing

	// The first repair fails as a transient error would, and the shutdown lands
	// while recovery is waiting to try again.
	mockProcessor := &subtreeprocessor.MockSubtreeProcessor{}
	mockProcessor.On("ReconcileCoinbases", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(errors.NewStorageError("store went away during shutdown"))
	swapSubtreeProcessor(t, items, mockProcessor)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	abortedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("aborted"))
	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	err := items.blockAssembler.recoverCoinbaseDivergence(ctx, 4)
	require.Error(t, err)
	require.True(t, errors.IsContextError(err), "the real reason must be reported, not a generic unrecoverable-divergence error")

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore)
	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("aborted"))-abortedBefore,
		"a cancelled recovery records its own outcome so the metric still balances")
	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore,
		"a shutdown must not raise the manual-intervention alarm")
}

// TestStartupCoinbaseDivergenceCheck_CancellationIsNotReportedAsIntervention is
// the caller-side half of ChiR10: the startup hook must stay non-fatal and must
// not restate a shutdown as a call to action.
func TestStartupCoinbaseDivergenceCheck_CancellationIsNotReportedAsIntervention(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(t.Context())
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(t.Context(), t, items, 4)
	seedCoinbase(t.Context(), t, items, headers, 1)
	items.blockAssembler.setBestBlockHeader(headers[3], 4)

	mockProcessor := &subtreeprocessor.MockSubtreeProcessor{}
	mockProcessor.On("ReconcileCoinbases", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(errors.NewStorageError("store went away during shutdown"))
	swapSubtreeProcessor(t, items, mockProcessor)

	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))
	abortedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("aborted"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx),
		"the startup hook stays non-fatal even when the context is cancelled under it")

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("aborted"))-abortedBefore)
	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore)
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

// TestStartupCoinbaseDivergenceCheck_RepairsSecondClusterBelowTheFirst is the
// regression test for ChiR6. The startup scan used to stop the moment one
// recovery pass had run, but that pass only proves
// CoinbaseRecoveryConsecutiveGood present coinbases beneath the gap it closed.
// A second hole separated from the first by that many present heights was
// therefore never looked for, and being startup-only the detector would not
// come back to it until the next reboot -- converging one cluster per restart
// with nothing forcing those restarts, while the unrepaired coinbase re-wedged
// the tip on its maturity spend.
//
// This uses the default-shaped ConsecutiveGood of 6 and two clusters with
// exactly six present heights between them, which is the case a single-cluster
// test with ConsecutiveGood=1 cannot reach.
func TestStartupCoinbaseDivergenceCheck_RepairsSecondClusterBelowTheFirst(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 6
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// Heights 1..20. Missing: 18 (upper cluster) and 11 (lower cluster), with
	// the six present heights 12..17 between them. Recovery triggered at 18
	// sees 17,16,15,14,13,12 present, declares its floor there and stops, so
	// the hole at 11 survives unless the scan keeps walking below the repair.
	const chainLen = 20

	missing := map[uint32]bool{18: true, 11: true}

	headers := buildCanonicalChain(ctx, t, items, chainLen)

	for h := uint32(1); h <= chainLen; h++ {
		if !missing[h] {
			seedCoinbase(ctx, t, items, headers, h)
		}
	}

	items.blockAssembler.setBestBlockHeader(headers[chainLen-1], chainLen)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	for h := uint32(1); h <= chainLen; h++ {
		present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, err)
		require.True(t, present, "coinbase at height %d must be present after startup recovery", h)
	}

	require.Equal(t, float64(2),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"both clusters must be detected and repaired in a single boot")
}

// TestStartupCoinbaseDivergenceCheck_StopsScanningAfterARefusal is the other
// half of ChiR6: a refusal is structural, so the same walk-back from a lower
// height would refuse for the same reason. The scan must stop rather than
// repeat the manual-intervention alarm once per missing height.
func TestStartupCoinbaseDivergenceCheck_StopsScanningAfterARefusal(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 1
	// CoinbaseRecoveryMaxGapBlocks does double duty: it caps the walk-back AND
	// sets the startup scan window. A cap of 1 would refuse as intended but also
	// give the loop a window of one height, so the loop body would run exactly
	// once whatever the refusal did -- deleting the refusal return would leave
	// this test green. Two heights is the smallest window that lets the scan
	// reach a second missing height, so escalated == 1 can only hold because the
	// refusal stopped it.
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 2

	// Heights 2..6 all missing above a good floor at 1. With a gap cap of 2 the
	// walk-back refuses, and it would refuse identically from 5, 4, 3 and 2.
	headers := buildCanonicalChain(ctx, t, items, 6)
	seedCoinbase(ctx, t, items, headers, 1)

	items.blockAssembler.setBestBlockHeader(headers[5], 6)

	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore,
		"a structural refusal must be reported once, not once per missing height")
}

// TestStartupCoinbaseDivergenceCheck_SkippedWhenTipIsNotCanonical covers ChiR8.
// Block assembly restarts on a branch the chain has since reorged away from.
// Every canonical height then resolves to a block whose coinbase block assembly
// legitimately never created, so a scan would report divergence everywhere and
// -- for a fork deeper than the consecutive-good window -- end in a false
// MANUAL INTERVENTION alarm for something the ordinary reconcile handles.
func TestStartupCoinbaseDivergenceCheck_SkippedWhenTipIsNotCanonical(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// The canonical chain is 1..6 with nothing seeded, so a scan that ran would
	// find every height missing.
	headers := buildCanonicalChain(ctx, t, items, 6)

	// Block assembly's tip is a block at height 6 that is not the canonical one
	// there -- the shape left by a restart on an abandoned branch.
	offBranchTip := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  headers[4].Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          9999,
		Bits:           *bits,
	}
	require.False(t, offBranchTip.Hash().IsEqual(headers[5].Hash()))

	items.blockAssembler.setBestBlockHeader(offBranchTip, 6)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a block assembler that is not on the canonical chain at its own tip must defer to the normal reconcile")

	// Control: put block assembly back on the canonical tip and the same chain
	// is scanned and reported, proving the skip came from the branch check.
	items.blockAssembler.setBestBlockHeader(headers[5], 6)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Greater(t,
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		float64(0), "the same chain is detected once block assembly is on the canonical tip")
}

// interposedBlockchainClient wraps the real sqlitememory-backed blockchain
// client and perturbs GetBlockByHeight for specific heights, so tests can
// exercise the startup scan's reaction to a transient failure or to a height
// the canonical chain has no block for.
//
// This is not a mocked blockchain client in the sense AGENTS.md rules out: the
// real client and store answer every call that is not deliberately perturbed,
// and the chain semantics under test stay real. It exists because neither a
// transient store blip nor a hole in the canonical chain can be produced from
// a healthy sqlitememory store.
type interposedBlockchainClient struct {
	blockchain.ClientI

	// failuresLeft is decremented on each intercepted call; while positive the
	// call returns a transient-looking error instead of reaching the real client.
	failuresLeft int

	// neverFailHeights are exempt from failuresLeft, so a test can let the
	// canonical-tip check through and aim the transient failures at the scan
	// loop beneath it.
	neverFailHeights map[uint32]bool

	// absentHeights always return a block-not-found error.
	absentHeights map[uint32]bool

	// cancelAtHeight, when non-nil, cancels the scan's context on the lookup for
	// that height, standing in for a shutdown that lands mid-probe.
	cancelAtHeight *uint32
	cancel         context.CancelFunc
}

func (c *interposedBlockchainClient) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	if c.absentHeights[height] {
		return nil, errors.NewBlockNotFoundError("no canonical block at height %d", height)
	}

	if c.cancelAtHeight != nil && *c.cancelAtHeight == height {
		c.cancel()

		return nil, errors.NewProcessingError("shutting down during the lookup at height %d", height, context.Canceled)
	}

	if c.failuresLeft > 0 && !c.neverFailHeights[height] {
		c.failuresLeft--

		return nil, errors.NewStorageError("transient blockchain-client blip at height %d", height)
	}

	return c.ClientI.GetBlockByHeight(ctx, height)
}

// capturingLogger records the Warnf and Errorf lines a test produces. Several of
// the paths below differ from each other only in which line they emit -- they all
// return nil and leave the metrics alone -- so the log line is the only thing a
// test can assert on.
type capturingLogger struct {
	ulogger.Logger

	mu    sync.Mutex
	warns []string
	errs  []string
}

func newCapturingLogger() *capturingLogger {
	return &capturingLogger{Logger: ulogger.TestLogger{}}
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errs = append(l.errs, fmt.Sprintf(format, args...))
}

// contains reports whether any captured line of the given kind holds substr.
func (l *capturingLogger) contains(lines []string, substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range lines {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

func (l *capturingLogger) sawWarn(substr string) bool {
	return l.contains(l.warns, substr)
}

func (l *capturingLogger) sawError(substr string) bool {
	return l.contains(l.errs, substr)
}

// notFoundTranslatingUtxoStore reports a missing record with errors.ErrNotFound
// instead of errors.ErrTxNotFound, which is how some UTXO-store backends answer.
// Before the absent-check was widened to accept both codes, canonicalCoinbaseAt
// wrapped this in a processing error whose chain still matched ErrNotFound, so
// canonicalBlockAbsent swallowed it and the height was skipped rather than
// repaired -- i.e. the feature silently did nothing on those backends.
type notFoundTranslatingUtxoStore struct {
	utxoStore.Store
}

func (s *notFoundTranslatingUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	data, err := s.Store.Get(ctx, hash, f...)
	if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
		// Deliberately not wrapping the original: the point is a backend whose
		// only signal is ErrNotFound. Wrapping would leave an ErrTxNotFound in
		// the chain for the old, narrower check to match on.
		return nil, errors.NewNotFoundError("record not found for %s", hash.String())
	}

	return data, err
}

// TestStartupCoinbaseDivergenceCheck_ProbeRetriesThroughATransientBlip covers
// the first half of ChiR9. The startup probe sat outside every retry loop, so
// one transient failure abandoned the scan for the whole boot -- and because
// detection is startup-only, that meant no detection at all until the next
// restart.
func TestStartupCoinbaseDivergenceCheck_ProbeRetriesThroughATransientBlip(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// heights 1..4, with 3 missing its coinbase.
	headers := buildCanonicalChain(ctx, t, items, 4)
	for _, h := range []uint32{1, 2, 4} {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[3], 4)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	// One transient failure, which lands on the canonical-tip check that opens
	// the scan. That check draws on the same budget as the probes, so it is not
	// a single point of failure either.
	items.blockAssembler.blockchainClient = &interposedBlockchainClient{
		ClientI:      items.blockchainClient,
		failuresLeft: 1,
	}

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 3)
	require.NoError(t, err)
	require.True(t, present, "a single transient probe failure must not cost the whole scan")
}

// TestStartupCoinbaseDivergenceCheck_SkipsHeightWithNoCanonicalBlock covers the
// second half of ChiR9. canonicalCoinbaseAt turns "the chain has no block at
// this height" into a hard error, which used to abandon the scan. That says
// nothing about the health of the client, so the scan must skip the height and
// carry on checking the ones below it.
func TestStartupCoinbaseDivergenceCheck_SkipsHeightWithNoCanonicalBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// heights 1..5, with 2 missing its coinbase, below a height (4) the
	// interposer reports as having no canonical block.
	headers := buildCanonicalChain(ctx, t, items, 5)
	for _, h := range []uint32{1, 3, 5} {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[4], 5)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	items.blockAssembler.blockchainClient = &interposedBlockchainClient{
		ClientI:       items.blockchainClient,
		absentHeights: map[uint32]bool{4: true},
	}

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 2)
	require.NoError(t, err)
	require.True(t, present, "a height the chain has no block for must not abandon the scan below it")
}

// TestStartupCoinbaseDivergenceCheck_TipAboveCanonicalChainHeight covers the
// trigger ChiR9 called out as the likeliest one: block assembly's persisted tip
// height sits above the canonical chain height, so the very first lookup gets a
// not-found.
//
// That used to be treated as the blockchain client being unable to answer,
// which abandoned the whole scan with a Warnf and left the boot with no
// detection at all. It is now recognised as a question about a height the chain
// has no block for. Here it is the tip itself, so the canonical-tip check
// (ChiR8) ends the scan first -- block assembly is by definition not on the
// canonical chain at a height the canonical chain does not reach. The point of
// this test is that the outcome is a clean, quiet skip rather than an
// abandoned scan, a false divergence, or a nil dereference.
func TestStartupCoinbaseDivergenceCheck_TipAboveCanonicalChainHeight(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// The canonical chain reaches height 3; block assembly thinks it is at 6.
	headers := buildCanonicalChain(ctx, t, items, 3)
	for h := uint32(1); h <= 3; h++ {
		seedCoinbase(ctx, t, items, headers, h)
	}

	aheadOfChain := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  headers[2].Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4242,
		Bits:           *bits,
	}
	items.blockAssembler.setBestBlockHeader(aheadOfChain, 6)

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	escalatedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))

	require.NotPanics(t, func() {
		require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))
	})

	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a tip above the canonical chain height is not a divergence")
	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated"))-escalatedBefore,
		"and it must not raise the manual-intervention alarm")
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

// TestStartupCoinbaseDivergenceCheck_ProbeBudgetExhaustionStopsTheScan covers
// ChiR13, which is the probe-budget half of the point @oskarszoon made about the
// retry/escalation branch being untested one level up.
//
// coinbaseRecoveryProbeRetries bounds how much a flapping store can add to boot
// time. Every existing test drove exactly one transient failure, which the budget
// always absorbed, so the exhausted branch never ran and a bound of two was
// indistinguishable from an unbounded retry loop.
//
// Here the store fails five times at height 5 -- more than the budget can absorb.
// A bounded scan gives up at 5 and never reaches the genuinely missing coinbase at
// height 4. An unbounded one would ride out the fifth failure and repair it, so
// height 4 staying absent is what pins the bound.
func TestStartupCoinbaseDivergenceCheck_ProbeBudgetExhaustionStopsTheScan(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// Heights 1..6 with 4 missing its coinbase. Height 6 (the tip) is present so
	// the scan gets past it and reaches the failing height 5.
	headers := buildCanonicalChain(ctx, t, items, 6)
	for _, h := range []uint32{1, 2, 3, 5, 6} {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[5], 6)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	logger := newCapturingLogger()
	items.blockAssembler.logger = logger

	// Height 6 is exempt so the canonical-tip check, which draws on the same
	// budget, does not consume it before the scan starts.
	items.blockAssembler.blockchainClient = &interposedBlockchainClient{
		ClientI:          items.blockchainClient,
		failuresLeft:     5,
		neverFailHeights: map[uint32]bool{6: true},
	}

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx),
		"the startup hook stays non-fatal when the store outlasts the probe budget")

	require.True(t, logger.sawError("startup divergence check abandoned at height 5"),
		"exhausting the probe budget must abandon the scan and say so")

	require.Equal(t, float64(0),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a scan that never got past the failing height cannot have detected anything")

	// Restore a healthy client to check the state the scan left behind.
	items.blockAssembler.blockchainClient = items.blockchainClient

	present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 4)
	require.NoError(t, err)
	require.False(t, present,
		"the scan must have stopped at the failing height, not retried past it to the hole below")
}

// TestStartupCoinbaseDivergenceCheck_RepairsWhenTheStoreSaysErrNotFound is the
// store-side half of ChiR13. Different UTXO-store backends report a missing
// record with different codes. Until the absent-check accepted ErrNotFound as
// well as ErrTxNotFound, a backend using the former had its "missing" answer
// wrapped in a processing error that canonicalBlockAbsent then read as "the chain
// has no block at this height" -- so every height was skipped and the whole
// feature silently did nothing on that backend.
func TestStartupCoinbaseDivergenceCheck_RepairsWhenTheStoreSaysErrNotFound(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	// Heights 1..4 with 3 missing its coinbase.
	headers := buildCanonicalChain(ctx, t, items, 4)
	for _, h := range []uint32{1, 2, 4} {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[3], 4)
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	// Only the detection path sees the translated code; the repair writes through
	// the real store underneath, which is where the assertion reads from.
	items.blockAssembler.utxoStore = &notFoundTranslatingUtxoStore{Store: items.utxoStore}

	detectedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))
	repairedBefore := testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected"))-detectedBefore,
		"a store that reports absence as ErrNotFound must still register as a divergence")
	require.Equal(t, float64(1),
		testutil.ToFloat64(prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired"))-repairedBefore)

	repaired, err := items.utxoStore.Get(ctx, coinbaseTxForHeader(t, headers[2]).TxIDChainHash(), fields.Tx)
	require.NoError(t, err, "the coinbase at height 3 must have been created in the real store")
	require.NotNil(t, repaired.Tx)
}

// TestStartupCoinbaseDivergenceCheck_ShutdownDuringAProbeIsNotReportedAsLostDetection
// covers the scan-loop half of ChiR14. ChiR10 taught the recovery call site to
// tell a shutdown apart from a failure, but a cancellation landing on a probe (or
// on withProbeRetry's backoff) still fell through to the line claiming no
// coinbase-divergence detection would run until the next restart -- which is a
// false alarm for a node that is simply stopping.
func TestStartupCoinbaseDivergenceCheck_ShutdownDuringAProbeIsNotReportedAsLostDetection(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(t.Context())
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(t.Context(), t, items, 5)
	for h := uint32(1); h <= 5; h++ {
		seedCoinbase(t.Context(), t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[4], 5)

	logger := newCapturingLogger()
	items.blockAssembler.logger = logger

	// The tip check at height 5 goes through; the shutdown lands on the next
	// probe down.
	cancelAt := uint32(4)
	items.blockAssembler.blockchainClient = &interposedBlockchainClient{
		ClientI:        items.blockchainClient,
		cancelAtHeight: &cancelAt,
		cancel:         cancel,
	}

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.False(t, logger.sawError("no coinbase-divergence detection will run until the next restart"),
		"a node that is shutting down has not lost its detection coverage")
}

// TestStartupCoinbaseDivergenceCheck_ShutdownDuringTheTipCheckIsNotBlamedOnTheClient
// is the other half of ChiR14. A cancellation on the canonical-tip lookup used to
// surface as the generic "cannot resolve the canonical block at block assembly's
// tip height" warning, pointing an operator at the blockchain client for something
// that was only a shutdown.
func TestStartupCoinbaseDivergenceCheck_ShutdownDuringTheTipCheckIsNotBlamedOnTheClient(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(t.Context())
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(t.Context(), t, items, 3)
	for h := uint32(1); h <= 3; h++ {
		seedCoinbase(t.Context(), t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[2], 3)

	logger := newCapturingLogger()
	items.blockAssembler.logger = logger

	cancelAt := uint32(3)
	items.blockAssembler.blockchainClient = &interposedBlockchainClient{
		ClientI:        items.blockchainClient,
		cancelAtHeight: &cancelAt,
		cancel:         cancel,
	}

	require.NoError(t, items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx))

	require.False(t, logger.sawWarn("cannot resolve the canonical block at block assembly's tip height"),
		"a shutdown on the tip lookup must not be diagnosed as the blockchain client failing")
}
