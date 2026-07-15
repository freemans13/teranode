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
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

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
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// height 1: canonical block carries cb1, and the store holds cb1 -> present.
	cb1 := coinbaseTxForHeader(t, blockHeader1)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader1, cb1)

	_, err := items.utxoStore.Create(ctx, cb1, 1)
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

	_, err := items.utxoStore.Create(ctx, coinbaseTx, height)
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
		items := setupBlockAssemblyTestWithUtxoStore(t)
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
		items := setupBlockAssemblyTestWithUtxoStore(t)
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
		items := setupBlockAssemblyTestWithUtxoStore(t)
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
// above a proven-good floor (height 1); since the test-helper-built blocks
// carry no subtrees (see addCanonicalBlockWithCoinbase), HasConflictingNodes
// finds nothing to conflict on, so Stage 1 auto-repair must succeed on the
// first attempt and recoverCoinbaseDivergence must return nil.
func TestRecoverCoinbaseDivergence_RepairsGapNoConflicts(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 1
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts = 3

	headers := buildCanonicalChain(ctx, t, items, 4)
	seedCoinbase(ctx, t, items, headers, 1) // floor good; 2,3,4 missing
	items.blockAssembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { items.blockAssembler.subtreeProcessor.Stop(context.Background()) })

	require.NoError(t, items.blockAssembler.recoverCoinbaseDivergence(ctx, 4))

	for h := uint32(2); h <= 4; h++ {
		present, _, err := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, err)
		require.True(t, present, "coinbase at height %d must be repaired", h)
	}
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
	items := setupBlockAssemblyTestWithUtxoStore(t)
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
	items := setupBlockAssemblyTestWithUtxoStore(t)
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

	err := items.blockAssembler.recoverCoinbaseDivergence(ctx, 6)
	require.Error(t, err)

	// The gap was never repaired -- heights 2..6 must still be absent.
	for h := uint32(2); h <= 6; h++ {
		present, _, presErr := items.blockAssembler.canonicalCoinbaseAt(ctx, h)
		require.NoError(t, presErr)
		require.False(t, present, "coinbase at height %d must remain unrepaired after escalation", h)
	}
}
