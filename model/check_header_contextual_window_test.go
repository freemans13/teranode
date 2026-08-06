package model

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// buildLinkedChain returns count properly linked headers in ascending height
// order (oldest first), with strictly increasing timestamps baseTime,
// baseTime+1, ... The first header's parent is the all-zero hash.
func buildLinkedChain(t *testing.T, count int, baseTime uint32) []*BlockHeader {
	t.Helper()

	bits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	chain := make([]*BlockHeader, count)
	prev := &chainhash.Hash{}

	for i := 0; i < count; i++ {
		chain[i] = &BlockHeader{
			Version:        1,
			HashPrevBlock:  prev,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + uint32(i),
			Bits:           *bits,
			Nonce:          uint32(i),
		}
		prev = chain[i].Hash()
	}

	return chain
}

// newestFirst returns a reversed copy: index 0 is the highest header — the
// order blockchainClient.GetBlockHeaders returns and every block-validation
// call site passes to CheckHeaderContextual.
func newestFirst(chain []*BlockHeader) []*BlockHeader {
	out := make([]*BlockHeader, len(chain))
	for i, h := range chain {
		out[len(chain)-1-i] = h
	}

	return out
}

// regtestGenesisParentChain returns the regtest genesis header as a
// single-element parent chain for the block1/block1Header fixtures (regtest
// block 1), self-verifying that it anchors at the given child's parent —
// CheckHeaderContextual now requires the chain to be anchored at the block's
// parent (issue 1467).
func regtestGenesisParentChain(t *testing.T, child *BlockHeader) []*BlockHeader {
	t.Helper()

	genesisBytes, err := hex.DecodeString("0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494dffff7f2002000000")
	require.NoError(t, err)

	genesis, err := NewBlockHeaderFromBytes(genesisBytes)
	require.NoError(t, err)
	require.True(t, genesis.Hash().IsEqual(child.HashPrevBlock), "regtest genesis must be the fixture block's parent")

	return []*BlockHeader{genesis}
}

// buildChildBlock returns a coinbase-only block whose header carries the given
// timestamp and is anchored on parent. Height 1 keeps it below every
// BIP34/66/65 activation so the version floor never interferes.
func buildChildBlock(t *testing.T, parent *BlockHeader, timestamp uint32) *Block {
	t.Helper()

	bits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &BlockHeader{
		Version:        1,
		HashPrevBlock:  parent.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      timestamp,
		Bits:           *bits,
		Nonce:          1,
	}

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	block, err := NewBlock(header, coinbase, []*chainhash.Hash{}, 1, 123, 1, 0)
	require.NoError(t, err)

	return block
}

// TestCheckHeaderContextualMedianWindow pins WHICH 11 headers the
// median-time-past rule is evaluated against: the 11 ending at the block's
// parent. Block validation passes the parent chain newest-first (the
// GetBlockHeaders order), and taking a tail slice of that run silently
// evaluated the rule against the OLDEST headers of the run — a median ~90
// blocks (~15 hours on mainnet) older than consensus requires, accepting
// past-stamped blocks that SV Node rejects (issue 1467).
func TestCheckHeaderContextualMedianWindow(t *testing.T) {
	logger := ulogger.TestLogger{}

	// Mainnet params: GenerateSupported == false makes an MTP violation a hard
	// error instead of a warning.
	newSettings := func(t *testing.T) *settings.Settings {
		tSettings := test.CreateBaseTestSettings(t)
		mainParams := chaincfg.MainNetParams
		tSettings.ChainCfgParams = &mainParams

		return tSettings
	}

	// A 100-header chain entirely in the past (newest header ~2h old), exactly
	// the window size block validation fetches. The true median-time-past —
	// over the 11 headers ending at the parent — is baseTime+94; the median of
	// the oldest 11 headers of the run is baseTime+5.
	baseTime := uint32(time.Now().Add(-2 * time.Hour).Unix())
	ascending := buildLinkedChain(t, 100, baseTime)
	parent := ascending[len(ascending)-1]
	trueMedianTimePast := baseTime + 94

	t.Run("timestamp below the parent-anchored median is rejected, newest-first order", func(t *testing.T) {
		// Above the oldest-11 median (baseTime+5) but at the true MTP: under
		// the old tail-slice selection this block was accepted; consensus
		// requires rejecting it.
		block := buildChildBlock(t, parent, trueMedianTimePast)

		err := block.CheckHeaderContextual(newestFirst(ascending), newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "median time past")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("timestamp above the parent-anchored median is accepted, newest-first order", func(t *testing.T) {
		block := buildChildBlock(t, parent, trueMedianTimePast+1)

		require.NoError(t, block.CheckHeaderContextual(newestFirst(ascending), newSettings(t), logger))
	})

	t.Run("oldest-first order selects the same parent-anchored window", func(t *testing.T) {
		rejected := buildChildBlock(t, parent, trueMedianTimePast)
		err := rejected.CheckHeaderContextual(ascending, newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "median time past")

		accepted := buildChildBlock(t, parent, trueMedianTimePast+1)
		require.NoError(t, accepted.CheckHeaderContextual(ascending, newSettings(t), logger))
	})

	t.Run("chain not anchored at the block's parent is a processing error, not an invalid block", func(t *testing.T) {
		block := buildChildBlock(t, parent, trueMedianTimePast+1)

		// Drop the parent from the run: neither end matches the block's
		// HashPrevBlock. The block must not be marked invalid — the supplied
		// context is wrong, not the block.
		err := block.CheckHeaderContextual(newestFirst(ascending)[1:], newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "anchored")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("short chain uses all available headers, both orders", func(t *testing.T) {
		shortAscending := buildLinkedChain(t, 5, baseTime)
		shortParent := shortAscending[len(shortAscending)-1]
		shortMedian := baseTime + 2 // median of baseTime..baseTime+4

		rejected := buildChildBlock(t, shortParent, shortMedian)
		err := rejected.CheckHeaderContextual(newestFirst(shortAscending), newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "median time past")

		accepted := buildChildBlock(t, shortParent, shortMedian+1)
		require.NoError(t, accepted.CheckHeaderContextual(newestFirst(shortAscending), newSettings(t), logger))
		require.NoError(t, accepted.CheckHeaderContextual(shortAscending, newSettings(t), logger))
	})

	t.Run("single-header chain anchors at genesis", func(t *testing.T) {
		single := buildLinkedChain(t, 1, baseTime)

		rejected := buildChildBlock(t, single[0], baseTime)
		err := rejected.CheckHeaderContextual(single, newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "median time past")

		accepted := buildChildBlock(t, single[0], baseTime+1)
		require.NoError(t, accepted.CheckHeaderContextual(single, newSettings(t), logger))
	})

	t.Run("header with no parent hash is a processing error, not a panic", func(t *testing.T) {
		block := buildChildBlock(t, parent, trueMedianTimePast+1)
		block.Header.HashPrevBlock = nil

		// chainhash.Hash.String() has a value receiver, so formatting a block whose
		// HashPrevBlock is nil panics; the anchoring check must reject before it gets there.
		err := block.CheckHeaderContextual(newestFirst(ascending), newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no parent hash")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})
}

// TestCheckHeaderContextualWindowIntegrity pins the two guarantees SV Node gets
// for free and Teranode has to enforce by hand.
//
// SV Node's CBlockIndex::GetMedianTimePast (src/block_index.h) walks pprev
// pointers from the parent, so its window is linked by construction and can only
// be shorter than 11 when the chain genuinely ends at genesis
// ("i < nMedianTimeSpan && pindex"). CheckHeaderContextual takes a
// caller-supplied slice instead, so a run can be unlinked, or short while the
// chain behind it is long — both of which silently produce a median that is not
// the consensus one.
func TestCheckHeaderContextualWindowIntegrity(t *testing.T) {
	logger := ulogger.TestLogger{}

	newSettings := func(t *testing.T) *settings.Settings {
		tSettings := test.CreateBaseTestSettings(t)
		mainParams := chaincfg.MainNetParams
		tSettings.ChainCfgParams = &mainParams

		return tSettings
	}

	baseTime := uint32(time.Now().Add(-2 * time.Hour).Unix())
	ascending := buildLinkedChain(t, 100, baseTime)

	// Comfortably above every timestamp in the chain and still in the past, so
	// only a window-integrity failure can produce an error.
	const futureEnough = 200

	t.Run("forward-ordered run starting at the parent is refused", func(t *testing.T) {
		// [parent, child, grandchild, ...]: the parent sits at index 0, so the
		// newest-first case matches, but the 10 headers after it are the parent's
		// DESCENDANTS. Anchoring alone cannot tell this from a real newest-first
		// run — only linkage can.
		parent := ascending[50]
		block := buildChildBlock(t, parent, baseTime+futureEnough)

		err := block.CheckHeaderContextual(ascending[50:], newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a linked chain")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("broken linkage inside the window is refused", func(t *testing.T) {
		parent := ascending[len(ascending)-1]
		block := buildChildBlock(t, parent, baseTime+futureEnough)

		unrelated := buildLinkedChain(t, 1, baseTime+5000)

		run := newestFirst(ascending)
		spliced := make([]*BlockHeader, len(run))
		copy(spliced, run)
		spliced[5] = unrelated[0]

		err := block.CheckHeaderContextual(spliced, newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a linked chain")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("short run that does not reach genesis is refused", func(t *testing.T) {
		// Five headers from the middle of a long chain. SV Node could never
		// produce this: it stops early only at genesis, so fewer than 11 samples
		// always means the chain really is that short.
		parent := ascending[len(ascending)-1]
		block := buildChildBlock(t, parent, baseTime+futureEnough)

		err := block.CheckHeaderContextual(newestFirst(ascending)[:5], newSettings(t), logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not reach genesis")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("short run reaching genesis is accepted, both orders", func(t *testing.T) {
		// buildLinkedChain roots at the all-zero parent hash, so this really is a
		// five-block chain and the shorter window is the consensus one.
		shortAscending := buildLinkedChain(t, 5, baseTime)
		shortParent := shortAscending[len(shortAscending)-1]

		block := buildChildBlock(t, shortParent, baseTime+futureEnough)
		require.NoError(t, block.CheckHeaderContextual(newestFirst(shortAscending), newSettings(t), logger))
		require.NoError(t, block.CheckHeaderContextual(shortAscending, newSettings(t), logger))
	})

	t.Run("full-length runs are unaffected by the genesis rule, both orders", func(t *testing.T) {
		parent := ascending[len(ascending)-1]
		block := buildChildBlock(t, parent, baseTime+futureEnough)

		require.NoError(t, block.CheckHeaderContextual(newestFirst(ascending), newSettings(t), logger))
		require.NoError(t, block.CheckHeaderContextual(ascending, newSettings(t), logger))
	})
}
