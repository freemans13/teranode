package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildQuickPathSubtrees returns two subtree slices of capacity 2 whose leaves are the
// given transaction hashes, with a coinbase placeholder occupying the first position of
// the first subtree — the shape validateSubtrees sees on the quick-validation path.
func buildQuickPathSubtrees(t *testing.T, second, third chainhash.Hash) []*subtreepkg.Subtree {
	t.Helper()

	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(second, 1, 100))

	last, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, last.AddNode(third, 1, 100))

	return []*subtreepkg.Subtree{first, last}
}

// newQuickPathBlock wraps the given subtree slices in the minimum viable block for
// validateSubtrees: a header (for block.Hash() and the merkle-root comparison), a real
// coinbase, and matching Subtrees/SubtreeSlices lengths. The header's merkle root is
// deliberately unrelated to the body, so a block that gets past the duplicate check fails
// on the merkle root — which is what the second sub-test asserts.
func newQuickPathBlock(t *testing.T, slices []*subtreepkg.Subtree) *model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, len(slices))
	for i, st := range slices {
		subtreeHashes[i] = st.RootHash()
	}

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1,
			Bits:           *nBits,
			Nonce:          0,
		},
		CoinbaseTx:    coinbase,
		Subtrees:      subtreeHashes,
		SubtreeSlices: slices,
	}
}

// TestValidateSubtrees_RejectsDuplicateTransaction pins the CVE-2012-2459 duplicate check
// this PR adds to the quick-validation path (issue 1424). That path is default-on for every
// block at or below the highest checkpoint and previously ran no duplicate check at all,
// while the merkle root provably cannot detect the mutation.
//
// A package coverage profile showed the rejection branch had zero coverage: the existing
// quick-path tests execute the call but never drive a duplicate through it, so removing the
// check entirely left every test in the package passing. This test closes that gap — it
// fails if the check is removed or moved after CheckMerkleRoot.
//
// It calls validateSubtrees directly because that is the single seam all three quick-path
// pipelines funnel through, and it needs no BlockValidation dependencies: the function
// reads only the block's subtree slices.
func TestValidateSubtrees_RejectsDuplicateTransaction(t *testing.T) {
	ctx := context.Background()

	txA := chainhash.Hash{0x01}
	txB := chainhash.Hash{0x02}

	u := &BlockValidation{}

	t.Run("duplicate transaction is rejected", func(t *testing.T) {
		// txA appears in both subtrees — the duplicate a mutated body carries.
		block := newQuickPathBlock(t, buildQuickPathSubtrees(t, txA, txA))

		_, err := u.validateSubtrees(ctx, block, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.Contains(t, err.Error(), "CVE-2012-2459")

		// The error must reach the caller as a consensus rejection, not a transient
		// processing fault: the legacy unified route returns it straight out with no
		// fallback, and peer punishment keys on ErrBlockInvalid.
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a duplicate on the quick path must be BlockInvalid, so the peer is punished")
		require.False(t, errors.Is(err, errors.ErrProcessing),
			"it must not be wrapped as a processing error, which reads as transient")
	})

	t.Run("distinct transactions pass the duplicate check", func(t *testing.T) {
		// Same shape, no duplicate: this must get past the duplicate check and fail only
		// on the merkle root (the block header here carries an unrelated root), proving
		// the new check does not misfire on a legitimate body — in particular that the
		// coinbase placeholder in the first slot is not treated as a duplicate.
		block := newQuickPathBlock(t, buildQuickPathSubtrees(t, txA, txB))

		_, err := u.validateSubtrees(ctx, block, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "merkle root mismatch")
		require.NotContains(t, err.Error(), "duplicate transaction")
	})
}

// newQuickPathSuiteWithRealUTXOStore returns a catchup suite whose mock UTXO store has been
// swapped for a real sqlitememory one. The duplicate has to genuinely survive create+spend to
// reach validateSubtrees at all, which a mock cannot model — and per AGENTS.md the store is
// not something to mock.
func newQuickPathSuiteWithRealUTXOStore(t *testing.T) *CatchupTestSuite {
	t.Helper()

	suite := NewCatchupTestSuite(t)

	storeURL, err := url.Parse("sqlitememory:///quick_validate_duplicate_test")
	require.NoError(t, err)

	realStore, err := sql.New(suite.Ctx, ulogger.TestLogger{}, suite.Server.blockValidation.settings, storeURL)
	require.NoError(t, err)

	suite.Server.blockValidation.utxoStore = realStore

	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()

	// Left on the default prefetch depth, so this runs the pipeline variant. The sequential
	// variant (blockvalidation_subtree_batch_prefetch_depth=0) cannot be driven end to end
	// today: processSubtreeBatch never allocates SubtreeProcessingBatch.fullSubtreeExists,
	// which prefetchSubtreeBatch does, so writeSubtreeFilesForBatch panics indexing a nil
	// slice. That is pre-existing on main and unrelated to this change. All variants funnel
	// through validateSubtrees, so the check itself is covered either way.

	return suite
}

// storeDuplicateBearingBlock builds a block whose body carries the same transaction in two
// different subtrees — the CVE-2012-2459 shape — and writes both subtrees and their data to
// the suite's subtree store so the quick-validation pipeline can read them back.
//
// The duplicate must survive UTXO processing to reach validateSubtrees at all: the repeated
// create is absorbed by createAndSpendUTXOsForBatch as ErrTxExists and the repeated spend
// takes the store's idempotent same-spender path, so nothing rejects the block earlier. That
// is precisely why the duplicate check is needed here and not merely nice to have.
func storeDuplicateBearingBlock(t *testing.T, suite *CatchupTestSuite) *model.Block {
	t.Helper()

	// A real coinbase plus one real spending transaction; readSubtree requires subtree 0
	// index 0 to be a coinbase. The helper returns count-1 transactions, so 3 gives two.
	txs := transactions.CreateTestTransactionChainWithCount(t, 3)
	require.Len(t, txs, 2)
	coinbaseTx, dupTx := txs[0], txs[1]

	storeSubtree := func(st *subtreepkg.Subtree, data *subtreepkg.Data) {
		stBytes, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, stBytes))

		dataBytes, err := data.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, dataBytes))
	}

	// Subtree 0: coinbase placeholder, then dupTx.
	first, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(*dupTx.TxIDChainHash(), 1, 1))

	firstData := subtreepkg.NewSubtreeData(first)
	require.NoError(t, firstData.AddTx(coinbaseTx, 0))
	require.NoError(t, firstData.AddTx(dupTx, 1))
	storeSubtree(first, firstData)

	// Subtree 1: dupTx again. Different leaf count, so a different root — the two subtrees
	// are distinct entries in the block, carrying the same transaction.
	second, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(*dupTx.TxIDChainHash(), 1, 1))

	secondData := subtreepkg.NewSubtreeData(second)
	require.NoError(t, secondData.AddTx(dupTx, 0))
	storeSubtree(second, secondData)

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = coinbaseTx
	block.Subtrees = []*chainhash.Hash{first.RootHash(), second.RootHash()}
	block.Height = 1 // the UTXO store rejects a spend at height zero

	return block
}

// TestQuickValidateBlock_DuplicateIsBlockInvalidNotProcessing pins the two entry-point
// pass-throughs in quickValidateBlock and quickValidateBlockAsync, which return an
// ErrBlockInvalid from the subtree pipeline unwrapped instead of re-wrapping it as a
// ProcessingError.
//
// A mutation check showed these had zero coverage: deleting BOTH pass-throughs left every
// test in the package passing. TestValidateSubtrees_RejectsDuplicateTransaction above does
// not reach them, because it calls validateSubtrees directly and so never crosses the site
// that does the wrapping.
//
// The assertion that matters is the negative one. (*Error).Is walks the wrapped chain, so
// errors.Is(err, ErrBlockInvalid) stays true even when the rejection IS re-wrapped —
// asserting only that would pass with the pass-throughs deleted. What re-wrapping changes is
// the error's own code: the rejection would additionally satisfy ErrProcessing, so a consumer
// classifying on the top-level code, or testing ErrProcessing first, would read a consensus
// rejection as a transient fault. Hence require.False on ErrProcessing.
func TestQuickValidateBlock_DuplicateIsBlockInvalidNotProcessing(t *testing.T) {
	// requireConsensusRejection asserts the shape both entry points must produce.
	requireConsensusRejection := func(t *testing.T, err error) {
		t.Helper()

		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a duplicate on the quick path must reach the caller as BlockInvalid")
		require.False(t, errors.Is(err, errors.ErrProcessing),
			"it must not carry a top-level ErrProcessing code, which reads as a transient fault")
	}

	t.Run("quickValidateBlock returns the rejection unwrapped", func(t *testing.T) {
		suite := newQuickPathSuiteWithRealUTXOStore(t)
		defer suite.Cleanup()

		block := storeDuplicateBearingBlock(t, suite)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		requireConsensusRejection(t, err)
	})

	t.Run("quickValidateBlockAsync returns the rejection unwrapped", func(t *testing.T) {
		suite := newQuickPathSuiteWithRealUTXOStore(t)
		defer suite.Cleanup()

		block := storeDuplicateBearingBlock(t, suite)

		// Buffered large enough that the async path never blocks queuing a write job.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test-peer", "", writeJobsChan)
		requireConsensusRejection(t, err)
	})
}
