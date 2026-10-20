package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// gap056FailingUnlockStore is the real sqlitememory UTXO store except that the first failUnlocks
// post-commit unlocks (SetLocked(..., false)) fail. Later unlocks go through to the real store.
type gap056FailingUnlockStore struct {
	utxo.Store

	failUnlocks int32
	unlockCalls atomic.Int32
}

func (s *gap056FailingUnlockStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value && s.unlockCalls.Add(1) <= s.failUnlocks {
		return errors.NewStorageError("gap056: simulated unlock failure after AddBlock")
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

// shrinkPostCommitUnlockRetryBackoff makes the post-commit unlock retries near-immediate for one test.
func shrinkPostCommitUnlockRetryBackoff(t *testing.T) {
	t.Helper()

	saved := postCommitUnlockRetryBackoff
	postCommitUnlockRetryBackoff = time.Millisecond

	t.Cleanup(func() { postCommitUnlockRetryBackoff = saved })
}

// gap056QuickValidate runs one honest single-subtree block through tryQuickValidation with a real
// write worker, with the UTXO store failing the first failUnlocks unlocks. It returns the block,
// its served subtree, the spending child, the store double and tryQuickValidation's results.
func gap056QuickValidate(t *testing.T, failUnlocks int32) (*preBindHarness, *model.Block, *chainhash.Hash, *chainhash.Hash, *gap056FailingUnlockStore, bool, error) {
	t.Helper()

	shrinkPostCommitUnlockRetryBackoff(t)

	h := newPreBindHarness(t, nil)

	failing := &gap056FailingUnlockStore{Store: h.utxoStore, failUnlocks: failUnlocks}
	h.bv.utxoStore = failing

	coinbase := preBindCoinbase(t, 0x56)
	parent := h.storeGenuineParent(0x56)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, quickErr := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	// Preconditions: the unlock was attempted (so the failure was injected), and AddBlock committed
	// the block with subtrees_set.
	require.Positive(t, failing.unlockCalls.Load(), "precondition: the post-commit unlock must have been attempted")

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(t, err, "precondition: the block must have been committed by AddBlock")
	require.True(t, meta.SubtreesSet, "precondition: the quick route commits with subtrees_set=true")

	return h, block, served.RootHash(), child.TxIDChainHash(), failing, tryNormal, quickErr
}

// TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles covers GAP-BV-056
// (BV-QUICK-015, BV-QUICK-021, BV-COMMIT-002), bitcoin-sv/teranode#4987.
//
// Once AddBlock has committed a block with subtrees_set and mined_set, a failing post-commit unlock
// is retried, and the block's .subtree files are never deleted. Here only the first unlock fails,
// so the retry clears the lock and quick validation succeeds.
//
// Mutation proof: calling unlockSubtreeTransactionsIfNeeded once instead of
// unlockSubtreeTransactionsWithRetry in commitBlock leaves the outputs locked and fails the last
// assertion.
func TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles(t *testing.T) {
	h, _, subtreeRoot, childHash, failing, tryNormal, quickErr := gap056QuickValidate(t, 1)

	require.NoError(t, quickErr, "a retried unlock that succeeds must leave quick validation successful")
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")
	require.GreaterOrEqual(t, failing.unlockCalls.Load(), int32(2), "the failed unlock must have been retried")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "committed block's outputs must not stay locked after the unlock failure")
}

// TestGapBV056_PostCommitUnlockExhaustedAbortsWithoutRemovingFiles covers the case the retries
// cannot fix: every unlock fails. tryQuickValidation must abort with the block-committed marker,
// keep the committed block's .subtree file, and return a transient error class, so the legacy park
// keeps the block for a retry instead of rejecting one that is already stored.
//
// Mutation proof: removing the isBlockCommitted branch in tryQuickValidation sends the error to the
// local-fault branch, which deletes the .subtree file and returns tryNormal=true.
func TestGapBV056_PostCommitUnlockExhaustedAbortsWithoutRemovingFiles(t *testing.T) {
	h, _, subtreeRoot, _, failing, tryNormal, quickErr := gap056QuickValidate(t, 1_000)

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "the post-commit failure must carry the block-committed marker: %v", quickErr)
	require.True(t, errors.IsTransientLocalError(quickErr), "the post-commit failure must read as transient to the legacy park: %v", quickErr)
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")
	require.Equal(t, int32(postCommitUnlockRetries+1), failing.unlockCalls.Load(), "every retry must have been attempted")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")
}
