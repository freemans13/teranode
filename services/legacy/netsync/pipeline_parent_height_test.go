package netsync

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestPipelineSink_ParentInHeaderCache_IsTheOrdinaryCase is FIX 2's core claim.
//
// A parent that is only in the in-flight header cache — not yet committed to
// the blockchain store — is the ordinary out-of-order case on this node:
// measured at roughly 91% of blocks. Before this fix, pipelineBlockSink asked
// only sm.blockchainClient.GetBlockHeader, which answers only for a committed
// block, so every one of these returned a BlockInvalidError. That error
// propagates out of the sink, out of readBlockMessage
// (services/legacy/peer/wire_streaming.go), out of streamingBlockHandler, and
// reaches peer.go's shouldHandleReadError, which treats ANY non-nil error
// other than an exact io.EOF, io.ErrUnexpectedEOF or non-temporary
// net.OpError as a malformed message: PushRejectMsg("malformed", ...) and
// DisconnectWithWarning("malformed message"). The delivering peer was
// disconnected for a message it did nothing wrong to send.
//
// This test proves the ordinary case now resolves a height from the header
// cache and lets the block through — no error, subtrees written — using a
// height taken from an in-flight cache entry the blockchain store was never
// told about.
func TestPipelineSink_ParentInHeaderCache_IsTheOrdinaryCase(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk) // computes a correct merkle root using genesis as a scratch parent

	// Point the block at a parent the fresh blockchain store has never heard
	// of, so a successful run can only have come from the header cache, not
	// from sm.blockchainClient.GetBlockHeader.
	parent := chainhash.HashH([]byte("fix2-header-cache-only-parent"))
	blk.MsgBlock().Header.PrevBlock = parent

	// Install that parent in the in-flight header cache only, at height 41, so
	// the block under test — its child — must resolve to height 42. Written
	// directly into the cache's maps, the same as headerCacheParent, because
	// this needs one specific hash named, not a real chained batch.
	sm.headerCache = newHeaderCache()
	sm.headerCache.byHeight[41] = parent
	sm.headerCache.byHash[parent] = 41
	sm.headerCache.filled = true

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a parent only in the in-flight header cache must not be treated as a fault")
	require.True(t, converted, "a parent resolved from the header cache must let the block convert, not fall back")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record for a header-cache-only parent must read back cleanly")
	require.NotNil(t, got, "the block must actually be converted, not merely accepted")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "the block must actually be converted, not merely accepted")

	require.Equal(t, uint32(42), got.Height, "height must come from the header-cache parent (41+1), proving the committed store was not what resolved it")

	for _, h := range hashes {
		exists, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.True(t, exists, "every subtree the sink reported must be in the store")
	}
}

// TestPipelineSink_UnresolvableParent_StillConverts is task 13's core claim:
// an unknown parent height no longer stops conversion. FIX 2 (below) already
// established that pipelineParentHeight not resolving is a genuine miss
// beyond the ordinary out-of-order case; task 13 removes the decline that
// used to sit behind that miss. The height feeds two decisions and both have
// a safe answer without one — see pipelineBlockSink's own doc comment — so
// this asserts both of them directly: the record's height is the 0 sentinel,
// and its structure files are FileTypeSubtreeToCheck, never FileTypeSubtree.
// The second assertion is the one genuinely unsafe outcome available in this
// task if it were ever flipped (see task-13-brief.md Step 10).
func TestPipelineSink_UnresolvableParent_StillConverts(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	// A parent nobody has ever heard of: not in the committed store (a fresh
	// store only has genesis) and not in the header cache (left empty).
	parent := chainhash.HashH([]byte("task13-nobody-has-heard-of-this-parent"))
	blk.MsgBlock().Header.PrevBlock = parent

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "an unresolvable parent must not surface as an error: any non-nil error here is what peer.shouldHandleReadError classifies as malformed and disconnects the peer over")
	require.True(t, converted, "an unresolvable parent must still convert, not fall back to a whole-body write")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record for an unresolved-height block must read back cleanly")
	require.NotNil(t, got, "the block must actually be converted, not merely accepted")
	require.Zero(t, got.Height, "height must be pipelineParentHeight's own unresolved sentinel, never a guessed real height")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "the block must actually be converted, not merely accepted")

	for _, h := range hashes {
		toCheck, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.True(t, toCheck, "an unresolved height must take the conservative .subtreeToCheck form")

		quick, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.False(t, quick, "an unresolved height must never write .subtree — that would assert no validation is owed, which is the one unsafe outcome in this task")
	}
}

// TestPipelineSink_NotUnifiedRouteStillConverts is task 13's second claim:
// legacyUnified is no longer consulted by the sink at all, so a below-checkpoint
// block converts whether or not the operator's unified flag is on. Before this
// task, pipelineBlockSink declined here and fell back to streamingBlockSink;
// see FIX 2/task-3's history in this file's git blame for why that fallback
// existed and why it is gone: Step 1 of task 13 established that blockID 0 is
// the universal "assign server-side" convention on every route, not only the
// unified one, so there is nothing left for legacyUnified to gate here.
func TestPipelineSink_NotUnifiedRouteStillConverts(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = false

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.True(t, converted, "a below-checkpoint block must convert regardless of legacyUnified")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotZero(t, got.Height, "sanity: this block's parent resolves via the header fixture, so its height must be real")

	for _, h := range got.Subtrees {
		exists, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.True(t, exists, "below the checkpoint the structure file must be FileTypeSubtree regardless of legacyUnified")
	}
}

// newPipelineManagerWithPark builds on newPipelineManager
// (pipeline_sink_test.go) with a real blockPark wired in, needed only by
// FIX 2's fallback test: the unresolvable-parent case defers to
// streamingBlockSink, which requires a park to write into.
func newPipelineManagerWithPark(t *testing.T, subtreeStore, parkStore blob.Store, maxItems int) *SyncManager {
	t.Helper()

	sm := newPipelineManager(t, subtreeStore, maxItems)
	sm.blockPark = &blockPark{
		logger:       ulogger.TestLogger{},
		store:        parkStore,
		storeTimeout: 5 * time.Second,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
	}

	return sm
}
