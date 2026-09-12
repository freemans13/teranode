package netsync

import (
	"bytes"
	"container/list"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestPipelineSink_ParentInHeaderIndex_IsTheOrdinaryCase is FIX 2's core claim.
//
// A parent that is only in the in-flight header list — not yet committed to
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
// list and lets the block through — no error, subtrees written — using a
// height taken from an in-flight headerNode the blockchain store was never
// told about.
func TestPipelineSink_ParentInHeaderIndex_IsTheOrdinaryCase(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk) // computes a correct merkle root using genesis as a scratch parent

	// Point the block at a parent the fresh blockchain store has never heard
	// of, so a successful run can only have come from the header list, not
	// from sm.blockchainClient.GetBlockHeader.
	parent := chainhash.HashH([]byte("fix2-header-list-only-parent"))
	blk.MsgBlock().Header.PrevBlock = parent

	// Install that parent in the in-flight header list only, at height 41, so
	// the block under test — its child — must resolve to height 42.
	sm.headerList = list.New()
	e := sm.headerList.PushBack(&headerNode{hash: &parent, height: 41, listEpoch: sm.headerListEpoch})
	sm.headerIndex = map[chainhash.Hash]*list.Element{parent: e}

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a parent only in the in-flight header list must not be treated as a fault")
	require.True(t, converted, "a parent resolved from the header list must let the block convert, not fall back")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record for a header-list-only parent must read back cleanly")
	require.NotNil(t, got, "the block must actually be converted, not merely accepted")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "the block must actually be converted, not merely accepted")

	require.Equal(t, uint32(42), got.Height, "height must come from the header-list parent (41+1), proving the committed store was not what resolved it")

	for _, h := range hashes {
		exists, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.True(t, exists, "every subtree the sink reported must be in the store")
	}
}

// TestPipelineSink_UnresolvableParent_FallsBackInsteadOfErroring is FIX 2's
// fallback claim.
//
// A parent that is in neither the header list nor the committed store is a
// genuine miss beyond the ordinary out-of-order case (see
// pipelineParentHeight's doc comment for why the header list covers the
// ordinary case). There is no error return from this sink that the wire layer
// treats as anything but a malformed message — peer.shouldHandleReadError
// disconnects on every error except an exact io.EOF, io.ErrUnexpectedEOF or
// non-temporary net.OpError, none of which fit "please retry me the ordinary
// way" — so instead of erroring, pipelineBlockSink defers to
// streamingBlockSink and lets the existing park/drain path carry this one
// block exactly as it would with PipelineReceive off.
//
// This must happen before anything is read from the stream: falling back
// after consuming bytes (the coinbase, in the pre-fix code order) would hand
// streamingBlockSink a reader missing its first transaction.
func TestPipelineSink_UnresolvableParent_FallsBackInsteadOfErroring(t *testing.T) {
	ctx := t.Context()

	subtreeStore := memory.New()
	parkStore := memory.New()
	sm := newPipelineManagerWithPark(t, subtreeStore, parkStore, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	// A parent nobody has ever heard of: not in the committed store (a fresh
	// store only has genesis) and not in the header list (left empty).
	parent := chainhash.HashH([]byte("fix2-nobody-has-heard-of-this-parent"))
	blk.MsgBlock().Header.PrevBlock = parent

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "an unresolvable parent must not surface as an error: any non-nil error here is what peer.shouldHandleReadError classifies as malformed and disconnects the peer over")
	require.False(t, converted, "the fallback must report that it did NOT convert, since it wrote the raw body instead")

	// The fallback must be a real one, not merely "return nil and drop the
	// block": prove the bytes actually landed via streamingBlockSink, the
	// same route this block would take with PipelineReceive off.
	exists, existsErr := parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, existsErr)
	require.True(t, exists, "the fallback must write the body to the park, the same as the non-pipeline sink would")

	isConverted, err := sm.blockPark.IsConverted(ctx, *blk.Hash())
	require.NoError(t, err, "checking for a converted record must not itself fail")
	require.False(t, isConverted, "a block that fell back to the raw sink was never converted, so it must have no converted record")
}

// TestPipelineSink_IneligibleForTheUnifiedRouteFallsBack is task-3 fix round
// 1's sink-side pin.
//
// Before this fix, pipelineBlockSink converted every block whose parent height
// resolved, regardless of whether the block's height was actually on the
// unified route. HandleConvertedBlock (handle_block.go) then refused any
// record it could not commit correctly — but by then the block's only copy
// was already the converted record, and refusing it at commit time meant
// parkCommitFailure's default classified the refusal as
// parkDispositionBlockRejected: delete the blob, rewind the download cursor,
// blame the delivering peer, and mark the block failed. Re-delivery converted
// it again and failed again, identically, forever, at that one height — and
// every descendant behind it was suppressed by the same recently-failed
// bookkeeping. Declining the conversion HERE instead costs nothing, because
// the block was never touched: it falls back to streamingBlockSink and
// commits later through the ordinary whole-block path
// (HandleBlockDirect), which does its own UTXO work and needs no blockID
// from the record.
//
// legacyUnified requires more than being below the checkpoint (which
// quickValidationAllowed alone would grant): it also requires the operator's
// unified flag. Turning that flag off — leaving the checkpoint and the
// outpoint-only flag exactly as newPipelineManager's default — is the
// narrowest way to make legacyUnified false while staying below the
// checkpoint, and it is one of the two scenarios the review named: an
// operator running PipelineReceive without LegacyUnifiedBelowCheckpoint. The
// other scenario (the first block AT or ABOVE the checkpoint on an otherwise
// unified node) flips the same legacyUnified predicate through BelowCheckpoint
// instead of through this flag; the sink's gate does not care which conjunct
// failed, so this is the same code path.
func TestPipelineSink_IneligibleForTheUnifiedRouteFallsBack(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = false

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "declining to convert must not surface as an error: any non-nil error here is what peer.shouldHandleReadError classifies as malformed and disconnects the peer over")
	require.False(t, converted, "a block not on the unified route must fall back, not convert")

	exists, err := store.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, err)
	require.True(t, exists, "the fallback must write the whole block to the park, the same as the non-pipeline sink would")

	isConverted, err := sm.blockPark.IsConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.False(t, isConverted, "a block that fell back must produce no converted record")

	// The point of falling back rather than converting-then-refusing: the
	// block's only copy is never at risk. Read must find the same whole block
	// that was handed to the sink.
	msgBlock, err := sm.blockPark.Read(ctx, *blk.Hash())
	require.NoError(t, err, "the park must still hold the block after declining to convert it")
	require.Equal(t, blk.Hash().String(), msgBlock.BlockHash().String(), "the block held must be the block that was handed to the sink")
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
