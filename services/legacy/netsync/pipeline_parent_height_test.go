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
	sm := newPipelineManager(t, store, 8)

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

	err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a parent only in the in-flight header list must not be treated as a fault")

	hashes := sm.pipelineSubtreeHashesFor(*blk.Hash())
	require.NotEmpty(t, hashes, "the block must actually be converted, not merely accepted")

	sm.pipelineVerifiedMu.Lock()
	got := sm.pipelineVerified[*blk.Hash()]
	sm.pipelineVerifiedMu.Unlock()

	require.Equal(t, uint32(42), got.height, "height must come from the header-list parent (41+1), proving the committed store was not what resolved it")

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

	err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "an unresolvable parent must not surface as an error: any non-nil error here is what peer.shouldHandleReadError classifies as malformed and disconnects the peer over")

	// The fallback must be a real one, not merely "return nil and drop the
	// block": prove the bytes actually landed via streamingBlockSink, the
	// same route this block would take with PipelineReceive off.
	exists, existsErr := parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, existsErr)
	require.True(t, exists, "the fallback must write the body to the park, the same as the non-pipeline sink would")

	require.Empty(t, sm.pipelineSubtreeHashesFor(*blk.Hash()), "a block that fell back to the raw sink was never converted, so it must have no pipeline-verified subtree record")
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
