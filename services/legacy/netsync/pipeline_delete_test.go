package netsync

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// TestInstallStreamingBlockPath_ChoosesTheSinkAndDeleteTogether is FIX 3's
// wiring claim.
//
// Before this change, installStreamingBlockPath always installed
// sm.streamingBlockDelete as the orphan-delete callback, whichever sink was
// active. That deletes from the park's blob store; the pipeline sink never
// writes there, so when the wire layer's own post-sink checks failed after a
// successful pipeline conversion, the delete callback removed a park body
// that was never written while the subtree files the pipeline sink actually
// wrote were left behind forever.
//
// This proves the sink and its matching delete callback are chosen together,
// the same pattern already used for the streamsEverySize policy: never one
// without the other.
func TestInstallStreamingBlockPath_ChoosesTheSinkAndDeleteTogether(t *testing.T) {
	sm := newPipelineManager(t, memory.New(), 8)
	sm.blockPark = &blockPark{}

	for _, pipelineOn := range []bool{false, true} {
		sm.settings.Legacy.PipelineReceive = pipelineOn

		var gotSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) error
		var gotGate func(chainhash.Hash, *wire.BlockHeader) error
		var gotDelete func(chainhash.Hash) error
		var gotStreamsEverySize bool

		sm.installStreamingBlockPath(func(
			sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) error,
			gate func(chainhash.Hash, *wire.BlockHeader) error,
			del func(chainhash.Hash) error,
			streamsEverySize bool,
		) {
			gotSink = sink
			gotGate = gate
			gotDelete = del
			gotStreamsEverySize = streamsEverySize
		})

		wantSink := reflect.ValueOf(sm.streamingBlockSink).Pointer()
		wantDelete := reflect.ValueOf(sm.streamingBlockDelete).Pointer()

		if pipelineOn {
			wantSink = reflect.ValueOf(sm.pipelineBlockSink).Pointer()
			wantDelete = reflect.ValueOf(sm.pipelineBlockDelete).Pointer()
		}

		require.Equal(t, wantSink, reflect.ValueOf(gotSink).Pointer(), "pipeline=%v must install the matching sink", pipelineOn)
		require.Equal(t, wantDelete, reflect.ValueOf(gotDelete).Pointer(), "pipeline=%v must install the matching delete callback, not always streamingBlockDelete", pipelineOn)
		require.NotNil(t, gotGate, "the gate must always be installed alongside a sink")
		require.Equal(t, pipelineOn, gotStreamsEverySize, "the size policy must track the same PipelineReceive check")
	}
}

// TestPipelineBlockDelete_RemovesTheSubtreeFilesTheSinkWrote is FIX 3's
// functional claim.
//
// Simulates the wire layer's own post-sink checks failing after
// pipelineBlockSink itself already succeeded — the short-body check or the
// transaction-count read in readBlockMessage
// (services/legacy/peer/wire_streaming.go) — which calls the installed
// delete callback with the block's hash. Before this fix that callback was
// always streamingBlockDelete, which only knows about the park's store and
// would find nothing there to remove.
func TestPipelineBlockDelete_RemovesTheSubtreeFilesTheSinkWrote(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	require.NoError(t, sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body))))

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "sanity: the sink must have recorded a verified block, or this test asserts nothing")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "sanity: the sink must have produced subtrees, or this test asserts nothing")

	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash()))

	for _, h := range hashes {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err := store.Exists(ctx, h[:], ft)
			require.NoError(t, err)
			require.False(t, exists, "pipelineBlockDelete must remove every subtree artefact the sink wrote, got %s still present for subtree %s", ft, h)
		}
	}

	isConverted, err := sm.blockPark.IsConverted(ctx, *blk.Hash())
	require.NoError(t, err, "checking for a converted record must not itself fail")
	require.False(t, isConverted, "the converted record must be cleared too, not just the subtree files")
}

// TestPipelineBlockDelete_AlsoCleansUpTheFallbackParkWrite covers FIX 2's
// out-of-order fallback: when pipelineBlockSink defers to streamingBlockSink
// because a block's parent is unresolvable, the body lands in the park under
// the pipeline path's own name for what "the active sink wrote". The delete
// callback installed for the pipeline path must still clean that up.
func TestPipelineBlockDelete_AlsoCleansUpTheFallbackParkWrite(t *testing.T) {
	ctx := t.Context()

	subtreeStore := memory.New()
	parkStore := memory.New()
	sm := newPipelineManagerWithPark(t, subtreeStore, parkStore, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	parent := chainhash.HashH([]byte("fix3-unresolvable-parent-for-delete-test"))
	blk.MsgBlock().Header.PrevBlock = parent

	body := blockBodyBytes(t, blk)

	require.NoError(t, sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body))))

	exists, err := parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, err)
	require.True(t, exists, "sanity: the fallback must have written the body to the park, or this test asserts nothing")

	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash()))

	exists, err = parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, err)
	require.False(t, exists, "the pipeline delete callback must also remove a body the fallback wrote to the park, not just subtree files")
}
