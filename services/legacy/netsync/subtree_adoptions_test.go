package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

var subtreeArtefactTypes = []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeToCheck}

// emitSubtree runs one subtree through w the way the stream builder does: the data file is
// streamed into a pending file, then Emit names it and writes the other two.
func emitSubtree(t *testing.T, ctx context.Context, w *subtreeWriter, leaves int) {
	t.Helper()

	st, data, meta := oneSubtree(t, leaves)

	sink, err := w.OpenData(ctx)(0)
	require.NoError(t, err)
	require.NotNil(t, sink)
	require.NoError(t, data.WriteTransactionsToWriter(sink, 0, st.Length()))
	require.NoError(t, w.Emit(ctx)(0, st, data, meta))
}

// Two blocks that carry an identical run of transactions name the same subtree files. Block A
// creates them and block B adopts them; B's record, and once B commits the block persister,
// rely on files A created. A's conversion then fails and its cleanup runs. It used to delete
// every file A created, leaving B naming subtree data that was gone, with no second source.
// Over one registry, A's cleanup leaves the files B adopted. Removing the adoption check from
// DeleteAll fails this test.
func TestSubtreeWriter_ACleanupLeavesTheFilesAnotherBlockAdopted(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	a.adoptions = reg

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800001)
	b.adoptions = reg

	emitSubtree(t, ctx, a, 8)
	emitSubtree(t, ctx, b, 8)

	require.Len(t, a.written, len(subtreeArtefactTypes), "A created all three files")
	require.Empty(t, b.written, "B created none")
	require.Len(t, b.adopted, len(subtreeArtefactTypes), "B adopted all three")

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	// B is handed over and commits, so its writer is never asked to delete. A fails.
	b.finish()
	require.NoError(t, a.DeleteAll(ctx))

	for _, ft := range subtreeArtefactTypes {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "A's cleanup must leave the %s that B adopted", ft)
	}

	require.Zero(t, reg.size(), "with A's conversion over, nothing is left that could delete the files")
}

// A hold matters only while a creator of the file is still converting, because only a creator
// deletes, and only inside its own conversion. So holds do not outlive the conversions they
// guard against: racing copies of one block, which adopt every file the first copy wrote and
// are common during IBD, leave nothing behind once both have converted, and a writer that adopts
// a file whose creator has already finished takes no hold at all.
func TestSubtreeAdoptions_HoldsEndWithTheCreatorsTheyGuardAgainst(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	first := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	first.adoptions = reg

	racing := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	racing.adoptions = reg

	emitSubtree(t, ctx, first, 8)
	emitSubtree(t, ctx, racing, 8)

	require.Len(t, racing.adopted, len(subtreeArtefactTypes), "the racing copy holds what the converting first copy wrote")

	racing.finish()
	require.Positive(t, reg.size(), "while the first copy converts, the holds stand")

	first.finish()
	require.Zero(t, reg.size(), "once both copies have converted, nothing is held")

	late := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	late.adoptions = reg

	emitSubtree(t, ctx, late, 8)
	late.finish()

	require.Empty(t, late.adopted, "no creator is converting, so a late adopter needs no hold")
	require.Zero(t, reg.size())
}

// A writer that created its files and that nobody adopted still removes them on failure, and a
// failed adopter gives its marks back, so a creator that fails after it removes its own files.
// Leaving files of a failed block to their delete-at-height would let a peer that keeps sending
// bodies which fail the merkle check fill the disk until retention passes.
func TestSubtreeWriter_ACleanupStillRemovesFilesNoLiveBlockAdopted(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	a.adoptions = reg

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800001)
	b.adoptions = reg

	emitSubtree(t, ctx, a, 8)
	emitSubtree(t, ctx, b, 8)

	// B fails first: its record is never read, so it gives its holds back.
	require.NoError(t, b.DeleteAll(ctx))
	require.Empty(t, b.adopted, "a failed adopter holds nothing")

	require.NoError(t, a.DeleteAll(ctx))
	require.Zero(t, reg.size(), "nothing is tracked once both conversions are over")

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	for _, ft := range subtreeArtefactTypes {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.False(t, exists, "with no live adopter the creator's cleanup removes %s", ft)
	}
}

// pipelineBlockDelete removes every file a refused record names, whoever created it, so it goes
// through the same registry: a file another block adopted is left.
func TestSyncManager_ARefusedRecordsDeleteLeavesAnAdoptedFile(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)

	sm := &SyncManager{ctx: ctx, subtreeStore: store}

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800001)
	b.adoptions = sm.subtreeAdoptionsFor()

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	a.adoptions = sm.subtreeAdoptionsFor()

	emitSubtree(t, ctx, a, 8)
	emitSubtree(t, ctx, b, 8)

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	for _, ft := range subtreeArtefactTypes {
		require.NoError(t, sm.deleteSubtreeFileUnlessAdopted(*root, ft))

		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "a refused record's delete must leave the %s another block adopted", ft)
	}
}
