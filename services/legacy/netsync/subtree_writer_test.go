package netsync

import (
	"context"
	"sync"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// recordingStore records the order of writes so the ordering discipline can be
// asserted. It is not a general blob store fake: it implements only what the
// writer calls.
type recordingStore struct {
	mu     sync.Mutex
	order  []string
	failOn string
}

func (r *recordingStore) note(fileType fileformat.FileType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.order = append(r.order, string(fileType))

	if r.failOn == string(fileType) {
		return errors.NewStorageError("write refused for %s", fileType)
	}

	return nil
}

func (r *recordingStore) writes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, len(r.order))
	copy(out, r.order)

	return out
}

// TestSubtreeWriter_WritesTheStructureLast pins the ordering that makes recovery
// decidable. The structure file is what everything else is found by, so its
// presence has to mean the transactions and the inpoints are already whole.
func TestSubtreeWriter_WritesTheStructureLast(t *testing.T) {
	store := &recordingStore{}

	w := newSubtreeWriter(store, true)

	st, data, meta := oneSubtree(t, 8)

	require.NoError(t, w.Emit(context.Background())(0, st, data, meta))

	order := store.writes()
	require.Len(t, order, 3)
	require.Equal(t, string(fileformat.FileTypeSubtree), order[2],
		"the structure file must be written last: its presence is the marker that the other two are complete")
}

// TestSubtreeWriter_NamesTheStructureByValidationMode pins the trust claim the
// file name makes. Below a checkpoint legacy has done the work itself and writes
// the already-validated name; otherwise it writes the to-check name and the
// subtree validation service re-checks it.
func TestSubtreeWriter_NamesTheStructureByValidationMode(t *testing.T) {
	quick := &recordingStore{}
	require.NoError(t, newSubtreeWriter(quick, true).Emit(context.Background())(mustEmitArgs(t)))
	require.Contains(t, quick.writes(), string(fileformat.FileTypeSubtree))

	normal := &recordingStore{}
	require.NoError(t, newSubtreeWriter(normal, false).Emit(context.Background())(mustEmitArgs(t)))
	require.Contains(t, normal.writes(), string(fileformat.FileTypeSubtreeToCheck))
}

// TestSubtreeWriter_DeleteAllRemovesEverythingItWrote pins the merkle-failure
// path. Nothing is reading these files yet, because no block references them
// until the block is handed over, so deleting them is safe and is what a failed
// root check does.
func TestSubtreeWriter_DeleteAllRemovesEverythingItWrote(t *testing.T) {
	store := &recordingStore{}

	w := newSubtreeWriter(store, true)

	require.NoError(t, w.Emit(context.Background())(mustEmitArgs(t)))
	require.Len(t, w.Written(), 3, "one entry per file written, so all three can be removed")

	require.NoError(t, w.DeleteAll(context.Background()))
	require.Empty(t, w.Written(),
		"DeleteAll must actually clear what was written, not just return nil: "+
			"a failed merkle root depends on this to know cleanup is done")
}

// TestSubtreeWriter_ReportsWhichWriteFailed pins that a storage failure names the
// artefact. A block that half-wrote its files has no second source, so the
// operator needs to know which one is missing.
func TestSubtreeWriter_ReportsWhichWriteFailed(t *testing.T) {
	store := &recordingStore{failOn: string(fileformat.FileTypeSubtreeData)}

	err := newSubtreeWriter(store, true).Emit(context.Background())(mustEmitArgs(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "subtreeData")
}

// oneSubtree builds a single complete subtree with its data and meta, for tests
// that only need something well-formed to hand the writer.
func oneSubtree(t *testing.T, leaves int) (*subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(leaves)
	require.NoError(t, err)

	require.NoError(t, st.AddCoinbaseNode())

	data := subtreepkg.NewSubtreeData(st)
	meta := subtreepkg.NewSubtreeMeta(st)

	for i := 1; i < leaves; i++ {
		tx, hash := streamTx(t, i)

		require.NoError(t, st.AddNode(*hash, 0, uint64(tx.Size())))
		require.NoError(t, data.AddTx(tx, st.Length()-1))
		require.NoError(t, meta.SetTxInpointsFromTx(tx))
	}

	return st, data, meta
}

// mustEmitArgs returns the four arguments a subtreeEmitFunc takes, for tests that
// do not care about the subtree's contents.
func mustEmitArgs(t *testing.T) (int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) {
	t.Helper()

	st, data, meta := oneSubtree(t, 8)

	return 0, st, data, meta
}
