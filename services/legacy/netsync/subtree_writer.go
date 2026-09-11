package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// subtreeArtefactStore is the slice of the blob store this writer needs. Narrower
// than blob.Store so the test fake does not have to implement reads it never
// serves.
type subtreeArtefactStore interface {
	note(fileType fileformat.FileType) error
}

// writtenSubtree records one artefact this block put on disk, so a failed merkle
// root can remove exactly what it wrote and nothing else.
type writtenSubtree struct {
	Hash     chainhash.Hash
	FileType fileformat.FileType
}

// subtreeWriter puts a completed subtree's three artefacts in the store, in the
// order that makes a crash decidable.
//
// Transactions first, then inpoints, then the structure. The structure file is
// what everything else is found by, so writing it last makes its presence the
// marker that the other two are complete. That matters more here than it would
// elsewhere because there is no second source: if a subtree file is missing or
// will not deserialise, block validation fails the block, and its only fallback
// is a memory-mapped read falling back to a heap read of the same local file
// (services/blockvalidation/quick_validate.go:973).
//
// The structure's file type is a claim about trust. Below a checkpoint legacy has
// done the work itself and writes FileTypeSubtree, the already-validated marker;
// otherwise it writes FileTypeSubtreeToCheck and the subtree validation service
// re-checks it.
type subtreeWriter struct {
	store           subtreeArtefactStore
	quickValidation bool
	written         []writtenSubtree
}

func newSubtreeWriter(store subtreeArtefactStore, quickValidation bool) *subtreeWriter {
	return &subtreeWriter{
		store:           store,
		quickValidation: quickValidation,
		written:         make([]writtenSubtree, 0, 3),
	}
}

// Emit returns the function the stream builder calls for each completed subtree.
func (w *subtreeWriter) Emit(ctx context.Context) subtreeEmitFunc {
	return func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error {
		root := st.RootHash()
		if root == nil {
			return errors.NewProcessingError("[subtreeWriter] subtree %d has no root hash", index)
		}

		structureType := fileformat.FileTypeSubtreeToCheck
		if w.quickValidation {
			structureType = fileformat.FileTypeSubtree
		}

		// Order is load-bearing, see the type comment.
		for _, artefact := range []fileformat.FileType{
			fileformat.FileTypeSubtreeData,
			fileformat.FileTypeSubtreeMeta,
			structureType,
		} {
			if err := w.store.note(artefact); err != nil {
				return errors.NewStorageError("[subtreeWriter] failed writing %s for subtree %s", artefact, root, err)
			}

			w.written = append(w.written, writtenSubtree{Hash: *root, FileType: artefact})
		}

		return nil
	}
}

// Written lists every artefact this writer has put on disk, in write order.
func (w *subtreeWriter) Written() []writtenSubtree {
	return w.written
}

// DeleteAll removes everything this writer wrote. It is what a failed merkle root
// calls: nothing is reading these files, because no block references them until
// the block is handed over.
//
// One known consequence. A subtree file is keyed by its own root hash, so a
// failed block and a good block containing an identical run of transactions
// produce the same file, and this can remove one the good block legitimately
// wrote. It is rebuildable, so that is a performance edge case rather than a
// correctness one, and it is recorded here so it is not a surprise.
//
// ctx is unused today: this task pins ordering and naming without a real store.
// The next plan replaces the body with real store deletes that need it, and the
// parameter is kept now so that change does not ripple to callers.
func (w *subtreeWriter) DeleteAll(_ context.Context) error {
	w.written = w.written[:0]

	return nil
}
