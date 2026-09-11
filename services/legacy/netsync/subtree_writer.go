package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// writtenSubtree records one artefact this block put in the store, so a failed
// merkle root can remove exactly what it wrote and nothing else.
type writtenSubtree struct {
	Hash     chainhash.Hash
	FileType fileformat.FileType
}

// subtreeWriter puts a completed subtree's three artefacts in the blob store, in
// the order that makes a crash decidable.
//
// Transactions first, then inpoints, then the structure. The structure file is
// what everything else is found by, so writing it last makes its presence the
// marker that the other two are complete. That matters more here than it would
// elsewhere because there is no second source: if a subtree file is missing or
// will not deserialise, block validation fails the block, and its only fallback is
// a memory-mapped read falling back to a heap read of the same local file
// (services/blockvalidation/quick_validate.go:973).
//
// The structure's file type is a claim about trust. Below a checkpoint legacy has
// done the work itself and writes FileTypeSubtree, the already-validated marker;
// otherwise it writes FileTypeSubtreeToCheck and the subtree validation service
// re-checks it.
type subtreeWriter struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	store           blob.Store
	height          uint32
	quickValidation bool
	written         []writtenSubtree
}

func newSubtreeWriter(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, height uint32, quickValidation bool) *subtreeWriter {
	return &subtreeWriter{
		logger:          logger,
		settings:        tSettings,
		store:           store,
		height:          height,
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

		dataBytes, err := data.Serialize()
		if err != nil {
			return errors.NewStorageError("[subtreeWriter][%s] failed to serialize subtree data", root, err)
		}

		metaBytes, err := meta.Serialize()
		if err != nil {
			return errors.NewStorageError("[subtreeWriter][%s] failed to serialize subtree meta", root, err)
		}

		structureBytes, err := st.Serialize()
		if err != nil {
			return errors.NewStorageError("[subtreeWriter][%s] failed to serialize subtree", root, err)
		}

		// Order is load-bearing, see the type comment. Serialisation happens first
		// for all three so a serialisation failure cannot leave a partial set on
		// disk.
		for _, artefact := range []struct {
			fileType fileformat.FileType
			payload  []byte
		}{
			{fileformat.FileTypeSubtreeData, dataBytes},
			{fileformat.FileTypeSubtreeMeta, metaBytes},
			{structureType, structureBytes},
		} {
			if err = w.put(ctx, *root, artefact.fileType, artefact.payload); err != nil {
				return err
			}

			w.written = append(w.written, writtenSubtree{Hash: *root, FileType: artefact.fileType})
		}

		return nil
	}
}

// put writes one artefact through the file storer, matching writeSubtree at
// services/legacy/netsync/handle_block.go:876.
//
// A blob that already exists is success, not failure: two peers can deliver blocks
// sharing an identical run of transactions, which produces the same subtree under
// the same key.
func (w *subtreeWriter) put(ctx context.Context, root chainhash.Hash, fileType fileformat.FileType, payload []byte) error {
	dah := w.height + w.settings.GetSubtreeValidationBlockHeightRetention()

	storer, err := filestorer.NewFileStorer(ctx, w.logger, w.settings, w.store, root[:], fileType, options.WithDeleteAt(dah))
	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return nil
		}

		return errors.NewStorageError("[subtreeWriter][%s] failed to create %s file", root, fileType, err)
	}

	if _, err = storer.Write(payload); err != nil {
		storer.Abort(errors.NewProcessingError("[subtreeWriter][%s] write failed for %s", root, fileType))

		return errors.NewStorageError("[subtreeWriter][%s] failed writing %s", root, fileType, err)
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("[subtreeWriter][%s] failed closing %s", root, fileType, err)
	}

	return nil
}

// Written lists every artefact this writer has put in the store, in write order.
func (w *subtreeWriter) Written() []writtenSubtree {
	return w.written
}

// DeleteAll removes everything this writer wrote. It is what a failed merkle root
// calls: nothing is reading these files, because no block references them until
// the block is handed over.
//
// One known consequence. A subtree file is keyed by its own root hash, so a failed
// block and a good block containing an identical run of transactions produce the
// same file, and this can remove one the good block legitimately wrote. It is
// rebuildable, so that is a performance edge case rather than a correctness one,
// and it is recorded here so it is not a surprise.
func (w *subtreeWriter) DeleteAll(ctx context.Context) error {
	var firstErr error

	for _, entry := range w.written {
		if err := w.store.Del(ctx, entry.Hash[:], entry.FileType); err != nil && firstErr == nil {
			firstErr = errors.NewStorageError("[subtreeWriter][%s] failed deleting %s", entry.Hash, entry.FileType, err)
		}
	}

	w.written = w.written[:0]

	return firstErr
}
