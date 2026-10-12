package netsync

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
)

// subtreeAdoptions records which subtree files a block other than their creator relies on, so
// a cleanup never removes one from under it.
//
// Subtree files are content-addressed, so two blocks that carry an identical run of
// transactions name the same file: the first to publish it is its creator, and every later
// writer finds it there and adopts it. Conversions of different blocks run at the same time
// (the admission budget is MaxBlocksInTransitPerPeer times an allowance, not one), so a
// creator's conversion can fail after another block adopted the file, even after that block
// committed and the block persister kept the file. The creator's cleanup used to delete it
// regardless, leaving the adopter's record, or a committed block, naming subtree data that was
// gone.
//
// Every writer marks a file before it tries to write it. The mark is what makes the check
// race-free: a creator's delete and a writer's mark are both taken under mu, so either the
// delete runs first and the writer then finds no file and creates its own, or the mark is
// first and the delete is skipped. A writer that turns out to be the creator drops its mark
// at once, because only its own cleanup can remove the file and that cleanup should. A writer
// that adopted the file keeps its mark for as long as its record may be read, which for a
// block that went on to commit is the life of the process; a failed block's cleanup drops
// the marks it took. Marks outlive nothing but the process, and they need not: a creator's
// cleanup runs inside the conversion that created the file, so the creator and any adopter it
// could hurt are always in the same process.
//
// Memory is bounded by collisions, not by blocks: a key is held only while some writer has
// adopted it, which is the shared-run case and nothing else.
type subtreeAdoptions struct {
	mu    sync.Mutex
	marks map[writtenSubtree]int
}

func newSubtreeAdoptions() *subtreeAdoptions {
	return &subtreeAdoptions{marks: make(map[writtenSubtree]int)}
}

// mark records that a writer is about to try to write key.
func (a *subtreeAdoptions) mark(key writtenSubtree) {
	a.mu.Lock()
	a.marks[key]++
	a.mu.Unlock()
}

// unmark drops one mark on key.
func (a *subtreeAdoptions) unmark(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if n := a.marks[key]; n > 1 {
		a.marks[key] = n - 1
	} else {
		delete(a.marks, key)
	}
}

// deleteUnlessAdopted deletes key from store unless a writer holds a mark on it, and reports
// whether it was left. The check and the delete happen under one lock, so no writer can mark
// the file between them.
func (a *subtreeAdoptions) deleteUnlessAdopted(ctx context.Context, store blob.Store, key writtenSubtree) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.marks[key] > 0 {
		return true, nil
	}

	if err := store.Del(ctx, key.Hash[:], key.FileType); err != nil {
		return false, err
	}

	return false, nil
}

// subtreeAdoptionsFor returns the manager's registry, creating it on first use. Lazy so a
// SyncManager built as a literal, as most tests do, still shares one registry across every
// writer it makes.
func (sm *SyncManager) subtreeAdoptionsFor() *subtreeAdoptions {
	if a := sm.adoptions.Load(); a != nil {
		return a
	}

	sm.adoptions.CompareAndSwap(nil, newSubtreeAdoptions())

	return sm.adoptions.Load()
}

// deleteSubtreeFileUnlessAdopted is pipelineBlockDelete's delete of one subtree file: a file
// that another block adopted is left to its delete-at-height. A file that is not there counts
// as deleted.
func (sm *SyncManager) deleteSubtreeFileUnlessAdopted(root chainhash.Hash, ft fileformat.FileType) error {
	_, err := sm.subtreeAdoptionsFor().deleteUnlessAdopted(sm.ctx, sm.subtreeStore, writtenSubtree{Hash: root, FileType: ft})
	if err != nil && errors.Is(err, errors.ErrNotFound) {
		return nil
	}

	return err
}
