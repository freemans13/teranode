package netsync

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
)

// subtreeAdoptions keeps a converting block's cleanup from removing a subtree file that another
// block relies on.
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
// Only a creator ever deletes a subtree file, and only while its own conversion is in flight:
// subtreeWriter.DeleteAll runs inside pipelineBlockSink, and pipelineBlockDelete runs in the
// same wire read, straight after the sink. So a creator holds its files "live" from the moment
// it publishes them until its writer finishes, and an adoption needs protecting only while some
// creator of the file is live. Three counts per file say so:
//
//   - pending: writers trying to write the file right now. Each writer counts itself before it
//     tries, so a delete and an attempt are always ordered under mu: either the delete runs
//     first and the writer finds no file and creates its own, or the attempt is counted first
//     and the delete is skipped.
//   - live: creators whose conversion has not finished.
//   - held: writers that adopted the file while a creator of it was live.
//
// A delete is skipped while pending or held is above zero. When the last live creator of a file
// finishes, no one is left who could delete it, so its entry, holds included, is dropped. The
// registry therefore holds only the files of conversions in flight, however many copies of a
// block convert and whatever happens to them afterwards. A failed adopter gives its holds back.
type subtreeAdoptions struct {
	mu    sync.Mutex
	files map[writtenSubtree]*subtreeFileCounts
}

type subtreeFileCounts struct {
	pending int
	live    int
	held    int
}

func newSubtreeAdoptions() *subtreeAdoptions {
	return &subtreeAdoptions{files: make(map[writtenSubtree]*subtreeFileCounts)}
}

// countsLocked returns key's counts, creating them. The caller holds mu.
func (a *subtreeAdoptions) countsLocked(key writtenSubtree) *subtreeFileCounts {
	c, ok := a.files[key]
	if !ok {
		c = &subtreeFileCounts{}
		a.files[key] = c
	}

	return c
}

// tidyLocked drops key's entry once nothing about it is left to protect. The caller holds mu.
func (a *subtreeAdoptions) tidyLocked(key writtenSubtree, c *subtreeFileCounts) {
	if c.live == 0 {
		c.held = 0
	}

	if c.pending == 0 && c.live == 0 {
		delete(a.files, key)
	}
}

// attempt records that a writer is about to try to write key.
func (a *subtreeAdoptions) attempt(key writtenSubtree) {
	a.mu.Lock()
	a.countsLocked(key).pending++
	a.mu.Unlock()
}

// abandoned ends an attempt that neither created nor adopted key: the write failed.
func (a *subtreeAdoptions) abandoned(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--
	a.tidyLocked(key, c)
}

// created ends an attempt that published key: the writer is now a live creator of it.
func (a *subtreeAdoptions) created(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--
	c.live++
}

// adopted ends an attempt that found key already published, and reports whether the writer now
// holds it: only while a creator of key is live is there anyone the hold protects against.
func (a *subtreeAdoptions) adopted(key writtenSubtree) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--

	held := c.live > 0
	if held {
		c.held++
	}

	a.tidyLocked(key, c)

	return held
}

// release gives back a hold a failed adopter took. A hold whose creators have all finished is
// already gone, and releasing it is a no-op.
func (a *subtreeAdoptions) release(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if c, ok := a.files[key]; ok && c.held > 0 {
		c.held--
	}
}

// finished closes one creator's window on key.
func (a *subtreeAdoptions) finished(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c, ok := a.files[key]
	if !ok {
		return
	}

	if c.live > 0 {
		c.live--
	}

	a.tidyLocked(key, c)
}

// deleteUnlessAdopted deletes key from store unless a writer is trying to write it or holds it,
// and reports whether it was left. The check and the delete happen under one lock.
func (a *subtreeAdoptions) deleteUnlessAdopted(ctx context.Context, store blob.Store, key writtenSubtree) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if c, ok := a.files[key]; ok && (c.pending > 0 || c.held > 0) {
		return true, nil
	}

	if err := store.Del(ctx, key.Hash[:], key.FileType); err != nil {
		return false, err
	}

	return false, nil
}

// size is the number of files the registry is tracking, for tests.
func (a *subtreeAdoptions) size() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.files)
}

// settleBackstop is how long a converted copy's writer may wait to be settled before the park
// sweep settles it. The wire layer settles a copy in the same read that converted it, so any
// writer still waiting this long was settled by a message that never arrived.
const settleBackstop = time.Minute

// settlingWriter is a converted copy's writer waiting for the wire layer's verdict on the copy.
type settlingWriter struct {
	writer *subtreeWriter
	since  time.Time
}

// holdUntilSettled keeps a converted copy's writer, and so its window as a creator, open past
// the sink. The wire layer can still refuse the copy after the sink succeeded, and its
// cleanup, pipelineBlockDelete, must then delete what this copy created while holds other
// blocks took on those files still stand.
//
// Writers wait in a queue per block hash, because neither verdict carries anything that names
// the delivery: pipelineBlockDelete gets the hash, QueueBlockOnDisk the hash and the peer. One
// writer is the usual case. A second copy of the same block can convert as soon as the first
// copy's sink has returned and before its verdict arrives (endConversion runs at the end of
// the sink, and the gate admits the hash until the park takes the block), so two can wait at
// once. A verdict then cannot be told apart from the other copy's, and the two settle paths
// treat that as such (takeSettling).
func (sm *SyncManager) holdUntilSettled(hash chainhash.Hash, w *subtreeWriter) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	if sm.settling == nil {
		sm.settling = make(map[chainhash.Hash][]settlingWriter)
	}

	sm.settling[hash] = append(sm.settling[hash], settlingWriter{writer: w, since: time.Now()})
}

// takeSettling removes and returns the writer waiting under hash when there is exactly one.
// With more than one waiting it removes nothing and reports the hash ambiguous: a refusal
// cannot know which copy it is about.
func (sm *SyncManager) takeSettling(hash chainhash.Hash) (w *subtreeWriter, ambiguous bool) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	waiting := sm.settling[hash]

	switch len(waiting) {
	case 0:
		return nil, false
	case 1:
		delete(sm.settling, hash)

		return waiting[0].writer, false
	default:
		return nil, true
	}
}

// settleAccepted closes the window of a copy the wire layer accepted. With more than one copy
// waiting it closes the oldest: verdicts on one peer's copies come in the order they were read,
// and if it is the other copy's window that closes early, the cost is only that copy's
// refusal, which RequireEnd keeps from happening, losing the holds it would have kept.
func (sm *SyncManager) settleAccepted(hash chainhash.Hash) {
	sm.settlingMu.Lock()

	waiting := sm.settling[hash]
	if len(waiting) == 0 {
		sm.settlingMu.Unlock()
		return
	}

	oldest := waiting[0].writer

	if len(waiting) == 1 {
		delete(sm.settling, hash)
	} else {
		sm.settling[hash] = waiting[1:]
	}

	sm.settlingMu.Unlock()

	oldest.finish()
}

// settleStale finishes every writer that has waited longer than settleBackstop.
func (sm *SyncManager) settleStale(now time.Time) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	for hash, waiting := range sm.settling {
		kept := waiting[:0]

		for _, s := range waiting {
			if now.Sub(s.since) >= settleBackstop {
				s.writer.finish()
			} else {
				kept = append(kept, s)
			}
		}

		if len(kept) == 0 {
			delete(sm.settling, hash)
		} else {
			sm.settling[hash] = kept
		}
	}
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

// deleteSubtreeFileUnlessAdopted is pipelineBlockDelete's delete of one subtree file: a file a
// writer is writing or holds is left to its delete-at-height. A file that is not there counts
// as deleted.
func (sm *SyncManager) deleteSubtreeFileUnlessAdopted(root chainhash.Hash, ft fileformat.FileType) error {
	_, err := sm.subtreeAdoptionsFor().deleteUnlessAdopted(sm.ctx, sm.subtreeStore, writtenSubtree{Hash: root, FileType: ft})
	if err != nil && errors.Is(err, errors.ErrNotFound) {
		return nil
	}

	return err
}
