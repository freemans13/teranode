package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/semaphore"
)

// quickWindow lets up to depth consecutive below-checkpoint blocks have their UTXO store work
// in flight at once while every chain-order step still runs in height order.
//
// Three rules make the overlap safe, and each has one home here:
//
//  1. A create never waits and a spend waits for the create that made its coin. Spends of an
//     in-block parent already wait on the two-phase path in quick_validate.go; spends of a coin
//     created by an in-flight PREDECESSOR wait on that predecessor's batchGate, looked up in
//     the open-gate map by parent transaction id. A gate closes when its batch's create side has
//     returned, and every store caller blocks until its statement commits, so a closed gate
//     means a committed create.
//  2. Commits run in admission order, one committer goroutine, and admission accepts only the
//     tail's child, so the chain store never sees height h+1 before h.
//  3. Fail closed: when an entry fails, every later entry is marked aborted with a service
//     error recorded on the entry before anything is signalled, so the block validation call
//     for an aborted block returns that error and legacy sync sees a local fault, never a peer
//     fault. Nothing is committed for the failed block or anything behind it; the store's own
//     create and spend guards make the re-delivery converge on the serial result.
//
// Nothing here is sized by the chain: the open-gate and retained maps hold the transaction ids
// of in-flight blocks only, and legacy sync's byte budget bounds how many blocks that is.
type quickWindow struct {
	logger ulogger.Logger
	depth  int
	commit func(context.Context, *windowEntry) error

	mu      sync.Mutex
	cond    *sync.Cond // signalled when an entry leaves or is admitted
	entries []*windowEntry
	byHash  map[chainhash.Hash]*windowEntry
	// open holds every registered id whose gate has not closed; retained holds every id
	// registered by an entry still in the window, closed or not, for the miss backstop.
	open     map[chainhash.Hash]*batchGate
	retained map[chainhash.Hash]*windowEntry

	callers *semaphore.Weighted // shared store-caller budget across every in-flight block

	wake chan struct{}
}

type windowEntry struct {
	w      *quickWindow
	block  *model.Block
	hash   chainhash.Hash
	height uint32
	parent *windowEntry // nil when the parent was already stored at admission
	added  time.Time

	// peerID identifies which sync peer delivered this block. Set at admission by the caller
	// (task 4); the window itself attaches no behaviour to it.
	peerID string

	ctx    context.Context
	cancel context.CancelFunc

	registered chan struct{} // every batch registered
	idAssigned chan struct{} // block.ID set
	storeDone  chan struct{} // pipeline returned without error
	committed  chan struct{} // ordered tail ran, or the entry failed

	regOnce   sync.Once
	idOnce    sync.Once
	storeOnce sync.Once

	commitOnce sync.Once
	leaveOnce  sync.Once

	failed atomic.Bool
	errMu  sync.Mutex
	err    error

	gatesMu sync.Mutex
	gates   []*batchGate
	txids   []chainhash.Hash
}

type batchGate struct {
	entry  *windowEntry
	txids  []chainhash.Hash
	done   chan struct{}
	failed atomic.Bool
	once   sync.Once
}

func newQuickWindow(logger ulogger.Logger, depth int, callerLimit int, commit func(context.Context, *windowEntry) error) *quickWindow {
	if depth < 0 {
		depth = 0
	}

	if callerLimit < 1 {
		callerLimit = 1
	}

	w := &quickWindow{
		logger:   logger,
		depth:    depth,
		commit:   commit,
		byHash:   make(map[chainhash.Hash]*windowEntry),
		open:     make(map[chainhash.Hash]*batchGate),
		retained: make(map[chainhash.Hash]*windowEntry),
		callers:  semaphore.NewWeighted(int64(callerLimit)),
		wake:     make(chan struct{}, 1),
	}
	w.cond = sync.NewCond(&w.mu)

	return w
}

func (w *quickWindow) Enabled() bool { return w != nil && w.depth >= 1 }
func (w *quickWindow) Depth() int    { return w.depth }

// Start runs the committer until ctx is cancelled.
func (w *quickWindow) Start(ctx context.Context) {
	go w.run(ctx)
}

func (w *quickWindow) AcquireCaller(ctx context.Context) error { return w.callers.Acquire(ctx, 1) }
func (w *quickWindow) ReleaseCaller()                          { w.callers.Release(1) }

// Admit adds block to the window once there is room. The block's parent must be the last
// admitted entry, or the window must be empty (the caller has then confirmed the parent is
// stored). A block already in flight is returned with duplicate=true and no new entry.
func (w *quickWindow) Admit(ctx context.Context, block *model.Block) (*windowEntry, bool, error) {
	hash := *block.Hash()

	w.mu.Lock()
	defer w.mu.Unlock()

	if e, ok := w.byHash[hash]; ok {
		return e, true, nil
	}

	for len(w.entries) >= w.depth {
		if err := w.waitLocked(ctx); err != nil {
			return nil, false, err
		}

		if e, ok := w.byHash[hash]; ok {
			return e, true, nil
		}
	}

	var parent *windowEntry

	if n := len(w.entries); n > 0 {
		tail := w.entries[n-1]
		if !tail.hash.IsEqual(block.Header.HashPrevBlock) {
			return nil, false, errors.NewServiceError("[quickWindow][%s] parent %s is not the window tail %s at height %d", hash.String(), block.Header.HashPrevBlock.String(), tail.hash.String(), tail.height)
		}

		parent = tail
	}

	ectx, cancel := context.WithCancel(context.Background())
	e := &windowEntry{
		w:          w,
		block:      block,
		hash:       hash,
		height:     block.Height,
		parent:     parent,
		added:      time.Now(),
		ctx:        ectx,
		cancel:     cancel,
		registered: make(chan struct{}),
		idAssigned: make(chan struct{}),
		storeDone:  make(chan struct{}),
		committed:  make(chan struct{}),
	}

	w.entries = append(w.entries, e)
	w.byHash[hash] = e
	prometheusBlockValidationQuickWindowDepth.Set(float64(len(w.entries)))
	w.cond.Broadcast()
	w.kick()

	return e, false, nil
}

// waitLocked waits on the condition variable with ctx cancellation. Called with w.mu held.
func (w *quickWindow) waitLocked(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()

	w.cond.Wait()

	return ctx.Err()
}

func (w *quickWindow) Lookup(hash *chainhash.Hash) *windowEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.byHash[*hash]
}

// AwaitParent returns the in-flight entry for parent, waiting up to timeout for it to be
// admitted. nil means the parent is not in flight; the caller decides what that means.
func (w *quickWindow) AwaitParent(ctx context.Context, parent *chainhash.Hash, timeout time.Duration) *windowEntry {
	deadline := time.Now().Add(timeout)

	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if e, ok := w.byHash[*parent]; ok {
			return e
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}

		tctx, cancel := context.WithTimeout(ctx, remaining)
		err := w.waitLocked(tctx)
		cancel()

		if err != nil {
			if e, ok := w.byHash[*parent]; ok {
				return e
			}

			return nil
		}
	}
}

// GateFor returns the open gate that will release txid, unless it belongs to owner: an entry
// never waits on its own gates (in-block parents are handled by the two-phase path).
func (w *quickWindow) GateFor(owner *windowEntry, txid *chainhash.Hash) *batchGate {
	w.mu.Lock()
	defer w.mu.Unlock()

	g, ok := w.open[*txid]
	if !ok || g.entry == owner {
		return nil
	}

	return g
}

func (w *quickWindow) Registered(txid *chainhash.Hash) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, ok := w.retained[*txid]

	return ok
}

func (w *quickWindow) kick() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run is the committer: it owns the ordered tail and the abort cascade.
func (w *quickWindow) run(ctx context.Context) {
	for {
		head := w.head()
		if head == nil {
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
				continue
			}
		}

		prometheusBlockValidationQuickWindowOldestAgeSeconds.Set(time.Since(head.added).Seconds())

		select {
		case <-ctx.Done():
			return
		case <-head.storeDone:
			if head.failed.Load() {
				w.abortFrom(head, "head_failed") // cause unused: failLocked already ran once
				continue
			}

			if err := w.commit(ctx, head); err != nil {
				// Record the commit error and let abortFrom's failLocked call do the rest
				// (CAS the failed flag, cancel, fail the head's own gates, close committed).
				// Setting head.failed here directly would make that later CompareAndSwap a
				// no-op and leave head.committed unclosed forever.
				head.recordErr(err)
				w.abortFrom(head, "commit_failed")

				continue
			}

			w.pop(head)
			head.commitOnce.Do(func() { close(head.committed) })
		case <-head.committed:
			// Closed by Fail before store work finished; cause unused, already recorded there.
			w.abortFrom(head, "head_failed")
		}
	}
}

func (w *quickWindow) head() *windowEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) == 0 {
		return nil
	}

	return w.entries[0]
}

// pop removes head from the ordered chain (not from byHash: the entry stays in the window,
// holding its dedup slot and its retained ids, until its owner calls Leave).
func (w *quickWindow) pop(e *windowEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, x := range w.entries {
		if x == e {
			w.entries = append(w.entries[:i], w.entries[i+1:]...)
			break
		}
	}
}

// abortFrom fails every entry from failed to the tail, records the abort before signalling,
// and pops them all from the ordered chain. The head keeps its own error and class; headCause
// labels the abort metric for the head's own failure (ignored if failLocked already ran for it,
// since the CAS it guards its one-time actions with will already be spent).
func (w *quickWindow) abortFrom(failed *windowEntry, headCause string) {
	w.mu.Lock()

	idx := -1

	for i, x := range w.entries {
		if x == failed {
			idx = i
			break
		}
	}

	if idx < 0 {
		w.mu.Unlock()
		return
	}

	doomed := append([]*windowEntry(nil), w.entries[idx:]...)
	w.entries = w.entries[:idx]
	w.mu.Unlock()

	for i, e := range doomed {
		if i == 0 {
			e.failLocked(nil, headCause)
			continue
		}

		e.failLocked(errors.NewServiceError("[quickWindow][%s] aborted at height %d: predecessor %s at height %d failed", e.hash.String(), e.height, failed.hash.String(), failed.height), "predecessor_failed")
	}
}

// leave removes e from the window entirely and frees its retained ids and its dedup slot.
func (w *quickWindow) leave(e *windowEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, x := range w.entries {
		if x == e {
			w.entries = append(w.entries[:i], w.entries[i+1:]...)
			break
		}
	}

	delete(w.byHash, e.hash)

	e.gatesMu.Lock()
	for _, id := range e.txids {
		if w.retained[id] == e {
			delete(w.retained, id)
		}

		if g, ok := w.open[id]; ok && g.entry == e {
			delete(w.open, id)
		}
	}
	e.gatesMu.Unlock()

	prometheusBlockValidationQuickWindowDepth.Set(float64(len(w.entries)))
	w.cond.Broadcast()
	w.kick()
}

// ---- windowEntry ----

func (e *windowEntry) Block() *model.Block      { return e.block }
func (e *windowEntry) Height() uint32           { return e.height }
func (e *windowEntry) Context() context.Context { return e.ctx }

func (e *windowEntry) recordErr(err error) {
	e.errMu.Lock()
	defer e.errMu.Unlock()

	if e.err == nil {
		e.err = err
	}
}

func (e *windowEntry) recordedErr() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()

	return e.err
}

// RegisterBatch records the transaction ids this batch will create and returns the gate that
// releases spends of them. A txid already held by another live entry is a fail-closed error.
func (e *windowEntry) RegisterBatch(txids []chainhash.Hash) (*batchGate, error) {
	g := &batchGate{entry: e, txids: txids, done: make(chan struct{})}

	e.w.mu.Lock()
	for _, id := range txids {
		if other, ok := e.w.retained[id]; ok && other != e {
			e.w.mu.Unlock()
			return nil, errors.NewProcessingError("[quickWindow][%s] transaction %s is already claimed by in-flight block %s at height %d", e.hash.String(), id.String(), other.hash.String(), other.height)
		}
	}

	for _, id := range txids {
		e.w.open[id] = g
		e.w.retained[id] = e
	}
	e.w.mu.Unlock()

	e.gatesMu.Lock()
	e.gates = append(e.gates, g)
	e.txids = append(e.txids, txids...)
	e.gatesMu.Unlock()

	if e.failed.Load() {
		g.fail()
	}

	return g, nil
}

func (e *windowEntry) RegistrationComplete() {
	e.regOnce.Do(func() { close(e.registered) })
}

// WaitPredecessorsRegistered blocks until every in-flight predecessor has registered all of
// its batches, so the open-gate map is complete before the first dependency check.
func (e *windowEntry) WaitPredecessorsRegistered(ctx context.Context) error {
	for p := e.parent; p != nil; p = p.parent {
		select {
		case <-p.registered:
		case <-p.committed:
			if p.failed.Load() {
				return errors.NewServiceError("[quickWindow][%s] predecessor %s failed before registering", e.hash.String(), p.hash.String())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (e *windowEntry) WaitPredecessorIDAssigned(ctx context.Context) error {
	if e.parent == nil {
		return nil
	}

	select {
	case <-e.parent.idAssigned:
		return nil
	case <-e.parent.committed:
		if e.parent.failed.Load() {
			return errors.NewServiceError("[quickWindow][%s] predecessor %s failed before assigning its id", e.hash.String(), e.parent.hash.String())
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *windowEntry) IDAssigned() {
	e.idOnce.Do(func() { close(e.idAssigned) })
}

// StoreDone tells the committer this entry's store work returned without error.
func (e *windowEntry) StoreDone() {
	e.storeOnce.Do(func() { close(e.storeDone) })
}

// Fail records err as this entry's outcome, fails its gates, cancels its context and wakes
// the committer, which aborts every later entry.
func (e *windowEntry) Fail(err error) {
	e.failLocked(err, "gate_failed")
	e.w.kick()
}

func (e *windowEntry) failLocked(err error, cause string) {
	if err != nil {
		e.recordErr(err)
	}

	if !e.failed.CompareAndSwap(false, true) {
		return
	}

	prometheusBlockValidationQuickWindowAbortsTotal.WithLabelValues(cause).Inc()
	e.cancel()

	e.gatesMu.Lock()
	gates := append([]*batchGate(nil), e.gates...)
	e.gatesMu.Unlock()

	for _, g := range gates {
		g.fail()
	}

	e.commitOnce.Do(func() { close(e.committed) })
}

// WaitCommitted returns nil once the ordered tail has run for this entry, or the recorded
// error once it failed or was aborted.
func (e *windowEntry) WaitCommitted(ctx context.Context) error {
	select {
	case <-e.committed:
	case <-ctx.Done():
		return ctx.Err()
	}

	if e.failed.Load() {
		if err := e.recordedErr(); err != nil {
			return err
		}

		return errors.NewServiceError("[quickWindow][%s] aborted", e.hash.String())
	}

	return nil
}

// Leave removes the entry from the window. The owner calls it exactly once, after its pipeline
// has returned, so no store call of this attempt can still be in flight.
func (e *windowEntry) Leave() {
	e.leaveOnce.Do(func() { e.w.leave(e) })
}

// ---- batchGate ----

// Close marks the batch's create side committed: waiters proceed and the ids leave the open map
// (they stay in the retained map until the entry leaves).
func (g *batchGate) Close() {
	g.once.Do(func() {
		g.entry.w.mu.Lock()
		for _, id := range g.txids {
			if cur, ok := g.entry.w.open[id]; ok && cur == g {
				delete(g.entry.w.open, id)
			}
		}
		g.entry.w.mu.Unlock()

		close(g.done)
	})
}

func (g *batchGate) fail() {
	g.failed.Store(true)
	g.Close()
}

// Wait blocks until the gate closes. A gate closed by failure returns a service error, so the
// waiting block fails as a local fault.
func (g *batchGate) Wait(ctx context.Context) error {
	select {
	case <-g.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if g.failed.Load() {
		return errors.NewServiceError("[quickWindow][%s] parent block %s at height %d failed; its outputs cannot be spent", g.entry.hash.String(), g.entry.hash.String(), g.entry.height)
	}

	return nil
}
