// Copyright (c) 2026 The Teranode developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Wave 2 tests: parallel prepare (#4) reorder-buffer ordering, work-driven
// window flush (#2), and subtree fan-out (#5).
//
// HONEST SCOPE NOTE. The reorder buffer, the sliding-window dispatch, the stale
// prune and the work-driven flush all live in closures INSIDE blockHandler()
// (manager.go), which needs a live sync peer, the headers-first header list, the
// window accumulator and the block-assembly maturity gate wired together to run
// end-to-end. Those closures are drain-goroutine-only-owned and cannot be
// invoked in isolation without the ~130KB manager machinery. So the reorder /
// prune / flush-trigger tests below drive:
//
//   (a) the REAL ordering-funnel primitives the design leans on — the parallel
//       prepare-worker pool (diskPrepareWorker / prepareDiskBlock), the window
//       accumulator (add/full/empty/drainJob) and the contiguity gate
//       (gateContiguousWindow) — with real out-of-order completions, plus
//   (b) a faithful in-test MODEL of the drain's height-keyed reorder emit /
//       stale-prune / work-driven-flush logic (each modelled function is a
//       line-for-line mirror of the cited manager.go site), asserting the model
//       preserves strict-ascending, contiguous-with-committed-tip order and that
//       its output, fed through the REAL funnel, commits ascending.
//
// The subtree fan-out test (#5) exercises the REAL writeSubtrees method directly.

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// reorderModel is a faithful, height-keyed mirror of the drain goroutine's
// reorder buffer (manager.go: diskReadyMap + diskInFlight, the feed branch at
// the "if res, ok := diskReadyMap[hash]; ok" site and the diskPreparedReadyC
// completion case). In the test heights map 1:1 to hashes, so keying by height
// is an exact reduction of the production hash-keyed map. nextNeeded is the
// lowest uncommitted contiguous height (the frontier walk's "lowest uncommitted
// non-owned header"): a completed block is fed to wa.add ONLY when it is the
// contiguous prefix head, so out-of-order completions wait in the buffer until
// the gap in front of them fills — exactly the production invariant.
type reorderModel struct {
	ready      map[int32]bool
	inFlight   map[int32]bool
	nextNeeded int32
	emitted    []int32 // heights fed to wa.add, in feed order
}

func newReorderModel(firstNeeded int32) *reorderModel {
	return &reorderModel{
		ready:      make(map[int32]bool),
		inFlight:   make(map[int32]bool),
		nextNeeded: firstNeeded,
	}
}

// dispatch marks a height as in flight (mirrors the sliding-window dispatch loop
// that sends diskPrepareReq and records diskInFlight[c.hash]).
func (m *reorderModel) dispatch(h int32) { m.inFlight[h] = true }

// complete mirrors the diskPreparedReadyC case: move the height from the
// in-flight set into the reorder buffer, then run the walk — emit the whole
// contiguous ascending prefix starting at nextNeeded.
func (m *reorderModel) complete(h int32) {
	delete(m.inFlight, h)
	m.ready[h] = true

	for m.ready[m.nextNeeded] {
		m.emitted = append(m.emitted, m.nextNeeded)
		delete(m.ready, m.nextNeeded)
		delete(m.inFlight, m.nextNeeded)
		m.nextNeeded++
	}
}

// requireAscendingContiguous asserts a height slice is strictly ascending with
// no gaps, rooted at first — the commit invariant the reorder buffer preserves.
func requireAscendingContiguous(t *testing.T, heights []int32, first int32) {
	t.Helper()

	for i, h := range heights {
		require.Equal(t, first+int32(i), h,
			"emit position %d must be exactly first+%d (strict-ascending, contiguous)", i, i)
	}
}

// TestWindowParallelPrepare_StrictAscendingReorder proves the reorder buffer
// re-serialises OUT-OF-ORDER prepare completions back into strict-ascending,
// contiguous-with-committed-tip order before the window — for every completion
// order (reverse, shuffled) — and that the emitted order, run through the REAL
// window accumulator + contiguity gate, commits ascending with no gap.
func TestWindowParallelPrepare_StrictAscendingReorder(t *testing.T) {
	const first = int32(500)
	const n = 8

	// A high completion must NOT emit before the gap in front of it fills.
	t.Run("reverse-order completion emits nothing until the lowest lands", func(t *testing.T) {
		m := newReorderModel(first)
		for h := first; h < first+n; h++ {
			m.dispatch(h)
		}

		// Complete highest-first; nothing may emit until nextNeeded (first) lands.
		for h := first + n - 1; h > first; h-- {
			m.complete(h)
			require.Empty(t, m.emitted,
				"height %d completing before %d must wait in the reorder buffer (contiguity)", h, first)
			require.Len(t, m.ready, int(first+n-1-h+1),
				"every early completion must sit buffered, not be dropped")
		}

		// The lowest lands last: the whole run now emits at once, ascending.
		m.complete(first)
		require.Len(t, m.emitted, n, "the contiguous prefix flushes once the head lands")
		requireAscendingContiguous(t, m.emitted, first)
		require.Empty(t, m.ready, "buffer drained after emit")
	})

	// Exhaustive: every permutation-ish order (a rotation family) still yields
	// strict-ascending emit.
	t.Run("assorted completion orders all emit ascending", func(t *testing.T) {
		orders := [][]int32{
			{500, 501, 502, 503, 504, 505, 506, 507}, // in order
			{507, 506, 505, 504, 503, 502, 501, 500}, // reverse
			{503, 501, 507, 500, 505, 502, 506, 504}, // shuffled
			{501, 502, 503, 504, 505, 506, 507, 500}, // head last
		}

		for _, order := range orders {
			m := newReorderModel(first)
			for h := first; h < first+n; h++ {
				m.dispatch(h)
			}
			for _, h := range order {
				m.complete(h)
			}

			require.Len(t, m.emitted, n)
			requireAscendingContiguous(t, m.emitted, first)

			// Feed the emitted order through the REAL funnel: window accumulator ->
			// drainJob -> gateContiguousWindow. The committed run must be ascending
			// and contiguous with the committed tip (first-1).
			wa := newWindowAccumulator(1<<40, 100)
			for _, h := range m.emitted {
				wa.add(newMinimalModelBlock(t, uint32(h))) //nolint:gosec // small test heights
			}

			job, ok := wa.drainJob()
			require.True(t, ok)

			sm := newGateTestManager()
			sm.lastHandedWindowEnd = uint32(first - 1) //nolint:gosec
			park := newParkStore(0, 1024)

			out := sm.gateContiguousWindow(job, park)
			require.Len(t, out.blocks, n, "the whole contiguous run must be handed to the committer")
			require.Equal(t, 0, park.len(), "nothing parked for a contiguous ascending run")
			for i, b := range out.blocks {
				require.Equal(t, uint32(first)+uint32(i), b.Height, //nolint:gosec
					"commit position %d out of ascending order", i)
			}
		}
	})

	// With the REAL parallel worker pool: dispatch N real on-disk blocks to N real
	// diskPrepareWorkers, collect completions in whatever (nondeterministic) order
	// the pool returns them, and prove the reorder buffer still emits ascending.
	// This also exercises prepareDiskBlock N-parallel under -race (the per-block
	// independence claim).
	t.Run("real worker pool, real out-of-order completions", func(t *testing.T) {
		store := dtdDiskStore(t)
		sm := dtdNewManager(t, true, store)

		const workers = 4
		reqC := make(chan diskPrepareReq, workers)
		readyC := make(chan *diskPrepared, workers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for i := 0; i < workers; i++ {
			go sm.diskPrepareWorker(ctx, reqC, readyC)
		}

		heights := []int32{first, first + 1, first + 2, first + 3}
		for _, h := range heights {
			hash, raw, _ := dtdMakeBlock(t, h, uint32(h)) //nolint:gosec
			require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, nil, raw))
			reqC <- diskPrepareReq{hash: hash, height: h}
		}

		m := newReorderModel(first)
		for _, h := range heights {
			m.dispatch(h)
		}

		for range heights {
			select {
			case res := <-readyC:
				require.NotNil(t, res)
				// Feed the completion into the reorder buffer in ARRIVAL order (the
				// pool's real, nondeterministic order).
				m.complete(res.height)
			case <-time.After(10 * time.Second):
				t.Fatal("prepare pool did not return all results")
			}
		}

		require.Len(t, m.emitted, len(heights))
		requireAscendingContiguous(t, m.emitted, first)
	})
}

// TestWindowParallelPrepare_FatBlockNonStall proves a slow prepare on ONE block
// does not block the pool from preparing its neighbours: the neighbours complete
// and wait in the reorder buffer, and commit is starved by no more than the one
// slow block. The moment the slow block completes, the whole contiguous run
// emits at once.
func TestWindowParallelPrepare_FatBlockNonStall(t *testing.T) {
	const first = int32(500)
	heights := []int32{first, first + 1, first + 2, first + 3, first + 4}

	// Model: the LOWEST (fat) block completes LAST; its neighbours complete first.
	t.Run("neighbours buffer while the fat block is outstanding", func(t *testing.T) {
		m := newReorderModel(first)
		for _, h := range heights {
			m.dispatch(h)
		}

		// The four higher neighbours finish while the fat lowest block is still
		// preparing (occupying its one worker).
		for _, h := range heights[1:] {
			m.complete(h)
		}

		require.Empty(t, m.emitted,
			"commit is starved by exactly the one outstanding low block — nothing emits yet")
		require.Len(t, m.ready, len(heights)-1,
			"all neighbours prepared and wait in the reorder buffer (pool was NOT blocked)")
		require.Equal(t, map[int32]bool{first: true}, m.inFlight,
			"only the one fat block remains in flight")

		// The fat block finally completes: the whole run emits at once, ascending.
		m.complete(first)
		require.Len(t, m.emitted, len(heights))
		requireAscendingContiguous(t, m.emitted, first)
	})

	// Real pool: a per-hash slow read makes the LOWEST block's prepare finish
	// last, and we assert its neighbours' results arrive on readyC BEFORE it —
	// i.e. the pool really did prepare them concurrently while the low block was
	// slow (not stalled behind it).
	t.Run("real pool: neighbours complete before the slow low block", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		base := dtdDiskStore(t)

		slowHash, slowRaw, _ := dtdMakeBlock(t, first, uint32(first)) //nolint:gosec
		slow := &perHashSlowReadStore{Store: base, slowKey: slowHash[:], delay: 500 * time.Millisecond}
		sm := dtdNewManager(t, true, slow)

		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, slowHash, nil, slowRaw))

		const workers = 4
		reqC := make(chan diskPrepareReq, len(heights))
		readyC := make(chan *diskPrepared, len(heights))
		for i := 0; i < workers; i++ {
			go sm.diskPrepareWorker(ctx, reqC, readyC)
		}

		// Dispatch the fat/slow low block FIRST, then its neighbours.
		reqC <- diskPrepareReq{hash: slowHash, height: first}
		for _, h := range heights[1:] {
			hash, raw, _ := dtdMakeBlock(t, h, uint32(h)) //nolint:gosec
			require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, nil, raw))
			reqC <- diskPrepareReq{hash: hash, height: h}
		}

		firstDone := (<-readyC).height
		require.NotEqual(t, first, firstDone,
			"a neighbour must finish before the slow low block — the pool prepared it concurrently, not stalled behind the fat block")

		// Drain the rest; the slow low block is guaranteed to be among the last.
		got := []int32{firstDone}
		for len(got) < len(heights) {
			got = append(got, (<-readyC).height)
		}
		require.Contains(t, got, first, "the slow block still completes")
	})
}

// perHashSlowReadStore delays GetIoReader for exactly one key (the "fat" block),
// so that block's prepareDiskBlock read finishes after its neighbours'.
type perHashSlowReadStore struct {
	blob.Store
	slowKey []byte
	delay   time.Duration
}

func (s *perHashSlowReadStore) GetIoReader(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if bytes.Equal(key, s.slowKey) {
		time.Sleep(s.delay)
	}

	return s.Store.GetIoReader(ctx, key, ft, opts...)
}

// TestWindow_WorkDrivenFlush covers Wave 2 #2: the disk path flushes WORK-DRIVEN
// — a full window (WindowMaxBlocks) flushes immediately; a drained prepare
// frontier flushes the partial window; a lone block at the tip still commits
// (the drained-frontier flush replaces the old 200ms k=1 timer). While prepares
// are still in flight the partial window does NOT flush (it defers to a
// completion re-poke or wa.full()).
func TestWindow_WorkDrivenFlush(t *testing.T) {
	// workDrivenFlushShouldFire mirrors the drainValidateFromDisk wrapper's flush
	// predicate (manager.go): "!wa.empty() && len(diskInFlight) == 0". Flush when
	// the window holds something AND no prepare is in flight to re-poke and feed
	// more, so whatever sits in the window is all we will get right now.
	workDrivenFlushShouldFire := func(wa *windowAccumulator, inFlight int) bool {
		return !wa.empty() && inFlight == 0
	}

	t.Run("full window flushes immediately via back-pressure", func(t *testing.T) {
		sm := &SyncManager{logger: ulogger.TestLogger{}}

		// maxBlocks = 4, huge byte budget, so full() trips on the count arm at 4.
		wa := newWindowAccumulator(1<<40, 4)
		for h := 0; h < 4; h++ {
			wa.add(newMinimalModelBlock(t, uint32(500+h))) //nolint:gosec
		}
		require.True(t, wa.full(), "a window at WindowMaxBlocks must report full")

		flushed := false
		armed := false
		sm.ackWindowedBlock(nil, wa,
			func() { flushed = true },
			func() { armed = true }, // the disk path's no-op diskFeedArmTimer stand-in
			func() {})
		require.True(t, flushed, "a full window must flush immediately (back-pressure), not wait")
		require.False(t, armed, "the full path stops the timer; it never arms it")
	})

	t.Run("drained prepare frontier flushes a partial window", func(t *testing.T) {
		wa := newWindowAccumulator(1<<40, 16)
		// A partial window below WindowMaxBlocks.
		for h := 0; h < 3; h++ {
			wa.add(newMinimalModelBlock(t, uint32(500+h))) //nolint:gosec
		}
		require.False(t, wa.full(), "3 of 16 is a partial window, not full")

		// Prepare pool idle (nothing in flight) -> the drained-frontier flush fires.
		require.True(t, workDrivenFlushShouldFire(wa, 0),
			"a partial window with no in-flight prepare must flush (drained frontier)")
	})

	t.Run("in-flight prepares defer the flush", func(t *testing.T) {
		wa := newWindowAccumulator(1<<40, 16)
		wa.add(newMinimalModelBlock(t, 500))
		require.False(t, wa.full())

		// A prepare is still running: defer — its completion re-pokes the walk and
		// grows the window toward WindowMaxBlocks.
		require.False(t, workDrivenFlushShouldFire(wa, 2),
			"a partial window must NOT flush while prepares are in flight")
	})

	t.Run("lone block at the tip still commits (timer-net replacement)", func(t *testing.T) {
		wa := newWindowAccumulator(1<<40, 16)
		wa.add(newMinimalModelBlock(t, 999))
		require.False(t, wa.full(), "a single block is not a full window")

		// At the tip the pool idles with this one block: the drained-frontier flush
		// commits it promptly WITHOUT a 200ms wait.
		require.True(t, workDrivenFlushShouldFire(wa, 0),
			"a lone block at the tip must still commit via the drained-frontier flush")
	})

	t.Run("empty window never flushes", func(t *testing.T) {
		wa := newWindowAccumulator(1<<40, 16)
		require.False(t, workDrivenFlushShouldFire(wa, 0), "an empty window has nothing to flush")
	})
}

// TestWindowParallelPrepare_ReorderStaleDiscard covers the N-slot generalisation
// of the depth-1 single-slot stale discard: after a header reset moves the
// frontier, buffered/prepared results for hashes no longer in the sliding window
// must be pruned from the reorder buffer across ALL slots (not just one). This
// replicates the drain's prune loop (manager.go: "for h := range diskReadyMap {
// if _, in := windowHashes[h]; !in { delete(diskReadyMap, h) } }").
func TestWindowParallelPrepare_ReorderStaleDiscard(t *testing.T) {
	// Buffered, completed prepares keyed by hash (the real diskReadyMap type).
	diskReadyMap := make(map[chainhash.Hash]*diskPrepared)

	inWindow := make([]chainhash.Hash, 0, 4)
	stale := make([]chainhash.Hash, 0, 4)

	// Four in-window heights and four stale (post-reset, off-frontier) heights.
	for _, h := range []int32{500, 501, 502, 503} {
		hash, _, _ := dtdMakeBlock(t, h, uint32(h)) //nolint:gosec
		diskReadyMap[hash] = &diskPrepared{hash: hash, height: h}
		inWindow = append(inWindow, hash)
	}
	for _, h := range []int32{700, 701, 702, 703} {
		hash, _, _ := dtdMakeBlock(t, h, uint32(h)) //nolint:gosec
		diskReadyMap[hash] = &diskPrepared{hash: hash, height: h}
		stale = append(stale, hash)
	}
	require.Len(t, diskReadyMap, 8)

	// The frontier walk (post-reset) resolves the current window's hashes.
	windowHashes := make(map[chainhash.Hash]struct{}, len(inWindow))
	for _, h := range inWindow {
		windowHashes[h] = struct{}{}
	}

	// Prune — the exact production loop, discarding every buffered result outside
	// the current window across all N slots.
	for h := range diskReadyMap {
		if _, in := windowHashes[h]; !in {
			delete(diskReadyMap, h)
		}
	}

	require.Len(t, diskReadyMap, len(inWindow), "every stale slot must be discarded, not just one")
	for _, h := range inWindow {
		require.Contains(t, diskReadyMap, h, "in-window buffered result must be retained")
	}
	for _, h := range stale {
		require.NotContains(t, diskReadyMap, h, "off-frontier stale result must be discarded")
	}
}

// concTrackStore records the peak number of CONCURRENT SetFromReader calls for a
// given file type, so a test can prove the outer subtree-write loop fans out
// (peak > 1 across subtrees) versus running serially (peak == 1). It also counts
// how many distinct subtree-data files were written.
type concTrackStore struct {
	blob.Store
	trackFT fileformat.FileType
	cur     atomic.Int32
	peak    atomic.Int32
	writes  atomic.Int32
	hold    time.Duration
}

func (s *concTrackStore) SetFromReader(ctx context.Context, key []byte, ft fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if ft == s.trackFT {
		s.writes.Add(1)
		n := s.cur.Add(1)
		for {
			p := s.peak.Load()
			if n <= p || s.peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(s.hold) // widen the overlap window so real concurrency is observable
		defer s.cur.Add(-1)
	}

	return s.Store.SetFromReader(ctx, key, ft, reader, opts...)
}

// failExistsStore fails the Exists check for exactly one key, which makes that
// subtree's writeSubtree return an error (NewFileStorer surfaces a non-
// AlreadyExists Exists error as a create failure) without ever touching the
// streaming pipe — a clean, deadlock-free single-subtree write failure.
type failExistsStore struct {
	blob.Store
	failKey []byte
}

func (s *failExistsStore) Exists(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if bytes.Equal(key, s.failKey) {
		return false, errors.NewStorageError("forced Exists failure for subtree fan-out test")
	}

	return s.Store.Exists(ctx, key, ft, opts...)
}

// buildPopulatedSubtrees builds a multi-subtree block and returns its blockIdent
// plus fully-populated subtree slices/datas/metas ready to be written — mirroring
// TestSyncManager_createSubtrees_MultiSubtreeDistribution's construction.
func buildPopulatedSubtrees(t *testing.T, sm *SyncManager, numRegular, maxItems int) (blockIdent, []*subtreepkg.Subtree, []*subtreepkg.Data, []*subtreepkg.Meta) {
	t.Helper()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: chainhash.Hash{},
			Timestamp: time.Now(),
			Bits:      0x1d00ffff,
			Nonce:     0,
		},
	}

	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)

	parentHash := chainhash.Hash{0x01}
	for i := 0; i < numRegular; i++ {
		regularMsgTx := wire.NewMsgTx(1)
		regularMsgTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: parentHash, Index: uint32(i)}, //nolint:gosec
			SignatureScript:  []byte{0x00, byte(i)},
			Sequence:         0xffffffff,
		})
		regularMsgTx.AddTxOut(&wire.TxOut{Value: 1000 + int64(i), PkScript: []byte{0x76, 0xa9, 0x14, byte(i)}})
		msgBlock.Transactions = append(msgBlock.Transactions, regularMsgTx)
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(100)

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	txOrder, err := sm.createTxMap(context.Background(), block, txMap)
	require.NoError(t, err)

	for _, wrapper := range txMap.Range() {
		for _, in := range wrapper.Tx.Inputs {
			in.PreviousTxSatoshis = 5_000
			in.PreviousTxScript = &bscript.Script{0x76, 0xa9, 0x14}
		}
	}

	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(len(block.Transactions()), maxItems)
	require.NoError(t, err)
	require.Greater(t, numSubtrees, 1, "test needs a genuinely multi-subtree block")

	slices := make([]*subtreepkg.Subtree, numSubtrees)
	datas := make([]*subtreepkg.Data, numSubtrees)
	metas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		require.NoError(t, terr)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		}

		slices[i] = st
		datas[i] = subtreepkg.NewSubtreeData(st)
		metas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	require.NoError(t, sm.createSubtrees(context.Background(), testBlockIdent(block), txOrder, txMap, slices, datas, metas, false))

	return testBlockIdent(block), slices, datas, metas
}

// TestDownloadToDisk_SubtreeFanOut covers Wave 2 #5: a block's subtrees write
// CONCURRENTLY (peak concurrent subtree-data writes > 1) at the default fan-out,
// while SubtreeWriteConcurrency=1 keeps them serial (byte-identical to the old
// loop). All subtree files land regardless.
func TestDownloadToDisk_SubtreeFanOut(t *testing.T) {
	initPrometheusMetrics()

	run := func(t *testing.T, concurrency int) (peak int32, writes int32, store blob.Store, slices []*subtreepkg.Subtree) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Legacy.SubtreeWriteConcurrency = concurrency

		track := &concTrackStore{
			Store:   memory.New(),
			trackFT: fileformat.FileTypeSubtreeData,
			hold:    40 * time.Millisecond,
		}

		sm := &SyncManager{
			logger:       ulogger.TestLogger{},
			settings:     tSettings,
			subtreeStore: track,
		}

		// 10 regular txs + coinbase, maxItems 4 -> 3 subtrees.
		bi, slices, datas, metas := buildPopulatedSubtrees(t, sm, 10, 4)

		require.NoError(t, sm.writeSubtrees(context.Background(), bi, slices, datas, metas, true))

		return track.peak.Load(), track.writes.Load(), track, slices
	}

	t.Run("default concurrency fans out (peak > 1)", func(t *testing.T) {
		peak, writes, store, slices := run(t, 8)

		require.Equal(t, int32(len(slices)), writes, "every subtree's data file must be written exactly once")
		require.Greater(t, peak, int32(1), "the outer subtree-write loop must fan out (concurrent writes)")

		// Every subtree file landed.
		for _, st := range slices {
			exists, err := store.Exists(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtree)
			require.NoError(t, err)
			require.True(t, exists, "subtree file must be written to the store")
		}
	})

	t.Run("concurrency=1 is serial (peak == 1, byte-identical)", func(t *testing.T) {
		peak, writes, _, slices := run(t, 1)

		require.Equal(t, int32(len(slices)), writes)
		require.Equal(t, int32(1), peak, "concurrency=1 must write subtrees one at a time (the old serial loop)")
	})
}

// TestDownloadToDisk_SubtreeFanOutFirstError proves a SINGLE subtree write
// failure aborts the whole block-prepare with that first error (errgroup
// first-error propagation), even under the concurrent fan-out.
func TestDownloadToDisk_SubtreeFanOutFirstError(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SubtreeWriteConcurrency = 8

	sm := &SyncManager{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	// Build the subtrees first (against a throwaway sm) so we know subtree 1's key,
	// then wire a store that fails Exists for exactly that key.
	bi, slices, datas, metas := buildPopulatedSubtrees(t, sm, 10, 4)
	require.Greater(t, len(slices), 1)

	failKey := slices[1].RootHash()[:]
	sm.subtreeStore = &failExistsStore{Store: memory.New(), failKey: failKey}

	err := sm.writeSubtrees(context.Background(), bi, slices, datas, metas, true)
	require.Error(t, err, "a single subtree write failure must abort the block with the first error")
}

// TestDownloadToDisk_FlagOffWave2 confirms the flag-off (downloadToDisk off) path
// is unchanged: arrival persistence is a no-op, presence is always false, and the
// prepare pool / reorder plumbing never engages. The subtree fan-out (#5) is
// independent of the flag; with SubtreeWriteConcurrency=1 it is the byte-identical
// serial loop (asserted above), so nothing about the off path changes.
func TestDownloadToDisk_FlagOffWave2(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, false, dtdDiskStore(t))

	require.False(t, sm.downloadToDisk(), "flag-off must report the feature disabled")

	// Arrival write is a no-op; nothing lands on disk, so the drain walk / prepare
	// pool have nothing to do and the reorder buffer is never constructed.
	hash, raw, _ := dtdMakeBlock(t, 700, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, nil, raw))
	require.False(t, sm.haveBlockOnDisk(ctx, hash), "flag-off must not persist blocks (byte-identical to today)")

	// prepareDiskBlock is never dispatched off the flag, but even called directly it
	// is a pure read+prepare with no drain-state effect — confirm it reads nothing
	// (the block was not persisted) and surfaces a transient read fault, disturbing
	// no ledger.
	res := sm.prepareDiskBlock(ctx, diskPrepareReq{hash: hash, height: 700})
	require.Error(t, res.err)
	require.True(t, res.readFault, "an absent block surfaces a read fault, never a peer-fault disconnect")
}
