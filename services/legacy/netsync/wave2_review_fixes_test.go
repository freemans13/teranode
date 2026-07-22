// Copyright (c) 2026 The Teranode developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the two MEDIUM review findings on Wave 2 (parallel prepare + real
// commit windows + subtree fan-out, commit 1ea53f71f):
//
//  1. diskPrepareByteTracker (manager.go) — a byte-budget companion to the
//     count-only diskPrepareWorkers cap on the download-to-disk prepare pool's
//     sliding window (diskInFlight + diskReadyMap). Covered by a direct unit
//     test of the tracker's contract, a deterministic model of the dispatch
//     loop's byte gate (manager.go: drainValidateFromDiskWalk's async-eligible
//     branch, the "for i := range cands" loop), and an end-to-end run against
//     the REAL diskPrepareWorker pool proving sustained throttling never stalls
//     forward progress.
//
//  2. subtreeWriteSem (manager.go / handle_block.go) — a single process-wide
//     semaphore bounding TOTAL concurrent subtree-file blob writes across every
//     prepare worker and block, superseding the naive DiskPrepareWorkers x
//     SubtreeWriteConcurrency x 3 product. Covered by driving several blocks'
//     writeSubtrees concurrently and asserting peak concurrency never exceeds
//     the configured cap, plus a first-error-preserved-under-the-cap case.

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// ---------------------------------------------------------------------------
// Fix #1: diskPrepareByteTracker
// ---------------------------------------------------------------------------

// TestDiskPrepareByteTracker_Semantics unit-tests the tracker's contract in
// isolation: the disabled (budget<=0) gate, the pre-sample seed estimate, how
// dispatch/complete/remove maintain the running total, and how a completion
// folds into the running average used to estimate future dispatches.
func TestDiskPrepareByteTracker_Semantics(t *testing.T) {
	t.Run("budget<=0 disables the gate regardless of tracked bytes", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(0, 4)
		require.False(t, tr.wouldExceed())

		h := chainhash.Hash{0x01}
		tr.dispatch(h)
		tr.complete(h, 1<<40) // absurdly large; still must not trip a disabled gate
		require.False(t, tr.wouldExceed(), "budget<=0 must behave as count-cap-only (pre-fix behaviour)")
	})

	t.Run("pre-sample estimate seeds an even split of the budget across the pool width", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 4)
		require.Equal(t, int64(250), tr.estimate(), "before any sample, estimate = budget/workers")
	})

	t.Run("workers clamps to a floor of 1", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 0)
		require.Equal(t, int64(1000), tr.estimate(), "workers<1 must clamp to 1, not divide by zero")
	})

	t.Run("dispatch adds the current estimate to the total", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 4)
		h1, h2 := chainhash.Hash{0x01}, chainhash.Hash{0x02}

		tr.dispatch(h1)
		require.Equal(t, int64(250), tr.total)

		tr.dispatch(h2)
		require.Equal(t, int64(500), tr.total)
	})

	t.Run("complete replaces the estimate with the actual size and updates the running average", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 4)
		h1 := chainhash.Hash{0x01}

		tr.dispatch(h1) // seeds total=250 (the pre-sample estimate)
		tr.complete(h1, 900)
		require.Equal(t, int64(900), tr.total, "total must reflect the ACTUAL size, not the stale estimate")
		require.Equal(t, int64(900), tr.estimate(), "estimate() must now use the real sample, not the seed")

		// A second, much smaller real block folds into the running average.
		h2 := chainhash.Hash{0x02}
		tr.dispatch(h2) // uses the now-updated estimate (900) as its provisional charge
		require.Equal(t, int64(1800), tr.total)
		tr.complete(h2, 100)
		require.Equal(t, int64(1000), tr.total, "900 (h1 actual) + 100 (h2 actual)")
		require.Equal(t, int64(500), tr.estimate(), "running average of the two real samples: (900+100)/2")
	})

	t.Run("remove frees the tracked bytes for a hash and is a safe no-op on an absent hash", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 4)
		h1 := chainhash.Hash{0x01}

		tr.dispatch(h1)
		tr.complete(h1, 900)
		require.Equal(t, int64(900), tr.total)

		tr.remove(h1)
		require.Equal(t, int64(0), tr.total)
		require.NotContains(t, tr.bytes, h1)

		require.NotPanics(t, func() { tr.remove(chainhash.Hash{0xff}) })
		require.Equal(t, int64(0), tr.total, "removing an absent hash must not perturb the total")
	})

	t.Run("wouldExceed flips true once total+estimate would breach budget", func(t *testing.T) {
		tr := newDiskPrepareByteTracker(1000, 4)
		h1 := chainhash.Hash{0x01}

		tr.dispatch(h1)
		tr.complete(h1, 900) // total=900; estimate()=900 -> 900+900=1800 > 1000
		require.True(t, tr.wouldExceed())

		tr.remove(h1)
		require.False(t, tr.wouldExceed(), "headroom must return once the oversized block is removed")
	})
}

// dispatchLoopModel mirrors manager.go's drainValidateFromDiskWalk async-eligible
// dispatch loop (the "for i := range cands" body) LINE FOR LINE: skip a candidate
// already pending or buffered, exempt cands[0] (the lowest uncommitted header,
// which both loops in production resolve identically) from the byte gate so the
// walk always makes forward progress, break at the first i>0 candidate that
// would breach budget, else dispatch (recording the estimate and marking it
// pending). It intentionally omits the unrelated structural gates (checkpoint /
// unified / recovery-exhausted / on-disk / maturity), which are orthogonal to
// the byte budget under test here — see the "real worker pool" test below for
// an end-to-end run through the actual production code path.
func dispatchLoopModel(tr *diskPrepareByteTracker, cands []chainhash.Hash, pending, buffered map[chainhash.Hash]bool) (sent []chainhash.Hash) {
	for i, h := range cands {
		if pending[h] {
			continue
		}

		if buffered[h] {
			continue
		}

		if i > 0 && tr.wouldExceed() {
			break
		}

		tr.dispatch(h)
		pending[h] = true
		sent = append(sent, h)
	}

	return sent
}

// TestDiskPrepareByteTracker_GateThrottlesLookaheadNotLowest proves the core
// safety property of review fix #1: a single already-in-flight/buffered
// oversized block (the motivating "123k-tx block" scenario) saturating the
// budget stops the walk from dispatching further LOOK-AHEAD candidates, but
// NEVER stops it from dispatching the lowest (cands[0]) — the exemption that
// guarantees the walk can always make forward progress and never deadlocks on
// its own gate. Once the oversized block is consumed (removed), headroom
// returns and look-ahead dispatch resumes.
func TestDiskPrepareByteTracker_GateThrottlesLookaheadNotLowest(t *testing.T) {
	const workers = 4
	tr := newDiskPrepareByteTracker(1000, workers)

	// One already-in-flight fat block whose ACTUAL size (10,000B) far exceeds
	// the whole budget (1000B).
	fatHash := chainhash.Hash{0xfa}
	tr.dispatch(fatHash)
	tr.complete(fatHash, 10_000)
	require.True(t, tr.wouldExceed(), "a single oversized in-flight block must saturate the budget")

	cands := []chainhash.Hash{{0x01}, {0x02}, {0x03}, {0x04}}
	pending := map[chainhash.Hash]bool{}
	buffered := map[chainhash.Hash]bool{}

	sent := dispatchLoopModel(tr, cands, pending, buffered)
	require.Equal(t, []chainhash.Hash{cands[0]}, sent,
		"only the lowest (i==0) candidate may dispatch while the budget is saturated by the in-flight fat block")

	// A further pass (nothing has completed yet) dispatches nothing new: cands[0]
	// is now pending (skipped), and every i>0 candidate is still gated.
	sentAgain := dispatchLoopModel(tr, cands, pending, buffered)
	require.Empty(t, sentAgain, "a repeat pass must not re-dispatch the pending lowest or admit any look-ahead while still saturated")
}

// TestDiskPrepareByteTracker_RemoveRestoresHeadroom proves remove() (called on
// the feed-consume path AND the stale-reorder-buffer prune path) frees tracked
// budget for further look-ahead dispatch. Uses ESTIMATE-only entries (none
// completed with an actual size) so the running average used by estimate()
// stays at its pre-sample seed throughout — isolating remove()'s effect on
// wouldExceed() from the running-average mechanics already covered by
// TestDiskPrepareByteTracker_Semantics.
func TestDiskPrepareByteTracker_RemoveRestoresHeadroom(t *testing.T) {
	const workers = 4
	tr := newDiskPrepareByteTracker(1000, workers) // seed estimate = 1000/4 = 250

	dispatched := make([]chainhash.Hash, workers)
	for i := 0; i < workers; i++ {
		dispatched[i] = chainhash.Hash{byte(i + 1)}
		tr.dispatch(dispatched[i])
	}

	require.Equal(t, int64(1000), tr.total, "a full window of seed-estimated dispatches sums to exactly the budget")
	require.True(t, tr.wouldExceed(), "a fifth candidate at the same estimate would breach budget")

	// A header reset prunes one in-flight dispatch before it ever completes
	// (manager.go's stale reorder-buffer prune loop calls diskBytes.remove the
	// same way).
	tr.remove(dispatched[1])
	require.Equal(t, int64(750), tr.total)
	require.False(t, tr.wouldExceed(), "removing a tracked entry must free headroom for further dispatch")
}

// TestDownloadToDisk_ByteBudgetThrottlesButNeverStalls drives the REAL
// diskPrepareWorker pool (not a model) against a permanently-saturating budget
// — after the first real completion, every subsequent estimate alone exceeds
// the budget, so cands[0]'s exemption is the ONLY thing keeping the walk alive
// for the rest of the run. Proves review fix #1's central deadlock-freedom /
// forward-progress claim end-to-end: every block still completes and the
// reorder buffer still emits the whole run strict-ascending, despite the pool
// being throttled to effective width 1 for nearly its entire duration.
func TestDownloadToDisk_ByteBudgetThrottlesButNeverStalls(t *testing.T) {
	const first = int32(5000)
	const totalBlocks = 12
	const workers = 4

	store := dtdDiskStore(t)
	sm := dtdNewManager(t, true, store)

	reqC := make(chan diskPrepareReq, workers)
	readyC := make(chan *diskPrepared, workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < workers; i++ {
		go sm.diskPrepareWorker(ctx, reqC, readyC)
	}

	heights := make([]int32, totalBlocks)
	hashOf := make(map[int32]chainhash.Hash, totalBlocks)

	for i := 0; i < totalBlocks; i++ {
		h := first + int32(i) //nolint:gosec
		heights[i] = h

		hash, raw, _ := dtdMakeBlock(t, h, uint32(h)) //nolint:gosec
		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, nil, raw))
		hashOf[h] = hash
	}

	// budget=1: the pre-sample seed (1/workers) rounds to 0, so the FIRST pass
	// dispatches a burst up to the pool width (nothing accumulates yet) — but
	// the instant a REAL block size is known (a few hundred bytes for these
	// single-coinbase test blocks), every future estimate alone exceeds the
	// budget, throttling every later pass to effective width 1: only cands[0]
	// (the exempted lowest) ever dispatches again.
	tr := newDiskPrepareByteTracker(1, workers)

	reorder := newReorderModel(first)
	pending := map[int32]bool{}

	dispatchPass := func() (sentCount int) {
		// Rebuild the candidate list fresh each pass from the current frontier —
		// heights already committed (< reorder.nextNeeded) or completed-but-
		// buffered (reorder.ready) are never candidates at all — exactly mirroring
		// manager.go's downloadFrontierAnchorLocked walk, which skips owned/fed
		// headers. This is what makes cands[0] genuinely "the current lowest
		// uncommitted header" on every pass, matching production, rather than a
		// fixed array position.
		cands := make([]int32, 0, len(heights))

		for _, h := range heights {
			if h < reorder.nextNeeded || reorder.ready[h] {
				continue
			}

			cands = append(cands, h)
		}

		for i, h := range cands {
			if pending[h] {
				continue // already in flight
			}

			if i > 0 && tr.wouldExceed() {
				break
			}

			if sentCount >= workers {
				break
			}

			select {
			case reqC <- diskPrepareReq{hash: hashOf[h], height: h}:
				pending[h] = true
				tr.dispatch(hashOf[h])
				sentCount++
			default:
			}
		}

		return sentCount
	}

	committed := 0
	sampleSeen := false
	maxSentAfterFirstSample := 0
	deadline := time.After(20 * time.Second)

	for committed < totalBlocks {
		sent := dispatchPass()

		if sampleSeen && sent > maxSentAfterFirstSample {
			maxSentAfterFirstSample = sent
		}

		select {
		case res := <-readyC:
			require.NotNil(t, res)

			delete(pending, res.height)
			tr.complete(hashOf[res.height], diskPreparedResultBytes(res))
			sampleSeen = true

			before := len(reorder.emitted)
			reorder.complete(res.height)

			for _, h := range reorder.emitted[before:] {
				tr.remove(hashOf[h])
			}

			committed = len(reorder.emitted)
		case <-deadline:
			t.Fatalf("stalled at %d/%d blocks committed: the byte-budget gate deadlocked instead of only throttling", committed, totalBlocks)
		}
	}

	requireAscendingContiguous(t, reorder.emitted, first)
	require.Len(t, reorder.emitted, totalBlocks, "every block must eventually commit despite sustained byte-budget throttling")
	require.LessOrEqual(t, maxSentAfterFirstSample, 1,
		"once a real sample exists, every later pass may dispatch at most the exempted lowest header (effective width 1)")
}

// ---------------------------------------------------------------------------
// Fix #2: subtreeWriteSem (joint subtree-write concurrency ceiling)
// ---------------------------------------------------------------------------

// jointConcTrackStore records the peak number of CONCURRENT SetFromReader calls
// across EVERY file type (subtree, subtreeData, subtreeMeta combined — unlike
// wave2_parallel_test.go's concTrackStore, which tracks one type), so a test can
// prove the JOINT semaphore bounds the TOTAL across every prepare worker/block,
// not merely one block's own internal fan-out.
type jointConcTrackStore struct {
	blob.Store
	cur    atomic.Int32
	peak   atomic.Int32
	writes atomic.Int32
	hold   time.Duration
}

func (s *jointConcTrackStore) SetFromReader(ctx context.Context, key []byte, ft fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
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

	return s.Store.SetFromReader(ctx, key, ft, reader, opts...)
}

// buildPopulatedSubtreesSalted builds a multi-subtree block exactly like
// buildPopulatedSubtrees (wave2_parallel_test.go), but with a caller-supplied
// salt/height so concurrently-built "blocks" in the same test never collide on
// content-addressed subtree keys (buildPopulatedSubtrees's fixed construction
// produces byte-identical subtrees on every call, which is fine for its own
// single-block tests but would silently collapse concurrency in a multi-block
// test via the store's short-circuit on already-existing keys).
func buildPopulatedSubtreesSalted(t *testing.T, sm *SyncManager, numRegular, maxItems int, salt byte, height int32) (blockIdent, []*subtreepkg.Subtree, []*subtreepkg.Data, []*subtreepkg.Meta) {
	t.Helper()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: chainhash.Hash{},
			Timestamp: time.Now(),
			Bits:      0x1d00ffff,
			Nonce:     uint32(salt), //nolint:gosec
		},
	}

	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00, salt},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14, salt}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)

	parentHash := chainhash.Hash{salt}
	for i := 0; i < numRegular; i++ {
		regularMsgTx := wire.NewMsgTx(1)
		regularMsgTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: parentHash, Index: uint32(i)}, //nolint:gosec
			SignatureScript:  []byte{0x00, salt, byte(i)},
			Sequence:         0xffffffff,
		})
		regularMsgTx.AddTxOut(&wire.TxOut{Value: 1000 + int64(i), PkScript: []byte{0x76, 0xa9, 0x14, salt, byte(i)}})
		msgBlock.Transactions = append(msgBlock.Transactions, regularMsgTx)
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

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

// TestDownloadToDisk_JointSubtreeWriteSemaphore_CapsConcurrency proves review
// fix #2's central invariant: with abundant demand (several blocks' worth of
// subtree writes running concurrently, each internally fanned out), the joint
// semaphore caps TOTAL concurrent subtree-file blob writes at the configured
// limit — never above it, and (with the demand this test generates) genuinely
// binding, not just incidentally low.
func TestDownloadToDisk_JointSubtreeWriteSemaphore_CapsConcurrency(t *testing.T) {
	initPrometheusMetrics()

	const jointCap = 6
	const numBlocks = 5
	const numRegular = 12
	const maxItems = 4

	track := &jointConcTrackStore{Store: memory.New(), hold: 25 * time.Millisecond}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SubtreeWriteConcurrency = 8 // no per-block throttling of its own

	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		subtreeStore:    track,
		subtreeWriteSem: semaphore.NewWeighted(jointCap),
	}

	type built struct {
		bi     blockIdent
		slices []*subtreepkg.Subtree
		datas  []*subtreepkg.Data
		metas  []*subtreepkg.Meta
	}

	blocks := make([]built, numBlocks)
	for b := 0; b < numBlocks; b++ {
		bi, slices, datas, metas := buildPopulatedSubtreesSalted(t, sm, numRegular, maxItems, byte(b+1), int32(100+b)) //nolint:gosec
		blocks[b] = built{bi: bi, slices: slices, datas: datas, metas: metas}
	}

	numSubtreesPerBlock := len(blocks[0].slices)

	var wg sync.WaitGroup

	errs := make([]error, numBlocks)

	done := make(chan struct{})

	go func() {
		defer close(done)

		for b := 0; b < numBlocks; b++ {
			b := b

			wg.Add(1)

			go func() {
				defer wg.Done()

				errs[b] = sm.writeSubtrees(context.Background(), blocks[b].bi, blocks[b].slices, blocks[b].datas, blocks[b].metas, true)
			}()
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("writeSubtrees across concurrent blocks did not complete - possible deadlock in the joint subtree-write semaphore")
	}

	for b, err := range errs {
		require.NoError(t, err, "block %d must write cleanly", b)
	}

	peak := track.peak.Load()
	require.LessOrEqual(t, peak, int32(jointCap), "joint semaphore must cap TOTAL concurrent subtree writes across all blocks at the configured limit")
	require.Greater(t, peak, int32(jointCap/2),
		"with abundant demand (numBlocks x numSubtrees x 3 far exceeds the cap) the cap should be substantially utilised, proving it actually binds")

	require.Equal(t, int32(numBlocks*numSubtreesPerBlock*3), track.writes.Load(), //nolint:gosec
		"every subtree's three files must still be written exactly once per block despite the joint cap")
}

// TestDownloadToDisk_JointSubtreeWriteSemaphore_FirstErrorPreserved proves a
// single subtree write failure still aborts its block with that first error
// even under a TIGHT joint cap that forces most writers to queue for a permit
// — and that the queued waiters are released promptly (no deadlock) once the
// errgroup's context cancels on the first error.
func TestDownloadToDisk_JointSubtreeWriteSemaphore_FirstErrorPreserved(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SubtreeWriteConcurrency = 8

	sm := &SyncManager{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	bi, slices, datas, metas := buildPopulatedSubtreesSalted(t, sm, 12, 4, 0x01, 100)
	require.Greater(t, len(slices), 1)

	failKey := slices[1].RootHash()[:]
	sm.subtreeStore = &failExistsStore{Store: memory.New(), failKey: failKey}
	// A tight joint cap forces the surviving writers to queue for a permit;
	// the first error must still propagate and every queued waiter must be
	// released promptly rather than hang.
	sm.subtreeWriteSem = semaphore.NewWeighted(2)

	resultC := make(chan error, 1)
	go func() {
		resultC <- sm.writeSubtrees(context.Background(), bi, slices, datas, metas, true)
	}()

	select {
	case err := <-resultC:
		require.Error(t, err, "a single subtree write failure must abort the block with the first error, even under a tight joint semaphore")
	case <-time.After(10 * time.Second):
		t.Fatal("writeSubtrees did not return - the joint semaphore deadlocked on the error path")
	}
}
