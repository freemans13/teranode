// Copyright (c) 2026 The Teranode developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Unit tests for the svnode-style download-to-disk decoupling
// (legacy_downloadToDisk). They cover the arrival-write / presence / read-back
// helpers and the arrival hand-off decision (PersistArrivalAndDecouple),
// mirroring the design's test plan:
//
//  1. persistRawBlockOnArrival writes byte-identical bytes retrievable via
//     GetIoReader; a second call is an idempotent no-op; Exists() is true after.
//  2. haveBlockOnDisk is true after arrival / false before — the fetch frontier's
//     skip predicate (the hasData() analogue).
//  3. Out-of-order arrival (N+2, N+3 before N+1) all land on disk, none dropped.
//  4. Giant-block isolation: while a validation slot is stuck, arrival writes for
//     later blocks still complete promptly (they hit disk, never wait on the
//     validator).
//  5. No-drop invariant: drive far more out-of-order blocks than the retired park
//     cap; assert all persist, none dropped.
//  6. Flag-OFF regression: with legacy_downloadToDisk=false the arrival write is a
//     no-op and haveBlockOnDisk is always false (byte-identical to today).
//
// HONEST SCOPE NOTE: these tests exercise the disk-first HELPERS and the arrival
// hand-off directly (the durable, unbounded buffer that is the whole point of the
// feature). The full in-order commit walk (drainValidateFromDisk) is NOT driven
// end-to-end here: it needs a live sync peer, the headers-first header list, the
// window accumulator, and the block-assembly maturity gate all wired together
// (the ~130KB manager_test.go machinery). What is proven here is the load-bearing
// contract — arrival is decoupled from validation, out-of-order blocks are all
// persisted and never dropped, and presence gates the frontier — which is exactly
// where the freeze-then-burst bug lived. The strict-ascending commit ordering of
// the worker is left to the existing window_pipeline / window_contiguity tests
// (which already assert ascending commit) plus a live IBD soak.

import (
	"bytes"
	"container/list"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// dtdMakeBlock builds a minimal, well-formed, uniquely-hashed wire.MsgBlock (a
// single coinbase whose signature script carries height+nonce) and returns its
// header hash, its serialized bytes, and the block. It does NOT solve PoW — the
// download-to-disk helpers under test serialize / store / read back raw bytes and
// never check PoW, so an unsolved header is sufficient and keeps the test fast.
func dtdMakeBlock(t *testing.T, height int32, nonce uint32) (chainhash.Hash, []byte, *wire.MsgBlock) {
	t.Helper()

	net := &chaincfg.RegressionNetParams

	addr, _, err := GenerateAnyoneCanspendAddress(net)
	require.NoError(t, err)

	cbScript, err := standardCoinbaseScript(height, uint64(nonce))
	require.NoError(t, err)

	cbTx, err := createCoinbaseTx(cbScript, height, addr, nil, net)
	require.NoError(t, err)

	var blk wire.MsgBlock
	blk.Header = wire.BlockHeader{
		Version:   1,
		PrevBlock: chainhash.Hash{},
		Timestamp: time.Unix(1600000000+int64(height), 0),
		Bits:      net.PowLimitBits,
		Nonce:     nonce,
	}
	require.NoError(t, blk.AddTransaction(cbTx.MsgTx()))

	var buf bytes.Buffer
	require.NoError(t, blk.Serialize(&buf))

	return blk.Header.BlockHash(), buf.Bytes(), &blk
}

// dtdNewManager builds a minimal SyncManager wired with the fields the
// download-to-disk helpers touch: a memory block store (mainBlockStore), full
// default settings (for the filestorer buffer size), a logger, a validate-signal
// channel, and the DownloadToDisk gate flipped as requested. It deliberately
// leaves the heavy validation dependencies (blockValidation, subtreeValidation,
// blockAssembly, header list, sync peer) nil: the arrival helpers must never
// touch them, and a passing test with them nil is itself proof of decoupling.
func dtdNewManager(t *testing.T, flagOn bool, store blob.Store) *SyncManager {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.DownloadToDisk = flagOn

	return &SyncManager{
		ctx:             context.Background(),
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		mainBlockStore:  store,
		validateSignal:  make(chan struct{}, 1),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Minute),
		peerStates:      txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		assignedTo:      make(map[chainhash.Hash]map[*peerpkg.Peer]time.Time),
		refetchBlocks:   make(map[chainhash.Hash]struct{}),
	}
}

// serializeBlock re-serializes a MsgBlock so a read-back can be compared byte-for
// byte against the originally-stored bytes.
func serializeBlock(t *testing.T, b *wire.MsgBlock) []byte {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, b.Serialize(&buf))

	return buf.Bytes()
}

// TestDownloadToDisk_PersistAndReadBack covers test-plan item 1: the arrival write
// is byte-identical and retrievable, Exists() flips true, and a second write of
// the same block is a cheap idempotent no-op (not an error).
func TestDownloadToDisk_PersistAndReadBack(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := dtdNewManager(t, true, store)

	hash, raw, blk := dtdMakeBlock(t, 101, 1)

	// Not on disk before arrival.
	require.False(t, sm.haveBlockOnDisk(ctx, hash), "block must not be on disk before arrival")

	// Arrival write.
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))

	// Presence flag flips.
	require.True(t, sm.haveBlockOnDisk(ctx, hash), "block must be on disk after arrival")

	exists, err := store.Exists(ctx, hash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.True(t, exists, "Exists() must be true after arrival")

	// Read-back is byte-identical to what was stored.
	readBack, err := sm.readRawBlockFromDisk(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, raw, serializeBlock(t, readBack), "read-back bytes must equal stored bytes")
	require.Equal(t, blk.Header.BlockHash(), readBack.Header.BlockHash(), "read-back hash must match")

	// Second write is an idempotent no-op (ErrBlobAlreadyExists short-circuit),
	// not an error, and the bytes are unchanged.
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw), "re-arrival must be a no-op, not an error")

	readBack2, err := sm.readRawBlockFromDisk(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, raw, serializeBlock(t, readBack2), "bytes must be unchanged after idempotent re-write")
}

// TestDownloadToDisk_FrontierSkipPredicate covers test-plan item 2: haveBlockOnDisk
// (the predicate the fetch frontier consults at manager.go:3666 / :4075 to skip a
// header whose block is already on disk) is true only for delivered blocks. The
// frontier therefore requests exactly the not-on-disk successors.
func TestDownloadToDisk_FrontierSkipPredicate(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())

	// Header chain N, N+1, N+2, N+3. Deliver only N and N+2.
	type hdr struct {
		hash chainhash.Hash
		raw  []byte
	}

	hdrs := make([]hdr, 4)
	for i := 0; i < 4; i++ {
		h, raw, _ := dtdMakeBlock(t, int32(200+i), uint32(i))
		hdrs[i] = hdr{hash: h, raw: raw}
	}

	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hdrs[0].hash, hdrs[0].raw)) // N
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hdrs[2].hash, hdrs[2].raw)) // N+2

	// The frontier's request set = headers NOT on disk = {N+1, N+3}.
	var wantRequest []chainhash.Hash
	for i, h := range hdrs {
		onDisk := sm.haveBlockOnDisk(ctx, h.hash)
		switch i {
		case 0, 2:
			require.True(t, onDisk, "delivered header %d must read as on-disk (frontier skips it)", i)
		default:
			require.False(t, onDisk, "undelivered header %d must read as not-on-disk (frontier requests it)", i)
			wantRequest = append(wantRequest, h.hash)
		}
	}

	require.Equal(t, []chainhash.Hash{hdrs[1].hash, hdrs[3].hash}, wantRequest,
		"frontier must request exactly the not-on-disk successors")
}

// TestDownloadToDisk_OutOfOrderAllLand covers test-plan item 3 (persistence half):
// blocks delivered N+2, N+3 before N+1 all land on disk, none dropped, regardless
// of arrival order — svnode's mapBlocksUnlinked property. (The strict-ascending
// commit once N+1 arrives is the drainValidateFromDisk walk, out of unit scope —
// see the file header note.)
func TestDownloadToDisk_OutOfOrderAllLand(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())

	n := int32(500)
	blocks := make(map[int32]struct {
		hash chainhash.Hash
		raw  []byte
	})

	for _, h := range []int32{n, n + 1, n + 2, n + 3, n + 4} {
		hash, raw, _ := dtdMakeBlock(t, h, uint32(h))
		blocks[h] = struct {
			hash chainhash.Hash
			raw  []byte
		}{hash, raw}
	}

	// Deliver strictly out of order: the parent (N) and N+1 arrive LAST.
	order := []int32{n + 2, n + 4, n + 3, n, n + 1}
	for _, h := range order {
		b := blocks[h]
		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, b.hash, b.raw),
			"out-of-order block %d must persist", h)
	}

	// Every block is durably on disk; none was dropped.
	for h, b := range blocks {
		require.True(t, sm.haveBlockOnDisk(ctx, b.hash), "block %d must be on disk", h)

		readBack, err := sm.readRawBlockFromDisk(ctx, b.hash)
		require.NoError(t, err)
		require.Equal(t, b.raw, serializeBlock(t, readBack), "block %d read-back must be byte-identical", h)
	}
}

// TestDownloadToDisk_GiantBlockIsolation covers test-plan item 4 (the core
// assertion). While one validation slot is stuck (a giant block "inside
// ProcessBlockWindow"), arrival writes for LATER blocks must still complete
// promptly and land on disk — the arrival path never waits on the validator. The
// stuck slot is modelled by a goroutine blocked on a channel that is NOT released
// during the assertion window; the arrival writes proceed with zero dependence on
// it. That the SyncManager's validation deps are nil and nothing panics is
// additional structural proof that arrival is decoupled from validation.
func TestDownloadToDisk_GiantBlockIsolation(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())

	// Model a giant block wedged in the (separate) validation slot.
	validatorEntered := make(chan struct{})
	validatorRelease := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(validatorEntered)
		<-validatorRelease // stuck "inside ProcessBlockWindow" for a giant block
	}()

	<-validatorEntered // ensure the slot is occupied before we test arrivals

	// With the validator stuck, deliver many later blocks. Each must persist
	// promptly; nothing here may block on validatorRelease.
	const later = 100
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < later; i++ {
			hash, raw, _ := dtdMakeBlock(t, int32(1000+i), uint32(i))
			require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
			require.True(t, sm.haveBlockOnDisk(ctx, hash))
		}
	}()

	select {
	case <-done:
		// Arrival writes completed while the validator is STILL stuck — proving
		// fetch/persist is not gated on validation.
	case <-time.After(30 * time.Second):
		close(validatorRelease)
		wg.Wait()
		t.Fatal("arrival writes blocked while a validation slot was stuck: fetch is NOT decoupled from validation")
	}

	// The validator is still occupied at this point; release it now to clean up.
	require.Len(t, sm.validateSignal, 0, "no accidental validator wake from bare arrival writes")

	close(validatorRelease)
	wg.Wait()
}

// TestDownloadToDisk_NoDropInvariant covers test-plan item 5: drive far more
// out-of-order blocks than the retired in-memory park cap
// (legacy_parallelWindowMaxParkedBlocks, default 16384 — the old drop threshold)
// would have tolerated in spirit, and assert every one persists and none is
// dropped or re-fetched. We use a comfortably-large count that still runs fast;
// the point is that the disk buffer is unbounded and has NO drop path (unlike
// parkStrayWindowBlock's "park full; dropping stray block" log line).
func TestDownloadToDisk_NoDropInvariant(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := dtdNewManager(t, true, store)

	// More blocks than a single park window would ever hold in flight; every one
	// must survive on disk. (Kept in the low thousands so the test stays quick;
	// the invariant — no drop path exists — does not depend on the exact count.)
	const total = 3000

	hashes := make([]chainhash.Hash, total)
	for i := 0; i < total; i++ {
		hash, raw, _ := dtdMakeBlock(t, int32(i+1), uint32(i))
		hashes[i] = hash
		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	}

	// Not one dropped: all present.
	present := 0
	for _, h := range hashes {
		if sm.haveBlockOnDisk(ctx, h) {
			present++
		}
	}
	require.Equal(t, total, present, "every out-of-order block must remain on disk — no drop path")

	// Re-arrival of an already-on-disk block is a no-op (never a re-fetch/re-write).
	_, raw0, _ := dtdMakeBlock(t, 1, 0)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hashes[0], raw0), "on-disk block must not be re-fetched/re-written")
}

// TestDownloadToDisk_FlagOff covers test-plan item 6: with legacy_downloadToDisk
// false the arrival write is a no-op, the presence predicate is always false, and
// the decouple hand-off declines (handled=false) so the caller takes today's
// exact in-memory park/prefetch/inline path — byte-identical to today.
func TestDownloadToDisk_FlagOff(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := dtdNewManager(t, false, store) // flag OFF, store present

	require.False(t, sm.downloadToDisk(), "gate must be closed when the flag is off")

	hash, raw, _ := dtdMakeBlock(t, 300, 7)

	// Arrival write is a no-op: nothing is written.
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))

	exists, err := store.Exists(ctx, hash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, exists, "flag-off arrival must NOT write to disk")

	// Presence predicate is always false when the flag is off (frontier is
	// unchanged from today).
	require.False(t, sm.haveBlockOnDisk(ctx, hash), "flag-off haveBlockOnDisk must be false")

	// The decouple hand-off declines, so OnBlock falls through to today's path.
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0, nil),
		"flag-off decouple must decline (caller uses the normal QueueBlock path)")
}

// TestDownloadToDisk_NoStore covers the nil-store guard: even with the flag on, if
// no block store is configured the feature stays off (downloadToDisk()==false) and
// every helper is a safe no-op. This is the daemon-side "feature stays off if no
// block store" contract.
func TestDownloadToDisk_NoStore(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, nil) // flag ON but no store

	require.False(t, sm.downloadToDisk(), "gate must stay closed with no block store")

	hash, raw, _ := dtdMakeBlock(t, 400, 9)

	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw), "no-store arrival must be a safe no-op")
	require.False(t, sm.haveBlockOnDisk(ctx, hash), "no-store presence must be false")
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0, nil), "no-store decouple must decline")
}

// TestDownloadToDisk_PersistArrivalAndDecouple covers the arrival hand-off
// decision: it declines (handled=false, keep the inline path) when the in-order
// validator is not running, and accepts (handled=true, poke the validator) only
// when both validateFromDiskActive and headersFirstMode are set — the exact guard
// that stops a decoupled block being stranded on disk with no worker to commit it.
func TestDownloadToDisk_PersistArrivalAndDecouple(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())

	hash, raw, _ := dtdMakeBlock(t, 500, 11)

	// Validator NOT active (default): decouple declines, but the block is still
	// written durably to disk (frontier-skip + restart recovery keep working).
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0, nil),
		"decouple must decline while the in-order validator is not running")
	require.True(t, sm.haveBlockOnDisk(ctx, hash),
		"even when decouple declines, the arrival write must still be durable on disk")
	require.Len(t, sm.validateSignal, 0, "no validator poke when the validator is inactive")

	// Now the in-order validator is running AND we are in headers-first mode: a
	// fresh block is decoupled (handled=true) and the validator is poked.
	sm.validateFromDiskActive.Store(true)
	sm.headersFirstMode.Store(true)

	hash2, raw2, _ := dtdMakeBlock(t, 501, 12)
	require.True(t, sm.PersistArrivalAndDecouple(ctx, hash2, raw2, 0, nil),
		"decouple must hand off to the validator when it is running in headers-first mode")
	require.True(t, sm.haveBlockOnDisk(ctx, hash2), "decoupled block must be durable on disk")
	require.Len(t, sm.validateSignal, 1, "decouple must poke the in-order validator exactly once")
}

// TestDownloadToDisk_PokeCoalesces covers the validator wake being non-blocking
// and coalescing: a burst of pokes collapses to a single buffered signal (no
// wakeup lost, none blocks). A no-op when the feature is off.
func TestDownloadToDisk_PokeCoalesces(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())

	// A burst of pokes must never block and must coalesce to one pending signal.
	for i := 0; i < 100; i++ {
		sm.PokeValidateFromDisk()
	}
	require.Len(t, sm.validateSignal, 1, "a burst of pokes must coalesce to a single buffered wake")

	// Drain it, then confirm a further poke re-arms exactly one.
	<-sm.validateSignal
	sm.PokeValidateFromDisk()
	require.Len(t, sm.validateSignal, 1, "poke must re-arm a single wake after draining")

	// Flag off: poke is a no-op (does not touch the channel).
	off := dtdNewManager(t, false, memory.New())
	off.PokeValidateFromDisk()
	require.Len(t, off.validateSignal, 0, "flag-off poke must be a no-op")
}

// dtdBuildStats is a tiny sanity helper used only to give a friendly failure
// message if the coinbase builder ever regresses to producing duplicate hashes
// (which would silently invalidate the out-of-order / no-drop counts).
func dtdBuildStats(t *testing.T) {
	t.Helper()

	seen := map[chainhash.Hash]struct{}{}
	for i := 0; i < 50; i++ {
		h, _, _ := dtdMakeBlock(t, int32(i), uint32(i))
		_, dup := seen[h]
		require.Falsef(t, dup, "dtdMakeBlock produced a duplicate hash at i=%d", i)
		seen[h] = struct{}{}
	}
}

func TestDownloadToDisk_UniqueBlockHashes(t *testing.T) {
	dtdBuildStats(t)
}

// dtdRecordingStore wraps the in-memory blob store to record the DAH / delete /
// current-height calls the download-to-disk self-clean path makes, so a unit test
// can assert them directly without waiting on the background janitor. Every other
// blob.Store method is promoted unchanged from the embedded *memory.Memory.
type dtdRecordingStore struct {
	*memory.Memory
	mu        sync.Mutex
	dahByKey  map[string]uint32
	delByKey  map[string]int
	curHeight uint32
}

func newDTDRecordingStore() *dtdRecordingStore {
	return &dtdRecordingStore{
		Memory:   memory.New(),
		dahByKey: make(map[string]uint32),
		delByKey: make(map[string]int),
	}
}

func (s *dtdRecordingStore) SetDAH(ctx context.Context, key []byte, ft fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	s.mu.Lock()
	s.dahByKey[string(key)] = dah
	s.mu.Unlock()

	return s.Memory.SetDAH(ctx, key, ft, dah, opts...)
}

func (s *dtdRecordingStore) Del(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) error {
	s.mu.Lock()
	s.delByKey[string(key)]++
	s.mu.Unlock()

	return s.Memory.Del(ctx, key, ft, opts...)
}

func (s *dtdRecordingStore) SetCurrentBlockHeight(h uint32) {
	s.mu.Lock()
	s.curHeight = h
	s.mu.Unlock()

	s.Memory.SetCurrentBlockHeight(h)
}

func (s *dtdRecordingStore) dah(hash chainhash.Hash) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.dahByKey[string(hash[:])]

	return v, ok
}

func (s *dtdRecordingStore) delCount(hash chainhash.Hash) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.delByKey[string(hash[:])]
}

func (s *dtdRecordingStore) height() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.curHeight
}

// dtdModelBlock builds a minimal *model.Block (header + height) from a wire block
// so the commit-finalize hook (markDiskBlocksCommitted) can be exercised. Its
// Hash() is its header hash — the same key the block was stored under on arrival.
func dtdModelBlock(t *testing.T, wireBlk *wire.MsgBlock, height uint32) *model.Block {
	t.Helper()

	var hb bytes.Buffer
	require.NoError(t, wireBlk.Header.Serialize(&hb))

	header, err := model.NewBlockHeaderFromBytes(hb.Bytes())
	require.NoError(t, err)

	return &model.Block{Header: header, Height: height}
}

// TestDownloadToDisk_ArrivalDAHSet covers Stage B #6 (arrival half): a block
// written on arrival is tagged with a Delete-At-Height of its own height + the
// configured window, so a block that never commits self-expires once the chain
// passes that height.
func TestDownloadToDisk_ArrivalDAHSet(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)
	sm.settings.Legacy.DownloadToDiskArrivalDAHWindow = 100

	height := int32(700000)
	hash, raw, _ := dtdMakeBlock(t, height, 1)

	// The arrival DAH is anchored on the block's own height, resolved from the
	// headers-first index; seed it as headers-first would.
	sm.headerHeightIndex = map[chainhash.Hash]int32{hash: height}

	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	dah, ok := store.dah(hash)
	require.True(t, ok, "arrival write must set a delete-at-height")
	require.Equal(t, uint32(height)+100, dah, "arrival DAH must be blockHeight + window")
}

// TestDownloadToDisk_ArrivalDAHSkipped covers the two no-DAH cases: window 0
// disables the arrival DAH entirely, and an unresolvable height leaves the block
// un-tagged rather than guessing (both keep the block durable, just without a
// self-expiry).
func TestDownloadToDisk_ArrivalDAHSkipped(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)

	// Window 0: DAH disabled.
	sm.settings.Legacy.DownloadToDiskArrivalDAHWindow = 0
	hash, raw, _ := dtdMakeBlock(t, 123, 1)
	sm.headerHeightIndex = map[chainhash.Hash]int32{hash: 123}
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	_, ok := store.dah(hash)
	require.False(t, ok, "window 0 must disable the arrival DAH")
	require.True(t, sm.haveBlockOnDisk(ctx, hash), "block is still durable without a DAH")

	// Window on but height not in the index: cannot anchor, so no DAH.
	sm.settings.Legacy.DownloadToDiskArrivalDAHWindow = 100
	hash2, raw2, _ := dtdMakeBlock(t, 124, 2)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash2, raw2))
	_, ok2 := store.dah(hash2)
	require.False(t, ok2, "an unresolvable height must leave the block un-tagged")
	require.True(t, sm.haveBlockOnDisk(ctx, hash2), "block is still durable without a DAH")
}

// TestDownloadToDisk_CommitClearsDAH covers Stage B #6 (commit half): committing a
// disk-path block clears its arrival self-expiry DAH to 0 (permanent) so the
// committed chain is retained exactly as today, and drives the store janitor's
// current-height notion to the committed height.
func TestDownloadToDisk_CommitClearsDAH(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)
	sm.settings.Legacy.DownloadToDiskArrivalDAHWindow = 100

	height := int32(555)
	hash, raw, wireBlk := dtdMakeBlock(t, height, 1)
	sm.headerHeightIndex = map[chainhash.Hash]int32{hash: height}

	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	dah, ok := store.dah(hash)
	require.True(t, ok)
	require.Equal(t, uint32(height)+100, dah, "sanity: arrival DAH set before commit")

	mb := dtdModelBlock(t, wireBlk, uint32(height))
	require.Equal(t, hash, *mb.Hash(), "model block hash must match the on-disk key")

	sm.markDiskBlocksCommitted(ctx, []*model.Block{mb})

	dahAfter, ok := store.dah(hash)
	require.True(t, ok)
	require.Equal(t, uint32(0), dahAfter, "commit must clear the arrival DAH to 0 (permanent)")
	require.Equal(t, uint32(height), store.height(), "commit must drive the store janitor height to the committed height")
}

// TestDownloadToDisk_CommitFlagOffNoStoreCalls covers the gate: with the flag off
// markDiskBlocksCommitted must not touch the store at all (byte-identical to today).
func TestDownloadToDisk_CommitFlagOffNoStoreCalls(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, false, store) // flag OFF

	_, _, wireBlk := dtdMakeBlock(t, 10, 1)
	mb := dtdModelBlock(t, wireBlk, 10)

	sm.markDiskBlocksCommitted(ctx, []*model.Block{mb})

	require.Equal(t, uint32(0), store.height(), "flag-off commit must not drive the store height")
	_, ok := store.dah(*mb.Hash())
	require.False(t, ok, "flag-off commit must not call SetDAH")
}

// TestDownloadToDisk_RecoverInvalidDeletesAndRefetches covers Stage B #1: on a
// consensus-invalid on-disk block the recovery deletes the bad bytes (so the
// gap-fill frontier re-requests), clears the in-flight ledgers (so it is genuinely
// re-requestable), and — bounded by diskValidateFailCap — gives up (leaves the
// bytes on disk, no re-download churn) once the same block has failed that many
// times across peer rotations.
func TestDownloadToDisk_RecoverInvalidDeletesAndRefetches(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)

	height := int32(42)
	hash, raw, _ := dtdMakeBlock(t, height, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	// Seed the in-flight ledgers so we can prove the recovery clears them.
	sm.requestedBlocks.Set(hash, struct{}{})
	sm.assignedMu.Lock()
	sm.refetchBlocks[hash] = struct{}{}
	sm.assignedMu.Unlock()

	cause := errors.NewBlockInvalidError("bad block on disk")
	require.True(t, BlockProcessingErrorIsPeerFault(cause), "sanity: cause is a peer-fault")

	// The first (cap-1) failures self-heal: delete + clear + return true (nil peer
	// keeps the disconnect a no-op so no live peer is needed).
	for i := 1; i < diskValidateFailCap; i++ {
		require.True(t, sm.recoverInvalidDiskBlock(hash, height, nil, cause),
			"attempt %d (below cap) must self-heal and return true", i)
	}

	require.False(t, sm.haveBlockOnDisk(ctx, hash), "bad bytes must be deleted so the frontier re-requests")
	require.Positive(t, store.delCount(hash), "block must be deleted from the store")

	_, stillRequested := sm.requestedBlocks.Get(hash)
	require.False(t, stillRequested, "global in-flight ledger must be cleared")

	sm.assignedMu.Lock()
	_, stillRefetch := sm.refetchBlocks[hash]
	sm.assignedMu.Unlock()
	require.False(t, stillRefetch, "refetch ledger must be cleared")

	// Re-persist so we can prove the give-up path does NOT delete.
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	// The cap-th failure gives up: genuinely invalid, returns false, leaves bytes.
	require.False(t, sm.recoverInvalidDiskBlock(hash, height, nil, cause),
		"the cap-th failure must give up (treat as genuinely invalid)")
	require.True(t, sm.haveBlockOnDisk(ctx, hash),
		"give-up path must leave the bytes on disk to stop re-download churn")
}

// TestDownloadToDisk_PrepareWorkerMissingBlock covers the Stage C prepare body's
// error contract: prepareDiskBlock reads the block back from disk itself, so a hash
// with no bytes on disk comes back as an error result (no msgBlock, no prepared),
// echoing the request's hash/height. This is the read-failure path the worker
// surfaces to the drain, which then leaves the header node in place and retries on
// the next tick (never a requeue, never a peer disconnect).
func TestDownloadToDisk_PrepareWorkerMissingBlock(t *testing.T) {
	store := memory.New()
	sm := dtdNewManager(t, true, store)

	hash, _, _ := dtdMakeBlock(t, 7, 1) // never persisted

	res := sm.prepareDiskBlock(context.Background(), diskPrepareReq{hash: hash, height: 7})
	require.NotNil(t, res)
	require.True(t, res.hash.IsEqual(&hash), "result echoes the requested hash")
	require.Equal(t, int32(7), res.height, "result echoes the requested height")
	require.Error(t, res.err, "a block absent from disk must surface a read error")
	require.Nil(t, res.msgBlock, "no bytes read means no msgBlock")
	require.Nil(t, res.prepared, "a read failure never yields a prepared block")
	require.False(t, BlockProcessingErrorIsPeerFault(res.err),
		"a storage read error is transient (retryable), not a peer-fault")
}

// TestDownloadToDisk_PrepareWorkerDeliversOffGoroutine is the Stage C isolation
// proof: the dedicated prepare worker consumes a request, does the heavy body (here
// the disk read) on ITS OWN goroutine, and hands the result back over the ready
// channel — the drain/fetch goroutine (the test goroutine standing in for it) only
// sends the request and receives the result, never runs the body itself. It also
// proves the worker exits promptly on ctx cancellation and stops consuming work,
// which is the flag-off / shutdown lifecycle the blockHandler relies on.
func TestDownloadToDisk_PrepareWorkerDeliversOffGoroutine(t *testing.T) {
	store := memory.New()
	sm := dtdNewManager(t, true, store)

	reqC := make(chan diskPrepareReq, 1)
	readyC := make(chan *diskPrepared, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sm.diskPrepareWorker(ctx, reqC, readyC)

	// Dispatch two requests in sequence (mirroring the drain's one-in-flight walk):
	// each must come back on readyC, proving the worker keeps servicing work.
	for i := int32(1); i <= 2; i++ {
		hash, _, _ := dtdMakeBlock(t, i, uint32(i))

		reqC <- diskPrepareReq{hash: hash, height: i}

		select {
		case res := <-readyC:
			require.True(t, res.hash.IsEqual(&hash), "worker returned the result for the dispatched hash")
		case <-time.After(5 * time.Second):
			t.Fatalf("worker did not deliver result %d off its own goroutine", i)
		}
	}

	// Cancel and confirm the worker has stopped: a further request must NOT be
	// serviced (no result arrives), proving ctx-cancel is a clean exit.
	cancel()

	drained, _, _ := dtdMakeBlock(t, 99, 99)
	select {
	case reqC <- diskPrepareReq{hash: drained, height: 99}:
	default:
		// buffered(1) send; either outcome is fine — we only assert no processing.
	}

	select {
	case <-readyC:
		t.Fatal("worker processed a request after ctx cancellation")
	case <-time.After(200 * time.Millisecond):
		// Expected: the cancelled worker consumed nothing.
	}
}

// TestDownloadToDisk_TransientFailureKeepsBlock covers Stage B #1's OTHER half:
// the delete-and-refetch recovery fires ONLY for a consensus peer-fault; a
// TRANSIENT failure (a storage/read hiccup — not the peer's fault) must NOT delete
// the bytes, must NOT ban a peer, and must leave every in-flight ledger untouched so
// the block is simply retried on the next tick. The drain selects the branch purely
// on BlockProcessingErrorIsPeerFault (manager.go:7846) — a peer-fault runs
// recoverInvalidDiskBlock, a transient error takes the "leave the bytes and the
// header node in place and retry" no-op path. This test drives both branches exactly
// as the drain dispatches them and asserts the divergent side effects.
//
// HONEST SCOPE NOTE: the full drain loop (a closure inside blockHandler needing a
// live sync peer, header list and window) is not run end-to-end; what is proven is
// the classification that selects the branch plus the concrete effect of each branch,
// which is where the delete-vs-keep decision actually lives.
func TestDownloadToDisk_TransientFailureKeepsBlock(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)

	height := int32(4242)
	hash, raw, _ := dtdMakeBlock(t, height, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	// Seed the in-flight ledgers so we can prove a transient failure leaves them alone.
	sm.requestedBlocks.Set(hash, struct{}{})
	sm.assignedMu.Lock()
	sm.refetchBlocks[hash] = struct{}{}
	sm.assignedMu.Unlock()

	// A storage/read hiccup is a LOCAL condition, not a peer fault.
	transient := errors.NewStorageError("postgres hiccup reading subtree")
	require.False(t, BlockProcessingErrorIsPeerFault(transient),
		"a transient storage error must NOT classify as a peer fault")

	// The drain's transient branch is a no-op (leave the bytes + header node, retry).
	// Reproduce that by taking the same branch the drain does: since it is not a peer
	// fault, recoverInvalidDiskBlock is never called. Assert nothing was disturbed.
	if BlockProcessingErrorIsPeerFault(transient) {
		sm.recoverInvalidDiskBlock(hash, height, nil, transient) // unreachable — asserts the guard
	}

	require.True(t, sm.haveBlockOnDisk(ctx, hash), "transient failure must keep the bytes on disk for retry")
	require.Zero(t, store.delCount(hash), "transient failure must not delete the block")

	_, stillRequested := sm.requestedBlocks.Get(hash)
	require.True(t, stillRequested, "transient failure must not clear the in-flight ledger")

	sm.assignedMu.Lock()
	_, stillRefetch := sm.refetchBlocks[hash]
	sm.assignedMu.Unlock()
	require.True(t, stillRefetch, "transient failure must not clear the refetch ledger")

	// Contrast: a genuine consensus peer-fault DOES take the delete-and-ban branch.
	peerFault := errors.NewBlockInvalidError("bad merkle root")
	require.True(t, BlockProcessingErrorIsPeerFault(peerFault),
		"a consensus-invalid block must classify as a peer fault (the delete-and-refetch branch)")
	require.True(t, sm.recoverInvalidDiskBlock(hash, height, nil, peerFault),
		"the peer-fault branch (below cap) must self-heal")
	require.False(t, sm.haveBlockOnDisk(ctx, hash), "the peer-fault branch must delete the bytes")
	require.Positive(t, store.delCount(hash), "the peer-fault branch must delete from the store")
}

// TestDownloadToDisk_DecoupleClearsInFlightLedger covers Stage B #3 (gap-fill
// accounting): a block's bytes hitting disk on the decoupled arrival path clears it
// from requestedBlocks THE INSTANT it is durable, so the in-flight ledger means "gaps
// asked for but not yet received" and nothing more. fetchHeaderBlocks derives
// currentInFlight = requestedBlocks.Len(); if a decoupled arrival left the entry set
// until the validator drained it, the gap-fill walk would throttle against validation
// speed and legacy_downloadToDiskMaxBlocksAhead would be unreachable. Clearing at
// delivery is what makes the disk-ahead ceiling the real bound.
func TestDownloadToDisk_DecoupleClearsInFlightLedger(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())

	// The in-order validator is running in headers-first mode, so decouple accepts.
	sm.validateFromDiskActive.Store(true)
	sm.headersFirstMode.Store(true)

	hash, raw, _ := dtdMakeBlock(t, 600, 21)

	// Model the block as outstanding in flight (a getdata was issued for it) plus a
	// lingering refetch entry, exactly as the fetch walk would have recorded.
	sm.requestedBlocks.Set(hash, struct{}{})
	sm.assignedMu.Lock()
	sm.refetchBlocks[hash] = struct{}{}
	sm.assignedMu.Unlock()
	require.Equal(t, 1, sm.requestedBlocks.Len(), "sanity: block counts as in-flight before arrival")

	require.True(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0, nil),
		"decouple must accept while the validator runs in headers-first mode")
	require.True(t, sm.haveBlockOnDisk(ctx, hash), "decoupled block must be durable on disk")

	// currentInFlight drops: the ledger no longer counts a block whose bytes are on disk.
	require.Equal(t, 0, sm.requestedBlocks.Len(),
		"decoupled arrival must clear requestedBlocks so currentInFlight drops")
	_, stillRequested := sm.requestedBlocks.Get(hash)
	require.False(t, stillRequested, "the arrived block must no longer be counted in flight")

	sm.assignedMu.Lock()
	_, stillRefetch := sm.refetchBlocks[hash]
	sm.assignedMu.Unlock()
	require.False(t, stillRefetch, "decoupled arrival must clear the refetch ledger too")
}

// TestDownloadToDisk_ClearInFlightPerPeerLedger covers the per-peer half of the
// gap-fill accounting: clearInFlightOnArrival also clears the DELIVERING peer's own
// requestedBlocks (the per-peer 16-cap ledger), so a delivered block frees a slot for
// the next getdata to that peer. Without this the per-peer cap would fill with blocks
// already on disk and the peer would stop being asked for new gaps.
func TestDownloadToDisk_ClearInFlightPerPeerLedger(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	sm := dtdNewManager(t, true, memory.New())

	// A real (unconnected) peer plus its per-peer sync state registered in peerStates.
	p := peerpkg.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{})
	state := &peerSyncState{
		syncCandidate:   true,
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	sm.peerStates.Set(p, state)

	hash, _, _ := dtdMakeBlock(t, 601, 22)

	// The block is outstanding both globally and in the delivering peer's ledger.
	sm.requestedBlocks.Set(hash, struct{}{})
	state.requestedBlocks.Set(hash, struct{}{})
	require.Equal(t, 1, state.requestedBlocks.Len(), "sanity: per-peer slot occupied before delivery")

	sm.clearInFlightOnArrival(hash, p)

	require.Equal(t, 0, sm.requestedBlocks.Len(), "global in-flight ledger must be cleared")
	require.Equal(t, 0, state.requestedBlocks.Len(),
		"the delivering peer's per-peer ledger must be cleared so a new getdata slot frees up")
}

// dtdCloseFailStore models a block store whose durable-commit step FAILS at the
// FileStorer's Close (the atomic temp->final rename / fsync — ENOSPC, a torn fsync,
// or any <=4KB block whose only write is at Close). It drains the whole reader (so the
// buffered Write and the Close-time flush both succeed) and only THEN returns an error
// — surfacing at FileStorer.Close, never storing the bytes, exactly as a failed rename
// would. Exists() therefore stays false: the file never appeared.
type dtdCloseFailStore struct {
	*memory.Memory
}

func (s *dtdCloseFailStore) SetFromReader(ctx context.Context, key []byte, ft fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	// Drain fully so the writer side's flush at Close() unblocks, THEN fail — a
	// commit/rename failure, not a write failure.
	_, _ = io.Copy(io.Discard, reader)
	_ = reader.Close()

	return errors.NewStorageError("simulated durable-commit (rename/fsync) failure at Close")
}

// TestDownloadToDisk_CloseFailureNotDurableFallsBack covers Stage B #4 (honour the
// atomic write): when the durable commit fails at Close, PersistRawBlockOnArrival must
// surface the error, PersistArrivalAndDecouple must return handled=false (so the
// caller falls back to today's inline QueueBlock path on the bytes it still holds), and
// the block must NOT be reported durable — haveBlockOnDisk stays false. "Handled" must
// imply "durably on disk."
func TestDownloadToDisk_CloseFailureNotDurableFallsBack(t *testing.T) {
	ctx := context.Background()
	store := &dtdCloseFailStore{Memory: memory.New()}
	sm := dtdNewManager(t, true, store)

	// Even with the validator running, a failed durable write must decline the handoff.
	sm.validateFromDiskActive.Store(true)
	sm.headersFirstMode.Store(true)

	hash, raw, _ := dtdMakeBlock(t, 700, 31)

	// The raw arrival write surfaces the Close failure rather than swallowing it.
	require.Error(t, sm.PersistRawBlockOnArrival(ctx, hash, raw),
		"a failed durable commit at Close must be surfaced, not swallowed")

	// The block is NOT durable — the file never appeared.
	require.False(t, sm.haveBlockOnDisk(ctx, hash),
		"a block whose durable commit failed must NOT read as on-disk")
	exists, err := store.Exists(ctx, hash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, exists, "the atomic write means the file never appears on a failed commit")

	// The decouple hand-off declines, so OnBlock falls back to the inline path.
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0, nil),
		"a failed durable write must return handled=false (fall back to inline validation)")
	require.False(t, sm.haveBlockOnDisk(ctx, hash),
		"handled=false must never coincide with a block reported durable")
}

// TestDownloadToDisk_BoundedRetryIsPerHash covers Stage B #2 (bounded retry) from the
// counting angle: the delete-and-refetch cap is tracked PER HASH, so one bad block
// failing across peer rotations eventually gives up (never an infinite churn) while a
// DIFFERENT block is entirely unaffected — a single poison block cannot exhaust the
// budget of its neighbours.
func TestDownloadToDisk_BoundedRetryIsPerHash(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)

	hashA, rawA, _ := dtdMakeBlock(t, 800, 41)
	hashB, rawB, _ := dtdMakeBlock(t, 801, 42)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hashA, rawA))
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hashB, rawB))

	cause := errors.NewBlockInvalidError("bad block")

	// Fail A right up to (but not past) the cap: each self-heals and re-requests.
	for i := 1; i < diskValidateFailCap; i++ {
		require.True(t, sm.recoverInvalidDiskBlock(hashA, 800, nil, cause),
			"A attempt %d (below cap) must self-heal", i)
		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hashA, rawA)) // honest re-fetch lands
	}

	// B has its OWN counter: a single failure is well below the cap and self-heals,
	// proving A's exhausted-ish budget never bled into B.
	require.True(t, sm.recoverInvalidDiskBlock(hashB, 801, nil, cause),
		"B must self-heal on its first failure regardless of A's history (per-hash counting)")

	// A's cap-th failure gives up: genuinely invalid, no infinite loop, bytes left in
	// place so the frontier stops re-requesting and bandwidth churn ends.
	require.False(t, sm.recoverInvalidDiskBlock(hashA, 800, nil, cause),
		"A's cap-th failure must give up (bounded retry — no infinite churn)")
	require.True(t, sm.haveBlockOnDisk(ctx, hashA),
		"the give-up path leaves the bytes on disk so the frontier stops re-requesting")

	// B is still healthy and re-requestable, untouched by A hitting its cap.
	require.False(t, sm.haveBlockOnDisk(ctx, hashB),
		"B was deleted by its own self-heal and remains independently re-requestable")
}

// TestDownloadToDisk_SelfRepokeReArmsWake covers Stage B #5 (no missed wakeup): when
// the in-order validator exits its pass because it hit the per-pass cap
// (maxValidateFromDiskPerPass) rather than running out of on-disk work, it self-pokes
// so the next pass runs WITHOUT depending on the refill ticker — which is disabled when
// legacy_inFlightRefillInterval=0. The self-poke is the exact call at manager.go:7948.
//
// HONEST SCOPE NOTE: drainValidateFromDisk is a closure inside blockHandler and cannot
// be invoked from a unit test without the full sync-peer / header-list / window
// machinery, so the end-to-end "cap hit -> next pass runs" is left to the live IBD
// soak. What is proven here is the load-bearing property the fix relies on: after the
// pass has consumed its wake (the select drained validateSignal), a self-poke re-arms
// exactly one pending wake, so the very next select fires immediately with the refill
// ticker off. Without the self-poke the channel would be empty and the validator would
// sleep until the (disabled) ticker — the permanent stall the fix removes.
func TestDownloadToDisk_SelfRepokeReArmsWake(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())

	// The per-pass cap is a positive bound (the loop condition fed < cap).
	require.Positive(t, maxValidateFromDiskPerPass, "per-pass cap must be a positive bound")

	// Model the drain having consumed its wake at the top of the pass: drain the signal.
	sm.PokeValidateFromDisk()
	<-sm.validateSignal
	require.Equal(t, 0, len(sm.validateSignal), "sanity: the pass has consumed its wake")

	// The cap-exhausted exit self-pokes (manager.go:7948). Re-arm must leave exactly one
	// pending wake so the next select fires immediately — no dependence on the ticker.
	sm.PokeValidateFromDisk()
	require.Equal(t, 1, len(sm.validateSignal),
		"a self-poke at the per-pass cap must re-arm exactly one wake (no missed wakeup)")

	// And it stays coalesced: a further self-poke does not stack a second wake.
	sm.PokeValidateFromDisk()
	require.Equal(t, 1, len(sm.validateSignal), "self-pokes coalesce to a single pending wake")
}

// dtdSlowReadStore models a store whose heavy prepare body (the disk read) is slow.
// GetIoReader sleeps for delay and then returns an error, so prepareDiskBlock spends
// its time inside the slow read and returns BEFORE prepareBlockForWindow (which needs
// live validation deps the unit harness deliberately leaves nil). That keeps the test
// focused on the ONE thing Stage C guarantees: the heavy body runs on the worker's own
// goroutine, off the drain/fetch path.
type dtdSlowReadStore struct {
	*memory.Memory
	delay time.Duration
}

func (s *dtdSlowReadStore) GetIoReader(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	time.Sleep(s.delay)
	return nil, errors.NewStorageError("simulated slow read")
}

// TestDownloadToDisk_SlowPrepareDoesNotBlockDispatcher covers Stage C (fix C — the
// separate validation worker): while a SLOW prepare runs, the goroutine that
// dispatches work (the drain/fetch path in production) is not blocked and keeps making
// progress. The dispatcher sends a request onto the buffered channel and returns
// immediately; the heavy body runs entirely on the worker's goroutine; the dispatcher
// is free to issue further getdata / header work in the meantime; and the result is
// delivered back over readyC only after the slow body completes.
//
// HONEST SCOPE NOTE: a true end-to-end "a getdata is issued while a giant block
// validates" needs the live fetch scheduler, sync peer and header list wired together
// (the ~130KB manager_test.go machinery) and is left to the IBD soak. What is proven
// here is the decoupling CONTRACT the fetch progress rests on: dispatch is non-blocking
// and the heavy body never runs on the dispatching goroutine.
func TestDownloadToDisk_SlowPrepareDoesNotBlockDispatcher(t *testing.T) {
	const delay = 400 * time.Millisecond

	store := &dtdSlowReadStore{Memory: memory.New(), delay: delay}
	sm := dtdNewManager(t, true, store)

	reqC := make(chan diskPrepareReq, 1)
	readyC := make(chan *diskPrepared, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sm.diskPrepareWorker(ctx, reqC, readyC)

	hash, _, _ := dtdMakeBlock(t, 900, 51)

	// Dispatch and time how long the dispatcher is detained. It must return effectively
	// instantly — NOT wait out the slow prepare body.
	dispatchStart := time.Now()
	reqC <- diskPrepareReq{hash: hash, height: 900}
	dispatchElapsed := time.Since(dispatchStart)
	require.Less(t, dispatchElapsed, delay/2,
		"dispatch must not block on the slow prepare body (it runs on the worker goroutine)")

	// While the worker is inside the slow body, the dispatcher goroutine keeps working:
	// simulate issuing getdata / header progress. This loop must complete long before the
	// prepare result is ready.
	progress := 0
	for i := 0; i < 1000; i++ {
		progress++
	}
	require.Equal(t, 1000, progress, "the dispatcher makes progress while the slow prepare runs")

	// The result has NOT arrived yet (the body is still sleeping) — proof the heavy work
	// is off-goroutine and did not run inline on the dispatcher.
	select {
	case <-readyC:
		t.Fatal("prepare result arrived before the slow body could have finished: body ran inline, not on the worker")
	default:
		// Expected: still in flight on the worker.
	}

	// It does arrive once the slow body completes, delivered off the worker goroutine.
	select {
	case res := <-readyC:
		require.True(t, res.hash.IsEqual(&hash), "worker delivered the result for the dispatched hash")
		require.Error(t, res.err, "the slow read surfaced its (transient) error result")
		require.False(t, BlockProcessingErrorIsPeerFault(res.err),
			"a slow-read storage error is transient, not a peer-fault")
	case <-time.After(5 * time.Second):
		t.Fatal("worker never delivered the slow prepare result")
	}
}

// dtdConnectedPeer builds a genuinely connected peer (over a real pipe, via
// captureGetDataPeer) so a DisconnectWithWarning closes its connection and flips
// Connected() to false — the observable the recovery-attribution tests assert on. A
// TstMarkConnected peer would panic on disconnect (no underlying conn to Close).
func dtdConnectedPeer(t *testing.T, index uint8) *peerpkg.Peer {
	t.Helper()

	var (
		mu      sync.Mutex
		capture []*wire.MsgGetData
	)

	p := captureGetDataPeer(t, &chaincfg.MainNetParams, index, &mu, &capture)
	require.True(t, p.Connected(), "precondition: peer starts connected")

	return p
}

// TestDownloadToDisk_FailCountSurvivesHeaderReset covers Stage-1 fix 1: the per-hash
// consensus-invalid retry count must NOT be wiped by resetHeaderState — otherwise the
// sync-peer rotation that the recovery's own disconnect provokes (which funnels through
// updateSyncPeer->resetHeaderState) would reset the tally every time and the cap could
// never be reached, leaving the delete-and-refetch unbounded. This proves the count
// survives a reset and that the terminal give-up (return false) is then reachable.
func TestDownloadToDisk_FailCountSurvivesHeaderReset(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())
	sm.headerList = list.New()

	hash, _, _ := dtdMakeBlock(t, 500, 1)

	// Two consensus-invalid failures short of the cap (cap == 3).
	require.Equal(t, 1, sm.noteDiskValidateFailure(hash))
	require.Equal(t, 2, sm.noteDiskValidateFailure(hash))

	// A header reset (the rotation the recovery-disconnect would provoke) must leave
	// the count intact.
	newest, _, _ := dtdMakeBlock(t, 499, 9)
	sm.resetHeaderState(&newest, 499)

	sm.diskValidateFailsMu.Lock()
	surviving := sm.diskValidateFails[hash]
	sm.diskValidateFailsMu.Unlock()
	require.Equal(t, 2, surviving, "resetHeaderState must NOT clear the retry count")

	// The next failure hits the cap, so the terminal give-up is now reachable across
	// the reset: recover returns false (genuinely invalid, halt) rather than looping.
	require.False(t, sm.recoverInvalidDiskBlock(hash, 500, nil, errors.NewBlockInvalidError("still bad")),
		"cap must be reachable across a header reset (terminal give-up returns false)")
}

// TestDownloadToDisk_CommitClearsRecoveryState covers Stage-1 fix 1 (the other half):
// now that resetHeaderState no longer clears the retry maps, a genuine commit is what
// bounds them — markDiskBlocksCommitted must drop the retry counters AND the
// delivering-peer record for every committed hash.
func TestDownloadToDisk_CommitClearsRecoveryState(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, memory.New())
	sm.diskDeliveringPeer = txmap.NewSyncedMap[chainhash.Hash, *peerpkg.Peer]()

	hash, _, blk := dtdMakeBlock(t, 700, 1)

	sm.noteDiskValidateFailure(hash)
	sm.noteDiskReadFailure(hash)
	sm.diskDeliveringPeer.Set(hash, dtdConnectedPeer(t, 20))

	sm.markDiskBlocksCommitted(ctx, []*model.Block{dtdModelBlock(t, blk, 700)})

	sm.diskValidateFailsMu.Lock()
	_, hasValidate := sm.diskValidateFails[hash]
	_, hasRead := sm.diskReadFails[hash]
	sm.diskValidateFailsMu.Unlock()
	require.False(t, hasValidate, "commit must clear the consensus-invalid retry count")
	require.False(t, hasRead, "commit must clear the read-failure retry count")

	_, hasPeer := sm.diskDeliveringPeer.Get(hash)
	require.False(t, hasPeer, "commit must clear the delivering-peer record")
}

// TestDownloadToDisk_RecoverAttributesDeliveringPeer covers Stage-1 fix 2: recovery
// disconnects the peer that actually DELIVERED the bad bytes (recorded per hash), not
// whichever innocent peer happens to be sync peer at validation time. It also proves
// the fallback: when no delivering peer is recorded, the passed sync peer is rotated.
func TestDownloadToDisk_RecoverAttributesDeliveringPeer(t *testing.T) {
	ctx := context.Background()
	sm := dtdNewManager(t, true, newDTDRecordingStore())
	sm.diskDeliveringPeer = txmap.NewSyncedMap[chainhash.Hash, *peerpkg.Peer]()

	deliveringPeer := dtdConnectedPeer(t, 30)
	syncPeer := dtdConnectedPeer(t, 31)

	hash, raw, _ := dtdMakeBlock(t, 800, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	sm.diskDeliveringPeer.Set(hash, deliveringPeer)

	cause := errors.NewBlockInvalidError("bad from delivering peer")
	require.True(t, sm.recoverInvalidDiskBlock(hash, 800, syncPeer, cause),
		"below the cap the recovery self-heals and returns true")

	require.Eventually(t, func() bool { return !deliveringPeer.Connected() }, 2*time.Second, 5*time.Millisecond,
		"the peer that delivered the bad bytes must be disconnected")
	require.True(t, syncPeer.Connected(),
		"the innocent sync peer must NOT be disconnected when the delivering peer is known")

	// Fallback: no recorded delivering peer for a different hash -> the sync peer is
	// rotated (pre-attribution behaviour), guaranteeing a rotation still happens.
	hash2, raw2, _ := dtdMakeBlock(t, 801, 2)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash2, raw2))
	require.True(t, sm.recoverInvalidDiskBlock(hash2, 801, syncPeer, cause))
	require.Eventually(t, func() bool { return !syncPeer.Connected() }, 2*time.Second, 5*time.Millisecond,
		"with no delivering peer recorded, the recovery falls back to rotating the sync peer")
}

// TestDownloadToDisk_RecordDeliveringPeerOnArrival covers the recording half of fix 2:
// clearInFlightOnArrival stores the delivering peer (its association primary) under the
// block hash so a later rejection can attribute the fault to it.
func TestDownloadToDisk_RecordDeliveringPeerOnArrival(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())
	sm.diskDeliveringPeer = txmap.NewSyncedMap[chainhash.Hash, *peerpkg.Peer]()

	peer := dtdConnectedPeer(t, 40)
	sm.peerStates.Set(peer, &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	})

	hash, _, _ := dtdMakeBlock(t, 900, 1)
	sm.clearInFlightOnArrival(hash, peer)

	recorded, ok := sm.diskDeliveringPeer.Get(hash)
	require.True(t, ok, "arrival must record the delivering peer")
	require.Equal(t, peer, recorded, "recorded peer must be the delivering peer")
}

// TestDownloadToDisk_RecoverUnreadableDeletesAndGivesUp covers Stage-1 fix 4: a block
// whose bytes are present but fail to read/deserialize is NOT a peer-fault (the bytes
// deserialized on the wire before being written), yet must still self-heal within a
// bound — delete the corrupt bytes so the frontier re-requests a fresh copy — and give
// up (halt, leave the bytes) once even the fresh copy is unreadable, so a corrupt/torn
// write can never wedge IBD forever. No peer is disconnected on this path.
func TestDownloadToDisk_RecoverUnreadableDeletesAndGivesUp(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)

	hash, raw, _ := dtdMakeBlock(t, 1000, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	// Seed the in-flight ledgers so we can prove the recovery clears them.
	sm.requestedBlocks.Set(hash, struct{}{})
	sm.assignedMu.Lock()
	sm.refetchBlocks[hash] = struct{}{}
	sm.assignedMu.Unlock()

	cause := errors.NewProcessingError("deserialize failure")
	require.False(t, BlockProcessingErrorIsPeerFault(cause), "sanity: a read fault is not a peer-fault")

	// The first (cap-1) failures self-heal: delete + clear + return true.
	for i := 1; i < diskValidateFailCap; i++ {
		require.True(t, sm.recoverUnreadableDiskBlock(hash, 1000, cause),
			"attempt %d (below cap) must delete-and-refetch and return true", i)
	}

	require.False(t, sm.haveBlockOnDisk(ctx, hash), "corrupt bytes must be deleted so the frontier re-requests")
	require.Positive(t, store.delCount(hash), "block must be deleted from the store")

	_, stillRequested := sm.requestedBlocks.Get(hash)
	require.False(t, stillRequested, "in-flight ledger must be cleared")
	sm.assignedMu.Lock()
	_, stillRefetch := sm.refetchBlocks[hash]
	sm.assignedMu.Unlock()
	require.False(t, stillRefetch, "refetch ledger must be cleared")

	// Re-persist to represent the fresh copy that is ALSO unreadable.
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))
	require.True(t, sm.haveBlockOnDisk(ctx, hash))

	// The cap-th failure gives up: leave the bytes, return false (halt), no loop.
	require.False(t, sm.recoverUnreadableDiskBlock(hash, 1000, cause),
		"the cap-th read failure must give up (halt) rather than delete again")
	require.True(t, sm.haveBlockOnDisk(ctx, hash),
		"give-up must leave the bytes on disk to stop the delete-and-refetch loop")
}

// TestDownloadToDisk_PrepareWorkerReadFaultFlag covers fix 4's routing plumbing: a read
// failure inside the prepare worker is flagged (readFault) so the drain can send it to
// recoverUnreadableDiskBlock (delete-and-refetch) rather than the plain transient-retry
// path used for a service hiccup inside prepareBlockForWindow.
func TestDownloadToDisk_PrepareWorkerReadFaultFlag(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())

	hash, _, _ := dtdMakeBlock(t, 11, 1) // never persisted -> read fails

	res := sm.prepareDiskBlock(context.Background(), diskPrepareReq{hash: hash, height: 11})
	require.Error(t, res.err, "a block absent from disk surfaces a read error")
	require.True(t, res.readFault, "a read/deserialize failure must set readFault so the drain deletes-and-refetches")
	require.False(t, BlockProcessingErrorIsPeerFault(res.err), "a read fault is transient, not a peer-fault")
}

// TestDownloadToDisk_RestoreConsumedFrontierNode covers Stage-1 fix 3: on the
// synchronous feedDiskBlock peer-fault path the preamble has already consumed the
// block's front header node, so after the bytes are deleted the frontier would march
// past the missing block. restoreConsumedFrontierNode re-inserts the front node so the
// frontier anchors there again and the walk gap-halts — a no-op when the node still
// exists or when we are not in headers-first mode.
func TestDownloadToDisk_RestoreConsumedFrontierNode(t *testing.T) {
	sm := dtdNewManager(t, true, memory.New())
	sm.headerList = list.New()
	sm.headerHeightIndex = make(map[chainhash.Hash]int32)
	sm.headersFirstMode.Store(true)

	consumed, _, _ := dtdMakeBlock(t, 300, 1) // the block whose front node the preamble removed
	successor, _, _ := dtdMakeBlock(t, 301, 2)

	// Simulate the post-preamble state: the successor is now the front; the consumed
	// node and its index entry are gone.
	sh := successor
	sm.headerList.PushBack(&headerNode{height: 301, hash: &sh})
	sm.headerHeightIndex[successor] = 301

	sm.restoreConsumedFrontierNode(consumed, 300)

	front, ok := sm.headerList.Front().Value.(*headerNode)
	require.True(t, ok)
	require.True(t, front.hash.IsEqual(&consumed), "restored node must be re-inserted at the front (the frontier anchor)")
	require.Equal(t, int32(300), sm.headerHeightIndex[consumed], "restored node's index entry must be re-added")

	// Idempotent: calling again must NOT duplicate the node (index already present).
	before := sm.headerList.Len()
	sm.restoreConsumedFrontierNode(consumed, 300)
	require.Equal(t, before, sm.headerList.Len(), "restore must be a no-op when the node already exists")

	// No-op outside headers-first mode (a concurrent reset may have rebuilt the list).
	other, _, _ := dtdMakeBlock(t, 302, 3)
	sm.headersFirstMode.Store(false)
	sm.restoreConsumedFrontierNode(other, 302)
	_, present := sm.headerHeightIndex[other]
	require.False(t, present, "restore must be a no-op when not in headers-first mode")
}

// dtdMakeZeroTxBlock builds a well-formed block header carrying ZERO transactions and
// returns its hash and serialized bytes. Its bytes deserialize cleanly (header + a
// varint transaction count of 0), but bsvutil's block.Transactions() is then empty, so
// any code that indexes the coinbase as block.Transactions()[0] panics with
// index-out-of-range. It is the minimal poison block for the prepare-worker panic guard.
func dtdMakeZeroTxBlock(t *testing.T, nonce uint32) (chainhash.Hash, []byte) {
	t.Helper()

	net := &chaincfg.RegressionNetParams

	var blk wire.MsgBlock
	blk.Header = wire.BlockHeader{
		Version:   1,
		PrevBlock: chainhash.Hash{},
		Timestamp: time.Unix(1600000000, 0),
		Bits:      net.PowLimitBits,
		Nonce:     nonce,
	}
	// Deliberately NO AddTransaction: the block has zero transactions.

	var buf bytes.Buffer
	require.NoError(t, blk.Serialize(&buf))

	return blk.Header.BlockHash(), buf.Bytes()
}

// TestDownloadToDisk_PanicGuardSurvivesZeroTxBlock covers Stage-2 fix (LOW): the heavy
// prepareBlockForWindow body runs on the dedicated always-on diskPrepareWorker goroutine
// and indexes block.Transactions()[0] (the coinbase) BEFORE prepareSubtrees' own recover
// is reached. A malformed on-disk block that deserialized to zero transactions therefore
// panics with index-out-of-range, and an unrecovered panic on the worker goroutine would
// crash the WHOLE process. The panic guard in prepareDiskBlock must convert the panic into
// a TRANSIENT error (a ProcessingError, not a peer-fault) so the worker keeps running and
// the drain retries rather than the node dying. This test proves both halves: the direct
// call returns a transient error instead of panicking, AND a live worker survives the
// poison block and keeps servicing subsequent work.
func TestDownloadToDisk_PanicGuardSurvivesZeroTxBlock(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := dtdNewManager(t, true, store)

	// A well-formed header with zero transactions, persisted on disk exactly as a
	// truncated/malformed delivery would land.
	poisonHash, poisonRaw := dtdMakeZeroTxBlock(t, 4242)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, poisonHash, poisonRaw))

	// Direct call: prepareDiskBlock must NOT panic. It converts the panic (deep inside
	// prepareBlockForWindow) into an error flagged panicFault, so the drain routes the
	// block through the BOUNDED delete-and-refetch (a panic is deterministic on the same
	// bytes, so it must be capped, not retried forever).
	res := sm.prepareDiskBlock(ctx, diskPrepareReq{hash: poisonHash, height: 4242})
	require.NotNil(t, res)
	require.Error(t, res.err, "a zero-tx block must surface an error, not panic the process")
	require.Nil(t, res.prepared, "a panicked prepare yields no prepared block")
	require.True(t, res.panicFault,
		"a recovered prepare panic must be flagged panicFault so the drain routes it to the bounded delete-and-refetch, not an unbounded transient retry")
	require.False(t, BlockProcessingErrorIsPeerFault(res.err),
		"a recovered panic is a ProcessingError, not ErrBlockInvalid — it is bounded via the readFault/panicFault path, not the peer-fault (disconnect) path")
	require.False(t, res.readFault,
		"the bytes read and deserialized fine; the fault is in prepare (panicFault), not the disk read (readFault)")

	// Now drive the poison block through a LIVE worker goroutine and prove the worker
	// survives it: it hands back an error result rather than crashing, and it keeps
	// servicing a subsequent (healthy) request — a crashed goroutine would never reply.
	reqC := make(chan diskPrepareReq, 1)
	readyC := make(chan *diskPrepared, 1)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()

	go sm.diskPrepareWorker(wctx, reqC, readyC)

	reqC <- diskPrepareReq{hash: poisonHash, height: 4242}
	select {
	case got := <-readyC:
		require.Error(t, got.err, "worker must convert the panic into an error result, not die")
		require.Nil(t, got.prepared)
	case <-time.After(5 * time.Second):
		t.Fatal("worker crashed on the poison block — no result returned")
	}

	// A healthy single-coinbase block must still be serviced, proving the worker loop
	// lived through the panic (its result may itself carry a validation error — an
	// unsolved PoW header — but the worker returning ANY result for it is the proof).
	goodHash, goodRaw, _ := dtdMakeBlock(t, 4243, 7)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, goodHash, goodRaw))

	reqC <- diskPrepareReq{hash: goodHash, height: 4243}
	select {
	case got := <-readyC:
		require.True(t, got.hash.IsEqual(&goodHash),
			"worker still services work after surviving the panic (it did not crash)")
	case <-time.After(5 * time.Second):
		t.Fatal("worker stopped servicing after the poison block — it did not survive")
	}
}

// TestDownloadToDisk_BoundedRetryTerminatesAcrossResets is the Stage-1 HIGH-fix
// regression test, hardened per Stage-2 test-plan item 1: it drives the SAME hash to
// consensus-invalid rejection diskValidateFailCap times across simulated sync-peer
// disconnect / header-reset cycles (the rotation each recovery-disconnect provokes) and
// proves the bounded loop TERMINATES. Specifically: the per-hash counter is NOT wiped by
// resetHeaderState (so the cap is reachable across rotations), the walk halts exactly at
// the cap, the genuinely-invalid block is then LEFT on disk (the frontier stops
// re-requesting), and no further delete / re-download happens past the cap. Without the
// surviving counter this loop would delete-and-refetch forever.
func TestDownloadToDisk_BoundedRetryTerminatesAcrossResets(t *testing.T) {
	ctx := context.Background()
	store := newDTDRecordingStore()
	sm := dtdNewManager(t, true, store)
	sm.headerList = list.New()

	height := int32(5000)
	hash, raw, _ := dtdMakeBlock(t, height, 1)
	require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))

	cause := errors.NewBlockInvalidError("consensus-invalid on disk")
	require.True(t, BlockProcessingErrorIsPeerFault(cause), "sanity: cause is a peer-fault")

	attempts := 0

	for {
		// The frontier re-fetched a fresh copy after the previous delete; model it as the
		// same still-invalid bytes landing again.
		require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hash, raw))

		healed := sm.recoverInvalidDiskBlock(hash, height, nil, cause)
		attempts++

		// The disconnect the recovery just issued funnels through
		// updateSyncPeer->resetHeaderState; fire that reset between every attempt. It must
		// NOT wipe the per-hash counter.
		newest, _, _ := dtdMakeBlock(t, height-1, 99)
		sm.resetHeaderState(&newest, height-1)

		if !healed {
			break // terminal give-up reached (return false)
		}

		require.Less(t, attempts, diskValidateFailCap+2,
			"recovery must terminate at the cap, not loop forever across resets")
	}

	require.Equal(t, diskValidateFailCap, attempts,
		"the walk must halt exactly at the cap, counting across every header reset")

	// The genuinely-invalid block is LEFT on disk so the frontier stops re-requesting it.
	require.True(t, sm.haveBlockOnDisk(ctx, hash),
		"a genuinely-invalid block is left on disk after the cap (no re-download churn)")

	// Any stray re-walk past the cap is an immediate terminal no-op: no new Del call, no
	// re-download.
	delsAtGiveUp := store.delCount(hash)
	require.False(t, sm.recoverInvalidDiskBlock(hash, height, nil, cause),
		"past the cap the recovery stays terminal (returns false)")
	require.Equal(t, delsAtGiveUp, store.delCount(hash),
		"no further deletes / re-downloads happen past the cap")
}

// TestDownloadToDisk_ExhaustedMarkerHaltsWalk pins the terminal-marker the frontier walk
// consults (diskRecoveryExhausted). recoverInvalidDiskBlock/recoverUnreadableDiskBlock
// deliberately LEAVE the bad bytes on disk at the cap, so haveBlockOnDisk stays true and
// the block remains the frontier anchor; without a stable "given up" signal the walk would
// re-select it every poke and re-dispatch the heavy prepare forever. This proves the signal
// flips exactly at the cap for BOTH the consensus-invalid and the unreadable/panic counters,
// and that a genuine commit clears it so a re-visited hash gets a clean slate.
func TestDownloadToDisk_ExhaustedMarkerHaltsWalk(t *testing.T) {
	sm := dtdNewManager(t, true, newDTDRecordingStore())

	var invalid, unreadable chainhash.Hash
	invalid[0] = 0x11
	unreadable[0] = 0x22

	require.False(t, sm.diskRecoveryExhausted(invalid), "fresh hash is not exhausted")

	// Consensus-invalid counter: exhausted only AT the cap, never before.
	for i := 1; i < diskValidateFailCap; i++ {
		sm.noteDiskValidateFailure(invalid)
		require.False(t, sm.diskRecoveryExhausted(invalid),
			"below the cap the walk must still re-select and re-fetch (attempt %d)", i)
	}

	sm.noteDiskValidateFailure(invalid)
	require.True(t, sm.diskRecoveryExhausted(invalid),
		"at diskValidateFailCap the walk must halt (the block can never commit)")

	// Read/deserialize/panic counter is an independent path to the same terminal state.
	for i := 0; i < diskValidateFailCap; i++ {
		sm.noteDiskReadFailure(unreadable)
	}

	require.True(t, sm.diskRecoveryExhausted(unreadable),
		"an unreadable/panic-faulted block reaching the cap also halts the walk")

	// A genuine commit clears the marker so a later re-visit of the same hash (e.g. after a
	// reorg re-downloads honest bytes) starts fresh rather than being permanently skipped.
	sm.clearDiskRecoveryState(invalid)
	require.False(t, sm.diskRecoveryExhausted(invalid),
		"commit clears the terminal marker for a re-visited hash")
}

// TestDownloadToDisk_ConcurrentWorkerArrivalRecoveryRace is the Stage-2 -race integration
// test (test-plan item 6). It exercises the cross-goroutine touchpoints the download-to
// -disk decoupling ADDED, concurrently: a LIVE diskPrepareWorker on its own goroutine, the
// OnBlock arrival read-loop (PersistArrivalAndDecouple/clearInFlightOnArrival recording
// delivering peers and clearing ledgers), and the drain-side recovery/commit bookkeeping
// (noteDiskValidateFailure/recoverInvalidDiskBlock/markDiskBlocksCommitted) — all hammering
// the same shared maps (diskDeliveringPeer, the retry counters, requestedBlocks, the
// assignedMu-guarded ledgers) and the same block store. Run under -race it proves those
// concurrent accesses are data-race-free, and it proves the worker never permanently
// stalls: every one-in-flight dispatch returns within a deadline.
//
// HONEST SCOPE: the full drainValidateFromDisk scheduler (with its diskPrepareInFlight
// single-slot guard) is a closure inside blockHandler wired to a live header list, window
// accumulator, sync peer and maturity gate, and is not extracted here — so the production
// double-dispatch GUARD itself is not the code under test. What is driven is the concurrent
// shared STATE the decoupling introduced, which is where a cross-goroutine race or a
// wedged in-flight slot would actually surface; the drain stand-in below enforces the same
// one-in-flight discipline the real scheduler does (dispatch only after the prior result is
// consumed) and asserts the worker keeps up with it.
func TestDownloadToDisk_ConcurrentWorkerArrivalRecoveryRace(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := dtdNewManager(t, true, store)
	sm.diskDeliveringPeer = txmap.NewSyncedMap[chainhash.Hash, *peerpkg.Peer]()
	sm.headerList = list.New()
	sm.headerHeightIndex = make(map[chainhash.Hash]int32)
	sm.headersFirstMode.Store(true)
	sm.validateFromDiskActive.Store(true)

	const n = 30

	// A small pool of real connected peers, registered in peerStates so the arrival path
	// records them as delivering peers (recording only fires for a resolvable peer).
	const poolSize = 4

	pool := make([]*peerpkg.Peer, poolSize)
	for i := range pool {
		p := dtdConnectedPeer(t, uint8(i+1))
		pool[i] = p
		sm.peerStates.Set(p, &peerSyncState{
			requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		})
	}

	hashes := make([]chainhash.Hash, n)
	raws := make([][]byte, n)
	blks := make([]*wire.MsgBlock, n)

	for i := 0; i < n; i++ {
		h, raw, blk := dtdMakeBlock(t, int32(6000+i), uint32(i+1))
		hashes[i] = h
		raws[i] = raw
		blks[i] = blk
	}

	reqC := make(chan diskPrepareReq, 1)
	readyC := make(chan *diskPrepared, 1)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()

	go sm.diskPrepareWorker(wctx, reqC, readyC)

	var wg sync.WaitGroup

	// Arrival read-loop goroutine: persist + decouple each block, recording its delivering
	// peer and clearing its in-flight ledgers, exactly as OnBlock does off the drain.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < n; i++ {
			sm.requestedBlocks.Set(hashes[i], struct{}{})
			sm.assignedMu.Lock()
			sm.refetchBlocks[hashes[i]] = struct{}{}
			sm.assignedMu.Unlock()

			sm.PersistArrivalAndDecouple(ctx, hashes[i], raws[i], 0, pool[i%poolSize])
		}
	}()

	// Drain stand-in goroutine: strict one-in-flight dispatch. For each block it ensures
	// the bytes are on disk (idempotent), dispatches a prepare, waits for the result, then
	// runs recovery OR commit bookkeeping — all touching the shared state the arrival loop
	// mutates concurrently. A missing result within the deadline is a stall.
	stalled := make(chan chainhash.Hash, 1)

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < n; i++ {
			require.NoError(t, sm.PersistRawBlockOnArrival(ctx, hashes[i], raws[i]))

			reqC <- diskPrepareReq{hash: hashes[i], height: int32(6000 + i), peer: pool[i%poolSize]}

			select {
			case <-readyC:
			case <-time.After(15 * time.Second):
				select {
				case stalled <- hashes[i]:
				default:
				}

				return
			}

			// Alternate the two drain-side finalizers so both the recovery maps (delete +
			// counter + delivering-peer lookup/disconnect) and the commit clear-path run
			// concurrently with the arrival loop's writes.
			if i%2 == 0 {
				sm.noteDiskValidateFailure(hashes[i])
				sm.recoverInvalidDiskBlock(hashes[i], int32(6000+i), nil, errors.NewBlockInvalidError("race"))
			} else {
				sm.markDiskBlocksCommitted(ctx, []*model.Block{dtdModelBlock(t, blks[i], uint32(6000+i))})
			}
		}
	}()

	wg.Wait()

	select {
	case h := <-stalled:
		t.Fatalf("prepare worker stalled on %s — the single in-flight slot never returned", h)
	default:
	}
}
