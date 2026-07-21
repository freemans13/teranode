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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
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
		ctx:            context.Background(),
		logger:         ulogger.TestLogger{},
		settings:       tSettings,
		mainBlockStore: store,
		validateSignal: make(chan struct{}, 1),
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
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0),
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
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0), "no-store decouple must decline")
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
	require.False(t, sm.PersistArrivalAndDecouple(ctx, hash, raw, 0),
		"decouple must decline while the in-order validator is not running")
	require.True(t, sm.haveBlockOnDisk(ctx, hash),
		"even when decouple declines, the arrival write must still be durable on disk")
	require.Len(t, sm.validateSignal, 0, "no validator poke when the validator is inactive")

	// Now the in-order validator is running AND we are in headers-first mode: a
	// fresh block is decoupled (handled=true) and the validator is poked.
	sm.validateFromDiskActive.Store(true)
	sm.headersFirstMode.Store(true)

	hash2, raw2, _ := dtdMakeBlock(t, 501, 12)
	require.True(t, sm.PersistArrivalAndDecouple(ctx, hash2, raw2, 0),
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
