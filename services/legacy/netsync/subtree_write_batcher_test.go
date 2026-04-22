package netsync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestSubtreeWriteBatcher_FlushesOnCountThreshold(t *testing.T) {
	ctx := context.Background()
	var flushCount int32
	var itemsFlushed int32

	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		atomic.AddInt32(&flushCount, 1)
		atomic.AddInt32(&itemsFlushed, int32(len(items)))
		return nil
	}

	b := NewSubtreeWriteBatcher(1, 1*time.Hour, nil, flushFn) // maxBlocks=1 → maxItems=3
	defer b.Stop(ctx)

	for i := 0; i < 3; i++ {
		require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData, RootHash: [32]byte{byte(i)}}))
	}
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&flushCount) == 1 && atomic.LoadInt32(&itemsFlushed) == 3
	}, time.Second, 10*time.Millisecond)
}

func TestSubtreeWriteBatcher_FlushesOnTimer(t *testing.T) {
	ctx := context.Background()
	var flushCount int32

	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		if len(items) > 0 {
			atomic.AddInt32(&flushCount, 1)
		}
		return nil
	}

	b := NewSubtreeWriteBatcher(100, 100*time.Millisecond, nil, flushFn)
	defer b.Stop(ctx)

	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData}))

	require.Eventually(t, func() bool { return atomic.LoadInt32(&flushCount) == 1 }, time.Second, 10*time.Millisecond)
}

func TestSubtreeWriteBatcher_FlushesOnStop(t *testing.T) {
	ctx := context.Background()
	var gotItems int32

	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		atomic.AddInt32(&gotItems, int32(len(items)))
		return nil
	}

	b := NewSubtreeWriteBatcher(100, 1*time.Hour, nil, flushFn)
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData}))
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindMeta}))

	require.NoError(t, b.Stop(ctx))
	require.Equal(t, int32(2), atomic.LoadInt32(&gotItems))
}

func TestSubtreeWriteBatcher_TreeItemPreservesFileType(t *testing.T) {
	ctx := context.Background()
	var gotItems []SubtreeWriteItem

	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		gotItems = append(gotItems, items...)
		return nil
	}

	b := NewSubtreeWriteBatcher(100, 1*time.Hour, nil, flushFn)
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{
		Kind:     SubtreeKindTree,
		FileType: fileformat.FileTypeSubtreeToCheck,
		RootHash: [32]byte{0xaa},
	}))
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{
		Kind:     SubtreeKindTree,
		FileType: fileformat.FileTypeSubtree,
		RootHash: [32]byte{0xbb},
	}))

	require.NoError(t, b.Stop(ctx))
	require.Len(t, gotItems, 2)
	require.Equal(t, fileformat.FileTypeSubtreeToCheck, gotItems[0].FileType)
	require.Equal(t, fileformat.FileTypeSubtree, gotItems[1].FileType)
}

// TestSubtreeWriteBatcher_BatchedDispatchFlushesToBlobStore asserts that when
// the batched path is active, items submitted to the batcher are durably
// written to the underlying blob store via flushSubtreeWriteBatch, and that
// after Stop (simulating the quickValidationMode→RUNNING transition) every
// submitted blob is immediately readable.
//
// This mirrors the production dispatch in writeSubtree: during catch-up the
// batcher accumulates writes, and on transition Stop() drains the batcher so
// subsequent direct-path reads observe the blobs on disk.
func TestSubtreeWriteBatcher_BatchedDispatchFlushesToBlobStore(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{
		logger:       ulogger.TestLogger{},
		settings:     settings.NewSettings(),
		subtreeStore: memory.New(),
	}

	// maxBlocks=100 and a long maxWait so nothing flushes on count or timer;
	// only Stop() will drain, which matches the transition semantics.
	b := NewSubtreeWriteBatcher(100, 1*time.Hour, sm.logger, sm.flushSubtreeWriteBatch)

	treeHash := [32]byte{0x01}
	dataHash := [32]byte{0x02}
	metaHash := [32]byte{0x02} // meta shares the data hash in production

	treeBytes := []byte("fake-subtree-bytes")
	dataBytes := []byte("fake-subtree-data-bytes")
	metaBytes := []byte("fake-subtree-meta-bytes")

	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindTree, FileType: fileformat.FileTypeSubtree, RootHash: treeHash, Bytes: treeBytes, DeleteAt: 100}))
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData, RootHash: dataHash, Bytes: dataBytes, DeleteAt: 100}))
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindMeta, RootHash: metaHash, Bytes: metaBytes, DeleteAt: 100}))

	// Before Stop, the blobs must not yet be on disk — we sized thresholds so
	// neither the count nor timer trigger fires.
	treeExists, err := sm.subtreeStore.Exists(ctx, treeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, treeExists, "batched items must not be written before flush")

	// Stop drains; after it returns, every item should be readable via the
	// memory blob store — this is the "immediate readability after transition"
	// contract the dispatch in writeSubtree relies on.
	require.NoError(t, b.Stop(ctx))

	treeExists, err = sm.subtreeStore.Exists(ctx, treeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, treeExists)

	dataExists, err := sm.subtreeStore.Exists(ctx, dataHash[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.True(t, dataExists)

	metaExists, err := sm.subtreeStore.Exists(ctx, metaHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.True(t, metaExists)

	gotTree, err := sm.subtreeStore.Get(ctx, treeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, treeBytes, gotTree)
	gotData, err := sm.subtreeStore.Get(ctx, dataHash[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, dataBytes, gotData)
	gotMeta, err := sm.subtreeStore.Get(ctx, metaHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, metaBytes, gotMeta)
}

// TestSubtreeWriteBatcher_FlushSkipsExistingMeta verifies that
// flushSubtreeWriteBatch honours the existence check inside the flush path for
// SubtreeKindMeta — if meta already exists in the blob store (e.g. arrived via
// P2P/block assembly), the batcher must not overwrite or error on it.
func TestSubtreeWriteBatcher_FlushSkipsExistingMeta(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := &SyncManager{
		logger:       ulogger.TestLogger{},
		settings:     settings.NewSettings(),
		subtreeStore: store,
	}

	metaHash := [32]byte{0xab}
	existingMeta := []byte("existing-meta-from-p2p")
	require.NoError(t, store.Set(ctx, metaHash[:], fileformat.FileTypeSubtreeMeta, existingMeta))

	b := NewSubtreeWriteBatcher(100, 1*time.Hour, sm.logger, sm.flushSubtreeWriteBatch)
	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindMeta, RootHash: metaHash, Bytes: []byte("would-overwrite"), DeleteAt: 100}))
	require.NoError(t, b.Stop(ctx))

	got, err := store.Get(ctx, metaHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, existingMeta, got, "existing meta blob must not be overwritten by batched flush")
}

func TestSubtreeWriteBatcher_TimerFlushErrorSurfacedOnNextSubmit(t *testing.T) {
	ctx := context.Background()

	boom := errors.New("boom")
	flushed := make(chan struct{}, 1)
	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		select {
		case flushed <- struct{}{}:
		default:
		}
		return boom
	}

	b := NewSubtreeWriteBatcher(100, 50*time.Millisecond, nil, flushFn)
	defer b.Stop(ctx)

	require.NoError(t, b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData}))

	// Wait deterministically for the timer-path flush to fire (and fail)
	// rather than sleeping for a fixed duration.
	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("timer flush never fired")
	}

	// Give the timerLoop a moment to publish the error to lastErr. The flush
	// signal is sent before b.lastErr is written, so use Eventually on the
	// observable side effect (Submit returning boom) to avoid a race.
	require.Eventually(t, func() bool {
		return errors.Is(b.Submit(ctx, SubtreeWriteItem{Kind: SubtreeKindData}), boom)
	}, time.Second, 10*time.Millisecond)
}
