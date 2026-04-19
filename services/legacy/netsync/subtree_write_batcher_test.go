package netsync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
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
		require.NoError(t, b.Submit(SubtreeWriteItem{Kind: SubtreeKindData, RootHash: [32]byte{byte(i)}}))
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

	require.NoError(t, b.Submit(SubtreeWriteItem{Kind: SubtreeKindData}))

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
	require.NoError(t, b.Submit(SubtreeWriteItem{Kind: SubtreeKindData}))
	require.NoError(t, b.Submit(SubtreeWriteItem{Kind: SubtreeKindMeta}))

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
	require.NoError(t, b.Submit(SubtreeWriteItem{
		Kind:     SubtreeKindTree,
		FileType: fileformat.FileTypeSubtreeToCheck,
		RootHash: [32]byte{0xaa},
	}))
	require.NoError(t, b.Submit(SubtreeWriteItem{
		Kind:     SubtreeKindTree,
		FileType: fileformat.FileTypeSubtree,
		RootHash: [32]byte{0xbb},
	}))

	require.NoError(t, b.Stop(ctx))
	require.Len(t, gotItems, 2)
	require.Equal(t, fileformat.FileTypeSubtreeToCheck, gotItems[0].FileType)
	require.Equal(t, fileformat.FileTypeSubtree, gotItems[1].FileType)
}

func TestSubtreeWriteBatcher_TimerFlushErrorSurfacedOnNextSubmit(t *testing.T) {
	ctx := context.Background()

	boom := errors.New("boom")
	flushFn := func(ctx context.Context, items []SubtreeWriteItem) error {
		return boom
	}

	b := NewSubtreeWriteBatcher(100, 50*time.Millisecond, nil, flushFn)
	defer b.Stop(ctx)

	require.NoError(t, b.Submit(SubtreeWriteItem{Kind: SubtreeKindData}))
	// Let the timer fire and the flush error to be captured
	time.Sleep(300 * time.Millisecond)

	// Next Submit should surface the captured error
	err := b.Submit(SubtreeWriteItem{Kind: SubtreeKindData})
	require.ErrorIs(t, err, boom)
}
