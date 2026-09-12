package netsync

import (
	"context"
	"encoding/hex"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// dahCapturingScheduler stands in for the blockchain client a real file store
// hands scheduled deletions to, and records the delete-at-height each call
// carried, keyed by blob key and file type.
//
// The file store keeps no delete-at-height of its own to interrogate
// afterwards: constructFilename (stores/blob/file/file.go) hands a non-zero
// DAH straight to this interface and its own job stops there. So this is what
// "reading it back through the store" means for this backend — the store's
// own next step, not a re-inspection of the WithDeleteAt option the writer
// built. See TestBlockPark_NeverSchedulesAParkedBlobForDeletion
// (block_park_dah_test.go) for the same pattern, counting calls rather than
// the value each one carried.
type dahCapturingScheduler struct {
	mu        sync.Mutex
	scheduled map[string]uint32
}

func newDAHCapturingScheduler() *dahCapturingScheduler {
	return &dahCapturingScheduler{scheduled: make(map[string]uint32)}
}

func dahSchedulerKey(key []byte, fileType string) string {
	return fileType + ":" + hex.EncodeToString(key)
}

func (s *dahCapturingScheduler) ScheduleBlobDeletion(_ context.Context, key []byte, fileType string, _ storetypes.BlobStoreType, dah uint32) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scheduled[dahSchedulerKey(key, fileType)] = dah

	return int64(len(s.scheduled)), true, nil
}

func (s *dahCapturingScheduler) CancelBlobDeletion(_ context.Context, _ []byte, _ string, _ storetypes.BlobStoreType) (bool, error) {
	return false, nil
}

// dahFor requires that a delete-at-height was scheduled for key/fileType and
// returns it, rather than handing back a zero value a missing entry could be
// confused with.
func (s *dahCapturingScheduler) dahFor(t *testing.T, key []byte, fileType fileformat.FileType) uint32 {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	dah, ok := s.scheduled[dahSchedulerKey(key, string(fileType))]
	require.True(t, ok, "no delete-at-height was ever scheduled for %s %x", fileType, key)

	return dah
}

// TestSubtreeWriter_StampsEveryFileWithADeleteAtHeight is Task 6. No production
// change: subtree_writer.go:121-123 already stamps every file a completed
// subtree produces with a delete-at-height of the block's height plus the
// configured retention, so a block that is converted and then never commits —
// its parent never arrives, or the node restarts before it does — leaves files
// the store reclaims by height on its own, rather than files nobody ever
// cleans up. Nothing asserted that claim before this test.
//
// This runs the real subtreeWriter over a real file-backed store and reads the
// delete-at-height back through the store's own deletion-scheduling path
// (dahCapturingScheduler), rather than inferring it from the WithDeleteAt
// option put() constructs. That distinction is the point: a test that only
// checked the option the writer built would still pass if put() were changed
// to never actually reach the store with it.
func TestSubtreeWriter_StampsEveryFileWithADeleteAtHeight(t *testing.T) {
	ctx := context.Background()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	scheduler := newDAHCapturingScheduler()

	store, err := file.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(scheduler),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	const height = uint32(12345)

	writer := newSubtreeWriter(ulogger.TestLogger{}, tSettings, store, height, true)

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	require.NoError(t, writer.Emit(ctx)(0, st, data, meta))

	retention := tSettings.GetSubtreeValidationBlockHeightRetention()
	require.NotZero(t, retention,
		"sanity: a zero retention would make every delete-at-height equal the block's own height, and the "+
			"equality assertion below would pass even if the retention term were silently dropped")

	expected := height + retention

	for _, ft := range []fileformat.FileType{
		fileformat.FileTypeSubtreeData,
		fileformat.FileTypeSubtreeMeta,
		fileformat.FileTypeSubtree,
	} {
		got := scheduler.dahFor(t, root[:], ft)
		require.Equal(t, expected, got,
			"%s must be stamped with the block's height plus the configured retention, so the store reclaims it by height rather than leaking it", ft)
	}
}
