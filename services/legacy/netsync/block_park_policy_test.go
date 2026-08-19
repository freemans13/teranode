package netsync

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// parkReadFaultStore wraps a real blob store and, when armed, fails every
// GetIoReader with an error of the test's choosing without touching the blob.
// That is exactly the shape of the two failures that must never destroy a
// parked block: the store's own permit wait running out, and the read being
// cancelled on shutdown. Both are raised by file.acquireReadPermit
// (stores/blob/file/file.go), which returns a ServiceUnavailable error when the
// deadline passes and a ContextCanceled error when the caller's context is
// cancelled — neither of which says anything at all about the blob.
type parkReadFaultStore struct {
	blob.Store

	mu  sync.Mutex
	err error
}

func (s *parkReadFaultStore) failReadsWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.err = err
}

func (s *parkReadFaultStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// requireStillParked asserts the end state a block that was NOT judged must
// reach: still in the index, still on disk, still charged, the download walk
// untouched and the peer unblamed.
func (h *parkWiringHarness) requireStillParked(t *testing.T, hash chainhash.Hash, bytesBefore int64) {
	t.Helper()

	require.Equal(t, 1, h.sm.blockPark.Len(), "the block must still be parked")
	require.Equal(t, bytesBefore, h.sm.blockPark.Bytes(), "putting a block back must not lose or double its budget")
	require.Contains(t, parkDirEntries(t, h.parkDir), hash.String()+".msgBlock",
		"the downloaded block must still be on disk")

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.Nil(t, startHeader, "a block that was not judged must not rewind the download walk")

	require.False(t, h.rec.wasRejected(hash), "a local fault is not the peer's fault")

	_, failed := h.sm.recentlyFailedBlocks.Get(hash)
	require.False(t, failed, "a block nobody could judge must not be written off as a failure")
}

// TestSyncManager_AParkedBlockSurvivesAReadThatSaysNothingAboutTheBlock is the
// data-loss case. Reading a parked block back can fail for two reasons that are
// not about the block at all: the blob store had no read permit free inside the
// park's deadline, and the read was cancelled because the node is shutting down.
// Treating either as "the blob is corrupt" throws away a block that is already
// fully downloaded, validated and on disk — and both fire in bursts, so it is
// many blocks at once, each of which is then downloaded a second time.
func TestSyncManager_AParkedBlockSurvivesAReadThatSaysNothingAboutTheBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			// file.acquireReadPermit's deadline branch, verbatim.
			name: "no read permit came free in time",
			err:  errors.NewServiceUnavailableError("[File] read operation timed out waiting for semaphore permit"),
		},
		{
			// file.acquireReadPermit's cancellation branch, verbatim.
			name: "the read was cancelled by shutdown",
			err:  errors.NewContextCanceledError("[File] read operation canceled while waiting for semaphore permit", context.Canceled),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)

			child := h.blocks[1].MsgBlock().BlockHash()
			parent := h.blocks[0].MsgBlock().BlockHash()

			h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())

			parkedBytes := h.sm.blockPark.Bytes()

			h.store.failReadsWith(tc.err)

			// The parent lands, so the drain reaches for the child and the read
			// fails for a reason that is nothing to do with the child.
			require.NoError(t, h.deliver(t, 0))

			h.requireStillParked(t, child, parkedBytes)
		})
	}
}

// TestSyncManager_AParkedBlockSurvivesATransientCommitFailure. A commit can fail
// because this node's own storage is briefly unwell — the UTXO store answering
// ErrServiceUnavailable when a batch does not complete in time is the common
// one. That is not a judgement on the block, and the block is already on disk,
// so throwing it away buys a re-download of something we already have. It stays
// parked and the sweep tries again.
func TestSyncManager_AParkedBlockSurvivesATransientCommitFailure(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()
	h.client.On("GetBlockExists", mock.Anything, &child).
		Return(false, errors.NewStorageError("the store is not answering")).Once()
	h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	require.NoError(t, h.deliver(t, 0))

	h.requireStillParked(t, child, parkedBytes)
}
