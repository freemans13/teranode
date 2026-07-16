// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for the flushWorker poison-recovery gate. A fatal window commit (e.g. a
// Postgres 40P01 deadlock in the UTXO create batcher) sets the flushWorker's
// poison latch, which — before this fix — was a goroutine-lifetime local that was
// never reset, so it discarded every subsequent window and froze the tip until a
// process restart. windowRelinksAfterPoison decides when to clear the latch: only
// for a window contiguous with the committed chain (the peer rotation re-requests
// from committedBest), never for a window that starts beyond it (a real gap).
import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWindowRelinksAfterPoison(t *testing.T) {
	relinks := func(bestHeight uint32, firstBlockHeight uint32, bestErr error) bool {
		bc := &blockchain2.Mock{}
		if bestErr != nil {
			bc.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, bestErr)
		} else {
			bc.On("GetBestBlockHeader", mock.Anything).
				Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: bestHeight}, nil)
		}

		sm := &SyncManager{logger: ulogger.TestLogger{}, blockchainClient: bc}
		job := windowFlushJob{blocks: []*model.Block{{Height: firstBlockHeight}}}
		return sm.windowRelinksAfterPoison(context.Background(), job)
	}

	require.True(t, relinks(585737, 585738, nil), "tip+1 (committedBest+1) relinks → clear poison, retry")
	require.True(t, relinks(585737, 585737, nil), "re-covering the committed tip relinks (idempotent re-commit)")
	require.True(t, relinks(585737, 500000, nil), "a window well below the tip relinks")
	require.False(t, relinks(585737, 585739, nil), "a window starting beyond committedBest+1 is a GAP → stay poisoned")
	require.False(t, relinks(585737, 600000, nil), "a far-ahead window is a gap → stay poisoned")

	// Best-block lookup failure must fail safe: stay poisoned.
	require.False(t, relinks(0, 585738, context.DeadlineExceeded), "on best-block lookup error, stay poisoned")

	// Empty window never relinks.
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockchainClient: &blockchain2.Mock{}}
	require.False(t, sm.windowRelinksAfterPoison(context.Background(), windowFlushJob{}), "empty window does not relink")
}
