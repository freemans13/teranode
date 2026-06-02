package netsync

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// recordingBlockValidation records the height of every ProcessBlock call (the
// finalize step) so a test can assert the order blocks are finalized in.
type recordingBlockValidation struct {
	*blockvalidation.MockBlockValidation
	mu    sync.Mutex
	order []uint32
}

func (r *recordingBlockValidation) ProcessBlock(_ context.Context, _ *model.Block, blockHeight uint32, _, _ string, _ uint32) error {
	r.mu.Lock()
	r.order = append(r.order, blockHeight)
	r.mu.Unlock()

	return nil
}

func (r *recordingBlockValidation) recorded() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]uint32, len(r.order))
	copy(out, r.order)

	return out
}

// makeFinalizeJob builds a finalizeJob whose teranodeBlock has a real (distinct)
// header so finalizeBlock → ProcessBlock can run; txHashes is empty so the
// orphan-processing goroutine is a no-op.
func makeFinalizeJob(t *testing.T, height uint32) *finalizeJob {
	t.Helper()

	wb := newTestBlock(chainhash.Hash{byte(height), byte(height >> 8)})

	var buf bytes.Buffer
	require.NoError(t, wb.MsgBlock().Header.Serialize(&buf))

	hdr, err := model.NewBlockHeaderFromBytes(buf.Bytes())
	require.NoError(t, err)

	return &finalizeJob{
		ctx:           context.Background(),
		teranodeBlock: &model.Block{Header: hdr, Height: height, ID: height},
		blockHeight:   height,
	}
}

// TestFinalizeLoop_FinalizesInHeightOrderUnderConcurrentSubmit is the end-to-end
// ordering guarantee for the concurrent pipeline: jobs whose (concurrent) PhaseA
// completes out of order are handed to the finalizer in arbitrary order, yet
// ProcessBlock (AddBlock/mined_set) must run in strict ascending height order so
// each block is added only after its parent.
func TestFinalizeLoop_FinalizesInHeightOrderUnderConcurrentSubmit(t *testing.T) {
	initPrometheusMetrics()

	bv := &recordingBlockValidation{MockBlockValidation: &blockvalidation.MockBlockValidation{}}

	tSettings := &settings.Settings{}

	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		quit:            make(chan struct{}),
		settings:        tSettings,
		blockValidation: bv,
	}
	defer close(sm.quit)

	const start = 100
	const n = 16

	// Start the finalizer at the authoritative start height (as the in-order
	// consumer would on the first pipelined block).
	sm.ensureFinalizer(start)

	// Submit heights start..start+n-1 concurrently in a shuffled order, mimicking
	// out-of-order PhaseA completion across the worker pool.
	order := []uint32{107, 100, 114, 103, 111, 101, 108, 115, 102, 110, 104, 113, 105, 109, 106, 112}
	require.Len(t, order, n)

	var wg sync.WaitGroup
	for _, h := range order {
		wg.Add(1)
		go func(h uint32) {
			defer wg.Done()
			sm.enqueueFinalize(makeFinalizeJob(t, h))
		}(h)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return len(bv.recorded()) == n
	}, 3*time.Second, 5*time.Millisecond, "all blocks finalized")

	got := bv.recorded()
	require.True(t, sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }),
		"blocks must be finalized in ascending height order, got %v", got)

	want := make([]uint32, n)
	for i := range want {
		want[i] = uint32(start + i)
	}
	require.Equal(t, want, got)
}
