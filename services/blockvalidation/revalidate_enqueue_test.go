package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestEnqueueRevalidation_DoesNotBlockWhenWorkerNeverStarted pins that a re-queue can always
// give up.
//
// enqueueRevalidation guards its blocking send with revalidateWorkerStopped, which is closed by
// the worker's defer — so the guard only works if the worker ran at all. A BlockValidation whose
// worker was never launched leaves that channel open forever, and once the two-slot buffer is
// full the send has no consumer and no escape: the caller's goroutine parks permanently. Every
// header-context failure adds another, unbounded.
//
// NewBlockValidation always launches start(), and start()'s early return is unreachable today
// (processSubtreesNotSet's errgroup closures return nil unconditionally, so g.Wait() cannot
// fail), so this is not a live production leak. It is reachable from any BlockValidation
// assembled directly, which several tests do, and it becomes live the moment either of those two
// facts changes — neither of which is a change anyone would expect to hang a caller.
func TestEnqueueRevalidation_DoesNotBlockWhenWorkerNeverStarted(t *testing.T) {
	bv := &BlockValidation{
		logger: ulogger.TestLogger{},
		// Same shape NewBlockValidation builds: a two-slot channel and an open stop signal.
		// The one thing missing is the worker that would drain it.
		revalidateBlockChan:     make(chan revalidateBlockData, 2),
		revalidateWorkerStopped: make(chan struct{}),
	}

	data := revalidateBlockData{block: testBlockForRequeue(t)}

	// Fill the buffer, so the next send has to block.
	bv.revalidateBlockChan <- data
	bv.revalidateBlockChan <- data

	returned := make(chan struct{})

	go func() {
		defer close(returned)

		bv.enqueueRevalidation(data)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueueRevalidation parked forever: with no worker to drain the channel and no " +
			"stop signal, the send has neither a consumer nor an escape")
	}

	require.Len(t, bv.revalidateBlockChan, 2,
		"the buffer still holds exactly what was put in it: the third block was dropped, not queued")
}

// TestEnqueueRevalidation_StillQueuesWhenThereIsRoom guards the other direction: the give-up path
// must not swallow a re-queue that the buffer could have taken. ReValidateBlock's whole contract
// is that the block reaches the worker.
func TestEnqueueRevalidation_StillQueuesWhenThereIsRoom(t *testing.T) {
	bv := &BlockValidation{
		logger:                  ulogger.TestLogger{},
		revalidateBlockChan:     make(chan revalidateBlockData, 2),
		revalidateWorkerStopped: make(chan struct{}),
	}

	bv.enqueueRevalidation(revalidateBlockData{block: testBlockForRequeue(t)})

	require.Len(t, bv.revalidateBlockChan, 1, "a re-queue with buffer space must be queued, not dropped")
}

// testBlockForRequeue returns the smallest block enqueueRevalidation can log: it calls
// block.String(), which hashes the header, and BlockHeader.Bytes clones both hashes unguarded.
func testBlockForRequeue(t *testing.T) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *nBits,
		},
	}
}
