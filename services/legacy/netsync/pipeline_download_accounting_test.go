package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestHandleBlockOnDiskMsg_ReleasesTheDownloadAssignment is the regression test
// for the ownership leak: the streamed route never called RemoveOwner or
// ForgiveOwners, the same pair handleBlockMsg calls the moment it dequeues a
// decoded block (manager.go, around handleBlockMsg's "This peer has answered"
// comment).
//
// Before this branch made the pipeline send every block down the on-disk route,
// that gap only bit blocks above the 64 MiB decode threshold, rare enough that
// nobody noticed a peer's assignment count creeping up. With PipelineReceive on,
// every block takes this route, so a peer that delivers sixteen blocks (the
// default MaxBlocksInTransitPerPeer) has its CountForPeer stick there forever —
// the scheduler's per-peer budget (block_scheduler.go:144) reaches zero and stops
// asking that peer for anything else until the hour-long assignment TTL expires.
//
// This drives three blocks through handleBlockOnDiskMsg for one peer and
// requires CountForPeer to return to zero, exactly as it does for the decoded
// route today.
func TestHandleBlockOnDiskMsg_ReleasesTheDownloadAssignment(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)
	h.sm.settings.Legacy.PipelineReceive = true

	// A parent already in the harness's header list, so every delivered block
	// is reachable and handleBlockOnDiskMsg does not discard it before reaching
	// the code under test.
	parent := h.blocks[1].MsgBlock().BlockHash()

	mkBody := func(nonce uint32) peerpkg.BlockBody {
		header := wire.BlockHeader{Version: 1, PrevBlock: parent, Nonce: nonce}
		return peerpkg.BlockBody{Header: header, TxCount: 1, Size: 4096, Hash: header.BlockHash()}
	}

	bodies := []peerpkg.BlockBody{mkBody(1), mkBody(2), mkBody(3)}

	for _, b := range bodies {
		require.True(t, h.sm.blockDownloads.Add(h.peer, b.Hash),
			"seed one assignment per block, exactly what the download walk does before asking a peer for it")
	}

	require.Equal(t, 3, h.sm.blockDownloads.CountForPeer(h.peer),
		"sanity: three assignments outstanding before any delivery")

	for _, b := range bodies {
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: b, peer: h.peer})
	}

	require.Equal(t, 0, h.sm.blockDownloads.CountForPeer(h.peer),
		"every delivered block must release its assignment, or CountForPeer sticks at MaxBlocksInTransitPerPeer and the scheduler stops asking this peer for anything else")
}

// TestHandleBlockOnDiskMsg_OwnershipUnchangedWhenPipelineOff pins the scope
// decision explicitly: this task fixes the route the pipeline made universal,
// not the pre-existing (rare, >64 MiB) on-disk route that ran before it, and
// with PipelineReceive off that pre-existing behaviour — leak included — must
// be untouched.
func TestHandleBlockOnDiskMsg_OwnershipUnchangedWhenPipelineOff(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)
	h.sm.settings.Legacy.PipelineReceive = false

	parent := h.blocks[1].MsgBlock().BlockHash()
	header := wire.BlockHeader{Version: 1, PrevBlock: parent}
	body := peerpkg.BlockBody{Header: header, TxCount: 1, Size: 4096, Hash: header.BlockHash()}

	require.True(t, h.sm.blockDownloads.Add(h.peer, body.Hash))
	require.Equal(t, 1, h.sm.blockDownloads.CountForPeer(h.peer))

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.Equal(t, 1, h.sm.blockDownloads.CountForPeer(h.peer),
		"with PipelineReceive off, this task must change nothing about the pre-existing on-disk route, including its leak")
}

// TestPipelineOnDiskRoute_AdmissionBoundsInFlightConversions is the regression
// test for the missing admission control. AcquireBlockPrefetch is reached only
// from OnBlock, which the peer dispatches only for a whole-block message; with
// the pipeline on every block comes back as *peer.MsgBlockOnDisk instead, so
// OnBlock — and the admission check inside it — never runs, and the pipeline
// path has no bound on how many blocks convert concurrently.
//
// This occupies the admission budget's only slot directly (simulating another
// peer's conversion already in flight), then drives a second, real, convertible
// block through the sink actually installed for the wire layer and requires that
// it stays blocked until the held slot is released.
func TestPipelineOnDiskRoute_AdmissionBoundsInFlightConversions(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.Legacy.PipelineReceive = true

	sm.blockPrefetchBudgetBytes = 1
	sm.blockPrefetchBudget = semaphore.NewWeighted(1)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	var installedSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)

	sm.installStreamingBlockPath(func(
		sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
		gate func(chainhash.Hash, *wire.BlockHeader) error,
		del func(chainhash.Hash) error,
		streamsEverySize bool,
	) {
		installedSink = sink
	})
	require.NotNil(t, installedSink, "the park is enabled, so a sink must have been installed")

	// Occupy the only slot, simulating a first block another peer's read loop
	// is already converting.
	heldHash := chainhash.Hash{0x01}
	weight, err := sm.AcquireBlockPrefetch(context.Background(), nil, heldHash, 999)
	require.NoError(t, err, "occupying the only slot must succeed before the second block can be shown to wait on it")

	// A second, distinct, well-formed block. Distinct transaction count from
	// any other fixture in this package: wireBlockWithTxs is deterministic and
	// the wire header's one-second timestamp resolution means two blocks built
	// moments apart with the same transaction count hash identically.
	blk := wireBlockWithTxs(t, 7, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)
	header := &blk.MsgBlock().Header
	hash := *blk.Hash()

	var (
		converted bool
		sinkErr   error
	)

	done := make(chan struct{})

	go func() {
		converted, sinkErr = installedSink(hash, header, bytes.NewReader(body), int64(len(body)))
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("the on-disk route converted a second block while the admission budget's only slot was already held (converted=%v err=%v) — admission control is not reachable from this path", converted, sinkErr)
	case <-time.After(200 * time.Millisecond):
		// Expected: still blocked on the budget.
	}

	sm.ReleaseBlockPrefetch(heldHash, weight)

	require.True(t, WaitUntil(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second), "releasing the held slot must let the second block's conversion proceed")

	require.NoError(t, sinkErr, "the second block is well-formed and must convert cleanly once admitted")
	require.True(t, converted, "a well-formed block below the checkpoint must convert, not merely be accepted")
}

// TestPipelineOnDiskRoute_AdmissionUntouchedWhenPipelineOff pins that the
// admission wrap only ever applies to the pipeline sink: with PipelineReceive
// off, the installed sink is streamingBlockSink, which never touches the
// download-admission budget, so a fully occupied budget must not affect it at
// all.
func TestPipelineOnDiskRoute_AdmissionUntouchedWhenPipelineOff(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.Legacy.PipelineReceive = false

	sm.blockPrefetchBudgetBytes = 1
	sm.blockPrefetchBudget = semaphore.NewWeighted(1)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	var installedSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)

	sm.installStreamingBlockPath(func(
		sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
		gate func(chainhash.Hash, *wire.BlockHeader) error,
		del func(chainhash.Hash) error,
		streamsEverySize bool,
	) {
		installedSink = sink
	})
	require.NotNil(t, installedSink)

	// Hold the only slot, exactly as in the pipeline-on test above.
	heldHash := chainhash.Hash{0x02}
	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, heldHash, 1)
	require.NoError(t, err)

	blk := wireBlockWithTxs(t, 5, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)
	header := &blk.MsgBlock().Header

	done := make(chan struct{})

	go func() {
		_, _ = installedSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
		close(done)
	}()

	select {
	case <-done:
		// Expected: completes promptly, unaffected by the held slot.
	case <-time.After(2 * time.Second):
		t.Fatal("with PipelineReceive off the on-disk route must not be gated by the admission budget at all")
	}
}
