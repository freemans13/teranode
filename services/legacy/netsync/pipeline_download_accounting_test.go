package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
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
