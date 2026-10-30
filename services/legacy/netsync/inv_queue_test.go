package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// txInvs builds n transaction announcements with distinct hashes. tag keeps
// the hashes of two calls apart.
func txInvs(n int, tag byte) []*wire.InvVect {
	items := make([]*wire.InvVect, n)

	for i := range items {
		h := chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), tag, 0x1f}
		items[i] = wire.NewInvVect(wire.InvTypeTx, &h)
	}

	return items
}

// droppedInvs reads the announcement drop counter for one reason.
func droppedInvs(reason string) float64 {
	return testutil.ToFloat64(prometheusLegacyNetsyncInvDropped.WithLabelValues(reason))
}

// TestInvQueueMaxHashes_FloorsAtOneChunk pins the legal range of
// legacy_invQueueMaxHashes: below one chunk falls back to the default, and zero
// is not unbounded, because an unbounded queue is the 2026-10-19 incident.
func TestInvQueueMaxHashes_FloorsAtOneChunk(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "the default is eight maximum-size announcements")

	tSettings.Legacy.InvQueueMaxHashes = 100
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "below one chunk falls back to the default")

	tSettings.Legacy.InvQueueMaxHashes = 0
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "zero is not unbounded")

	tSettings.Legacy.InvQueueMaxHashes = txInvLookupChunk
	require.Equal(t, txInvLookupChunk, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "one chunk is the floor")
}

// TestAdmitTxInvs_OneMaximumMessageIsAdmittedWhole pins the case the first
// draft of the design failed: a single 50,000-hash announcement from an honest
// peer on an idle node is queued whole, thirteen chunks, nothing dropped.
func TestAdmitTxInvs_OneMaximumMessageIsAdmittedWhole(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	before := droppedInvs("peer_share")

	sm.admitTxInvs(p, state, txInvs(wire.MaxInvPerMsg, 0x01))

	require.Len(t, sm.invQueue, 13)
	require.Equal(t, int64(wire.MaxInvPerMsg), sm.invQueued.Load())
	require.Equal(t, int64(wire.MaxInvPerMsg), state.queuedInvs.Load())
	require.Equal(t, before, droppedInvs("peer_share"))
}

// TestAdmitTxInvs_DropsAPeerOverItsShare pins the per-peer share: a peer with
// two maximum-size announcements queued has its third dropped and counted,
// and a second peer is not affected.
func TestAdmitTxInvs_DropsAPeerOverItsShare(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	before := droppedInvs("peer_share")

	for tag := byte(1); tag <= 3; tag++ {
		sm.admitTxInvs(p, state, txInvs(wire.MaxInvPerMsg, tag))
	}

	require.Equal(t, int64(invPeerShare), state.queuedInvs.Load(), "two maximum-size messages are the share")
	require.Equal(t, int64(invPeerShare), sm.invQueued.Load())
	require.Equal(t, before+float64(wire.MaxInvPerMsg), droppedInvs("peer_share"))

	p2, state2 := newHarnessPeer(t, sm)
	sm.admitTxInvs(p2, state2, txInvs(wire.MaxInvPerMsg, 0x09))
	require.Equal(t, int64(wire.MaxInvPerMsg), state2.queuedInvs.Load(), "a second peer is unaffected")
	require.Equal(t, int64(invPeerShare+wire.MaxInvPerMsg), sm.invQueued.Load())
}

// TestAdmitTxInvs_DropsAtTheNodeCapAndClampsTheShare pins the node cap and
// the clamp: with the cap below the share, the share is the cap, and a chunk
// that fits its peer's share but not the node is dropped under node_cap and
// not charged to the peer.
func TestAdmitTxInvs_DropsAtTheNodeCapAndClampsTheShare(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.InvQueueMaxHashes = 2 * txInvLookupChunk

	sm, p, state, _ := newInvHarness(t, tSettings, nil, blockchain2.FSMStateRUNNING)
	require.Equal(t, 2*txInvLookupChunk, sm.invShare(), "the share clamps to a cap below it")

	p2, state2 := newHarnessPeer(t, sm)
	before := droppedInvs("node_cap")

	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk, 0x01))
	sm.admitTxInvs(p2, state2, txInvs(txInvLookupChunk, 0x02))
	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk, 0x03))

	require.Equal(t, int64(2*txInvLookupChunk), sm.invQueued.Load())
	require.Equal(t, int64(txInvLookupChunk), state.queuedInvs.Load(), "the dropped chunk is not charged to the peer")
	require.Equal(t, int64(txInvLookupChunk), state2.queuedInvs.Load())
	require.Equal(t, before+float64(txInvLookupChunk), droppedInvs("node_cap"))
}

// TestAdmitTxInvs_AFullChannelDropsUnderNodeCap pins the non-blocking send's
// default branch: production sizes the channel to the cap so it never binds
// first, but if it did, the chunk is dropped and both counters given back.
func TestAdmitTxInvs_AFullChannelDropsUnderNodeCap(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	sm.invQueue = make(chan *invChunk, 1)
	before := droppedInvs("node_cap")

	sm.admitTxInvs(p, state, txInvs(1, 0x01))
	sm.admitTxInvs(p, state, txInvs(1, 0x02))

	require.Equal(t, int64(1), sm.invQueued.Load())
	require.Equal(t, int64(1), state.queuedInvs.Load())
	require.Equal(t, before+1, droppedInvs("node_cap"))
}
