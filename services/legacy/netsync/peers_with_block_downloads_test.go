package netsync

import (
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newTestPeer(t *testing.T, addr string) *peerpkg.Peer {
	t.Helper()

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, settings.NewSettings(), &peerpkg.Config{}, addr)
	require.NoError(t, err)

	return p
}

// TestPeersWithBlockDownloads pins the count that drives the per-peer widening
// of a block's download deadline. The number this returns is multiplied by
// BlockDownloadTimeoutPerPeerPercent, so counting a peer that is not actually
// downloading hands every peer extra patience it did not earn, and missing one
// that is disconnects peers that are only slow because we are sharing our own
// downstream link between them.
func TestPeersWithBlockDownloads(t *testing.T) {
	tests := []struct {
		name string
		// requested holds the outstanding block count per peer.
		requested []int
		want      int
	}{
		{name: "no peers at all", requested: nil, want: 0},
		{name: "one peer, nothing outstanding", requested: []int{0}, want: 0},
		{name: "one peer, one block outstanding", requested: []int{1}, want: 1},
		{name: "one peer, several blocks outstanding counts once", requested: []int{5}, want: 1},
		{name: "three peers all downloading", requested: []int{1, 2, 3}, want: 3},
		{name: "only the downloading peers are counted", requested: []int{0, 3, 0, 1}, want: 2},
		{name: "every peer idle", requested: []int{0, 0, 0}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SyncManager{
				peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
				blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
			}

			for i, count := range tt.requested {
				// Distinct ports so each peer is a distinct map key.
				p := newTestPeer(t, "localhost:"+strconv.Itoa(8333+i))
				sm.peerStates.Set(p, &peerSyncState{})

				for j := 0; j < count; j++ {
					sm.blockDownloads.Add(p, chainhash.Hash{byte(i), byte(j)})
				}
			}

			require.Equal(t, tt.want, sm.PeersWithBlockDownloads())
		})
	}
}

// TestPeersWithBlockDownloadsWithoutALedger covers a manager built as a struct
// literal that never got a download ledger. The stall handler reads this count
// from its own goroutine and must get a number back rather than panic.
func TestPeersWithBlockDownloadsWithoutALedger(t *testing.T) {
	sm := &SyncManager{
		peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	sm.peerStates.Set(newTestPeer(t, "localhost:8333"), &peerSyncState{})

	require.NotPanics(t, func() {
		require.Equal(t, 0, sm.PeersWithBlockDownloads())
	})
}

// TestPeersWithBlockDownloadsIgnoresUnregisteredPeers pins that the count comes
// from the ledger, not from peerStates. A peer that is registered but has never
// been asked for anything must not count, and a peer that has been asked counts
// even though the two are tracked in different places.
func TestPeersWithBlockDownloadsIgnoresUnregisteredPeers(t *testing.T) {
	sm := &SyncManager{
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
	}

	idle := newTestPeer(t, "localhost:8333")
	busy := newTestPeer(t, "localhost:8334")

	sm.peerStates.Set(idle, &peerSyncState{})
	sm.peerStates.Set(busy, &peerSyncState{})
	sm.blockDownloads.Add(busy, chainhash.Hash{0x01})
	sm.blockDownloads.Add(busy, chainhash.Hash{0x02})

	require.Equal(t, 1, sm.PeersWithBlockDownloads())
}
