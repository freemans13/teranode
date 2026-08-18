package netsync

import (
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// requestedBlocksFor builds a peerSyncState holding the given number of
// outstanding block requests. A count of -1 means the map itself is absent,
// which is what a state constructed outside the normal peer-setup path looks
// like and must not panic.
func requestedBlocksFor(t *testing.T, count int) *peerSyncState {
	t.Helper()

	if count < 0 {
		return &peerSyncState{}
	}

	m := expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	t.Cleanup(m.Stop)

	for i := 0; i < count; i++ {
		m.Set(chainhash.Hash{byte(i)}, struct{}{})
	}

	return &peerSyncState{requestedBlocks: m}
}

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
		// requested holds the outstanding block count per peer; -1 means the
		// peer's requestedBlocks map is nil.
		requested []int
		want      int
	}{
		{name: "no peers at all", requested: nil, want: 0},
		{name: "one peer, nothing outstanding", requested: []int{0}, want: 0},
		{name: "one peer, one block outstanding", requested: []int{1}, want: 1},
		{name: "one peer, several blocks outstanding counts once", requested: []int{5}, want: 1},
		{name: "three peers all downloading", requested: []int{1, 2, 3}, want: 3},
		{name: "only the downloading peers are counted", requested: []int{0, 3, 0, 1}, want: 2},
		{name: "a nil requestedBlocks map is skipped, not counted", requested: []int{-1, 1}, want: 1},
		{name: "every peer idle", requested: []int{0, 0, 0}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SyncManager{
				peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
			}

			for i, count := range tt.requested {
				// Distinct ports so each peer is a distinct map key.
				sm.peerStates.Set(newTestPeer(t, "localhost:"+strconv.Itoa(8333+i)), requestedBlocksFor(t, count))
			}

			require.Equal(t, tt.want, sm.PeersWithBlockDownloads())
		})
	}
}

// TestPeersWithBlockDownloadsIgnoresNilState guards the entry itself being nil
// rather than its map. Range hands back whatever was stored, so a nil state must
// be skipped rather than dereferenced.
func TestPeersWithBlockDownloadsIgnoresNilState(t *testing.T) {
	sm := &SyncManager{
		peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	sm.peerStates.Set(newTestPeer(t, "localhost:8333"), nil)
	sm.peerStates.Set(newTestPeer(t, "localhost:8334"), requestedBlocksFor(t, 2))

	require.Equal(t, 1, sm.PeersWithBlockDownloads())
}

// TestPeersWithBlockDownloadsAfterStop covers a peer being torn down while the
// count is being taken: handleDonePeerMsg calls Stop on the expiring map, and a
// stall handler on another goroutine may read it immediately afterwards. That
// must return a count rather than panic.
func TestPeersWithBlockDownloadsAfterStop(t *testing.T) {
	m := expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	m.Set(chainhash.Hash{0x01}, struct{}{})
	m.Stop()

	sm := &SyncManager{
		peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}
	sm.peerStates.Set(newTestPeer(t, "localhost:8333"), &peerSyncState{requestedBlocks: m})

	require.NotPanics(t, func() {
		_ = sm.PeersWithBlockDownloads()
	})
}
