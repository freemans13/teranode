package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// Active and standby peers: only the fewest fastest peers that together give 80% of the measured
// bandwidth take blocks. The rest stay connected and idle unless the active peers go. The rates are the seven
// mainnet peers of 2026-10-07 12:29Z, when the chain stopped every few blocks on the slow ones.

const standbyTypical = 300_000_000

// standbyAssigner has the chain at 10, applying 2 blocks a second, with a typical block of 300 MB.
func standbyAssigner(peers, full []*assignerPeer) *downloadAssigner {
	remaining := 0
	for _, p := range peers {
		remaining += p.budget
	}

	return &downloadAssigner{peers: peers, full: full, remaining: remaining, tip: 10, commitRate: 2, typical: standbyTypical, now: time.Now()}
}

func measuredPeer(t *testing.T, sm *SyncManager, idx uint8, budget int, rate float64) *assignerPeer {
	t.Helper()

	p, _ := schedulerPeer(t, sm, idx, 5000)

	return &assignerPeer{peer: p, budget: budget, rate: rate, measured: true}
}

func wantedAt(height int32) wantedBlock {
	return wantedBlock{height: height, hash: chainhash.Hash{byte(height), byte(height >> 8), 0x7e}}
}

func placed(p *assignerPeer) []chainhash.Hash {
	if p.getData == nil {
		return nil
	}

	out := make([]chainhash.Hash, 0, len(p.getData.InvList))
	for _, iv := range p.getData.InvList {
		out = append(out, iv.Hash)
	}

	return out
}

const mb = 1_000_000

// The top three of 2026-10-07 take blocks; 8.2 MB/s and below stand by, room or not, while the
// top three are full. The blocks wait for them.
func TestStandbyOnlyTheTopPeersTakeBlocks(t *testing.T) {
	sm := schedulerManager(t)

	top := make([]*assignerPeer, 0, 3)
	rest := make([]*assignerPeer, 0, 4)

	for i, r := range []float64{38 * mb, 27.8 * mb, 26 * mb} {
		top = append(top, measuredPeer(t, sm, uint8(i+1), 0, r))
	}

	for i, r := range []float64{8.2 * mb, 2.5 * mb, 1.9 * mb, 1.5 * mb} {
		rest = append(rest, measuredPeer(t, sm, uint8(i+10), 1, r))
	}

	sm.requestBlocks(standbyAssigner(rest, top), []wantedBlock{wantedAt(12), wantedAt(13), wantedAt(14)}, 0)

	for _, p := range rest {
		require.Empty(t, placed(p), "a peer at %.1f MB/s stands by: the top three give 91.8 of 105.9 MB/s", p.rate/mb)
	}
}

// An active peer with room takes the block, fastest first, as before.
func TestStandbyAnActivePeerWithRoomTakesTheBlock(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 1, 38*mb)
	second := measuredPeer(t, sm, 2, 1, 26*mb)
	slow := measuredPeer(t, sm, 3, 1, 2*mb)

	sm.requestBlocks(standbyAssigner([]*assignerPeer{slow, second, fast}, nil), []wantedBlock{wantedAt(12), wantedAt(13)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(fast))
	require.Equal(t, []chainhash.Hash{wantedAt(13).hash}, placed(second))
	require.Empty(t, placed(slow))
}

// The set follows the peers connected. With the top three gone, 80% of the 12.2 MB/s left is 9.8,
// which 8.2 and 2.5 make up between them.
func TestStandbyPeersTakeOverWhenTheFastOnesGo(t *testing.T) {
	sm := schedulerManager(t)
	eight := measuredPeer(t, sm, 1, 1, 8.2*mb)
	two := measuredPeer(t, sm, 2, 1, 2.5*mb)
	one := measuredPeer(t, sm, 3, 1, 1.5*mb)

	sm.requestBlocks(standbyAssigner([]*assignerPeer{eight, two, one}, nil), []wantedBlock{wantedAt(12), wantedAt(13), wantedAt(14)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(eight))
	require.Equal(t, []chainhash.Hash{wantedAt(13).hash}, placed(two), "8.2 alone is short of 80%")
	require.Empty(t, placed(one), "and 1.5 MB/s is the last fifth")
}

// A peer with no measured rate takes blocks, so it gets measured.
func TestStandbyAnUnmeasuredPeerStillGetsBlocks(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 38*mb)
	unmeasured := measuredPeer(t, sm, 2, 1, 2*mb)
	unmeasured.measured = false

	sm.requestBlocks(standbyAssigner([]*assignerPeer{unmeasured}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(unmeasured))
}

// A standby peer is re-measured: at most once per probe interval it may take the highest block of
// the pass, and only when that block would land in time if the peer were four times faster than
// last measured, so a peer still slow is late only where the lead can absorb it.
func TestStandbyAPeerIsProbedOnTheFarBlockOnce(t *testing.T) {
	sm := schedulerManager(t)
	now := time.Now()

	newPass := func(slow *assignerPeer) *downloadAssigner {
		fast := measuredPeer(t, sm, 2, 0, 80*mb)
		a := standbyAssigner([]*assignerPeer{slow}, []*assignerPeer{fast})
		a.now = now

		return a
	}

	slow := measuredPeer(t, sm, 1, 1, 1*mb)

	// 170 is 159 blocks away: 79.5 s. At 1 MB/s the block takes 300 s; at 4 MB/s, 75 s.
	sm.requestBlocks(newPass(slow), []wantedBlock{wantedAt(12), wantedAt(170)}, 0)
	require.Equal(t, []chainhash.Hash{wantedAt(170).hash}, placed(slow), "the far block is the probe")

	again := &assignerPeer{peer: slow.peer, budget: 1, rate: 1 * mb, measured: true}
	sm.requestBlocks(newPass(again), []wantedBlock{wantedAt(13), wantedAt(171)}, 0)
	require.Empty(t, placed(again), "and not again inside the probe interval")
}

// The probe is never a block near the front: at four times its last rate the peer would still be
// late for it.
func TestStandbyAProbeIsNeverANearBlock(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 1, 1*mb)
	fast := measuredPeer(t, sm, 2, 0, 80*mb)

	sm.requestBlocks(standbyAssigner([]*assignerPeer{slow}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12), wantedAt(40)}, 0)

	require.Empty(t, placed(slow))
}
