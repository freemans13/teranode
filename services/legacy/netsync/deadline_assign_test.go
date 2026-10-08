package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const mb = 1_000_000

// deadlineAssigner has the chain at 10, applying 2 blocks a second when it does not wait, with a
// largest recent block of 4 GB.
func deadlineAssigner(peers, full []*assignerPeer) *downloadAssigner {
	remaining := 0
	for _, p := range peers {
		remaining += p.budget
	}

	return &downloadAssigner{peers: peers, full: full, remaining: remaining, tip: 10, pace: 2, size: 4_000 * mb}
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

// Block 755,254 (2026-10-08): 4 GB, second in the queue of a 7.3 MB/s peer behind another 4 GB
// block. A near block goes to the fast peer: the 7.3 MB/s peer cannot send 4 GB before the chain
// needs it.
func TestDeadlineANearBlockGoesToTheFastPeer(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 2, 7.3*mb)
	fast := measuredPeer(t, sm, 2, 2, 40*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{slow, fast}, nil), []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(fast))
	require.Empty(t, placed(slow))
}

// A far block goes to the slowest peer that can still send 4 GB before the chain needs it, which
// keeps the fast peers free for near blocks. 5 MB/s sends 4 GB in 800 s; at 2 blocks a second the
// chain needs block 1,700 in 844 s.
func TestDeadlineAFarBlockGoesToTheSlowestPeerInTime(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 2, 5*mb)
	medium := measuredPeer(t, sm, 2, 2, 20*mb)
	fast := measuredPeer(t, sm, 3, 2, 40*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{fast, medium, slow}, nil), []wantedBlock{wantedAt(1700)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(1700).hash}, placed(slow))
	require.Empty(t, placed(medium))
	require.Empty(t, placed(fast))
}

// Block 740,742 (2026-10-07): a 5.1 MB/s peer with an empty queue looked soonest. When no peer is
// in time, the earliest arrival wins, and that counts the full 4 GB at each peer's rate.
func TestDeadlineWhenNobodyIsInTimeTheEarliestArrivalWins(t *testing.T) {
	sm := schedulerManager(t)
	idleSlow := measuredPeer(t, sm, 1, 2, 5.1*mb)

	busyFast := measuredPeer(t, sm, 2, 2, 50*mb)
	busyFast.backlog = 60 * time.Second

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{idleSlow, busyFast}, nil), []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(busyFast), "60 s + 80 s at 50 MB/s is earlier than 784 s at 5.1 MB/s")
	require.Empty(t, placed(idleSlow))
}

// When the earliest arrival is a full peer, the block waits for it, and does not go to a slow
// peer with room.
func TestDeadlineABlockWaitsForAFullFastPeer(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 2, 2*mb)
	fast := measuredPeer(t, sm, 2, 0, 50*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{slow}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12)}, 0)

	require.Empty(t, placed(slow))
}

// Review focus 1: a node with one peer continues to download.
func TestDeadlineASinglePeerGetsBlocks(t *testing.T) {
	sm := schedulerManager(t)
	only := measuredPeer(t, sm, 1, 2, 2*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{only}, nil), []wantedBlock{wantedAt(12), wantedAt(13)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash, wantedAt(13).hash}, placed(only))
}

// Review focus 3: with no pace yet each deadline is zero, and blocks go by earliest arrival.
func TestDeadlineWithNoPaceBlocksGoByEarliestArrival(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 2, 2*mb)
	fast := measuredPeer(t, sm, 2, 2, 50*mb)

	a := deadlineAssigner([]*assignerPeer{slow, fast}, nil)
	a.pace = 0

	sm.requestBlocks(a, []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(fast))
}

// Review focus 2 and the restart of 2026-10-08 06:33: with no measured and no remembered rates,
// each peer gets one block, near blocks first.
func TestDeadlineAColdStartGivesEachPeerOneNearBlock(t *testing.T) {
	sm := schedulerManager(t)
	a1, _ := schedulerPeer(t, sm, 1, 5000)
	a2, _ := schedulerPeer(t, sm, 2, 5000)

	p1 := &assignerPeer{peer: a1, budget: 1}
	p2 := &assignerPeer{peer: a2, budget: 1}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{p1, p2}, nil), []wantedBlock{wantedAt(11), wantedAt(12), wantedAt(13)}, 0)

	require.ElementsMatch(t, []chainhash.Hash{wantedAt(11).hash, wantedAt(12).hash}, append(placed(p1), placed(p2)...))
}

// A new peer with no rate, beside measured peers, gets one block from the far end of the pass, so
// its first block cannot stop the chain.
func TestDeadlineAnUnmeasuredPeerGetsTheFarBlock(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 50*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{np}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12), wantedAt(900)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(900).hash}, placed(np))
}

// A queued block counts at the recent mean size, and only the new block at the largest size. A
// fast peer with small blocks queued is then still earlier than an idle slow peer, which the
// largest size for each queued block hid (replay of heights 755,000 to 760,000).
func TestDeadlineAQueueOfSmallBlocksDoesNotHideAFastPeer(t *testing.T) {
	sm := schedulerManager(t)
	wireStreamingPath(sm)

	for range 10 {
		sm.blockSizeTracker.addBlockSize(20 * mb)
	}

	sm.blockSizeTracker.addBlockSize(4_000 * mb)

	fast, _ := schedulerPeer(t, sm, 1, 5000)
	slow, _ := schedulerPeer(t, sm, 2, 5000)
	sm.streams.rates[fast] = 50 * mb
	sm.streams.rates[slow] = 6 * mb

	for i := range 10 {
		require.True(t, sm.blockDownloads.Add(fast, chainhash.Hash{byte(i), 0x55}))
	}

	a := sm.newDownloadAssigner()
	require.NotNil(t, a)

	a.tip = 10
	sm.requestBlocks(a, []wantedBlock{wantedAt(12)}, 0)

	for _, p := range a.peers {
		if p.peer == fast {
			require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(p), "10 queued at the 418 MB mean and 4 GB, at 50 MB/s, is 164 s, earlier than 667 s at 6 MB/s; at 4 GB each it was 880 s")
		}
	}
}

// Equal peers share the blocks in turn: on the same arrival the peer with more room wins.
func TestDeadlineEqualPeersShareTheBlocks(t *testing.T) {
	sm := schedulerManager(t)
	a := measuredPeer(t, sm, 1, 2, 50*mb)
	b := measuredPeer(t, sm, 2, 2, 50*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{a, b}, nil), []wantedBlock{wantedAt(12), wantedAt(13)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(a))
	require.Equal(t, []chainhash.Hash{wantedAt(13).hash}, placed(b))
}

// After a restart the rates file gives peers their rates, but no block has completed, so no size
// is known. Every peer then looked in time, and the slowest took the 16 nearest blocks (review of
// 2026-10-08). With no size the fastest peer takes the near blocks.
func TestDeadlineWithNoSizeTheFastestPeerTakesTheNearBlocks(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 16, 0.5*mb)
	fast := measuredPeer(t, sm, 2, 16, 50*mb)

	a := deadlineAssigner([]*assignerPeer{slow, fast}, nil)
	a.size = 0

	var blocks []wantedBlock
	for h := int32(11); h <= 26; h++ {
		blocks = append(blocks, wantedAt(h))
	}

	sm.requestBlocks(a, blocks, 0)

	require.Len(t, placed(fast), 16)
	require.Empty(t, placed(slow))
}

// A new peer does not get a block the chain needs before a measured peer could land it: when the
// measured peers are full, the highest candidate of the pass can be the next block (review of
// 2026-10-08).
func TestDeadlineAnUnmeasuredPeerDoesNotGetANearBlock(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 50*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{np}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12)}, 0)

	require.Empty(t, placed(np), "block 12 is needed in 0.5 s; the fast peer lands 4 GB in 80 s")
}
