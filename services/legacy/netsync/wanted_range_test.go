package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// seedHeaders builds a SyncManager whose header list holds one node for every
// height in [from, to], indexed through indexHeaderLocked so the map and the
// list agree the same way a live node's do. A bare struct literal is enough:
// wantedBlocksLocked reads only headerList/headersByHeight, never sm.settings,
// so this harness must not be mistaken for one that can stand in for a peer or
// blockchain-client test.
func seedHeaders(t *testing.T, from, to int32) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		headerList: list.New(),
	}

	for h := from; h <= to; h++ {
		hash := chainhash.Hash{}
		hash[0] = byte(h)
		hash[1] = byte(h >> 8)

		node := &headerNode{height: h, hash: &hash}
		e := sm.headerList.PushBack(node)
		sm.indexHeaderLocked(e, hash)
	}

	return sm
}

// removeHeightForTest removes the node at height through the real removal
// path — unindexHeaderLocked followed by headerList.Remove — rather than
// deleting the map entry directly, so the gap it makes is the same shape a
// live removal (a delivered block leaving the list, a rewind) would make.
func (sm *SyncManager) removeHeightForTest(height int32) {
	e, ok := sm.headersByHeight[height]
	if !ok {
		return
	}

	node, ok := e.Value.(*headerNode)
	if !ok || node.hash == nil {
		return
	}

	sm.unindexHeaderLocked(e, *node.hash)
	sm.headerList.Remove(e)
}

// TestWantedBlocks_IsTheNextDepthAboveTheBestBlock is the contract. The range is
// computed from the best block processed and nothing else, so it cannot drift
// the way a stored position can.
func TestWantedBlocks_IsTheNextDepthAboveTheBestBlock(t *testing.T) {
	sm := seedHeaders(t, 1, 20)

	sm.headerMu.Lock()
	got := sm.wantedBlocksLocked(10, 4)
	sm.headerMu.Unlock()

	require.Len(t, got, 4, "the range is bounded by the depth, whatever the list holds")
	require.Equal(t, int32(11), got[0].height, "and it starts one above the best block processed")
	require.Equal(t, int32(14), got[3].height)
}

// TestWantedBlocks_StopsAtTheFirstHeightItCannotName pins the gap rule. The
// range must be contiguous: a block whose parent is missing cannot commit, so
// asking for blocks beyond a hole buys nothing and fills the park with blocks
// that have to wait.
func TestWantedBlocks_StopsAtTheFirstHeightItCannotName(t *testing.T) {
	sm := seedHeaders(t, 1, 20)

	sm.headerMu.Lock()
	sm.removeHeightForTest(13)
	got := sm.wantedBlocksLocked(10, 8)
	sm.headerMu.Unlock()

	require.Len(t, got, 2, "the range stops at the hole rather than skipping it")
	require.Equal(t, int32(11), got[0].height)
	require.Equal(t, int32(12), got[1].height)
}

// TestWantedBlocks_IsEmptyWhenTheChainIsCaughtUp pins the ordinary quiet case:
// nothing above the best block is known, so nothing is wanted.
func TestWantedBlocks_IsEmptyWhenTheChainIsCaughtUp(t *testing.T) {
	sm := seedHeaders(t, 1, 10)

	sm.headerMu.Lock()
	got := sm.wantedBlocksLocked(10, 8)
	sm.headerMu.Unlock()

	require.Empty(t, got, "a caught-up node wants nothing, and must not report a phantom range")
}

// TestWantedBlocks_RefusesANonPositiveDepth pins the floor. A depth of zero
// would ask for nothing for ever, which is a stall dressed as a setting.
func TestWantedBlocks_RefusesANonPositiveDepth(t *testing.T) {
	sm := seedHeaders(t, 1, 20)

	sm.headerMu.Lock()
	got := sm.wantedBlocksLocked(10, 0)
	sm.headerMu.Unlock()

	require.Len(t, got, 1, "a depth below one is clamped to one, never to zero")
}
