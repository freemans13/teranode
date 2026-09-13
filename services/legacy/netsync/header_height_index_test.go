package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// newHeightIndexManager builds the minimal SyncManager these tests need: a
// bare struct literal with only headerList set, following the shape at
// unified_parity_test.go:186. Nothing else here touches settings, so the
// 4 KB guard page trap does not apply.
func newHeightIndexManager(t *testing.T) *SyncManager {
	t.Helper()

	return &SyncManager{
		headerList: list.New(),
	}
}

// TestHeaderHeightIndex_TracksTheListExactly pins the invariant that makes the
// wanted range cheap: a header can be found by height without walking. The two
// indexes are maintained in the same two functions precisely so they cannot
// disagree, and this is what proves it.
func TestHeaderHeightIndex_TracksTheListExactly(t *testing.T) {
	sm := newHeightIndexManager(t)

	hashes := make([]chainhash.Hash, 0, 5)

	for i := int32(1); i <= 5; i++ {
		h := chainhash.Hash{byte(i)}
		hashes = append(hashes, h)

		sm.headerMu.Lock()
		e := sm.headerList.PushBack(&headerNode{height: i, hash: &hashes[i-1]})
		sm.indexHeaderLocked(e, h)
		sm.headerMu.Unlock()
	}

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	for i := int32(1); i <= 5; i++ {
		node, ok := sm.headerAtHeightLocked(i)
		require.True(t, ok, "height %d must be findable without walking the list", i)
		require.Equal(t, i, node.height, "and it must be the header for that height")
	}

	_, ok := sm.headerAtHeightLocked(6)
	require.False(t, ok, "a height the list does not hold must report missing, not a neighbour")
}

// TestHeaderHeightIndex_ForgetsWhatTheListForgets pins the other half. A stale
// entry here would hand the walk a header whose block has already arrived, and
// the node would ask a peer for a block it holds.
func TestHeaderHeightIndex_ForgetsWhatTheListForgets(t *testing.T) {
	sm := newHeightIndexManager(t)

	h := chainhash.Hash{0x01}

	sm.headerMu.Lock()
	e := sm.headerList.PushBack(&headerNode{height: 1, hash: &h})
	sm.indexHeaderLocked(e, h)
	sm.unindexHeaderLocked(e, h)

	_, ok := sm.headerAtHeightLocked(1)
	sm.headerMu.Unlock()

	require.False(t, ok, "unindexing must clear the height entry too, or the walk requests a block it already has")
}
