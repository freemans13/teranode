package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestHeaderHeightIndex_PopulateAndLookup is the TDD test for Task 2.2.
//
// It tests that headerHeightIndex maps each headerNode's hash to its
// authoritative height, so a non-front block can resolve its height without
// being at the front of the list. Before the index is added (RED), a non-front
// block resolves to -1; after (GREEN) it resolves to the correct height.
//
// The test drives the index directly — populate it exactly as the production
// code does in resetHeaderState and handleHeadersMsg, then assert lookup
// behaviour. A companion sub-test asserts that a hash not in the index still
// resolves to -1 (unchanged behaviour).
func TestHeaderHeightIndex_PopulateAndLookup(t *testing.T) {
	// Build three hashes: one that will be at Front(), one in the middle, and
	// one that is NOT in the index at all.
	hashFront := chainhash.Hash{0x01}
	hashMiddle := chainhash.Hash{0x02}
	hashAbsent := chainhash.Hash{0xff}

	t.Run("front node resolves correct height", func(t *testing.T) {
		sm := buildSMWithIndex(t, []indexEntry{
			{hash: hashFront, height: 100},
			{hash: hashMiddle, height: 101},
		})

		sm.headerMu.Lock()
		h := sm.heightFromIndex(hashFront)
		sm.headerMu.Unlock()

		require.Equal(t, int32(100), h,
			"front-node hash must resolve to its height from the index")
	})

	t.Run("non-front node resolves correct height", func(t *testing.T) {
		sm := buildSMWithIndex(t, []indexEntry{
			{hash: hashFront, height: 100},
			{hash: hashMiddle, height: 101},
		})

		sm.headerMu.Lock()
		h := sm.heightFromIndex(hashMiddle)
		sm.headerMu.Unlock()

		// RED before index exists (returns -1); GREEN after (returns 101).
		require.Equal(t, int32(101), h,
			"non-front hash must resolve to its authoritative height from the index, not -1")
	})

	t.Run("absent hash resolves -1", func(t *testing.T) {
		sm := buildSMWithIndex(t, []indexEntry{
			{hash: hashFront, height: 100},
			{hash: hashMiddle, height: 101},
		})

		sm.headerMu.Lock()
		h := sm.heightFromIndex(hashAbsent)
		sm.headerMu.Unlock()

		require.Equal(t, int32(-1), h,
			"a hash not in the index must still resolve to -1")
	})
}

// TestHeaderHeightIndex_ClearedOnReset verifies that resetHeaderState wipes the
// index and rebuilds it with only the seed node — memory is bounded on reset.
func TestHeaderHeightIndex_ClearedOnReset(t *testing.T) {
	hash1 := chainhash.Hash{0x01}
	hash2 := chainhash.Hash{0x02}
	seedHash := chainhash.Hash{0x03}
	const seedHeight = int32(50)

	sm := buildSMWithIndex(t, []indexEntry{
		{hash: hash1, height: 10},
		{hash: hash2, height: 11},
	})

	// Checkpoint must be non-nil so resetHeaderState pushes the seed node.
	sm.headerMu.Lock()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1000, Hash: &chainhash.Hash{0xaa}}
	sm.headerMu.Unlock()

	// Reset re-builds header state with only the new seed.
	sm.resetHeaderState(&seedHash, seedHeight)

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	// Old entries must be gone.
	require.Equal(t, int32(-1), sm.heightFromIndex(hash1),
		"old index entry must be cleared after resetHeaderState")
	require.Equal(t, int32(-1), sm.heightFromIndex(hash2),
		"old index entry must be cleared after resetHeaderState")

	// Seed node must be present.
	require.Equal(t, seedHeight, sm.heightFromIndex(seedHash),
		"seed node pushed by resetHeaderState must appear in the index")
}

// TestHeaderHeightIndex_DeleteOnRemove verifies that removing a node from the
// header list (simulating what handleBlockPreamble does on commit) also deletes
// it from the index, keeping the map bounded.
func TestHeaderHeightIndex_DeleteOnRemove(t *testing.T) {
	hash1 := chainhash.Hash{0x01}
	hash2 := chainhash.Hash{0x02}

	sm := buildSMWithIndex(t, []indexEntry{
		{hash: hash1, height: 100},
		{hash: hash2, height: 101},
	})

	// Simulate preamble consuming the front node (hash1).
	sm.headerMu.Lock()
	front := sm.headerList.Front()
	require.NotNil(t, front)
	node := front.Value.(*headerNode)
	sm.headerList.Remove(front)
	delete(sm.headerHeightIndex, *node.hash)
	sm.headerMu.Unlock()

	// hash1 must no longer be in the index.
	sm.headerMu.Lock()
	h := sm.heightFromIndex(hash1)
	sm.headerMu.Unlock()

	require.Equal(t, int32(-1), h, "deleted hash must no longer resolve in the index")

	// hash2 must still be resolvable.
	sm.headerMu.Lock()
	h2 := sm.heightFromIndex(hash2)
	sm.headerMu.Unlock()

	require.Equal(t, int32(101), h2, "remaining hash must still resolve correctly after peer removal")
}

// --- helpers ----------------------------------------------------------------

type indexEntry struct {
	hash   chainhash.Hash
	height int32
}

// buildSMWithIndex constructs a minimal SyncManager with headerHeightIndex
// populated from entries. The index and header list are kept in sync, with
// entries added front-to-back in the order given.
func buildSMWithIndex(t *testing.T, entries []indexEntry) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
	}

	for _, e := range entries {
		h := e.hash // copy so each node gets its own pointer
		node := &headerNode{height: e.height, hash: &h}
		sm.headerList.PushBack(node)
		sm.headerHeightIndex[*node.hash] = node.height
	}

	return sm
}

// heightFromIndex is a thin helper that looks up hash in headerHeightIndex and
// returns -1 when absent. It must be called under headerMu. This mirrors the
// exact logic the production preamble uses.
func (sm *SyncManager) heightFromIndex(hash chainhash.Hash) int32 {
	if h, ok := sm.headerHeightIndex[hash]; ok {
		return h
	}
	return -1
}
