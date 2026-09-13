package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// seedFetchHeaders lands a prebuilt headers batch behind anchor, at height 10,
// exactly the state a real node is in when a headers batch has just arrived.
// The batch is passed in rather than built here because a caller may need the
// hashes before the manager exists.
//
// This used to hand the batch to handleHeadersMsg and let it splice the batch
// onto the list itself. Task 3 turned handleHeadersMsg into a cache fill with no
// list underneath it, so this helper does the splice that function no longer
// does, for the tests here whose subject is a header-list reader
// (advanceHeaderListFor, removeHeaderAnchorLocked) rather than the headers-round
// handler itself.
//
// It also fills the header cache with the same run, because assignWantedBlocks
// reads that and never the list: a caller that goes on to call
// fetchHeaderBlocks needs both seeded, or the wanted-range pass finds nothing to
// ask for even though the list looks fully populated.
func seedFetchHeaders(t *testing.T, sm *SyncManager, p *peerpkg.Peer, anchor chainhash.Hash, msg *wire.MsgHeaders) {
	t.Helper()

	sm.resetHeaderState(&anchor, 10)
	// resetHeaderState turns headers-first mode off; the walk under test only
	// runs in headers-first mode.
	sm.headersFirstMode.Store(true)

	spliceHeadersForTest(t, sm, msg.Headers)

	require.Equal(t, len(msg.Headers)+1, sm.headerListLen(), "the seeded headers should all have linked")

	// lookaheadCeilingLocked anchors on sm.committedHeight() now, not on the
	// front of the header list, so this fixture's own anchor height has to be
	// reflected there too or every ceiling-bearing test in this package would
	// see a committed height of zero against headers seeded at 11 and up — an
	// absolute ceiling below every header it seeded, rather than the depth
	// relative to height 10 these tests are written against. anchor really is
	// the block at height 10 in this fixture's model (spliced headers start at
	// 11), so this is not a stand-in value, it is what resetHeaderState(&anchor,
	// 10) already asserts.
	sm.noteCommittedHeight(10, anchor)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 11, msg.Headers),
		"the same run spliceHeadersForTest just linked must also link for the cache")
}

// spliceHeadersForTest pushes a batch of already-linked headers onto the back
// of sm.headerList, the way handleHeadersMsg did before Task 3 replaced that
// path with a cache fill. Test-only: it exists so the many tests that seed the
// list through seedFetchHeaders keep seeing the state they saw before that
// change, without reaching through handleHeadersMsg, which no longer touches
// the list at all.
func spliceHeadersForTest(t *testing.T, sm *SyncManager, headers []*wire.BlockHeader) {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	for _, blockHeader := range headers {
		blockHash := blockHeader.BlockHash()

		prevNodeEl := sm.headerList.Back()
		require.NotNil(t, prevNodeEl, "seeding a header batch requires an anchor already on the list")

		prevNode, ok := prevNodeEl.Value.(*headerNode)
		require.True(t, ok)
		require.True(t, prevNode.hash.IsEqual(&blockHeader.PrevBlock),
			"test fixture built a batch that does not link onto the seeded anchor")

		node := headerNode{hash: &blockHash, height: prevNode.height + 1, listEpoch: sm.headerListEpoch}
		e := sm.headerList.PushBack(&node)
		sm.indexHeaderLocked(e, blockHash)
	}
}
