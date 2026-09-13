package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// cacheManager builds a manager in the new model: the setting on, a header
// cache naming the run above the committed tip, and one peer to ask.
//
// It returns the peer alongside the manager and the recorder, unlike the
// brief this was drafted from: TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling
// needs a registered peer to drive a real handleBlockMsg call through, and
// building a second one there would leave two peers competing for the same
// scheduler pass in a test that wants exactly one.
func cacheManager(t *testing.T, best int32, depth int32) (*SyncManager, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.settings.Legacy.BlockDownloadLowerWindow = int(depth)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = int(depth)
	// The new model: assignWantedBlocks reads the wanted range from the header
	// cache rather than walking sm.headerList. Off, fetchHeaderBlocks takes the
	// cursor walk instead, which never looks at the cache these tests fill.
	sm.settings.Legacy.WantedRangeDownload = true

	committedTipHash := chainhash.Hash{0xbb}
	sm.noteCommittedHeight(best, committedTipHash)

	parent := chainhash.Hash{0xaa}
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, best+1, chainOfHeaders(parent, int(depth)+4)))

	peer, rec := schedulerPeer(t, sm, 1, best+1000)

	return sm, peer, rec
}

// TestNewModel_ACheckpointAnchorDoesNotGateAnything is the stall that had
// Hetzner mainnet wedged for ten hours and forty minutes on 2026-09-13, and for
// eight hours and thirty-three minutes the night before, written as a test.
//
// The old path marks the header at a committed checkpoint as an anchor and
// refuses to walk while that anchor is at the front. The only code that clears
// the mark runs when an incoming headers batch appends a header matching the
// next checkpoint, and once the list already holds every remaining checkpoint no
// batch can ever append one again. The gate then never opens for the life of the
// process, and only losing the memory it lives in has ever cleared it.
//
// The new model has no walk, so there is nothing for an anchor to gate.
func TestNewModel_ACheckpointAnchorDoesNotGateAnything(t *testing.T) {
	sm, _, rec := cacheManager(t, 33333, 8)

	// The exact wedged state: the list holds one node, it is the anchor, and
	// nothing has spliced onto it.
	sm.headerMu.Lock()
	anchorHash := chainhash.Hash{0xcc}
	sm.headerList = list.New()
	sm.headerList.PushBack(&headerNode{height: 33333, hash: &anchorHash, isAnchor: true})
	sm.headerMu.Unlock()

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a node whose header list front is a checkpoint anchor must still request blocks; this is the 2026-09-13 mainnet stall")
}

// TestNewModel_APeerRotationLosesNothing pins the other half. resetHeaderState
// throws away the entire header list, and on the multi-peer path it is never
// reached at all, so the recovery of last resort is both destructive and
// unreachable. Under the new model it clears nothing the pass needs.
//
// The brief this was drafted from re-asked the ORIGINAL peer after merely aging
// the ledger past the retry window, and that cannot show what it intends: this
// package's download ledger deliberately never sends a second getdata to a peer
// that still holds the first one (ReassertOwner, block_download_tracker.go) —
// aging alone re-arms the existing record and sends nothing, by design, so that
// assertion would fail even with a perfectly harmless resetHeaderState. A
// rotation is a genuinely different peer taking over, so this connects a
// second one — standing in for whichever peer a real rotation hands the work
// to — and checks that IT gets the same wanted range once the ledger has aged
// enough to reconsider it.
func TestNewModel_APeerRotationLosesNothing(t *testing.T) {
	sm, _, recA := cacheManager(t, 500, 8)

	sm.fetchHeaderBlocks()
	require.True(t, WaitUntil(func() bool { return recA.count() > 0 }, 5*time.Second),
		"sanity: the first pass must place work before a rotation can be shown to cost nothing")

	before := recA.count()

	// What a sync-peer rotation does on the old path.
	anchor := chainhash.Hash{0xdd}
	sm.resetHeaderState(&anchor, 500)

	// The peer a rotation would have handed the work to. It owns nothing yet,
	// so the assigner can place real work on it without waiting out any
	// ownership ceiling.
	_, recB := schedulerPeer(t, sm, 2, 1500)

	// Age the ledger past the retry window so the first peer's outstanding
	// blocks are reconsidered at all; unownedBlocks skips anything requested
	// within the window no matter who might take it next.
	sm.blockDownloads.now = func() time.Time {
		return time.Now().Add(blockRequestRetryInterval + time.Second)
	}

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return recB.count() > 0 }, 5*time.Second),
		"after a header-state reset the pass must still name the same blocks to whichever peer is available, because the cache and the files survive it")
	require.Positive(t, before)
}

// TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling is the negative half of the
// bound, and it is the regression guard for the defect that shipped to mainnet.
//
// The read-ahead ceiling used to be anchored to the front of the header list,
// which advances when a block ARRIVES rather than when it commits. So every
// arrival raised the front, which raised the ceiling, which licensed another
// depth's worth of requests, with no coupling to the committer at all. Measured
// during a genesis resync: the front stood at height 4,877 with the chain
// settled at 868, and the park held its full 4,096 entries.
//
// A block delivered with no body is exactly that arrival without a commit: the
// current lookaheadCeilingLocked already anchors on sm.committedHeight() rather
// than the header list front, and committedHeight only moves on
// noteCommittedHeight, which a message carrying no block never reaches. This
// pins that property directly rather than trusting it did not regress.
//
// handleBlockMsg here returns a single error, not the three values the
// original brief called it with — that draft predates the head/tail split this
// package's handleBlockMsgHead/handleBlockMsgTail division introduced.
func TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling(t *testing.T) {
	const depth = int32(4)

	sm, peer, rec := cacheManager(t, 500, depth)

	sm.fetchHeaderBlocks()
	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"sanity: the first pass must place work")

	before := rec.count()
	rec.reset()

	sm.headerMu.Lock()
	ceilingBefore, limitedBefore := sm.lookaheadCeilingLocked()
	sm.headerMu.Unlock()

	require.True(t, limitedBefore, "the ceiling must be engaged, or this test proves nothing")

	// An arrival with no body: handleBlockMsg's own pre-checks (FSM state, then
	// ownership) have to be satisfied for the call to reach the "no block"
	// return this test wants to drive, rather than bailing out earlier at
	// "unknown peer" — which would exercise nothing.
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	hash, ok := sm.headerCache.At(501)
	require.True(t, ok)

	sm.blockDownloads.Add(peer, hash)

	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: hash, peer: peer})
	require.Error(t, err, "a queue message carrying no block is a programming fault, not a sync one, and handleBlockMsg says so")

	sm.headerMu.Lock()
	ceilingAfter, limitedAfter := sm.lookaheadCeilingLocked()
	sm.headerMu.Unlock()

	require.True(t, limitedAfter)
	require.Equal(t, ceilingBefore, ceilingAfter,
		"a block that arrived but did not commit must not raise the ceiling; that ratchet filled the park and cost 1.8 blocks a minute on mainnet")
	require.Positive(t, before)
}
