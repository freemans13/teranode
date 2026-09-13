package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestFetchHeaderBlocks_ChoosesThePathFromTheSetting pins that the existing walk
// is untouched by default. Mainnet is soaking this branch, so the off path is
// the live path and must not move.
//
// This uses newFetchLockManager and seedFetchHeaders, the harness the other
// fetchHeaderBlocks tests in this package already use, rather than assignHarness
// from wanted_range_assign_test.go. assignHarness builds a manager for
// assignWantedBlocks, which never reads startHeader and never calls the
// blockchain client, so it leaves both a nil startHeader and a nil
// blockchainClient. The cursor walk needs both: it bails out immediately on a
// nil startHeader (its own "nothing to do" guard, working as intended), and
// haveInventory dereferences blockchainClient on every candidate. Run verbatim
// against assignHarness this test fails the first assertion with a nil
// startHeader and, once a startHeader is seeded by hand, segfaults on the nil
// blockchainClient instead of failing cleanly - neither is what a wiring test
// should exercise, so this uses the harness built for the cursor walk.
func TestFetchHeaderBlocks_ChoosesThePathFromTheSetting(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.WantedRangeDownload = false
	sm.lastCommittedTip.Store(&committedTip{height: 10})

	syncPeer, _, rec := connectRacePeer(t, 1, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa1}
	msg, _ := linkedHeaders(anchor, 20, &nonce)
	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.headerMu.Lock()
	startedAt := sm.startHeader
	sm.headerMu.Unlock()

	require.NotNil(t, startedAt, "seedFetchHeaders must leave the cursor on the first seeded header")

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"with the setting off the existing cursor walk must still request blocks")

	// != nil alone would not distinguish the two paths: assignWantedBlocks never
	// touches startHeader either, so a seeded cursor stays non-nil whichever path
	// ran. Advancing it is specifically what the cursor walk does as it works
	// through a batch (see commitHeaderCandidates), and specifically what
	// assignWantedBlocks cannot do, since it never reads the field at all. So the
	// cursor having moved from where seedFetchHeaders left it is what pins this
	// as the cursor walk rather than the wanted-range pass.
	sm.headerMu.Lock()
	cursorMoved := sm.startHeader != startedAt
	sm.headerMu.Unlock()

	require.True(t, cursorMoved,
		"and the cursor must have advanced, which only the cursor walk does")
}

// TestFetchHeaderBlocks_SettingOnNeverTouchesTheCursorWalk pins the other half of
// the dispatch: with the setting on, none of the cursor walk's code below the
// dispatch point runs, not just that its visible output does not appear.
//
// assignHarness builds a manager with startHeader seeded but blockchainClient
// left nil, which is the harness the wanted-range tests already run against
// (assignWantedBlocks never reads either field). Every candidate the cursor walk
// would consider calls haveInventory, which dereferences blockchainClient
// unconditionally - so if the dispatch's return statement did not actually
// exit the function before the old walk's first line, this test would panic on
// that nil dereference instead of passing. A clean return is therefore not just
// a behavioural difference but the only way this test can finish at all.
func TestFetchHeaderBlocks_SettingOnNeverTouchesTheCursorWalk(t *testing.T) {
	sm, _ := assignHarness(t, 1, 20)
	sm.settings.Legacy.WantedRangeDownload = true
	sm.lastCommittedTip.Store(&committedTip{height: 10})

	sm.headerMu.Lock()
	sm.startHeader = sm.headerList.Front()
	sm.headerMu.Unlock()

	require.Nil(t, sm.blockchainClient,
		"the harness must leave this nil for the panic-on-touch argument above to hold")

	sm.fetchHeaderBlocks()
}
