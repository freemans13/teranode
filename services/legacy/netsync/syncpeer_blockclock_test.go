package netsync

// Tests for F5: the regime-aware last-block-time window (syncPeerBlockClockLimit).
//
// During headers-first IBD the sync peer streams 80-byte headers and completes NO
// block for long stretches, so the 3-minute maxLastBlockTime window rotated a
// perfectly healthy header source mid-delivery. Because headers are single-sourced,
// that rotation froze the header frontier for 30s-3min while a replacement was
// chosen — the dominant "stick" during far-ahead mainnet sync. F5 widens the window
// to SyncPeerHeadersFirstStaleBlockTimeout (default 30m) during headers-first ONLY;
// post-IBD is unchanged, a truly silent peer is still caught in ~90s by the silence
// detector, and any value <= 3m is the byte-identical rollback.
//
// Rotation running is observable via panic in the minimal SyncManager
// (clearRequestedState on nil maps), exactly as silent_syncpeer_test.go uses it;
// suppression returns cleanly with the sync peer kept.

import (
	"testing"
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// TestSyncPeerBlockClock_HeadersFirstDeliveringPeerNotRotated is the RED/GREEN
// lock-in: a headers-first sync peer that has completed no block for 5 minutes
// (past the 3-minute post-IBD window) but is NOT silent must NOT be rotated —
// today it is (5m > 3m, and the throughput carve-out does not save an 80-byte
// header stream below the speed floor).
func TestSyncPeerBlockClock_HeadersFirstDeliveringPeerNotRotated(t *testing.T) {
	sp := &peerpkg.Peer{}
	sps := &syncPeerState{
		lastBlockTime: time.Now().Add(-5 * time.Minute), // > 3m post-IBD window, < 30m headers-first
		ticks:         3,
		silentTicks:   0, // still delivering headers — not silent
	}
	sm := newSilentTestManager(t, sp, sps)
	sm.headersFirstMode.Store(true)
	sm.settings.Legacy.SyncPeerHeadersFirstStaleBlockTimeout = 30 * time.Minute

	require.NotPanics(t, func() { sm.handleCheckSyncPeer() },
		"a delivering headers-first sync peer inside the 30m window must NOT be rotated for no-blocks-complete")
	require.Equal(t, sp, sm.loadSyncPeer(), "the sync peer must be kept")
}

// TestSyncPeerBlockClock_HeadersFirstBackstopStillRotates: the widened window is a
// backstop, not a disable — a peer that has completed no block for longer than the
// 30-minute headers-first window is still rotated.
func TestSyncPeerBlockClock_HeadersFirstBackstopStillRotates(t *testing.T) {
	sp := &peerpkg.Peer{}
	sps := &syncPeerState{
		lastBlockTime: time.Now().Add(-31 * time.Minute), // past the 30m backstop
		ticks:         3,
		silentTicks:   0,
	}
	sm := newSilentTestManager(t, sp, sps)
	sm.headersFirstMode.Store(true)
	sm.settings.Legacy.SyncPeerHeadersFirstStaleBlockTimeout = 30 * time.Minute

	require.Panics(t, func() { sm.handleCheckSyncPeer() },
		"a headers-first peer past the 30m backstop must still be rotated")
}

// TestSyncPeerBlockClock_PostIBDWindowUnchanged: post-IBD the 3-minute window is
// untouched — a 4-minute-stale peer still rotates. F5 must not relax the tip path.
func TestSyncPeerBlockClock_PostIBDWindowUnchanged(t *testing.T) {
	sp := &peerpkg.Peer{}
	sps := &syncPeerState{
		lastBlockTime: time.Now().Add(-4 * time.Minute), // > 3m maxLastBlockTime
		ticks:         3,
		silentTicks:   0,
	}
	sm := newSilentTestManager(t, sp, sps)
	sm.headersFirstMode.Store(false) // post-IBD
	// Even a wide headers-first setting must not leak into the post-IBD path.
	sm.settings.Legacy.SyncPeerHeadersFirstStaleBlockTimeout = 30 * time.Minute

	require.Panics(t, func() { sm.handleCheckSyncPeer() },
		"post-IBD the 3-minute last-block-time window is unchanged: a 4-minute-stale peer rotates")
}

// TestSyncPeerBlockClock_RollbackReproducesTodays: a headers-first value <= 3m is
// the byte-identical rollback lever — a 5-minute-stale peer rotates as it does today.
func TestSyncPeerBlockClock_RollbackReproducesTodays(t *testing.T) {
	sp := &peerpkg.Peer{}
	sps := &syncPeerState{
		lastBlockTime: time.Now().Add(-5 * time.Minute),
		ticks:         3,
		silentTicks:   0,
	}
	sm := newSilentTestManager(t, sp, sps)
	sm.headersFirstMode.Store(true)
	sm.settings.Legacy.SyncPeerHeadersFirstStaleBlockTimeout = 3 * time.Minute // rollback

	require.Panics(t, func() { sm.handleCheckSyncPeer() },
		"a headers-first stale window of 3m reproduces today: a 5-minute-stale peer rotates")
}
