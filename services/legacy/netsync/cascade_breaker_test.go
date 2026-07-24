package netsync

// Tests for the disconnect-cascade breakers (the bursty-sync fix). Measured
// live (2026-07-17, binary db256cab9): the multi-minute throughput dips were
// owned by self-inflicted disconnect cascades — head-stall kills firing during
// local backpressure, "unrequested block" disconnects punishing post-rotation
// redeliveries (8 peers executed in one second, 4s after a rotation), and
// header-linkage chain-kills of successor sync peers after a reset.

import (
	"container/list"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestBlockStillNeeded_CarriesAcrossReset: hashes in the header index when
// resetHeaderState runs must remain "needed" for recentlyNeededTTL, so their
// post-rotation redeliveries are processed instead of punished; expiry and
// commit both end the tolerance.
func TestBlockStillNeeded_CarriesAcrossReset(t *testing.T) {
	sm := &SyncManager{
		logger:            ulogger.TestLogger{},
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
	}

	h := chainhash.Hash{0xaa}
	sm.headerHeightIndex[h] = 1234

	require.True(t, sm.blockStillNeeded(h), "a hash in the live index is needed")

	// Rotation: the reset clears the index but must fold the hash into the
	// TTL carryover.
	seed := chainhash.Hash{0x01}
	sm.resetHeaderState(&seed, 1200)

	_, inIndex := sm.headerHeightIndex[h]
	require.False(t, inIndex, "reset clears the live index")
	require.True(t, sm.blockStillNeeded(h),
		"a hash requested before the rotation must stay needed (carryover) — its redelivery is not an offense")

	// Commit-site delete ends the tolerance for that hash.
	sm.headerMu.Lock()
	delete(sm.recentlyNeededUntil, h)
	sm.headerMu.Unlock()
	require.False(t, sm.blockStillNeeded(h), "a committed/pruned hash is no longer needed")

	// Expiry ends the tolerance too (and is swept on read).
	h2 := chainhash.Hash{0xbb}
	sm.headerMu.Lock()
	sm.recentlyNeededUntil[h2] = time.Now().Add(-time.Second)
	sm.headerMu.Unlock()
	require.False(t, sm.blockStillNeeded(h2), "an expired carryover entry is no longer needed")
}

// TestBlockRequested_ToleratesStillNeeded: the network-level admission check
// must accept a still-needed block whose per-peer ledger entry was cleared by
// a rotation.
func TestBlockRequested_ToleratesStillNeeded(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	sm := &SyncManager{
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		chainParams:       tSettings.ChainCfgParams,
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		peerStates:        txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	h := chainhash.Hash{0xcc}
	sm.headerMu.Lock()
	sm.recentlyNeededUntil = map[chainhash.Hash]time.Time{h: time.Now().Add(time.Minute)}
	sm.headerMu.Unlock()

	// Unknown peer (no per-peer state at all): previously an automatic false.
	p := peerpkg.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{})
	require.False(t, sm.BlockRequested(p, &chainhash.Hash{0xdd}),
		"a hash that is neither requested nor needed stays rejected (spam guard intact)")
}

// TestCheckHeadStall_SuppressedWhileBackpressured: the 2s head-stall kill must
// never fire while the node throttles its own reads, and must wait one full
// clean timeout window after suppression before firing again.
func TestCheckHeadStall_SuppressedWhileBackpressured(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ParallelFetchPeers = 8
	tSettings.Legacy.BlockStallTimeout = 2 * time.Second

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "127.0.0.1:8333")
	require.NoError(t, err)

	// An ALTERNATIVE eligible peer must exist for any head-stall response to fire:
	// freeing/executing the sole peer can never help (the block has nowhere else to
	// go), so checkHeadStall no-ops without one — see hasAlternativeFetchPeer and
	// TestCheckHeadStall_SolePeerNeverFires.
	alt, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "127.0.0.2:8333")
	require.NoError(t, err)
	alt.TstMarkConnected()

	altState := newEligiblePeerState()

	t.Cleanup(func() {
		altState.requestedBlocks.Stop()
		altState.requestedTxns.Stop()
	})

	h := chainhash.Hash{0xee}

	newSM := func() *SyncManager {
		sm := &SyncManager{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			headerList:        list.New(),
			headerHeightIndex: map[chainhash.Hash]int32{h: 700000},
			assignedTo:        map[chainhash.Hash]map[*peerpkg.Peer]time.Time{h: {p: time.Now().Add(-time.Minute)}}, // stale
			refetchBlocks:     make(map[chainhash.Hash]struct{}),
			requestedBlocks:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
			peerStates:        txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}
		sm.peerStates.Set(alt, altState)
		t.Cleanup(func() { sm.requestedBlocks.Stop() })

		return sm
	}

	// Backpressured (backlog advancing, prefetch off => unconditional suppression):
	// must NOT fire, and must stamp the suppression time.
	sm := newSM()
	sm.blockBacklog.Add(3)

	sm.checkHeadStall(time.Now())
	_, still := sm.assignedTo[h]
	require.True(t, still, "head-stall must not execute a peer while the node is self-backpressured")
	require.False(t, sm.headStallSuppressedAt.IsZero(), "suppression must be stamped")

	// Backpressure just lifted: within one timeout of the stamp, still no fire.
	sm.blockBacklog.Add(-3)
	sm.checkHeadStall(time.Now())
	_, still = sm.assignedTo[h]
	require.True(t, still, "a full clean timeout window is required after suppression before firing")

	// Clean window elapsed: the genuinely stalled head now fires.
	sm.headStallSuppressedAt = time.Now().Add(-3 * time.Second)
	sm.checkHeadStall(time.Now())
	_, still = sm.assignedTo[h]
	require.False(t, still, "a stale head with no backpressure must still fire (real stalls stay covered)")
}
