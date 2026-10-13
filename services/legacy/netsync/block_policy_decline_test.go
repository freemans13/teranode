package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// A block whose body the gate declines on this node's own block policy was drained and asked for
// again on every pass that wanted it (review of 76be891d6). One peer's declared length is only its
// claim, so one peer, however often it declares it, does not stop the block being asked for; two
// different peers declaring it does, for recentlyFailedBlocksTTL, as the park's policy_declined
// row would.
func TestNotePolicyDecline_TwoPeersWriteTheBlockOffAndOneDoesNot(t *testing.T) {
	sm := schedulerManager(t)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL)
	t.Cleanup(sm.recentlyFailedBlocks.Stop)

	first, _ := schedulerPeer(t, sm, 1, 2000)
	second, _ := schedulerPeer(t, sm, 2, 2000)

	hash := chainhash.Hash{0x0d}
	now := time.Now()

	sm.notePolicyDecline(hash, first, now)
	sm.notePolicyDecline(hash, first, now.Add(time.Second))

	_, failed := sm.recentlyFailedBlocks.Get(hash)
	require.False(t, failed, "one peer's claim, however often repeated, stops nothing")

	sm.notePolicyDecline(hash, second, now.Add(2*time.Second))

	_, failed = sm.recentlyFailedBlocks.Get(hash)
	require.True(t, failed, "two peers declaring the block above policy write it off")

	sm.policyDeclinesMu.Lock()
	defer sm.policyDeclinesMu.Unlock()

	require.Empty(t, sm.policyDeclines, "the count is done with once the block is written off")
}

// Declines further apart than recentlyFailedBlocksTTL do not add up: the count starts again.
func TestNotePolicyDecline_OldDeclinesDoNotCount(t *testing.T) {
	sm := schedulerManager(t)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL)
	t.Cleanup(sm.recentlyFailedBlocks.Stop)

	first, _ := schedulerPeer(t, sm, 1, 2000)
	second, _ := schedulerPeer(t, sm, 2, 2000)

	hash := chainhash.Hash{0x0e}
	now := time.Now()

	sm.notePolicyDecline(hash, first, now)
	sm.notePolicyDecline(hash, second, now.Add(recentlyFailedBlocksTTL+time.Second))

	_, failed := sm.recentlyFailedBlocks.Get(hash)
	require.False(t, failed)
}
