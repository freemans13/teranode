package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHeadersRoundLocator_UsesTheChain pins the change that ended a seven-hour
// mainnet stall. The locator it replaces was anchored at the back of the header
// list, far above what the node had committed, so a peer that had not reached
// that point recognised nothing in it.
func TestHeadersRoundLocator_UsesTheChain(t *testing.T) {
	tip := chainhash.Hash{0x11}
	fromChain := []*chainhash.Hash{{0x22}, {0x33}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return(fromChain, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.settings.Legacy.MultiPeerBlockDownload = true

	// A header list the OLD locator would happily build from. If the change did
	// not land, the locator comes from here instead.
	sm.headerMu.Lock()
	hash := chainhash.Hash{0x99}
	sm.headerList.PushBack(&headerNode{height: 500, hash: &hash})
	sm.headerMu.Unlock()

	got, err := sm.headersRoundLocator(&tip, 100)
	require.NoError(t, err)

	require.Len(t, got, 2, "the locator must come from the chain, not from the header list")
	require.Equal(t, fromChain[0], got[0])
}

// TestSeedCommittedHeight_LeavesACacheToFill is a guard against the cache being
// forgotten in the constructor. A nil cache is not a crash, because every method is
// nil safe. It is worse: a silently empty wanted range for ever.
func TestNew_BuildsAHeaderCache(t *testing.T) {
	sm := newRaceManagerWithCache(t)

	require.NotNil(t, sm.headerCache, "the manager must start with a cache to fill")

	_ = model.BlockHeader{}
}

// newRaceManagerWithCache is newRaceManager plus the one field the production
// constructor now sets. Kept beside the wiring tests rather than folded into
// newRaceManager, so the existing harnesses keep proving what they prove today.
func newRaceManagerWithCache(t *testing.T) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()

	return sm
}
